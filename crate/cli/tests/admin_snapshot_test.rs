//! Tests for `sinexctl admin snapshot`.
//!
//! These tests exercise the snapshot command using a tempdir-based fake state
//! directory.  They do NOT require a live Postgres or NATS instance — instead
//! they pass a deliberately invalid DATABASE_URL to verify that pg_dump
//! failure is surfaced cleanly, or they exercise only the `--dry-run` path.

use assert_cmd::cargo;
use std::fs;
use std::process::Command;
use tempfile::TempDir;
use xtask::sandbox::sinex_test;

use sinexctl::admin::snapshot::{AdminSnapshotCommand, Component};

/// Helper: build a fake state directory with recognizable fixture files.
fn make_fake_state_dir() -> TempDir {
    let dir = tempfile::tempdir().expect("create tempdir");
    let root = dir.path();

    // postgres — not in state dir, but pg_dump goes to staging
    // nats/jetstream
    let nats_js = root.join("nats").join("jetstream");
    fs::create_dir_all(&nats_js).unwrap();
    fs::write(nats_js.join("meta.inf"), b"nats-jetstream-fixture").unwrap();

    // blob-repository (CAS)
    let cas = root.join("blob-repository");
    fs::create_dir_all(&cas).unwrap();
    fs::write(cas.join("blob1.bin"), b"blob-content-1").unwrap();
    fs::write(cas.join("blob2.bin"), b"blob-content-2").unwrap();

    // spool
    let spool = root.join("spool");
    fs::create_dir_all(&spool).unwrap();
    fs::write(spool.join("checkpoint.bin"), b"checkpoint-data").unwrap();

    fs::write(
        root.join("source-units.json"),
        r#"{
          "source_units": [
            { "id": "terminal.atuin-history" },
            { "id": "desktop.clipboard" },
            { "id": "desktop.clipboard" }
          ]
        }"#,
    )
    .unwrap();

    dir
}

fn sinexctl_bin() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

// ── Dry-run test ─────────────────────────────────────────────────────────────

/// `--dry-run` should print size estimates and NOT create an archive or staging
/// directory.
#[sinex_test]
async fn dry_run_reports_estimates_and_creates_no_archive() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir();
    let output_dir = tempfile::tempdir().expect("output tempdir");
    let output_path = output_dir.path().join("test.tar.zst");

    let output = sinexctl_bin()
        .args([
            "admin",
            "snapshot",
            "--output",
            output_path.to_str().unwrap(),
            "--dry-run",
            "--state-dir",
            state_dir.path().to_str().unwrap(),
            "--database-url",
            "postgresql://sinex:sinex@localhost/sinex_prod",
            "--components",
            "nats,cas,state",
        ])
        .output()
        .expect("run sinexctl admin snapshot --dry-run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Must succeed (exit 0).
    assert!(
        output.status.success(),
        "dry-run must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );

    // No archive should be created.
    assert!(
        !output_path.exists(),
        "dry-run must NOT create an archive at {output_path:?}"
    );

    // Staging directories must be absent.
    let staging_entries: Vec<_> = std::fs::read_dir(output_dir.path())
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".sinex-snapshot-staging-")
        })
        .collect();
    assert!(
        staging_entries.is_empty(),
        "staging directory must be cleaned up after dry-run"
    );

    // Output must mention "dry-run".
    assert!(
        stdout.contains("dry-run"),
        "stdout must mention dry-run mode\nstdout: {stdout}"
    );

    Ok(())
}

/// Non-Postgres component subsets do not need DATABASE_URL, even on the binary
/// path. This keeps state-only forensic snapshots usable when Postgres is the
/// broken component being investigated.
#[sinex_test]
async fn dry_run_non_postgres_components_do_not_require_database_url()
-> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir();
    let output_dir = tempfile::tempdir().expect("output tempdir");
    let output_path = output_dir.path().join("test.tar.zst");

    let output = sinexctl_bin()
        .args([
            "admin",
            "snapshot",
            "--output",
            output_path.to_str().unwrap(),
            "--dry-run",
            "--state-dir",
            state_dir.path().to_str().unwrap(),
            "--components",
            "nats,cas,state",
        ])
        .output()
        .expect("run sinexctl admin snapshot --dry-run without DATABASE_URL");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "non-postgres dry-run must not require DATABASE_URL\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("dry-run"),
        "stdout must mention dry-run mode\nstdout: {stdout}"
    );
    assert!(
        !output_path.exists(),
        "dry-run must NOT create an archive at {output_path:?}"
    );

    Ok(())
}

// ── Staging cleanup on pg_dump failure ──────────────────────────────────────

/// When pg_dump fails (bad DATABASE_URL), staging must be cleaned up and the
/// command must exit non-zero.
#[sinex_test]
async fn staging_cleaned_up_on_pg_dump_failure() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir();
    let output_dir = tempfile::tempdir().expect("output tempdir");
    let output_path = output_dir.path().join("should-not-exist.tar.zst");

    // Use an intentionally invalid DATABASE_URL.
    let output = sinexctl_bin()
        .args([
            "admin",
            "snapshot",
            "--output",
            output_path.to_str().unwrap(),
            "--state-dir",
            state_dir.path().to_str().unwrap(),
            "--database-url",
            "postgresql://bad:creds@127.0.0.1:1/nonexistent",
            "--components",
            "postgres",
        ])
        .output()
        .expect("run sinexctl admin snapshot with bad DB URL");

    // Must fail (non-zero exit) — pg_dump cannot connect.
    assert!(
        !output.status.success(),
        "snapshot with bad DATABASE_URL must fail"
    );

    // No archive must exist.
    assert!(
        !output_path.exists(),
        "archive must not be created after failure"
    );

    // Staging directory must be absent.
    let staging_entries: Vec<_> = std::fs::read_dir(output_dir.path())
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".sinex-snapshot-staging-")
        })
        .collect();
    assert!(
        staging_entries.is_empty(),
        "staging directory must be cleaned up after pg_dump failure; found: {staging_entries:?}"
    );

    Ok(())
}

// ── Unit tests (no binary invocation) ────────────────────────────────────────

/// `Component::all()` must include all four expected components.
#[sinex_test]
async fn component_all_covers_all_four() -> xtask::sandbox::TestResult<()> {
    let all = Component::all();
    let names: Vec<&str> = all.iter().map(|c| c.name()).collect();
    for expected in &["postgres", "nats", "cas", "state"] {
        assert!(
            names.contains(expected),
            "Component::all() must include '{expected}'"
        );
    }
    assert_eq!(all.len(), 4, "Component::all() must have exactly 4 entries");
    Ok(())
}

/// Dry-run via the library API exercises the non-postgres components against
/// a real fake state dir and returns a valid SnapshotResult.
#[sinex_test]
async fn library_dry_run_returns_valid_result() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir();
    let output_dir = tempfile::tempdir().expect("output tempdir");
    let output_path = output_dir.path().join("test.tar.zst");

    let cmd = AdminSnapshotCommand {
        output: output_path.clone(),
        compression: 3,
        workers: 0,
        mode: "quiesce".to_string(),
        dry_run: true,
        database_url: None,
        state_dir: Some(state_dir.path().to_path_buf()),
        auto_stop: false,
        components: vec![Component::Nats, Component::Cas, Component::State],
    };

    let result = cmd.execute().expect("dry-run execute must succeed");

    assert_eq!(result.mode, "dry-run");
    assert_snapshot_id_is_uuidv7(&result.snapshot_id);
    assert!(
        result.output_path.is_none(),
        "dry-run must not report an output path"
    );
    assert!(
        result.archive_bytes.is_none(),
        "dry-run must not report archive bytes"
    );
    assert!(
        !result.components_captured.is_empty(),
        "dry-run must return at least one component record"
    );
    assert_eq!(
        result.source_unit_ids,
        vec![
            "desktop.clipboard".to_string(),
            "terminal.atuin-history".to_string()
        ]
    );

    // Nats, CAS, and state should all appear.
    let names: Vec<&str> = result
        .components_captured
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    for expected in &["nats", "cas", "state"] {
        assert!(
            names.contains(expected),
            "component '{expected}' must appear in dry-run result"
        );
    }

    Ok(())
}

fn assert_snapshot_id_is_uuidv7(id: &str) {
    assert_eq!(id.len(), 36, "snapshot ID must be canonical UUID text");
    assert_eq!(
        id.as_bytes().get(14),
        Some(&b'7'),
        "snapshot ID must be UUIDv7"
    );
    sinex_primitives::Uuid::parse_str(id).expect("snapshot ID must parse as UUID");
}

/// Manifest JSON round-trips correctly through serde.
#[sinex_test]
async fn manifest_round_trips_through_serde() -> xtask::sandbox::TestResult<()> {
    use sinexctl::admin::manifest::{
        CasExtras, ComponentExtras, ComponentRecord, PostgresExtras, SnapshotManifest, Totals,
    };
    use std::collections::BTreeMap;

    let mut row_counts = BTreeMap::new();
    row_counts.insert("core.events".to_string(), 124_920_000i64);

    let manifest = SnapshotManifest {
        snapshot_id: "test-id".to_string(),
        created_at: "2026-05-15T11:30:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: Some("abc1234".to_string()),
        host: "sinnix-prime".to_string(),
        mode: "quiesce".to_string(),
        source_unit_ids: vec![
            "desktop.clipboard".to_string(),
            "terminal.atuin-history".to_string(),
        ],
        components: vec![
            ComponentRecord {
                name: "postgres".to_string(),
                path: "postgres/sinex_prod.dump".to_string(),
                bytes: 12345678,
                blake3: "a".repeat(64),
                extras: Some(ComponentExtras::Postgres(PostgresExtras { row_counts })),
            },
            ComponentRecord {
                name: "cas".to_string(),
                path: "cas/blob-repository/".to_string(),
                bytes: 1024,
                blake3: "b".repeat(64),
                extras: Some(ComponentExtras::Cas(CasExtras { blob_count: 2 })),
            },
        ],
        totals: Totals {
            uncompressed_bytes: 12346702,
            archive_bytes: Some(3_000_000),
        },
    };

    let json = serde_json::to_string_pretty(&manifest).expect("serialise manifest");
    let back: SnapshotManifest = serde_json::from_str(&json).expect("deserialise manifest");

    assert_eq!(back.snapshot_id, "test-id");
    assert_eq!(back.source_unit_ids.len(), 2);
    assert_eq!(back.components.len(), 2);
    assert_eq!(back.totals.archive_bytes, Some(3_000_000));

    Ok(())
}
