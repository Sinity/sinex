//! Tests for `sinexctl ops state` snapshot surfaces.
//!
//! These tests exercise the snapshot command using a tempdir-based fake state
//! directory.  They do NOT require a live Postgres or NATS instance — instead
//! they pass a deliberately invalid `DATABASE_URL` to verify that `pg_dump`
//! failure is surfaced cleanly, or they exercise only the `--dry-run` path.

use assert_cmd::cargo;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

use sinex_primitives::source_contracts;
use sinexctl::admin::exec;
use sinexctl::admin::manifest::{ComponentExtras, QuiesceReceipt};
use sinexctl::admin::snapshot::{
    AdminSnapshotCommand, AdminSnapshotInspectCommand, AdminSnapshotRestoreCommand, Component,
    format_snapshot_inspect_result, format_snapshot_restore_plan_result,
};

/// Helper: build a fake state directory with recognizable fixture files.
fn make_fake_state_dir() -> TestResult<TempDir> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();

    // postgres — captured through pg_dump, not the generic state component.
    let postgres = root.join("postgresql");
    fs::create_dir_all(&postgres)?;
    fs::write(postgres.join("PG_VERSION"), b"18")?;

    // nats/jetstream
    let nats_js = root.join("nats").join("jetstream");
    fs::create_dir_all(&nats_js)?;
    fs::write(nats_js.join("meta.inf"), b"nats-jetstream-fixture")?;

    // blob-repository (CAS)
    let cas = root.join("blob-repository");
    fs::create_dir_all(&cas)?;
    fs::write(cas.join("blob1.bin"), b"blob-content-1")?;
    fs::write(cas.join("blob2.bin"), b"blob-content-2")?;

    // spool
    let spool = root.join("spool");
    fs::create_dir_all(&spool)?;
    fs::write(spool.join("checkpoint.bin"), b"checkpoint-data")?;

    Ok(dir)
}

fn sinexctl_bin() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

fn registered_fixture_source_ids() -> Vec<String> {
    let mut ids: Vec<String> = source_contracts::all_source_contracts()
        .map(|descriptor| descriptor.id.to_string())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

#[sinex_test]
async fn state_snapshot_help_points_to_restore_drill() -> TestResult<()> {
    let output = sinexctl_bin()
        .args(["ops", "state", "snapshot", "--help"])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "ops state snapshot help must exit 0\nstdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("sinexctl ops state restore --archive <archive>"),
        "help should point operators at the restore drill command\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Inspect and run an isolated drill before any manual live restore"),
        "help should distinguish isolated restore drills from manual live restores\nstdout: {stdout}"
    );

    Ok(())
}

fn make_snapshot_archive() -> TestResult<(TempDir, std::path::PathBuf)> {
    use sinexctl::admin::manifest::{ComponentRecord, SnapshotManifest, Totals};

    let dir = tempfile::tempdir()?;
    let staging = dir.path().join("staging");
    fs::create_dir_all(staging.join("state"))?;
    fs::write(
        staging.join("state").join("checkpoint.bin"),
        b"checkpoint-data",
    )?;
    fs::create_dir_all(staging.join("state").join("private-mode"))?;
    fs::write(
        staging
            .join("state")
            .join("private-mode")
            .join("state.json"),
        br#"{"enabled":false}"#,
    )?;
    let state_blake3 = snapshot_component_blake3(&staging.join("state"))?;

    let manifest = SnapshotManifest {
        snapshot_id: "01970a7f-391b-7000-8000-000000000001".to_string(),
        created_at: "2026-05-15T11:30:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: Some("abc1234".to_string()),
        host: "sinnix-prime".to_string(),
        mode: "quiesce".to_string(),
        quiesce_receipt: None,
        source_ids: registered_fixture_source_ids(),
        components: vec![ComponentRecord {
            name: "state".to_string(),
            path: "state/".to_string(),
            bytes: 15,
            blake3: state_blake3,
            extras: None,
        }],
        totals: Totals {
            uncompressed_bytes: 15,
            archive_bytes: Some(512),
        },
    };
    fs::write(
        staging.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    let archive_path = dir.path().join("fixture.sinex.tar.zst");
    exec::tar_create_zstd(&staging, &archive_path, 1, 1)?;
    Ok((dir, archive_path))
}

fn make_postgres_snapshot_archive() -> TestResult<(TempDir, PathBuf)> {
    use sinexctl::admin::manifest::{
        ComponentExtras, ComponentRecord, PostgresExtras, SnapshotManifest, Totals,
    };
    use std::collections::BTreeMap;

    let dir = tempfile::tempdir()?;
    let staging = dir.path().join("staging");
    fs::create_dir_all(staging.join("postgres"))?;
    fs::write(
        staging.join("postgres").join("sinex_prod.dump"),
        b"custom pg dump fixture",
    )?;
    let postgres_blake3 = blake3::hash(b"custom pg dump fixture").to_hex().to_string();

    let mut row_counts = BTreeMap::new();
    row_counts.insert("core.events".to_string(), 7);
    row_counts.insert("pg_temp_141.sinex_batch_staging".to_string(), 50);
    let manifest = SnapshotManifest {
        snapshot_id: "01970a7f-391b-7000-8000-000000000002".to_string(),
        created_at: "2026-05-15T11:31:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: Some("abc1234".to_string()),
        host: "sinnix-prime".to_string(),
        mode: "quiesce".to_string(),
        quiesce_receipt: None,
        source_ids: registered_fixture_source_ids(),
        components: vec![ComponentRecord {
            name: "postgres".to_string(),
            path: "postgres/sinex_prod.dump".to_string(),
            bytes: 22,
            blake3: postgres_blake3,
            extras: Some(ComponentExtras::Postgres(PostgresExtras {
                row_counts: Some(row_counts),
            })),
        }],
        totals: Totals {
            uncompressed_bytes: 22,
            archive_bytes: Some(512),
        },
    };
    fs::write(
        staging.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    let archive_path = dir.path().join("postgres-fixture.sinex.tar.zst");
    exec::tar_create_zstd(&staging, &archive_path, 1, 1)?;
    Ok((dir, archive_path))
}

fn make_nats_snapshot_archive_with_summary() -> TestResult<(TempDir, PathBuf)> {
    use sinexctl::admin::manifest::{ComponentRecord, NatsExtras, SnapshotManifest, Totals};

    let dir = tempfile::tempdir()?;
    let staging = dir.path().join("staging");
    let jetstream = staging.join("nats").join("jetstream");
    fs::create_dir_all(jetstream.join("streams").join("events"))?;
    fs::write(
        jetstream.join("streams").join("events").join("meta.json"),
        b"stream-state",
    )?;
    let nats_blake3 = snapshot_component_blake3(&staging.join("nats"))?;
    fs::write(staging.join("nats").join("streams.summary.json"), b"[]")?;

    let manifest = SnapshotManifest {
        snapshot_id: "01970a7f-391b-7000-8000-000000000003".to_string(),
        created_at: "2026-05-15T11:32:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: Some("abc1234".to_string()),
        host: "sinnix-prime".to_string(),
        mode: "quiesce".to_string(),
        quiesce_receipt: None,
        source_ids: registered_fixture_source_ids(),
        components: vec![ComponentRecord {
            name: "nats".to_string(),
            path: "nats/jetstream/".to_string(),
            bytes: 12,
            blake3: nats_blake3,
            extras: Some(ComponentExtras::Nats(NatsExtras {
                member_paths: vec!["streams/events/meta.json".to_string()],
            })),
        }],
        totals: Totals {
            uncompressed_bytes: 12,
            archive_bytes: Some(512),
        },
    };
    fs::write(
        staging.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    let archive_path = dir.path().join("nats-fixture.sinex.tar.zst");
    exec::tar_create_zstd(&staging, &archive_path, 1, 1)?;
    Ok((dir, archive_path))
}

fn make_unsupported_component_snapshot_archive() -> TestResult<(TempDir, PathBuf)> {
    use sinexctl::admin::manifest::{ComponentRecord, SnapshotManifest, Totals};

    let dir = tempfile::tempdir()?;
    let staging = dir.path().join("staging");
    fs::create_dir_all(staging.join("legacy-index"))?;
    fs::write(
        staging.join("legacy-index").join("snapshot.json"),
        br#"{"shape":"old"}"#,
    )?;
    let component_blake3 = snapshot_component_blake3(&staging.join("legacy-index"))?;

    let manifest = SnapshotManifest {
        snapshot_id: "01970a7f-391b-7000-8000-000000000004".to_string(),
        created_at: "2026-05-15T11:33:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: Some("abc1234".to_string()),
        host: "sinnix-prime".to_string(),
        mode: "quiesce".to_string(),
        quiesce_receipt: None,
        source_ids: registered_fixture_source_ids(),
        components: vec![ComponentRecord {
            name: "legacy-index".to_string(),
            path: "legacy-index/".to_string(),
            bytes: 15,
            blake3: component_blake3,
            extras: None,
        }],
        totals: Totals {
            uncompressed_bytes: 15,
            archive_bytes: Some(512),
        },
    };
    fs::write(
        staging.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    let archive_path = dir.path().join("unsupported-component.sinex.tar.zst");
    exec::tar_create_zstd(&staging, &archive_path, 1, 1)?;
    Ok((dir, archive_path))
}

fn make_executable_script(dir: &TempDir, name: &str, body: &str) -> TestResult<PathBuf> {
    let path = dir.path().join(name);
    fs::write(&path, body)?;
    let mut permissions = fs::metadata(&path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions)?;
    Ok(path)
}

fn snapshot_component_blake3(path: &std::path::Path) -> TestResult<String> {
    let mut entries = collect_snapshot_component_files(path, path)?;
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));

    let mut hasher = blake3::Hasher::new();
    for (relative_path, absolute_path) in entries {
        let file_data = fs::read(absolute_path)?;
        let file_hash = blake3::hash(&file_data);
        hasher.update(relative_path.as_bytes());
        hasher.update(file_hash.as_bytes());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn collect_snapshot_component_files(
    base: &std::path::Path,
    dir: &std::path::Path,
) -> TestResult<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_symlink() {
            continue;
        }
        if path.is_file() {
            let relative_path = path.strip_prefix(base)?.to_string_lossy().to_string();
            out.push((relative_path, path));
        } else if path.is_dir() {
            out.extend(collect_snapshot_component_files(base, &path)?);
        }
    }
    Ok(out)
}

// ── Dry-run test ─────────────────────────────────────────────────────────────

/// `--dry-run` should print size estimates and NOT create an archive or staging
/// directory.
#[sinex_test]
async fn dry_run_reports_estimates_and_creates_no_archive() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("test.tar.zst");

    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--dry-run",
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--database-url",
            "postgresql://sinex:sinex@localhost/sinex_prod",
            "--components",
            "nats,cas,state",
        ])
        .output()?;

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
    let staging_entries: Vec<_> = std::fs::read_dir(output_dir.path())?
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
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("test.tar.zst");

    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--dry-run",
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--components",
            "nats,cas,state",
        ])
        .output()?;

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

/// Non-Postgres archive creation preserves the component paths declared in
/// the manifest, including nested NATS and CAS state roots.
#[sinex_test]
async fn snapshot_archive_preserves_component_paths_and_nats_member_manifest()
-> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("test.tar.zst");
    let tools = tempfile::tempdir()?;
    let nats_config = tools.path().join("nats.conf");
    fs::write(
        &nats_config,
        format!(
            "{{\"jetstream\":{{\"store_dir\":\"{}\"}}}}\n",
            state_dir.path().join("nats/jetstream").display()
        ),
    )?;
    let _systemctl = make_executable_script(
        &tools,
        "systemctl",
        &format!(
            "#!/bin/sh\ncase \"$*\" in\n  *'nats.service'*'ExecStart'*) printf '%s\\n' '{{ path=/nix/store/nats-server/bin/nats-server ; argv[]=/nix/store/nats-server/bin/nats-server -c {} ; }}' ;;\n  *) exit 0 ;;\nesac\n",
            nats_config.display()
        ),
    )?;
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = sinexctl_bin()
        .env("PATH", path)
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--nats-store-dir",
            &state_dir.path().join("nats/jetstream").to_string_lossy(),
            "--components",
            "nats,cas,state",
            "--compression",
            "1",
            "--workers",
            "1",
        ])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "snapshot archive creation must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(output_path.exists(), "snapshot archive should be created");

    let inspect = AdminSnapshotInspectCommand {
        archive: output_path.clone(),
    }
    .execute()?;
    assert!(
        inspect.missing_component_paths.is_empty(),
        "archive should contain every non-empty manifest component path: {:?}",
        inspect.missing_component_paths
    );
    assert!(
        inspect.state_source_count.is_some_and(|count| count > 0),
        "state extras should carry compiled source inventory"
    );
    assert_eq!(inspect.state_private_mode_state_present, Some(false));
    let inspect_table = sinexctl::admin::snapshot::format_snapshot_inspect_result(&inspect);
    assert!(
        inspect_table.contains("State source contracts: "),
        "inspect table should summarize state source contracts\n{inspect_table}"
    );
    assert!(
        inspect_table.contains("Private-mode state: absent"),
        "inspect table should summarize private-mode state presence\n{inspect_table}"
    );
    let nats_record = inspect
        .manifest
        .components
        .iter()
        .find(|component| component.name == "nats")
        .ok_or_else(|| color_eyre::eyre::eyre!("snapshot should include nats component"))?;
    assert_eq!(nats_record.path, "nats/jetstream/");
    let nats_member_paths = match &nats_record.extras {
        Some(ComponentExtras::Nats(extras)) => &extras.member_paths,
        other => {
            return Err(color_eyre::eyre::eyre!(
                "nats component should carry member paths, got {other:?}"
            ));
        }
    };
    assert_eq!(
        nats_member_paths,
        &vec!["meta.inf".to_string()],
        "nats member manifest should be relative to the JetStream root"
    );
    let state_record = inspect
        .manifest
        .components
        .iter()
        .find(|component| component.name == "state")
        .ok_or_else(|| color_eyre::eyre::eyre!("snapshot should include state component"))?;
    let state_extras = match &state_record.extras {
        Some(ComponentExtras::State(extras)) => extras,
        other => {
            return Err(color_eyre::eyre::eyre!(
                "state component should carry runtime-state metadata, got {other:?}"
            ));
        }
    };
    assert!(
        state_extras
            .source_ids
            .contains(&"desktop.clipboard".to_string()),
        "state extras should include compiled source descriptor ids"
    );
    assert!(!state_extras.private_mode_state_present);

    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("restore-target");
    let restore = AdminSnapshotRestoreCommand {
        archive: output_path,
        target_dir: target.clone(),
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    }
    .execute()?;

    assert!(
        target
            .join("nats")
            .join("jetstream")
            .join("meta.inf")
            .exists()
    );
    assert!(
        target
            .join("cas")
            .join("blob-repository")
            .join("blob1.bin")
            .exists()
    );
    assert!(
        !target.join("state").join("postgresql").exists(),
        "state component must not copy postgres storage; postgres is captured via pg_dump"
    );
    let observed = restore
        .observed_checks
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("restore drill should report observations"))?;
    assert!(observed.nats_state_present);
    assert_eq!(observed.nats_member_count, Some(1));
    assert_eq!(observed.nats_member_paths_match, Some(true));
    assert_eq!(observed.component_blake3_matches.get("nats"), Some(&true));
    assert_eq!(observed.component_blake3_matches.get("cas"), Some(&true));

    Ok(())
}

/// `sinexctl ops state snapshot` is the operator-facing route to the snapshot
/// implementation.
#[sinex_test]
async fn state_snapshot_dry_run_uses_snapshot_implementation() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("state-alias.tar.zst");

    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--dry-run",
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--components",
            "nats,cas,state",
        ])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ops state snapshot dry-run must use the snapshot implementation\nstdout: {stdout}\nstderr: {stderr}"
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

/// Live snapshots are an explicit weaker-consistency mode. The command should
/// accept the mode, preserve it in the snapshot path, and avoid creating an
/// archive during dry-run.
#[sinex_test]
async fn state_snapshot_live_mode_dry_run_reports_estimates() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("state-live.tar.zst");

    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--dry-run",
            "--mode",
            "live",
            "--components",
            "nats,cas,state",
        ])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "live snapshot dry-run must succeed\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Mode: dry-run"),
        "stdout should report dry-run mode for the command result\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !output_path.exists(),
        "live dry-run must not create an archive at {output_path:?}"
    );

    Ok(())
}

/// `ops state inspect` reads manifest.json from the compressed archive
/// and validates that non-empty manifest component paths exist in the tar.
#[sinex_test]
async fn snapshot_inspect_reports_manifest_and_archive_paths() -> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;

    let cmd = AdminSnapshotInspectCommand {
        archive: archive_path.clone(),
    };
    let result = cmd.execute()?;

    assert_eq!(result.snapshot_id, "01970a7f-391b-7000-8000-000000000001");
    assert_eq!(result.source_count, registered_fixture_source_ids().len());
    assert_eq!(result.component_count, 1);
    assert!(
        result.missing_component_paths.is_empty(),
        "fixture archive should contain every non-empty manifest path"
    );

    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "inspect",
            "--archive",
            &archive_path.to_string_lossy(),
            "--format",
            "json",
        ])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ops state inspect must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("\"snapshot_id\":\"01970a7f-391b-7000-8000-000000000001\""),
        "json output should include the manifest snapshot id\nstdout: {stdout}"
    );

    Ok(())
}

#[sinex_test]
async fn snapshot_inspect_reports_missing_component_paths_before_hash_failure()
-> xtask::sandbox::TestResult<()> {
    use sinexctl::admin::manifest::{ComponentRecord, SnapshotManifest, Totals};

    let dir = tempfile::tempdir()?;
    let staging = dir.path().join("staging");
    fs::create_dir_all(&staging)?;
    let manifest = SnapshotManifest {
        snapshot_id: "01970a7f-391b-7000-8000-000000000006".to_string(),
        created_at: "2026-05-15T11:35:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: None,
        host: "fixture".to_string(),
        mode: "quiesce".to_string(),
        quiesce_receipt: None,
        source_ids: Vec::new(),
        components: vec![ComponentRecord {
            name: "state".to_string(),
            path: "state/".to_string(),
            bytes: 1,
            blake3: "unavailable".to_string(),
            extras: None,
        }],
        totals: Totals {
            uncompressed_bytes: 1,
            archive_bytes: Some(128),
        },
    };
    fs::write(
        staging.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    let archive = dir.path().join("missing-component.sinex.tar.zst");
    exec::tar_create_zstd(&staging, &archive, 1, 1)?;

    let result = AdminSnapshotInspectCommand { archive }.execute()?;
    assert_eq!(result.missing_component_paths, ["state/"]);
    Ok(())
}

#[sinex_test]
async fn snapshot_inspect_rejects_empty_required_nats_component() -> xtask::sandbox::TestResult<()>
{
    use sinexctl::admin::manifest::{ComponentRecord, NatsExtras, SnapshotManifest, Totals};

    let dir = tempfile::tempdir()?;
    let staging = dir.path().join("staging");
    fs::create_dir_all(staging.join("nats").join("jetstream"))?;
    let manifest = SnapshotManifest {
        snapshot_id: "01970a7f-391b-7000-8000-000000000005".to_string(),
        created_at: "2026-05-15T11:34:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: None,
        host: "sinnix-prime".to_string(),
        mode: "quiesce".to_string(),
        quiesce_receipt: None,
        source_ids: registered_fixture_source_ids(),
        components: vec![ComponentRecord {
            name: "nats".to_string(),
            path: "nats/jetstream/".to_string(),
            bytes: 0,
            blake3: snapshot_component_blake3(&staging.join("nats"))?,
            extras: Some(ComponentExtras::Nats(NatsExtras {
                member_paths: Vec::new(),
            })),
        }],
        totals: Totals {
            uncompressed_bytes: 0,
            archive_bytes: Some(128),
        },
    };
    fs::write(
        staging.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    let archive = dir.path().join("empty-nats.sinex.tar.zst");
    exec::tar_create_zstd(&staging, &archive, 1, 1)?;

    let error = AdminSnapshotInspectCommand { archive }
        .execute()
        .expect_err("inspect must reject an empty required NATS component");
    assert!(
        format!("{error:#}").contains("empty required components: nats"),
        "error should identify the empty required component: {error:#}"
    );
    Ok(())
}

/// `ops state restore --dry-run` validates archive structure and returns
/// a non-destructive restore drill plan.
#[sinex_test]
async fn snapshot_restore_dry_run_reports_plan_and_policy() -> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;
    let target = tempfile::tempdir()?;

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path.clone(),
        target_dir: target.path().to_path_buf(),
        state_dir: None,
        dry_run: true,
        allow_non_empty_target: false,
        confirm_restore: false,
        allow_active_services: false,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    };
    let result = cmd.execute()?;

    assert_eq!(result.snapshot_id, "01970a7f-391b-7000-8000-000000000001");
    assert!(result.dry_run);
    assert!(result.target_empty);
    assert_eq!(result.planned_steps.len(), 1);
    assert_eq!(result.planned_steps[0].component, "state");
    assert!(
        result.archive_sensitivity.contains("secret"),
        "archive sensitivity should classify state snapshots as secret"
    );
    assert!(
        result.key_policy.contains("exclude"),
        "key policy should explain key inclusion/exclusion"
    );
    assert!(result.drill_checks.private_mode_state_present);
    assert!(
        result.observed_checks.is_none(),
        "dry-run should not report observed target state"
    );

    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "restore",
            "--archive",
            &archive_path.to_string_lossy(),
            "--target-dir",
            &target.path().to_string_lossy(),
            "--dry-run",
            "--format",
            "json",
        ])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ops state restore dry-run must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("\"archive_sensitivity\""),
        "json output should include archive sensitivity\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("\"key_policy\""),
        "json output should include key policy\nstdout: {stdout}"
    );

    Ok(())
}

#[sinex_test]
async fn snapshot_restore_rejects_symlink_target() -> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let real_target = target_parent.path().join("real-target");
    fs::create_dir(&real_target)?;
    let symlink_target = target_parent.path().join("restore-target");
    std::os::unix::fs::symlink(&real_target, &symlink_target)?;

    let error = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: symlink_target,
        state_dir: None,
        dry_run: true,
        allow_non_empty_target: false,
        confirm_restore: false,
        allow_active_services: false,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    }
    .execute()
    .expect_err("restore must not follow a symlink target");
    assert!(format!("{error:#}").contains("symbolic link"));
    Ok(())
}

/// Restore planning refuses an ambiguous non-empty target unless explicitly
/// allowed, even though dry-run itself writes nothing.
#[sinex_test]
async fn snapshot_restore_dry_run_refuses_non_empty_target_without_override()
-> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;
    let target = tempfile::tempdir()?;
    fs::write(target.path().join("existing"), b"do-not-overwrite")?;

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target.path().to_path_buf(),
        state_dir: None,
        dry_run: true,
        allow_non_empty_target: false,
        confirm_restore: false,
        allow_active_services: false,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    };
    let error = cmd
        .execute()
        .expect_err("non-empty restore target should require an explicit override");
    assert!(
        format!("{error:#}").contains("not empty"),
        "error should mention non-empty target: {error:#}"
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_execute_extracts_state_archive_into_empty_target()
-> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("restore-target");

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path.clone(),
        target_dir: target.clone(),
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    };
    let result = cmd.execute()?;

    assert!(!result.dry_run);
    assert!(target.join("manifest.json").exists());
    assert!(target.join("state").join("checkpoint.bin").exists());
    assert!(
        target
            .join("state")
            .join("private-mode")
            .join("state.json")
            .exists()
    );
    let observed = result
        .observed_checks
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("restore drill should report observations"))?;
    assert!(observed.checks_passed);
    assert!(
        observed.failed_checks.is_empty(),
        "successful restore drill should report no failed checks"
    );
    assert!(observed.private_mode_state_present);
    assert!(observed.private_mode_state_matches_manifest);
    assert!(observed.source_ids_match);
    assert_eq!(
        observed.component_blake3_matches.get("state"),
        Some(&true),
        "restore drill should compare restored state content hash with the manifest"
    );
    let table = format_snapshot_restore_plan_result(&result);
    assert!(
        table.contains("Mode:    isolated-drill"),
        "table output should label non-dry-run restores as isolated drills:\n{table}"
    );
    assert!(
        table.contains("Observed after isolated drill:"),
        "table output should describe observed checks as drill evidence:\n{table}"
    );

    let binary_target = target_parent.path().join("binary-target");
    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "restore",
            "--archive",
            &archive_path.to_string_lossy(),
            "--target-dir",
            &binary_target.to_string_lossy(),
            "--confirm-restore",
            "--allow-active-services",
            "--format",
            "json",
        ])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ops state restore execute must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("\"observed_checks\""),
        "json output should include observed restore checks\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("\"checks_passed\":true"),
        "json output should include aggregate restore verdict\nstdout: {stdout}"
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_execute_preserves_dry_run_plan() -> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let restore_target = target_parent.path().join("restore-target");

    let dry_run = AdminSnapshotRestoreCommand {
        archive: archive_path.clone(),
        target_dir: restore_target.clone(),
        state_dir: None,
        dry_run: true,
        allow_non_empty_target: false,
        confirm_restore: false,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    }
    .execute()?;

    let executed = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: restore_target,
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    }
    .execute()?;

    assert_eq!(
        serde_json::to_value(&executed.planned_steps)?,
        serde_json::to_value(&dry_run.planned_steps)?,
        "execution must preserve the dry-run restore plan"
    );
    assert_eq!(
        serde_json::to_value(&executed.drill_checks)?,
        serde_json::to_value(&dry_run.drill_checks)?,
        "execution must preserve the dry-run drill-check contract"
    );
    assert!(
        dry_run.observed_checks.is_none(),
        "dry-run should not report observed restore checks"
    );
    assert!(
        executed.observed_checks.is_some(),
        "execution should add observations without changing the plan"
    );
    Ok(())
}

/// Full backup -> restore verification against the deployed NixOS topology.
///
/// This is opt-in because it reads the live state root and restores PostgreSQL
/// into an operator-provided empty drill database. Run it with
/// `SINEX_REAL_TOPOLOGY_TEST=1`, `DATABASE_URL=<live-source-url>`, and
/// `SINEX_REAL_RESTORE_DATABASE_URL=<dedicated-empty-drill-url>`.
#[sinex_test]
async fn real_deployed_topology_backup_restore_round_trip() -> TestResult<()> {
    if std::env::var("SINEX_REAL_TOPOLOGY_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let source_database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must identify the real deployed source database");
    let restore_database_url = std::env::var("SINEX_REAL_RESTORE_DATABASE_URL")
        .expect("SINEX_REAL_RESTORE_DATABASE_URL must identify a dedicated empty drill database");
    let output_dir = tempfile::tempdir()?;
    let archive = output_dir.path().join("real-topology.sinex.tar.zst");

    let snapshot = AdminSnapshotCommand {
        output: archive.clone(),
        compression: 3,
        workers: 1,
        mode: "live".to_string(),
        dry_run: false,
        database_url: Some(source_database_url),
        state_dir: None,
        nats_store_dir: None,
        auto_stop: false,
        components: Component::all(),
    }
    .execute()?;
    assert_eq!(
        snapshot.output_path.as_deref(),
        Some(archive.to_str().unwrap())
    );

    let target_parent = tempfile::tempdir()?;
    let restore = AdminSnapshotRestoreCommand {
        archive,
        target_dir: target_parent.path().join("restore-target"),
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: Some(restore_database_url),
        pg_restore_bin: None,
        psql_bin: None,
    }
    .execute()?;
    let observed = restore
        .observed_checks
        .expect("real restore drill must produce observations");
    assert!(
        observed.checks_passed,
        "real deployed-topology restore checks failed: {:?}",
        observed.failed_checks
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_executes_postgres_drill_with_row_count_check()
-> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_postgres_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("postgres-restore-target");
    let tools = tempfile::tempdir()?;
    let pg_restore = make_executable_script(&tools, "pg_restore", "#!/bin/sh\nexit 0\n")?;
    let psql_log = target_parent.path().join("psql.log");
    let psql = make_executable_script(
        &tools,
        "psql",
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >> {}\ncase \"$*\" in\n  *'pg_stat_user_tables'*) printf 'core.events\\npg_temp_141.sinex_batch_staging\\n' ;;\n  *'pg_class'*) printf '0\\n' ;;\n  *'count(*)'*) printf '7\\n' ;;\nesac\nexit 0\n",
            psql_log.display()
        ),
    )?;

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target.clone(),
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: Some("postgresql://restore/sinex_drill".to_string()),
        pg_restore_bin: Some(pg_restore),
        psql_bin: Some(psql),
    };
    let result = cmd.execute()?;
    let observed = result
        .observed_checks
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("restore drill should report observations"))?;

    assert!(observed.checks_passed);
    assert!(observed.failed_checks.is_empty());
    assert_eq!(observed.postgres_row_counts.get("core.events"), Some(&7));
    assert!(
        !observed
            .postgres_row_counts
            .contains_key("pg_temp_141.sinex_batch_staging"),
        "temporary staging tables must not be part of durable restore comparisons"
    );
    assert_eq!(observed.postgres_row_counts_match, Some(true));
    assert_eq!(
        observed.component_blake3_matches.get("postgres"),
        Some(&true),
        "restore drill should compare restored postgres dump hash with the manifest"
    );
    assert!(target.join("postgres").join("sinex_prod.dump").exists());
    let psql_calls = fs::read_to_string(psql_log)?;
    assert!(
        psql_calls.contains("CREATE EXTENSION IF NOT EXISTS timescaledb"),
        "postgres restore should install TimescaleDB before entering restore mode\n{psql_calls}"
    );
    assert!(
        psql_calls.contains("timescaledb_pre_restore()"),
        "postgres restore should enter TimescaleDB restore mode\n{psql_calls}"
    );
    assert!(
        psql_calls.contains("timescaledb_post_restore()"),
        "postgres restore should leave TimescaleDB restore mode\n{psql_calls}"
    );
    assert!(
        !psql_calls.contains("pg_temp_141"),
        "postgres restore should not query temp-table manifest rows\n{psql_calls}"
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_checks_database_empty_before_extracting_target()
-> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_postgres_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("nonempty-database-target");
    let tools = tempfile::tempdir()?;
    let restore_marker = target_parent.path().join("pg-restore-ran");
    let pg_restore = make_executable_script(
        &tools,
        "pg_restore",
        &format!("#!/bin/sh\n: > {}\nexit 0\n", restore_marker.display()),
    )?;
    let psql = make_executable_script(
        &tools,
        "psql",
        "#!/bin/sh\ncase \"$*\" in\n  *'pg_class'*) printf '1\\n' ;;\nesac\nexit 0\n",
    )?;
    let _systemctl = make_executable_script(
        &tools,
        "systemctl",
        &format!(
            "#!/bin/sh\ncase \"$*\" in\n  *'sinexd.service'*'Environment'*) printf 'SINEX_STATE_DIR={}\\n' ;;\nesac\nexit 0\n",
            target_parent.path().display()
        ),
    )?;
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = sinexctl_bin()
        .env("PATH", path)
        .args([
            "ops",
            "state",
            "restore",
            "--archive",
            &archive_path.to_string_lossy(),
            "--target-dir",
            &target.to_string_lossy(),
            "--restore-database-url",
            "postgresql://restore/sinex_restore_nonempty",
            "--pg-restore-bin",
            &pg_restore.to_string_lossy(),
            "--psql-bin",
            &psql.to_string_lossy(),
            "--confirm-restore",
            "--allow-active-services",
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "non-empty target must fail: {stderr}"
    );
    assert!(
        stderr.contains("not empty"),
        "failure should name target state: {stderr}"
    );
    assert!(
        !target.exists(),
        "filesystem target must not be extracted first"
    );
    assert!(
        !restore_marker.exists(),
        "pg_restore must not run on a non-empty target"
    );
    Ok(())
}

#[sinex_test]
async fn quiesced_snapshot_auto_stop_targets_real_writer_units() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("auto-stop.sinex.tar.zst");
    let tools = tempfile::tempdir()?;
    let active_marker = tools.path().join("active-writers");
    let timer_marker = tools.path().join("active-timer");
    let postgres_marker = tools.path().join("active-postgres");
    let inventory = tools.path().join("runtime-inventory.json");
    fs::write(&active_marker, b"running")?;
    fs::write(&timer_marker, b"scheduled")?;
    fs::write(&postgres_marker, b"running")?;
    fs::write(
        &inventory,
        r#"{
  "surfaces": {
    "sinexd": {"unit": "sinexd.service", "resourceClass": "capture-runtime"},
    "nats": {"unit": "nats.service", "resourceClass": "capture-substrate"},
    "sinex-postgres-dump-timer": {"unit": "sinex-postgres-dump.timer", "resourceClass": "backup-maintenance"}
  }
}"#,
    )?;
    let stop_log = tools.path().join("stop.log");
    let _systemctl = make_executable_script(
        &tools,
        "systemctl",
        &format!(
            "#!/bin/sh\ncase \"$1\" in\n  list-units) if [ -e '{}' ]; then printf '%s\\n' 'sinexd.service loaded active running' 'nats.service loaded active running'; fi; if [ -e '{}' ]; then printf '%s\\n' 'sinex-postgres-dump.timer loaded active waiting'; fi; if [ -e '{}' ]; then printf '%s\\n' 'postgresql.service loaded active running'; fi ;;\n  stop) printf '%s\\n' \"$*\" >> '{}'; rm -f '{}' ;;\n  *) exit 0 ;;\nesac\n",
            active_marker.display(),
            timer_marker.display(),
            postgres_marker.display(),
            stop_log.display(),
            active_marker.display(),
        ),
    )?;
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = sinexctl_bin()
        .env("PATH", path)
        .env("SINEX_RUNTIME_INVENTORY", &inventory)
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--components",
            "state",
            "--auto-stop",
            "--compression",
            "1",
            "--workers",
            "1",
        ])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "auto-stop snapshot should succeed against active deployed writer names\nstdout: {stdout}\nstderr: {stderr}"
    );
    let manifest = AdminSnapshotInspectCommand {
        archive: output_path,
    }
    .execute()?;
    assert_eq!(manifest.mode, "quiesce");
    assert_eq!(
        manifest.manifest.quiesce_receipt,
        Some(QuiesceReceipt {
            active_writer_units_before: vec!["sinexd.service".to_string(), "nats.service".to_string()],
            stopped_writer_units: vec!["sinexd.service".to_string(), "nats.service".to_string()],
            active_writer_units_after: Vec::new(),
        }),
        "the archived receipt must preserve the exact stop targets and successful post-stop verification"
    );
    let rendered = format_snapshot_inspect_result(&manifest);
    assert!(
        rendered.contains("active after: none"),
        "the default inspect surface must expose the quiescence verdict\n{rendered}"
    );
    let stop_log = fs::read_to_string(stop_log)?;
    assert!(stop_log.contains("stop sinexd.service nats.service"));
    assert!(!stop_log.contains("postgresql.service"));
    assert!(!stop_log.contains("sinex-postgres-dump.timer"));
    assert!(
        !active_marker.exists(),
        "auto-stop must leave writer units inactive"
    );
    assert!(timer_marker.exists(), "a timer is not a writer stop target");
    assert!(postgres_marker.exists(), "PostgreSQL must remain available for pg_dump");
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_rejects_production_shaped_database_target()
-> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_postgres_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("production-shaped-target");

    let command = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target.clone(),
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: Some("postgresql:///sinex_prod".to_string()),
        pg_restore_bin: None,
        psql_bin: None,
    };
    let error = command
        .execute()
        .expect_err("production-shaped database URLs must not be restore targets");
    assert!(
        format!("{error:#}").contains("disposable rehearsal target"),
        "error should explain the rehearsal-target policy: {error:#}"
    );
    assert!(!target.exists(), "target extraction must not begin");
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_ignores_nats_summary_in_component_hash() -> xtask::sandbox::TestResult<()>
{
    let (_dir, archive_path) = make_nats_snapshot_archive_with_summary()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("nats-restore-target");

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target.clone(),
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    };
    let result = cmd.execute()?;
    let observed = result
        .observed_checks
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("restore drill should report observations"))?;

    assert!(
        target
            .join("nats/jetstream/streams/events/meta.json")
            .exists(),
        "restore target should contain the captured JetStream member"
    );
    assert!(observed.checks_passed);
    assert!(observed.failed_checks.is_empty());
    assert_eq!(observed.nats_member_count, Some(1));
    assert_eq!(observed.nats_member_paths_match, Some(true));
    assert_eq!(observed.component_blake3_matches.get("nats"), Some(&true));
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_postgres_requires_target_database_url() -> xtask::sandbox::TestResult<()>
{
    let (_dir, archive_path) = make_postgres_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("postgres-restore-target");

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target,
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    };
    let error = cmd
        .execute()
        .expect_err("postgres restore drill should require a target database url");
    assert!(
        format!("{error:#}").contains("restore drill execution requires --restore-database-url"),
        "error should explain restore database requirement: {error:#}"
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_execute_requires_confirmation() -> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;
    let target = tempfile::tempdir()?;

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target.path().to_path_buf(),
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: false,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    };
    let error = cmd
        .execute()
        .expect_err("restore drill should require explicit confirmation");
    assert!(
        format!("{error:#}").contains("restore drill execution requires --confirm-restore"),
        "error should explain confirmation flag: {error:#}"
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_restore_rejects_unsupported_archive_components() -> xtask::sandbox::TestResult<()>
{
    let (_dir, archive_path) = make_unsupported_component_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("unsupported-restore-target");

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target,
        state_dir: None,
        dry_run: false,
        allow_non_empty_target: false,
        confirm_restore: true,
        allow_active_services: true,
        restore_database_url: None,
        pg_restore_bin: None,
        psql_bin: None,
    };
    let error = cmd
        .execute()
        .expect_err("unknown archive components should be rejected before extraction");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("state, cas, nats, and postgres"),
        "restore drill error should name the supported restore-drill components: {rendered}"
    );
    assert!(
        rendered.contains("legacy-index"),
        "restore drill error should name the unsupported archive component: {rendered}"
    );
    Ok(())
}

// ── Staging cleanup on pg_dump failure ──────────────────────────────────────

/// When pg_dump fails (bad DATABASE_URL), staging must be cleaned up and the
/// command must exit non-zero.
#[sinex_test]
async fn staging_cleaned_up_on_pg_dump_failure() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("should-not-exist.tar.zst");

    // Use an intentionally invalid DATABASE_URL.
    let output = sinexctl_bin()
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--database-url",
            "postgresql://bad:creds@127.0.0.1:1/nonexistent",
            "--components",
            "postgres",
        ])
        .output()?;

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
    let staging_entries: Vec<_> = std::fs::read_dir(output_dir.path())?
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

/// A successful dump without authoritative row-count evidence must still fail
/// the capture. The archive must not be published with a vacuous restore
/// baseline.
#[sinex_test]
async fn snapshot_fails_when_postgres_row_count_query_fails() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("row-count-failure.tar.zst");
    let tools = tempfile::tempdir()?;
    let _pg_dump = make_executable_script(
        &tools,
        "pg_dump",
        "#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"--file\" ]; then shift; : > \"$1\"; fi\n  shift\ndone\nexit 0\n",
    )?;
    let psql = make_executable_script(
        &tools,
        "psql",
        "#!/bin/sh\nprintf '%s\\n' 'row-count query unavailable' >&2\nexit 17\n",
    )?;
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = sinexctl_bin()
        .env("PATH", path)
        .env("SINEX_PSQL_BIN", &psql)
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--database-url",
            "postgresql://snapshot-test/sinex",
            "--components",
            "postgres",
            "--mode",
            "live",
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "row-count failure must fail snapshot capture\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("row-count") || stderr.contains("psql"),
        "failure should identify row-count evidence\nstderr: {stderr}"
    );
    assert!(
        !output_path.exists(),
        "snapshot must not publish an archive without row-count evidence"
    );
    Ok(())
}

/// A quiesced snapshot must not treat a failed systemd inventory query as an
/// empty active-unit set.
#[sinex_test]
async fn quiesced_snapshot_fails_closed_when_systemctl_inventory_fails()
-> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("systemctl-failure.tar.zst");
    let tools = tempfile::tempdir()?;
    let _systemctl = make_executable_script(
        &tools,
        "systemctl",
        "#!/bin/sh\nprintf '%s\\n' 'systemd inventory unavailable' >&2\nexit 23\n",
    )?;
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = sinexctl_bin()
        .env("PATH", path)
        .args([
            "ops",
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--state-dir",
            &state_dir.path().to_string_lossy(),
            "--components",
            "state",
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "systemd inventory failure must block quiesced snapshot\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("systemctl") || stderr.contains("active-unit"),
        "failure should identify systemd inspection\nstderr: {stderr}"
    );
    assert!(!output_path.exists());
    Ok(())
}

// ── Unit tests (no binary invocation) ────────────────────────────────────────

/// `Component::all()` must include all four expected components.
#[sinex_test]
async fn component_all_covers_all_four() -> xtask::sandbox::TestResult<()> {
    let all = Component::all();
    let names: Vec<&str> = all
        .iter()
        .map(sinexctl::admin::snapshot::Component::name)
        .collect();
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
    let state_dir = make_fake_state_dir()?;
    let explicit_nats_dir = tempfile::tempdir()?;
    let explicit_nats_file = explicit_nats_dir.path().join("explicit-stream-state");
    fs::write(&explicit_nats_file, b"explicit-nats-fixture")?;
    let explicit_nats_bytes = fs::metadata(&explicit_nats_file)?.len();
    let state_nats_bytes = fs::metadata(state_dir.path().join("nats/jetstream/meta.inf"))?.len();
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("test.tar.zst");

    let cmd = AdminSnapshotCommand {
        output: output_path.clone(),
        compression: 3,
        workers: 0,
        mode: "quiesce".to_string(),
        dry_run: true,
        database_url: None,
        state_dir: Some(state_dir.path().to_path_buf()),
        nats_store_dir: Some(explicit_nats_dir.path().to_path_buf()),
        auto_stop: false,
        components: vec![Component::Nats, Component::Cas, Component::State],
    };

    let result = cmd.execute()?;

    assert_eq!(result.mode, "dry-run");
    assert_snapshot_id_is_uuidv7(&result.snapshot_id)?;
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
    assert!(
        result.source_ids.contains(&"desktop.clipboard".to_string()),
        "snapshot should report compiled source descriptor ids"
    );

    // NATS, CAS, and state should appear in the dry-run estimate.
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

    let nats = result
        .components_captured
        .iter()
        .find(|component| component.name == "nats")
        .expect("dry-run result must include the NATS component");
    assert_eq!(
        nats.bytes, explicit_nats_bytes,
        "dry-run NATS estimate must use the explicit NATS store root"
    );
    assert_ne!(
        nats.bytes, state_nats_bytes,
        "dry-run NATS estimate must not silently use state_dir/nats/jetstream"
    );

    Ok(())
}

#[sinex_test]
async fn library_live_snapshot_archive_records_live_manifest() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("live.tar.zst");

    let cmd = AdminSnapshotCommand {
        output: output_path.clone(),
        compression: 1,
        workers: 0,
        mode: "live".to_string(),
        dry_run: false,
        database_url: None,
        state_dir: Some(state_dir.path().to_path_buf()),
        nats_store_dir: None,
        auto_stop: false,
        // A live archive test uses fixture-owned directories. NATS is
        // discovered from the deployed systemd configuration and is covered
        // by the dry-run path above, so it is excluded from this unprivileged
        // fixture test.
        components: vec![Component::Cas, Component::State],
    };

    let result = cmd.execute()?;
    assert_eq!(result.mode, "live");
    assert!(output_path.exists(), "live snapshot should create archive");

    let inspect = AdminSnapshotInspectCommand {
        archive: output_path,
    }
    .execute()?;
    assert_eq!(inspect.mode, "live");
    assert_eq!(inspect.manifest.mode, "live");
    assert!(inspect.missing_component_paths.is_empty());
    Ok(())
}

fn assert_snapshot_id_is_uuidv7(id: &str) -> TestResult<()> {
    assert_eq!(id.len(), 36, "snapshot ID must be canonical UUID text");
    assert_eq!(
        id.as_bytes().get(14),
        Some(&b'7'),
        "snapshot ID must be UUIDv7"
    );
    sinex_primitives::Uuid::parse_str(id)?;
    Ok(())
}

/// Manifest JSON round-trips correctly through serde.
#[sinex_test]
async fn manifest_round_trips_through_serde() -> xtask::sandbox::TestResult<()> {
    use sinexctl::admin::manifest::{
        CasExtras, ComponentExtras, ComponentRecord, PostgresExtras, SnapshotManifest, StateExtras,
        Totals,
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
        quiesce_receipt: None,
        source_ids: vec![
            "desktop.clipboard".to_string(),
            "terminal.atuin-history".to_string(),
        ],
        components: vec![
            ComponentRecord {
                name: "postgres".to_string(),
                path: "postgres/sinex_prod.dump".to_string(),
                bytes: 12345678,
                blake3: "a".repeat(64),
                extras: Some(ComponentExtras::Postgres(PostgresExtras {
                    row_counts: Some(row_counts),
                })),
            },
            ComponentRecord {
                name: "cas".to_string(),
                path: "cas/blob-repository/".to_string(),
                bytes: 1024,
                blake3: "b".repeat(64),
                extras: Some(ComponentExtras::Cas(CasExtras { blob_count: 2 })),
            },
            ComponentRecord {
                name: "state".to_string(),
                path: "state/".to_string(),
                bytes: 256,
                blake3: "c".repeat(64),
                extras: Some(ComponentExtras::State(StateExtras {
                    source_ids: vec!["desktop.clipboard".to_string()],
                    private_mode_state_present: true,
                })),
            },
        ],
        totals: Totals {
            uncompressed_bytes: 12346958,
            archive_bytes: Some(3_000_000),
        },
    };

    let json = serde_json::to_string_pretty(&manifest)?;
    let back: SnapshotManifest = serde_json::from_str(&json)?;

    assert_eq!(back.snapshot_id, "test-id");
    assert_eq!(back.source_ids.len(), 2);
    assert_eq!(back.components.len(), 3);
    let state = back
        .components
        .iter()
        .find(|component| component.name == "state")
        .expect("state component should round-trip");
    match &state.extras {
        Some(ComponentExtras::State(extras)) => {
            assert_eq!(extras.source_ids, ["desktop.clipboard"]);
            assert!(extras.private_mode_state_present);
        }
        other => panic!("state component extras should round-trip, got {other:?}"),
    }
    let cas = back
        .components
        .iter()
        .find(|component| component.name == "cas")
        .expect("CAS component should round-trip");
    match &cas.extras {
        Some(ComponentExtras::Cas(extras)) => assert_eq!(extras.blob_count, 2),
        other => panic!("CAS component extras should preserve blob_count, got {other:?}"),
    }
    assert_eq!(back.totals.archive_bytes, Some(3_000_000));

    Ok(())
}
