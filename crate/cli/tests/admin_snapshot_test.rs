//! Tests for `sinexctl admin snapshot` and the `sinexctl state` snapshot aliases.
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

use sinexctl::admin::exec;
use sinexctl::admin::manifest::ComponentExtras;
use sinexctl::admin::snapshot::{
    AdminSnapshotCommand, AdminSnapshotInspectCommand, AdminSnapshotRestoreCommand, Component,
};

/// Helper: build a fake state directory with recognizable fixture files.
fn make_fake_state_dir() -> TestResult<TempDir> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();

    // postgres — not in state dir, but pg_dump goes to staging
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

    fs::write(
        root.join("source-units.json"),
        r#"{
          "source_units": [
            { "id": "terminal.atuin-history" },
            { "id": "desktop.clipboard" },
            { "id": "desktop.clipboard" }
          ]
        }"#,
    )?;

    Ok(dir)
}

fn sinexctl_bin() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

#[sinex_test]
async fn state_snapshot_help_points_to_restore_drill() -> TestResult<()> {
    let output = sinexctl_bin()
        .args(["state", "snapshot", "--help"])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "state snapshot help must exit 0\nstdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("sinexctl state restore --archive <archive>"),
        "help should point operators at the restore drill command\nstdout: {stdout}"
    );
    assert!(
        !stdout.contains("Restore is manual"),
        "help must not claim restore remains manual\nstdout: {stdout}"
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
    fs::write(
        staging.join("state").join("source-units.json"),
        r#"{"source_units":[{"id":"terminal.atuin-history"}]}"#,
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
        source_unit_ids: vec!["terminal.atuin-history".to_string()],
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
    let manifest = SnapshotManifest {
        snapshot_id: "01970a7f-391b-7000-8000-000000000002".to_string(),
        created_at: "2026-05-15T11:31:00Z".to_string(),
        sinex_version: "0.1.0".to_string(),
        git_sha: Some("abc1234".to_string()),
        host: "sinnix-prime".to_string(),
        mode: "quiesce".to_string(),
        source_unit_ids: vec![],
        components: vec![ComponentRecord {
            name: "postgres".to_string(),
            path: "postgres/sinex_prod.dump".to_string(),
            bytes: 22,
            blake3: postgres_blake3,
            extras: Some(ComponentExtras::Postgres(PostgresExtras { row_counts })),
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
            "admin",
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
            "admin",
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
    let _systemctl = make_executable_script(&tools, "systemctl", "#!/bin/sh\nexit 0\n")?;
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = sinexctl_bin()
        .env("PATH", path)
        .args([
            "admin",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
            "--state-dir",
            &state_dir.path().to_string_lossy(),
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
    assert_eq!(inspect.state_source_unit_count, Some(2));
    assert_eq!(inspect.state_private_mode_state_present, Some(false));
    let inspect_table = sinexctl::admin::snapshot::format_snapshot_inspect_result(&inspect);
    assert!(
        inspect_table.contains("State source units: 2"),
        "inspect table should summarize state source units\n{inspect_table}"
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
    assert_eq!(
        state_extras.source_unit_ids,
        vec![
            "desktop.clipboard".to_string(),
            "terminal.atuin-history".to_string()
        ]
    );
    assert!(!state_extras.private_mode_state_present);

    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("restore-target");
    let restore = AdminSnapshotRestoreCommand {
        archive: output_path,
        target_dir: target.clone(),
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
    let observed = restore
        .observed_checks
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("restore execution should report observations"))?;
    assert!(observed.nats_state_present);
    assert_eq!(observed.nats_member_count, Some(1));
    assert_eq!(observed.nats_member_paths_match, Some(true));
    assert_eq!(observed.component_blake3_matches.get("nats"), Some(&true));
    assert_eq!(observed.component_blake3_matches.get("cas"), Some(&true));

    Ok(())
}

/// `sinexctl state snapshot` is the operator-facing route to the same
/// implementation as `admin snapshot`.
#[sinex_test]
async fn state_snapshot_dry_run_uses_snapshot_implementation() -> xtask::sandbox::TestResult<()> {
    let state_dir = make_fake_state_dir()?;
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("state-alias.tar.zst");

    let output = sinexctl_bin()
        .args([
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
        "state snapshot dry-run must use the snapshot implementation\nstdout: {stdout}\nstderr: {stderr}"
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

/// Live snapshots are intentionally unsupported until there is a real hot
/// capture implementation. The binary path must fail closed rather than
/// silently running the quiesce-mode code with a misleading mode label.
#[sinex_test]
async fn state_snapshot_live_mode_fails_closed() -> xtask::sandbox::TestResult<()> {
    let output_dir = tempfile::tempdir()?;
    let output_path = output_dir.path().join("state-live.tar.zst");

    let output = sinexctl_bin()
        .args([
            "state",
            "snapshot",
            "--output",
            &output_path.to_string_lossy(),
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
        !output.status.success(),
        "live snapshot mode must fail closed until implemented\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("only mode=quiesce is supported"),
        "stderr should explain the unsupported live mode\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !output_path.exists(),
        "unsupported live mode must not create an archive at {output_path:?}"
    );

    Ok(())
}

/// `admin snapshot-inspect` reads manifest.json from the compressed archive
/// and validates that non-empty manifest component paths exist in the tar.
#[sinex_test]
async fn snapshot_inspect_reports_manifest_and_archive_paths() -> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;

    let cmd = AdminSnapshotInspectCommand {
        archive: archive_path.clone(),
    };
    let result = cmd.execute()?;

    assert_eq!(result.snapshot_id, "01970a7f-391b-7000-8000-000000000001");
    assert_eq!(result.source_unit_count, 1);
    assert_eq!(result.component_count, 1);
    assert!(
        result.missing_component_paths.is_empty(),
        "fixture archive should contain every non-empty manifest path"
    );

    let output = sinexctl_bin()
        .args([
            "admin",
            "snapshot-inspect",
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
        "snapshot-inspect must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("\"snapshot_id\":\"01970a7f-391b-7000-8000-000000000001\""),
        "json output should include the manifest snapshot id\nstdout: {stdout}"
    );

    Ok(())
}

/// `admin snapshot-restore --dry-run` validates archive structure and returns
/// a non-destructive restore drill plan.
#[sinex_test]
async fn snapshot_restore_dry_run_reports_plan_and_policy() -> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_snapshot_archive()?;
    let target = tempfile::tempdir()?;

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path.clone(),
        target_dir: target.path().to_path_buf(),
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
            "admin",
            "snapshot-restore",
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
        "snapshot-restore dry-run must exit 0\nstdout: {stdout}\nstderr: {stderr}"
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
        .ok_or_else(|| color_eyre::eyre::eyre!("restore execution should report observations"))?;
    assert!(observed.checks_passed);
    assert!(
        observed.failed_checks.is_empty(),
        "successful restore drill should report no failed checks"
    );
    assert!(observed.private_mode_state_present);
    assert!(observed.private_mode_state_matches_manifest);
    assert!(observed.source_unit_ids_match);
    assert_eq!(
        observed.component_blake3_matches.get("state"),
        Some(&true),
        "restore execution should compare restored state content hash with the manifest"
    );

    let binary_target = target_parent.path().join("binary-target");
    let output = sinexctl_bin()
        .args([
            "admin",
            "snapshot-restore",
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
        "snapshot-restore execute must exit 0\nstdout: {stdout}\nstderr: {stderr}"
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
async fn snapshot_restore_executes_postgres_drill_with_row_count_check()
-> xtask::sandbox::TestResult<()> {
    let (_dir, archive_path) = make_postgres_snapshot_archive()?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("postgres-restore-target");
    let tools = tempfile::tempdir()?;
    let pg_restore = make_executable_script(&tools, "pg_restore", "#!/bin/sh\nexit 0\n")?;
    let psql = make_executable_script(&tools, "psql", "#!/bin/sh\nprintf '7\\n'\n")?;

    let cmd = AdminSnapshotRestoreCommand {
        archive: archive_path,
        target_dir: target.clone(),
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
        .ok_or_else(|| color_eyre::eyre::eyre!("restore execution should report observations"))?;

    assert!(observed.checks_passed);
    assert!(observed.failed_checks.is_empty());
    assert_eq!(observed.postgres_row_counts.get("core.events"), Some(&7));
    assert_eq!(observed.postgres_row_counts_match, Some(true));
    assert_eq!(
        observed.component_blake3_matches.get("postgres"),
        Some(&true),
        "restore execution should compare restored postgres dump hash with the manifest"
    );
    assert!(target.join("postgres").join("sinex_prod.dump").exists());
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
        .expect_err("postgres restore execution should require a target database url");
    assert!(
        format!("{error:#}").contains("--restore-database-url"),
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
        .expect_err("restore execution should require explicit confirmation");
    assert!(
        format!("{error:#}").contains("--confirm-restore"),
        "error should explain confirmation flag: {error:#}"
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
            "admin",
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
            ComponentRecord {
                name: "state".to_string(),
                path: "state/".to_string(),
                bytes: 256,
                blake3: "c".repeat(64),
                extras: Some(ComponentExtras::State(StateExtras {
                    source_unit_ids: vec!["desktop.clipboard".to_string()],
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
    assert_eq!(back.source_unit_ids.len(), 2);
    assert_eq!(back.components.len(), 3);
    let state = back
        .components
        .iter()
        .find(|component| component.name == "state")
        .expect("state component should round-trip");
    match &state.extras {
        Some(ComponentExtras::State(extras)) => {
            assert_eq!(extras.source_unit_ids, ["desktop.clipboard"]);
            assert!(extras.private_mode_state_present);
        }
        other => panic!("state component extras should round-trip, got {other:?}"),
    }
    assert_eq!(back.totals.archive_bytes, Some(3_000_000));

    Ok(())
}
