use super::*;
use crate::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

#[sinex_test]
async fn test_remove_file_if_present_reports_remove_failures() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let error = remove_file_if_present(temp.path(), false).unwrap_err();
    assert!(format!("{error:#}").contains("remove "));
    Ok(())
}

#[sinex_test]
async fn test_remove_file_if_present_returns_false_for_missing_path() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let removed = remove_file_if_present(&temp.path().join("missing.txt"), false)?;
    assert!(!removed);
    Ok(())
}

#[sinex_test]
async fn test_reset_hot_reload_checkpoints_removes_only_checkpoint_json() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let cache = temp.path().join(".cache/sinex");
    std::fs::create_dir_all(&cache)?;
    std::fs::write(cache.join("raindrop-bookmarks.checkpoint.json"), "{}")?;
    std::fs::write(cache.join("keep.json"), "{}")?;
    std::fs::write(cache.join("notes.checkpoint.txt"), "")?;

    let mut env = EnvGuard::new();
    env.set("HOME", temp.path().to_string_lossy().as_ref());

    reset_hot_reload_checkpoints(false)?;

    assert!(!cache.join("raindrop-bookmarks.checkpoint.json").exists());
    assert!(cache.join("keep.json").exists());
    assert!(cache.join("notes.checkpoint.txt").exists());
    Ok(())
}

#[sinex_test]
async fn test_reset_runtime_material_tmpfiles_removes_only_material_fragments() -> TestResult<()>
{
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("sinex_material_abc.tmp"), "fragment")?;
    std::fs::write(temp.path().join("sinex_material_abc.txt"), "keep")?;
    std::fs::write(temp.path().join("other.tmp"), "keep")?;

    reset_runtime_material_tmpfiles_in_dirs(&[temp.path().to_path_buf()], false)?;

    assert!(!temp.path().join("sinex_material_abc.tmp").exists());
    assert!(temp.path().join("sinex_material_abc.txt").exists());
    assert!(temp.path().join("other.tmp").exists());
    Ok(())
}

#[sinex_test]
async fn test_target_dirs_for_reset_includes_historical_sinex_target() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let configured = workspace.path().join(".sinex/cache/target");

    let dirs = target_dirs_for_reset(&configured, workspace.path());

    assert_eq!(
        dirs,
        vec![configured, workspace.path().join(".sinex/target")]
    );
    Ok(())
}

#[sinex_test]
async fn test_target_dirs_for_reset_deduplicates_historical_target() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let configured = workspace.path().join(".sinex/target");

    let dirs = target_dirs_for_reset(&configured, workspace.path());

    assert_eq!(dirs, vec![configured]);
    Ok(())
}

#[sinex_test]
async fn test_stale_build_classifier_requires_age_orphan_tool_and_target() -> TestResult<()> {
    let target = std::path::PathBuf::from("/tmp/sinex-target");
    let probe = BuildProcessProbe {
        pid: 42,
        ppid: 1,
        age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
        command: "/nix/store/bin/ld.mold /tmp/sinex-target/debug/deps/libfoo.rlib".to_string(),
        parent_command: Some("/sbin/init".to_string()),
    };

    let classified = classify_stale_build_process(
        &probe,
        std::slice::from_ref(&target),
        STALE_BUILD_PROCESS_MIN_AGE_SECS,
    );

    assert_eq!(
        classified,
        Some(StaleBuildProcess {
            pid: 42,
            ppid: 1,
            age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
            command: probe.command.clone(),
        })
    );

    let fresh = BuildProcessProbe {
        age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS - 1,
        ..probe.clone()
    };
    assert!(
        classify_stale_build_process(&fresh, &[target], STALE_BUILD_PROCESS_MIN_AGE_SECS)
            .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn test_stale_build_classifier_rejects_live_parent() -> TestResult<()> {
    let target = std::path::PathBuf::from("/tmp/sinex-target");
    let probe = BuildProcessProbe {
        pid: 42,
        ppid: 99,
        age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
        command: "rustc --crate-name foo /tmp/sinex-target/debug/deps/foo.rs".to_string(),
        parent_command: Some("cargo check -p xtask".to_string()),
    };

    assert!(
        classify_stale_build_process(&probe, &[target], STALE_BUILD_PROCESS_MIN_AGE_SECS)
            .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn test_stale_build_classifier_rejects_non_target_commands() -> TestResult<()> {
    let probe = BuildProcessProbe {
        pid: 42,
        ppid: 1,
        age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
        command: "gcc /tmp/other-target/debug/build/foo.o".to_string(),
        parent_command: Some("/sbin/init".to_string()),
    };

    assert!(
        classify_stale_build_process(
            &probe,
            &[std::path::PathBuf::from("/tmp/sinex-target")],
            STALE_BUILD_PROCESS_MIN_AGE_SECS,
        )
        .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn test_orphaned_build_parent_accepts_user_systemd() -> TestResult<()> {
    assert!(orphaned_build_parent_for_reset(
        3492,
        Some("/nix/store/systemd/lib/systemd/systemd --user")
    ));
    assert!(!orphaned_build_parent_for_reset(
        3492,
        Some("cargo check -p xtask")
    ));
    Ok(())
}

#[sinex_test]
async fn test_stale_test_postgres_classifier_accepts_orphaned_test_cluster() -> TestResult<()> {
    let probe = BuildProcessProbe {
        pid: 42,
        ppid: 99,
        age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
        command: "/nix/store/postgresql/bin/postgres -D /dev/shm/sinex-test-sinity-hash/xtask-sqlx.ABCD/pgdata".to_string(),
        parent_command: Some("/nix/store/systemd/lib/systemd/systemd --user".to_string()),
    };

    let classified =
        classify_stale_test_postgres_process(&probe, STALE_TEST_POSTGRES_MIN_AGE_SECS);

    assert_eq!(
        classified,
        Some(StaleTestPostgresProcess {
            pid: 42,
            ppid: 99,
            age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
            data_dir: std::path::PathBuf::from(
                "/dev/shm/sinex-test-sinity-hash/xtask-sqlx.ABCD/pgdata"
            ),
            command: probe.command.clone(),
        })
    );
    Ok(())
}

#[sinex_test]
async fn test_stale_test_postgres_classifier_rejects_checkout_dev_postgres() -> TestResult<()> {
    let probe = BuildProcessProbe {
        pid: 42,
        ppid: 99,
        age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
        command: "/nix/store/postgresql/bin/postgres -D /var/cache/sinex/sinity/hash/dev-state/data/postgres".to_string(),
        parent_command: Some("/nix/store/systemd/lib/systemd/systemd --user".to_string()),
    };

    assert!(
        classify_stale_test_postgres_process(&probe, STALE_TEST_POSTGRES_MIN_AGE_SECS)
            .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn test_stale_test_postgres_classifier_rejects_live_parent() -> TestResult<()> {
    let probe = BuildProcessProbe {
        pid: 42,
        ppid: 99,
        age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
        command: "/nix/store/postgresql/bin/postgres -D /dev/shm/sinex-test-sinity-hash/xtask-sqlx.ABCD/pgdata".to_string(),
        parent_command: Some("xtask test -p xtask".to_string()),
    };

    assert!(
        classify_stale_test_postgres_process(&probe, STALE_TEST_POSTGRES_MIN_AGE_SECS)
            .is_none()
    );
    Ok(())
}

#[sinex_serial_test]
async fn test_reset_test_tmp_removes_readonly_stale_dirs() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    std::fs::write(workspace.path().join("Cargo.toml"), "[workspace]\n")?;
    std::fs::create_dir_all(workspace.path().join("xtask"))?;
    std::fs::write(
        workspace.path().join("xtask/Cargo.toml"),
        "[package]\nname = \"xtask\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
    )?;
    let stale_dir = workspace
        .path()
        .join(".sinex/test-tmp/stale/.git/annex/objects");
    std::fs::create_dir_all(&stale_dir)?;
    let readonly_file = stale_dir.join("readonly.tmp");
    std::fs::write(&readonly_file, "stale")?;
    let mut permissions = std::fs::metadata(&readonly_file)?.permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&readonly_file, permissions)?;

    let cwd = std::env::current_dir()?;
    std::env::set_current_dir(workspace.path())?;
    let result = reset_test_tmp(false);
    std::env::set_current_dir(cwd)?;

    assert!(result?);
    assert!(!workspace.path().join(".sinex/test-tmp/stale").exists());
    Ok(())
}

#[sinex_serial_test]
async fn test_reset_target_removes_configured_and_historical_dirs() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    std::fs::write(workspace.path().join("Cargo.toml"), "[workspace]\n")?;
    std::fs::create_dir_all(workspace.path().join("xtask"))?;
    std::fs::write(
        workspace.path().join("xtask/Cargo.toml"),
        "[package]\nname = \"xtask\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
    )?;
    let configured = workspace.path().join(".sinex/cache/target");
    let historical = workspace.path().join(".sinex/target");
    std::fs::create_dir_all(&configured)?;
    std::fs::create_dir_all(&historical)?;

    let mut env = crate::sandbox::EnvGuard::with_keys(&["CARGO_TARGET_DIR"]);
    env.set("CARGO_TARGET_DIR", &configured);
    let cwd = std::env::current_dir()?;
    std::env::set_current_dir(workspace.path())?;

    let result = reset_target(false);
    std::env::set_current_dir(cwd)?;
    let removed = result?;

    assert_eq!(removed, vec![configured.clone(), historical.clone()]);
    assert!(!configured.exists());
    assert!(!historical.exists());
    Ok(())
}
