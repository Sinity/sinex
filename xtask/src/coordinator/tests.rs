use super::*;
use crate::history::InvocationStatus;
use crate::sandbox::sinex_test;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn run_git(args: &[&str], cwd: &Path) -> ::xtask::sandbox::TestResult<()> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    assert!(
        output.status.success(),
        "git {} failed: stdout={} stderr={}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

use xtask::sandbox::EnvGuard;

fn env_set_path(key: &str, value: &std::path::Path) -> EnvGuard {
    let mut guard = EnvGuard::new();
    guard.set(key, value);
    guard
}

#[sinex_test]
async fn test_should_coordinate() -> TestResult<()> {
    assert!(JobCoordinator::should_coordinate("check", &[]));
    assert!(JobCoordinator::should_coordinate("build", &[]));
    assert!(!JobCoordinator::should_coordinate(
        "build",
        &["--dry-run".into()]
    ));
    assert!(JobCoordinator::should_coordinate("vm", &["test".into()]));
    assert!(JobCoordinator::should_coordinate(
        "test",
        &["-p".into(), "sinex-db".into()]
    ));
    assert!(!JobCoordinator::should_coordinate(
        "test",
        &["--debug".into()]
    ));
    assert!(!JobCoordinator::should_coordinate(
        "test",
        &["--fuzz".into()]
    ));
    assert!(!JobCoordinator::should_coordinate(
        "test",
        &["--coverage".into()]
    ));
    assert!(!JobCoordinator::should_coordinate(
        "test",
        &["--mutants".into()]
    ));
    assert!(!JobCoordinator::should_coordinate(
        "test",
        &["--bench".into()]
    ));
    assert!(JobCoordinator::should_coordinate("fix", &[]));
    Ok(())
}

#[sinex_test]
async fn test_supports_fresh_reuse_only_for_buildish_commands() -> TestResult<()> {
    assert!(supports_fresh_reuse("check"));
    assert!(supports_fresh_reuse("build"));
    assert!(!supports_fresh_reuse("fix"));
    assert!(!supports_fresh_reuse("test"));
    assert!(!supports_fresh_reuse("vm"));
    assert!(supports_fresh_reuse_for("check", &[]));
    assert!(supports_fresh_reuse_for("check", &["--full".into()]));
    assert!(!supports_fresh_reuse_for("check", &["--fix".into()]));
    assert!(supports_fresh_reuse_for("build", &[]));
    assert!(!supports_fresh_reuse_for("build", &["--dry-run".into()]));
    assert!(!supports_fresh_reuse_for("fix", &[]));
    assert!(supports_fresh_reuse_for(
        "test",
        &["--scope=packages:xtask".into()]
    ));
    assert!(supports_fresh_reuse_for(
        "test",
        &["--scope=packages:xtask".into(), "--lib".into()]
    ));
    assert!(!supports_fresh_reuse_for(
        "test",
        &[
            "--scope=packages:xtask".into(),
            "--lib".into(),
            "--update-snapshots".into()
        ]
    ));
    assert!(!supports_fresh_reuse_for(
        "test",
        &["--scope=packages:xtask".into(), "--dry-run".into()]
    ));
    assert!(!supports_fresh_reuse_for(
        "test",
        &["--scope=packages:xtask".into(), "--debug".into()]
    ));
    assert!(!supports_fresh_reuse_for(
        "test",
        &["--scope=packages:xtask".into(), "-l".into()]
    ));
    assert!(!supports_fresh_reuse_for(
        "test",
        &["--scope=packages:xtask".into(), "--no-reuse".into()]
    ));
    Ok(())
}

#[sinex_test]
async fn test_test_binary_args_are_scope_relevant() -> TestResult<()> {
    let without_args = scope_key("test", &["-p".into(), "xtask".into()]);
    let with_args = scope_key(
        "test",
        &[
            "-p".into(),
            "xtask".into(),
            "--".into(),
            "--exact".into(),
            "case-name".into(),
        ],
    );
    let with_args_as_semantic = scope_key(
        "test",
        &[
            "--scope=packages:xtask".into(),
            "--test-arg=--exact".into(),
            "--test-arg=case-name".into(),
        ],
    );

    assert_ne!(without_args, with_args);
    assert_eq!(with_args, with_args_as_semantic);
    assert!(supports_fresh_reuse_for(
        "test",
        &[
            "-p".into(),
            "xtask".into(),
            "--".into(),
            "--exact".into(),
            "case-name".into(),
        ]
    ));
    Ok(())
}

#[sinex_test]
async fn test_cargo_features_are_scope_relevant() -> TestResult<()> {
    let without_features = scope_key("test", &["-p".into(), "xtask".into()]);
    let with_features = scope_key(
        "test",
        &[
            "-p".into(),
            "xtask".into(),
            "--features".into(),
            "extra-feature".into(),
        ],
    );
    let with_features_combined = scope_key(
        "test",
        &[
            "--scope=packages:xtask".into(),
            "--features=extra-feature".into(),
        ],
    );

    assert_ne!(without_features, with_features);
    assert_eq!(with_features, with_features_combined);
    Ok(())
}

#[sinex_test]
async fn test_test_binary_args_preserve_order_in_scope_key() -> TestResult<()> {
    let first_order = scope_key(
        "test",
        &[
            "-p".into(),
            "xtask".into(),
            "--".into(),
            "--exact".into(),
            "case-name".into(),
        ],
    );
    let second_order = scope_key(
        "test",
        &[
            "-p".into(),
            "xtask".into(),
            "--".into(),
            "case-name".into(),
            "--exact".into(),
        ],
    );

    assert_ne!(
        first_order, second_order,
        "test binary args are order-sensitive and must not be sorted into the same proof key"
    );
    Ok(())
}

#[sinex_test]
async fn test_test_binary_args_do_not_become_package_scope() -> TestResult<()> {
    let packages = extract_explicit_packages(
        "test",
        &[
            "-p".into(),
            "xtask".into(),
            "--".into(),
            "-p".into(),
            "fake-test-arg".into(),
        ],
    );

    assert_eq!(packages, vec!["xtask".to_string()]);
    Ok(())
}

#[sinex_test]
async fn test_test_execution_shape_flags_are_scope_relevant() -> TestResult<()> {
    let base = vec!["-p".into(), "xtask".into()];
    for flag in [
        "--threads=1",
        "--retries=2",
        "--timeout=30s",
        "--db-pool-size-env=48",
        "--runtime-binary=sinexd:sinexd",
        "--debug",
        "--impact-mode=aggressive",
        "--impact-planner-version=impact-v2",
        "--impact-coverage-schema=llvm-json-v1",
    ] {
        let mut with_flag = base.clone();
        with_flag.push(flag.to_string());
        assert_ne!(
            scope_key("test", &base),
            scope_key("test", &with_flag),
            "{flag} must be part of the test proof scope key"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_runtime_binary_requirements_extend_package_fingerprint_scope() -> TestResult<()> {
    let packages = extract_explicit_packages(
        "test",
        &[
            "--scope=packages:sinex-db".into(),
            "--runtime-binary=sinexd:sinexd".into(),
        ],
    );

    assert_eq!(packages, vec!["sinex-db".to_string(), "sinexd".to_string()]);
    Ok(())
}

#[sinex_test]
async fn test_coordination_family_groups_heavy_commands() -> TestResult<()> {
    assert_eq!(coordination_family("check"), "heavy-work");
    assert_eq!(coordination_family("build"), "heavy-work");
    assert_eq!(coordination_family("test"), "heavy-work");
    assert_eq!(coordination_family("fix"), "heavy-work");
    assert_eq!(coordination_family("vm"), "heavy-work");
    Ok(())
}

#[sinex_test]
async fn test_scope_key_deterministic() -> TestResult<()> {
    let args1 = vec!["-p".into(), "sinex-db".into(), "--all".into()];
    let args2 = vec!["--all".into(), "-p".into(), "sinex-db".into()];
    assert_eq!(scope_key("test", &args1), scope_key("test", &args2));
    Ok(())
}

#[sinex_test]
async fn test_scope_key_different() -> TestResult<()> {
    let args1 = vec!["-p".into(), "sinex-db".into()];
    let args2 = vec!["-p".into(), "sinexd".into()];
    assert_ne!(scope_key("test", &args1), scope_key("test", &args2));
    Ok(())
}

#[sinex_test]
async fn test_scope_key_ignores_irrelevant() -> TestResult<()> {
    // --fail-fast and --skip-preflight are not
    // proof-relevant for successful test runs: if the run is green, the
    // selected tests passed regardless of those scheduling guards.
    let args1 = vec!["-p".into(), "sinex-db".into()];
    let args2 = vec![
        "-p".into(),
        "sinex-db".into(),
        "--fail-fast".into(),
        "--skip-preflight".into(),
    ];
    assert_eq!(scope_key("test", &args1), scope_key("test", &args2));
    Ok(())
}

#[sinex_test]
async fn test_scope_key_uses_semantic_scope_marker() -> TestResult<()> {
    let args1 = vec!["--scope=packages:sinex-db,xtask".into()];
    let args2 = vec!["--scope=packages:sinexd,xtask".into()];
    assert_ne!(scope_key("test", &args1), scope_key("test", &args2));
    Ok(())
}

#[sinex_test]
async fn test_scope_key_prefers_semantic_scope_marker() -> TestResult<()> {
    let args1 = vec![
        "--scope=packages:sinex-db,xtask".into(),
        "-p".into(),
        "sinexd".into(),
    ];
    let args2 = vec!["--scope=packages:sinex-db,xtask".into()];
    assert_eq!(scope_key("test", &args1), scope_key("test", &args2));
    Ok(())
}

#[sinex_test]
async fn test_scope_key_canonicalizes_package_scope_marker() -> TestResult<()> {
    assert_eq!(
        scope_key("check", &["-p".into(), "xtask".into()]),
        scope_key("check", &["--scope=packages:xtask".into()])
    );
    assert_eq!(
        scope_key(
            "test",
            &[
                "-p".into(),
                "xtask".into(),
                "-E".into(),
                "test(example)".into()
            ]
        ),
        scope_key(
            "test",
            &[
                "--scope=packages:xtask".into(),
                "--filter=test(example)".into()
            ]
        )
    );
    Ok(())
}

#[sinex_test]
async fn test_check_fresh_returns_none_when_history_db_is_unopenable() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _history_db_guard = env_set_path("XTASK_HISTORY_DB", tempdir.path());
    let coordinator = JobCoordinator::new()?;

    assert!(
        coordinator
            .check_fresh("check", &[], "tree-fingerprint", "scope-key")
            .is_none(),
        "unopenable history DB should disable freshness checks instead of panicking"
    );
    Ok(())
}

#[sinex_test]
async fn test_check_scope_varies_with_packages() -> TestResult<()> {
    let args_p1 = vec!["-p".into(), "sinex-db".into()];
    let args_p2 = vec!["-p".into(), "sinexd".into()];
    let args_all = vec!["--all".into()];
    let args_lint = vec!["--lint".into()];
    let args_empty: Vec<String> = vec![];

    // Different packages → different scope
    assert_ne!(scope_key("check", &args_p1), scope_key("check", &args_p2));

    // -p vs --all → different scope
    assert_ne!(scope_key("check", &args_p1), scope_key("check", &args_all));

    // Lint flags affect proof identity even though they target the same package.
    assert_ne!(
        scope_key("check", &args_lint),
        scope_key("check", &args_empty)
    );
    assert_ne!(
        scope_key("check", &["-p".into(), "sinex-db".into(), "--lint".into()]),
        scope_key("check", &["-p".into(), "sinex-db".into()])
    );

    Ok(())
}

#[sinex_test]
async fn test_tree_fingerprint_fails_outside_git_repo() -> TestResult<()> {
    let dir = tempfile::Builder::new()
        .prefix("xtask-nongit-")
        .tempdir_in("/tmp")?;
    let error = tree_fingerprint_in(dir.path()).expect_err("expected non-repo to fail");
    assert!(
        error
            .to_string()
            .contains("git update-index -q --refresh failed")
    );
    Ok(())
}

#[sinex_test]
async fn test_scoped_tree_fingerprint_fails_outside_git_repo() -> TestResult<()> {
    let dir = tempfile::Builder::new()
        .prefix("xtask-nongit-")
        .tempdir_in("/tmp")?;
    let args = vec!["-p".into(), "xtask".into()];
    let error = scoped_tree_fingerprint_in(dir.path(), "check", &args)
        .expect_err("expected non-repo to fail");
    assert!(
        error
            .to_string()
            .contains("git update-index -q --refresh failed")
    );
    Ok(())
}

#[sinex_test]
async fn test_scoped_tree_fingerprint_succeeds_in_initialized_repo() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
    std::fs::create_dir_all(dir.path().join("xtask/src"))?;
    std::fs::write(dir.path().join("xtask/src/lib.rs"), "fn main() {}\n")?;
    run_git(&["add", "xtask/src/lib.rs"], dir.path())?;
    run_git(&["commit", "-qm", "init"], dir.path())?;
    std::fs::write(
        dir.path().join("xtask/src/lib.rs"),
        "fn main() { println!(\"dirty\"); }\n",
    )?;

    let args = vec!["-p".into(), "xtask".into()];
    let fingerprint = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;

    assert!(!fingerprint.is_empty());
    Ok(())
}

/// Regression: clean-tree per-package invocations across different packages
/// must NOT collide. Pre-#1212, all clean-tree fingerprints hashed zero bytes
/// and SHA256("")'d into one bucket — 117 collisions in 7d on master.
#[sinex_test]
async fn test_scoped_tree_fingerprint_clean_tree_distinguishes_packages() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;

    std::fs::create_dir_all(dir.path().join("crate/sinex-db/src"))?;
    std::fs::write(
        dir.path().join("Cargo.toml"),
        r#"[workspace]
members = ["crate/sinex-db", "crate/sinex-primitives"]
"#,
    )?;
    std::fs::write(
        dir.path().join("crate/sinex-db/Cargo.toml"),
        r#"[package]
name = "sinex-db"
version = "0.0.0"
edition = "2024"
"#,
    )?;
    std::fs::write(dir.path().join("crate/sinex-db/src/lib.rs"), "fn db() {}\n")?;
    std::fs::create_dir_all(dir.path().join("crate/sinex-primitives/src"))?;
    std::fs::write(
        dir.path().join("crate/sinex-primitives/Cargo.toml"),
        r#"[package]
name = "sinex-primitives"
version = "0.0.0"
edition = "2024"
"#,
    )?;
    std::fs::write(
        dir.path().join("crate/sinex-primitives/src/lib.rs"),
        "fn p() {}\n",
    )?;
    run_git(
        &[
            "add",
            "Cargo.toml",
            "crate/sinex-db/Cargo.toml",
            "crate/sinex-db/src/lib.rs",
            "crate/sinex-primitives/Cargo.toml",
            "crate/sinex-primitives/src/lib.rs",
        ],
        dir.path(),
    )?;
    run_git(&["commit", "-qm", "init"], dir.path())?;

    let fp_db = scoped_tree_fingerprint_in(dir.path(), "check", &["-p".into(), "sinex-db".into()])?;
    let fp_primitives = scoped_tree_fingerprint_in(
        dir.path(),
        "check",
        &["-p".into(), "sinex-primitives".into()],
    )?;

    assert_ne!(
        fp_db, fp_primitives,
        "Clean-tree fingerprints must distinguish packages (no SHA256(\"\") collision)"
    );
    assert_ne!(
        fp_db, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "Clean-tree fingerprint must not be SHA256(\"\")"
    );
    Ok(())
}

/// Regression: the same package against different HEAD commits must produce
/// different fingerprints, even with a clean working tree.
#[sinex_test]
async fn test_scoped_tree_fingerprint_clean_tree_distinguishes_head() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;

    std::fs::create_dir_all(dir.path().join("crate/sinex-db/src"))?;
    std::fs::write(dir.path().join("crate/sinex-db/src/lib.rs"), "fn db() {}\n")?;
    run_git(&["add", "crate/sinex-db/src/lib.rs"], dir.path())?;
    run_git(&["commit", "-qm", "first"], dir.path())?;
    let args = vec!["-p".into(), "sinex-db".into()];
    let fp_first = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;

    std::fs::write(
        dir.path().join("crate/sinex-db/src/lib.rs"),
        "fn db() { /* v2 */ }\n",
    )?;
    run_git(&["add", "crate/sinex-db/src/lib.rs"], dir.path())?;
    run_git(&["commit", "-qm", "second"], dir.path())?;
    let fp_second = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;

    assert_ne!(
        fp_first, fp_second,
        "Clean-tree fingerprints must distinguish HEAD commits"
    );
    Ok(())
}

#[sinex_test]
async fn test_tree_fingerprint_succeeds_in_dirty_repo() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
    std::fs::write(dir.path().join("tracked.txt"), "clean\n")?;
    run_git(&["add", "tracked.txt"], dir.path())?;
    run_git(&["commit", "-qm", "init"], dir.path())?;
    std::fs::write(dir.path().join("tracked.txt"), "dirty\n")?;

    let fingerprint = tree_fingerprint_in(dir.path())?;

    assert!(!fingerprint.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_tree_fingerprint_distinguishes_dirty_content_same_path() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
    std::fs::write(dir.path().join("tracked.txt"), "clean\n")?;
    run_git(&["add", "tracked.txt"], dir.path())?;
    run_git(&["commit", "-qm", "init"], dir.path())?;

    std::fs::write(dir.path().join("tracked.txt"), "dirty one\n")?;
    let fp_one = tree_fingerprint_in(dir.path())?;
    std::fs::write(dir.path().join("tracked.txt"), "dirty two\n")?;
    let fp_two = tree_fingerprint_in(dir.path())?;

    assert_ne!(
        fp_one, fp_two,
        "dirty tracked content changes must invalidate freshness even when the path set is unchanged"
    );
    Ok(())
}

#[sinex_test]
async fn test_scoped_tree_fingerprint_distinguishes_dirty_content_same_path() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
    std::fs::create_dir_all(dir.path().join("crate/sinex-db/src"))?;
    std::fs::write(dir.path().join("crate/sinex-db/src/lib.rs"), "fn db() {}\n")?;
    run_git(&["add", "crate/sinex-db/src/lib.rs"], dir.path())?;
    run_git(&["commit", "-qm", "init"], dir.path())?;
    let args = vec!["-p".into(), "sinex-db".into()];

    std::fs::write(
        dir.path().join("crate/sinex-db/src/lib.rs"),
        "fn db() { let _x = 1; }\n",
    )?;
    let fp_one = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;
    std::fs::write(
        dir.path().join("crate/sinex-db/src/lib.rs"),
        "fn db() { let _x = 2; }\n",
    )?;
    let fp_two = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;

    assert_ne!(
        fp_one, fp_two,
        "scoped dirty content changes must invalidate freshness even when the path set is unchanged"
    );
    Ok(())
}

#[sinex_test]
async fn test_scoped_tree_fingerprint_distinguishes_untracked_content_same_path() -> TestResult<()>
{
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
    std::fs::create_dir_all(dir.path().join("crate/sinex-db/src"))?;
    std::fs::write(dir.path().join("crate/sinex-db/src/lib.rs"), "fn db() {}\n")?;
    run_git(&["add", "crate/sinex-db/src/lib.rs"], dir.path())?;
    run_git(&["commit", "-qm", "init"], dir.path())?;
    let args = vec!["-p".into(), "sinex-db".into()];
    let scratch = dir.path().join("crate/sinex-db/src/scratch.rs");

    std::fs::write(&scratch, "const VALUE: u8 = 1;\n")?;
    let fp_one = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;
    std::fs::write(&scratch, "const VALUE: u8 = 2;\n")?;
    let fp_two = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;

    assert_ne!(
        fp_one, fp_two,
        "scoped untracked content changes must invalidate freshness even when the path set is unchanged"
    );
    Ok(())
}

#[sinex_test]
async fn test_scoped_tree_fingerprint_includes_shared_workspace_inputs() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
    std::fs::create_dir_all(dir.path().join("crate/sinex-db/src"))?;
    std::fs::write(dir.path().join("crate/sinex-db/src/lib.rs"), "fn db() {}\n")?;
    std::fs::write(dir.path().join("Cargo.lock"), "# v1\n")?;
    run_git(
        &["add", "crate/sinex-db/src/lib.rs", "Cargo.lock"],
        dir.path(),
    )?;
    run_git(&["commit", "-qm", "init"], dir.path())?;
    let args = vec!["-p".into(), "sinex-db".into()];

    let fp_one = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;
    std::fs::write(dir.path().join("Cargo.lock"), "# v2\n")?;
    let fp_two = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;

    assert_ne!(
        fp_one, fp_two,
        "scoped package freshness must include shared workspace inputs like Cargo.lock"
    );
    Ok(())
}

#[sinex_test]
async fn test_scoped_tree_fingerprint_includes_dirty_workspace_dependencies() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    run_git(&["init", "-q"], dir.path())?;
    run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
    std::fs::write(
        dir.path().join("Cargo.toml"),
        r#"[workspace]
members = ["crate/sinex-primitives", "crate/sinex-db"]
resolver = "2"
"#,
    )?;
    std::fs::create_dir_all(dir.path().join("crate/sinex-primitives/src"))?;
    std::fs::write(
        dir.path().join("crate/sinex-primitives/Cargo.toml"),
        r#"[package]
name = "sinex-primitives"
version = "0.1.0"
edition = "2024"
"#,
    )?;
    std::fs::write(
        dir.path().join("crate/sinex-primitives/src/lib.rs"),
        "pub fn primitive() -> u8 { 1 }\n",
    )?;
    std::fs::create_dir_all(dir.path().join("crate/sinex-db/src"))?;
    std::fs::write(
        dir.path().join("crate/sinex-db/Cargo.toml"),
        r#"[package]
name = "sinex-db"
version = "0.1.0"
edition = "2024"

[dependencies]
sinex-primitives = { path = "../sinex-primitives" }
"#,
    )?;
    std::fs::write(
        dir.path().join("crate/sinex-db/src/lib.rs"),
        "pub fn db() -> u8 { sinex_primitives::primitive() }\n",
    )?;
    run_git(&["add", "."], dir.path())?;
    run_git(&["commit", "-qm", "init"], dir.path())?;
    let args = vec!["-p".into(), "sinex-db".into()];

    std::fs::write(
        dir.path().join("crate/sinex-primitives/src/lib.rs"),
        "pub fn primitive() -> u8 { 2 }\n",
    )?;
    let fp_one = scoped_tree_fingerprint_in(dir.path(), "test", &args)?;
    std::fs::write(
        dir.path().join("crate/sinex-primitives/src/lib.rs"),
        "pub fn primitive() -> u8 { 3 }\n",
    )?;
    let fp_two = scoped_tree_fingerprint_in(dir.path(), "test", &args)?;

    assert_ne!(
        fp_one, fp_two,
        "package-scoped test proofs must invalidate on dirty workspace dependencies"
    );
    Ok(())
}

#[sinex_test]
async fn test_build_release_different_scope() -> TestResult<()> {
    let args1: Vec<String> = vec![];
    let args2: Vec<String> = vec!["--release".into()];
    assert_ne!(scope_key("build", &args1), scope_key("build", &args2));
    Ok(())
}

// --- Queue and state serialization tests ---

#[sinex_test]
async fn test_queue_serialization_roundtrip() -> TestResult<()> {
    let state = CoordinationState {
        command: "check".into(),
        job_id: 42,
        pid: 1234,
        process_start_ticks: 0,
        is_foreground: false,
        tree_fingerprint: "abc123".into(),
        scope_key: "def456".into(),
        started_at: "2026-01-01T00:00:00Z".into(),
        args: vec!["-p".into(), "sinex-db".into()],
        queue: vec![
            QueuedWork {
                command: "check".into(),
                args: vec!["-p".into(), "sinexd".into()],
                is_foreground: false,
                output_format: OutputFormat::Human,
                tree_fingerprint: "queued-fp-1".into(),
                scope_key: "queued-scope-1".into(),
                reason: String::new(),
            },
            QueuedWork {
                command: "test".into(),
                args: vec!["-p".into(), "sinex-primitives".into()],
                is_foreground: true,
                output_format: OutputFormat::Json,
                tree_fingerprint: "queued-fp-2".into(),
                scope_key: "queued-scope-2".into(),
                reason: String::new(),
            },
        ],
    };

    let json = serde_json::to_string(&state)?;
    let deserialized: CoordinationState = serde_json::from_str(&json)?;
    assert_eq!(deserialized.queue.len(), 2);
    assert_eq!(deserialized.queue[0].args, vec!["-p", "sinexd"]);
    assert_eq!(deserialized.queue[1].args, vec!["-p", "sinex-primitives"]);
    assert!(deserialized.queue[1].is_foreground);
    assert_eq!(deserialized.queue[0].output_format.as_cli_str(), "human");
    assert_eq!(deserialized.queue[1].output_format.as_cli_str(), "json");
    assert_eq!(deserialized.queue[0].tree_fingerprint, "queued-fp-1");
    assert_eq!(deserialized.queue[1].scope_key, "queued-scope-2");
    Ok(())
}

#[sinex_test]
async fn test_queue_field_is_required() -> TestResult<()> {
    let json = r#"{
            "job_id": 1,
            "pid": 100,
            "is_foreground": false,
            "tree_fingerprint": "abc",
            "scope_key": "def",
            "started_at": "2026-01-01T00:00:00Z",
            "args": []
        }"#;
    let err = serde_json::from_str::<CoordinationState>(json).unwrap_err();
    assert!(
        err.to_string().contains("queue"),
        "expected missing queue error, got: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn test_queue_fifo_ordering_via_state_file() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let state_path = dir.path().join("test.state.json");

    // Create initial state with empty queue
    let state = CoordinationState {
        command: "check".into(),
        job_id: 1,
        pid: 100,
        process_start_ticks: 0,
        is_foreground: false,
        tree_fingerprint: "fp1".into(),
        scope_key: "sk1".into(),
        started_at: "2026-01-01T00:00:00Z".into(),
        args: vec![],
        queue: Vec::new(),
    };
    write_state(&state_path, &state)?;

    // Queue three items
    let mut s = read_state(&state_path)?.expect("state should exist");
    s.queue.push(QueuedWork {
        command: "check".into(),
        args: vec!["first".into()],
        is_foreground: false,
        output_format: OutputFormat::Human,
        tree_fingerprint: "fp-first".into(),
        scope_key: "scope-first".into(),
        reason: String::new(),
    });
    s.queue.push(QueuedWork {
        command: "build".into(),
        args: vec!["second".into()],
        is_foreground: false,
        output_format: OutputFormat::Json,
        tree_fingerprint: "fp-second".into(),
        scope_key: "scope-second".into(),
        reason: String::new(),
    });
    s.queue.push(QueuedWork {
        command: "vm".into(),
        args: vec!["third".into()],
        is_foreground: true,
        output_format: OutputFormat::Compact,
        tree_fingerprint: "fp-third".into(),
        scope_key: "scope-third".into(),
        reason: String::new(),
    });
    write_state(&state_path, &s)?;

    // Read back and verify FIFO order
    let s = read_state(&state_path)?.expect("state should exist");
    assert_eq!(s.queue.len(), 3);
    assert_eq!(s.queue[0].args, vec!["first"]);
    assert_eq!(s.queue[1].args, vec!["second"]);
    assert_eq!(s.queue[2].args, vec!["third"]);

    // Pop first (simulating handle_completion)
    let mut s = s;
    let popped = s.queue.remove(0);
    assert_eq!(popped.args, vec!["first"]);
    assert_eq!(popped.tree_fingerprint, "fp-first");
    assert_eq!(s.queue.len(), 2);
    assert_eq!(s.queue[0].args, vec!["second"]);
    Ok(())
}

#[sinex_test]
async fn test_handle_completion_promotes_next_queued_scope_and_fingerprint() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let state_path = tempdir.path().join("coordinator/heavy-work.state.json");
    fs::create_dir_all(state_path.parent().expect("state path parent"))?;
    write_state(
        &state_path,
        &CoordinationState {
            command: "check".into(),
            job_id: 41,
            pid: 4242,
            process_start_ticks: 0,
            is_foreground: false,
            tree_fingerprint: "running-fp".into(),
            scope_key: "running-scope".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec!["--lint".into()],
            queue: vec![
                QueuedWork {
                    command: "test".into(),
                    args: vec!["-p".into(), "sinexd".into()],
                    is_foreground: false,
                    output_format: OutputFormat::Json,
                    tree_fingerprint: "queued-fp".into(),
                    scope_key: "queued-scope".into(),
                    reason: String::new(),
                },
                QueuedWork {
                    command: "vm".into(),
                    args: vec!["-p".into(), "xtask".into()],
                    is_foreground: false,
                    output_format: OutputFormat::Human,
                    tree_fingerprint: "queued-fp-2".into(),
                    scope_key: "queued-scope-2".into(),
                    reason: String::new(),
                },
            ],
        },
    )?;

    let next = coordinator
        .handle_completion("check")?
        .expect("queued work should be promoted");
    assert_eq!(next.command, "test");
    assert_eq!(next.args, vec!["-p", "sinexd"]);
    assert_eq!(next.tree_fingerprint, "queued-fp");
    assert_eq!(next.scope_key, "queued-scope");

    let promoted = coordinator
        .state("check")?
        .expect("remaining queued state should still exist");
    assert_eq!(promoted.command, "test");
    assert_eq!(promoted.job_id, -1);
    assert_eq!(promoted.pid, 0);
    assert_eq!(promoted.args, vec!["-p", "sinexd"]);
    assert_eq!(promoted.tree_fingerprint, "queued-fp");
    assert_eq!(promoted.scope_key, "queued-scope");
    assert_eq!(promoted.queue.len(), 1);
    assert_eq!(promoted.queue[0].scope_key, "queued-scope-2");

    Ok(())
}

#[sinex_test]
async fn test_handle_completion_preserves_state_for_final_queued_job() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let state_path = tempdir.path().join("coordinator/heavy-work.state.json");
    fs::create_dir_all(state_path.parent().expect("state path parent"))?;
    write_state(
        &state_path,
        &CoordinationState {
            command: "check".into(),
            job_id: 52,
            pid: 5252,
            process_start_ticks: 0,
            is_foreground: false,
            tree_fingerprint: "running-fp".into(),
            scope_key: "running-scope".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec!["--lint".into()],
            queue: vec![QueuedWork {
                command: "build".into(),
                args: vec!["-p".into(), "sinex-primitives".into()],
                is_foreground: false,
                output_format: OutputFormat::Json,
                tree_fingerprint: "queued-fp-final".into(),
                scope_key: "queued-scope-final".into(),
                reason: String::new(),
            }],
        },
    )?;

    let next = coordinator
        .handle_completion("check")?
        .expect("final queued work should be promoted");
    assert_eq!(next.command, "build");
    assert_eq!(next.args, vec!["-p", "sinex-primitives"]);
    assert_eq!(next.tree_fingerprint, "queued-fp-final");
    assert_eq!(next.scope_key, "queued-scope-final");

    let pending = coordinator
        .state("check")?
        .expect("promoted final queued work should still hold sentinel state");
    assert_eq!(pending.command, "build");
    assert_eq!(pending.job_id, -1);
    assert_eq!(pending.pid, 0);
    assert_eq!(pending.args, vec!["-p", "sinex-primitives"]);
    assert_eq!(pending.tree_fingerprint, "queued-fp-final");
    assert_eq!(pending.scope_key, "queued-scope-final");
    assert!(pending.queue.is_empty());

    coordinator.update_state("check", 77, 7777, 0)?;

    let running = coordinator
        .state("check")?
        .expect("update_state should replace sentinel for final queued work");
    assert_eq!(running.command, "build");
    assert_eq!(running.job_id, 77);
    assert_eq!(running.pid, 7777);
    assert_eq!(running.args, vec!["-p", "sinex-primitives"]);
    assert_eq!(running.tree_fingerprint, "queued-fp-final");
    assert_eq!(running.scope_key, "queued-scope-final");
    assert!(running.queue.is_empty());

    Ok(())
}

#[sinex_test]
async fn test_cross_command_running_work_queues_instead_of_attaching() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let state_path = tempdir.path().join("coordinator/heavy-work.state.json");
    fs::create_dir_all(state_path.parent().expect("state path parent"))?;
    let running = CoordinationState {
        command: "check".into(),
        job_id: 77,
        pid: std::process::id(),
        process_start_ticks: 0,
        is_foreground: false,
        tree_fingerprint: "running-fp".into(),
        scope_key: "running-scope".into(),
        started_at: "2026-01-01T00:00:00Z".into(),
        args: vec!["-p".into(), "sinex-db".into()],
        queue: Vec::new(),
    };
    write_state(&state_path, &running)?;

    let result = coordinator.handle_running_job(
        "test",
        &["-p".into(), "xtask".into()],
        false,
        OutputFormat::Json,
        "queued-fp",
        "queued-scope",
        &running,
        &state_path,
    )?;

    assert!(matches!(
        result,
        CoordinationResult::Queued { current_job_id: 77 }
    ));

    let queued = coordinator
        .state("test")?
        .expect("queued heavy-work state should exist");
    assert_eq!(queued.queue.len(), 1);
    assert_eq!(queued.queue[0].command, "test");
    assert_eq!(queued.queue[0].scope_key, "queued-scope");
    Ok(())
}

#[sinex_test]
async fn test_is_process_alive_sentinel() -> TestResult<()> {
    assert!(!is_process_alive(0)); // Sentinel PID should always return false
    Ok(())
}

#[sinex_test]
async fn test_is_process_alive_self() -> TestResult<()> {
    // Our own process should be alive
    let pid = std::process::id();
    assert!(is_process_alive(pid));
    Ok(())
}

#[sinex_test]
async fn test_is_process_alive_nonexistent() -> TestResult<()> {
    // PID 999999999 is almost certainly not alive
    assert!(!is_process_alive(999_999_999));
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_build_package() -> TestResult<()> {
    let args: Vec<String> = vec!["-p".into(), "sinex-db".into(), "--release".into()];
    let scope = extract_scope_args("build", &args);
    assert_eq!(
        scope,
        vec![
            "--scope=packages:sinex-db".to_string(),
            "--release".to_string()
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_build_combined() -> TestResult<()> {
    let args: Vec<String> = vec!["--package=sinex-db".into()];
    let scope = extract_scope_args("build", &args);
    assert_eq!(scope, vec!["--scope=packages:sinex-db"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_test_filter() -> TestResult<()> {
    let args: Vec<String> = vec![
        "-E".into(),
        "test(my_test)".into(),
        "-p".into(),
        "xtask".into(),
    ];
    let scope = extract_scope_args("test", &args);
    assert_eq!(
        scope,
        vec![
            "--scope=packages:xtask".to_string(),
            "--filter=test(my_test)".to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_ignores_non_scope() -> TestResult<()> {
    let args: Vec<String> = vec![
        "-p".into(),
        "sinex-db".into(),
        "--fail-fast".into(),
        "--skip-preflight".into(),
        "--prime".into(),
    ];
    let scope = extract_scope_args("test", &args);
    assert_eq!(
        scope,
        vec![
            "--scope=packages:sinex-db".to_string(),
            "--prime".to_string(),
        ]
    );
    assert!(!scope.contains(&"--fail-fast".to_string()));
    assert!(!scope.contains(&"--skip-preflight".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_check_package() -> TestResult<()> {
    // -p is scope-relevant for check
    let args: Vec<String> = vec![
        "-p".into(),
        "sinex-db".into(),
        "--lint".into(),
        "--fmt".into(),
        "--forbidden".into(),
    ];
    let scope = extract_scope_args("check", &args);
    assert_eq!(
        scope,
        vec![
            "--scope=packages:sinex-db".to_string(),
            "--lint".to_string(),
            "--fmt".to_string(),
            "--forbidden".to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_check_all_flag() -> TestResult<()> {
    let args: Vec<String> = vec!["--all".into(), "--lint".into()];
    let scope = extract_scope_args("check", &args);
    assert_eq!(scope, vec!["--all", "--lint"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_check_lint_only_empty() -> TestResult<()> {
    // Lint/fmt/forbidden are proof-mode selectors even without package scope.
    let args: Vec<String> = vec!["--fmt".into(), "--lint".into(), "--forbidden".into()];
    let scope = extract_scope_args("check", &args);
    assert_eq!(scope, vec!["--fmt", "--lint", "--forbidden"]);
    Ok(())
}

#[sinex_test]
async fn test_state_write_read_roundtrip() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.json");

    let state = CoordinationState {
        command: "check".into(),
        job_id: 42,
        pid: 1234,
        process_start_ticks: 0,
        is_foreground: true,
        tree_fingerprint: "abc".into(),
        scope_key: "def".into(),
        started_at: "2026-01-01T00:00:00Z".into(),
        args: vec!["-p".into(), "foo".into()],
        queue: vec![QueuedWork {
            command: "test".into(),
            args: vec!["bar".into()],
            is_foreground: false,
            output_format: OutputFormat::Human,
            tree_fingerprint: "queued-fp".into(),
            scope_key: "queued-scope".into(),
            reason: String::new(),
        }],
    };

    write_state(&path, &state)?;
    let loaded = read_state(&path)?.expect("state should exist");

    assert_eq!(loaded.job_id, 42);
    assert_eq!(loaded.pid, 1234);
    assert!(loaded.is_foreground);
    assert_eq!(loaded.queue.len(), 1);
    assert_eq!(loaded.queue[0].args, vec!["bar"]);
    assert_eq!(loaded.queue[0].tree_fingerprint, "queued-fp");
    assert_eq!(loaded.queue[0].scope_key, "queued-scope");
    Ok(())
}

#[sinex_test]
async fn test_read_state_missing_file() -> TestResult<()> {
    let result = read_state(std::path::Path::new("/nonexistent/path/state.json"))?;
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_clear_pending_state_removes_sentinel_reservation() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let state_path = tempdir.path().join("coordinator/heavy-work.state.json");
    fs::create_dir_all(state_path.parent().expect("state path parent"))?;
    write_state(
        &state_path,
        &CoordinationState {
            command: "check".into(),
            job_id: -1,
            pid: 0,
            process_start_ticks: 0,
            is_foreground: false,
            tree_fingerprint: "old".into(),
            scope_key: "scope".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec![],
            queue: Vec::new(),
        },
    )?;

    assert!(coordinator.clear_pending_state("check")?);
    assert!(!state_path.exists());
    Ok(())
}

#[sinex_test]
async fn test_clear_pending_state_keeps_live_state() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let state_path = tempdir.path().join("coordinator/heavy-work.state.json");
    fs::create_dir_all(state_path.parent().expect("state path parent"))?;
    write_state(
        &state_path,
        &CoordinationState {
            command: "check".into(),
            job_id: 41,
            pid: 4242,
            process_start_ticks: 0,
            is_foreground: false,
            tree_fingerprint: "old".into(),
            scope_key: "scope".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec![],
            queue: Vec::new(),
        },
    )?;

    assert!(!coordinator.clear_pending_state("check")?);
    assert!(state_path.exists());
    Ok(())
}

#[sinex_test]
async fn test_update_coordinator_state_clears_pending_reservation_when_pid_missing()
-> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let state_path = tempdir.path().join("coordinator/heavy-work.state.json");
    fs::create_dir_all(state_path.parent().expect("state path parent"))?;
    write_state(
        &state_path,
        &CoordinationState {
            command: "check".into(),
            job_id: -1,
            pid: 0,
            process_start_ticks: 0,
            is_foreground: false,
            tree_fingerprint: "old".into(),
            scope_key: "scope".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec![],
            queue: Vec::new(),
        },
    )?;

    let bg_result = CommandResult::success().with_data(serde_json::json!({
        "job_id": 41,
    }));
    let error = update_coordinator_state("check", &bg_result)
        .expect_err("missing pid must surface as a spawn recording failure");
    let message = format!("{error:#}");
    assert!(message.contains("background spawn returned no pid"));
    assert!(message.contains("cleared_pending_state=true"));

    assert!(coordinator.state("check")?.is_none());
    Ok(())
}

#[sinex_test]
async fn test_mark_cancelled_finishes_background_job_and_invocation() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let _history_guard = env_set_path("XTASK_HISTORY_DB", &db_path);
    let db = crate::history::HistoryDb::open(&db_path)?;
    let stdout_path = dir.path().join("stdout.log");
    let stderr_path = dir.path().join("stderr.log");
    let (invocation_id, job_id) =
        db.start_background_job("check", &[], Some(42_424), &stdout_path, &stderr_path)?;
    drop(db);

    mark_cancelled(job_id)?;

    let db = crate::history::HistoryDb::open(&db_path)?;
    let invocation = db.get_invocation_full(invocation_id)?.ok_or_else(|| {
        color_eyre::eyre::eyre!("missing invocation after supersede cancellation")
    })?;
    assert_eq!(invocation.invocation.status, InvocationStatus::Cancelled);
    assert!(invocation.invocation.finished_at.is_some());
    assert_eq!(
        db.get_invocation_cancel_metadata(invocation_id)?,
        Some((Some("superseded".into()), Some("coordinator".into())))
    );

    let job = db.get_background_job_by_id(job_id)?.ok_or_else(|| {
        color_eyre::eyre::eyre!("missing background job after supersede cancellation")
    })?;
    assert!(matches!(job.job_status, JobLifecycleStatus::Killed));
    Ok(())
}

#[sinex_test]
async fn test_mark_cancelled_surfaces_missing_background_job() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let _history_guard = env_set_path("XTASK_HISTORY_DB", &db_path);
    let _db = crate::history::HistoryDb::open(&db_path)?;

    let error = mark_cancelled(999).expect_err("missing background job must be surfaced");
    let message = format!("{error:#}");
    assert!(message.contains("background job 999 missing"));
    Ok(())
}

#[sinex_test]
async fn test_read_state_corrupt_json() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.json");
    fs::write(&path, "not json at all {{{")?;
    let error = read_state(&path).expect_err("corrupt coordinator state must surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to parse coordinator state"));
    assert!(message.contains(path.display().to_string().as_str()));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_state_surfaces_unreadable_state_path() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let state_path = tempdir.path().join("coordinator/heavy-work.state.json");
    fs::create_dir_all(&state_path)?;

    let error = coordinator
        .state("check")
        .expect_err("directory state path must surface as unreadable");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read coordinator state"));
    assert!(message.contains(state_path.display().to_string().as_str()));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_request_surfaces_stale_state_cleanup_failures() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let _state_guard = env_set_path("SINEX_STATE_DIR", tempdir.path());
    let coordinator = JobCoordinator::new()?;
    let coordinator_dir = tempdir.path().join("coordinator");
    let state_path = coordinator_dir.join("heavy-work.state.json");
    let lock_path = coordinator_dir.join("heavy-work.lock");

    fs::write(&lock_path, [])?;
    write_state(
        &state_path,
        &CoordinationState {
            command: "check".into(),
            job_id: 41,
            pid: 999_999_999,
            process_start_ticks: 0,
            is_foreground: false,
            tree_fingerprint: "old".into(),
            scope_key: "old".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec![],
            queue: Vec::new(),
        },
    )?;

    let original_mode = fs::metadata(&coordinator_dir)?.permissions().mode();
    let mut read_only = fs::metadata(&coordinator_dir)?.permissions();
    read_only.set_mode(0o555);
    fs::set_permissions(&coordinator_dir, read_only)?;

    let result = coordinator.request_with_format("check", &[], &[], false, OutputFormat::Human);

    let mut restore = fs::metadata(&coordinator_dir)?.permissions();
    restore.set_mode(original_mode);
    fs::set_permissions(&coordinator_dir, restore)?;

    let error = result.expect_err("stale state cleanup failure must surface");
    let message = format!("{error:#}");
    assert!(message.contains("remove stale coordinator state before restart"));
    assert!(message.contains(state_path.display().to_string().as_str()));
    Ok(())
}

#[sinex_test]
async fn test_cancel_process_sentinel_noop() -> TestResult<()> {
    // cancel_process(0, _) should be a no-op (sentinel PID)
    cancel_process(0, 0); // Should not panic
    Ok(())
}

// PID reuse detection tests (#1141)
//
// These tests validate that cancel_process does not kill unrelated processes
// whose PID matches a stale coordinator state. The kernel recycles PIDs, and
// `kill(pid, 0)` alone cannot distinguish "our process" from "a new process
// that got the same PID."

#[sinex_test]
async fn test_cancel_process_skips_wrong_start_ticks() -> TestResult<()> {
    // Spawn an innocent long-running process.
    let mut child = std::process::Command::new("sleep").arg("10").spawn()?;
    let pid = child.id();

    // Read its actual start_ticks from /proc.
    let actual = crate::process::read_proc_sample(pid)
        .expect("should be able to read /proc/{pid}/stat for spawned child");
    let wrong_ticks = actual.start_ticks.wrapping_add(1000);

    // Call cancel_process with WRONG start_ticks — must NOT kill.
    cancel_process(pid, wrong_ticks);

    // The sleep process must still be alive.
    assert!(
        is_process_alive(pid),
        "cancel_process with wrong start_ticks must not kill the process \
             (PID reuse protection failed — innocent process was killed)"
    );

    // Now call cancel_process with CORRECT start_ticks — should kill.
    cancel_process(pid, actual.start_ticks);

    // Reap the zombie — kill(pid, 0) returns success for zombies that
    // haven't been waited on, so is_process_alive would be a false positive.
    let _ = child.wait();

    assert!(
        !is_process_alive(pid),
        "cancel_process with correct start_ticks should kill the process"
    );

    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

#[sinex_test]
async fn test_cancel_process_with_sentinel_start_ticks_does_kill() -> TestResult<()> {
    // start_ticks=0 is the sentinel: "not captured, pre-existing state."
    // In this case cancel_process must still deliver signals (backward
    // compatible with state files written before the #1141 fix).
    let mut child = std::process::Command::new("sleep").arg("10").spawn()?;
    let pid = child.id();

    cancel_process(pid, 0);

    // Reap before checking — zombies register as alive via kill(pid, 0).
    let _ = child.wait();
    assert!(
        !is_process_alive(pid),
        "cancel_process with sentinel start_ticks=0 must still deliver signals \
             (backward compatibility with pre-#1141 state files)"
    );

    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

#[sinex_test]
async fn test_process_identity_valid_rejects_stolen_pid() -> TestResult<()> {
    let mut child = std::process::Command::new("sleep").arg("10").spawn()?;
    let pid = child.id();
    let actual = crate::process::read_proc_sample(pid).unwrap();

    // A real process with matching start_ticks should validate.
    assert!(
        process_identity_valid(pid, actual.start_ticks),
        "same start_ticks should validate"
    );

    // Wrong start_ticks should be rejected.
    assert!(
        !process_identity_valid(pid, actual.start_ticks.wrapping_add(500)),
        "different start_ticks should not validate (PID reused)"
    );

    // Sentinel 0 should pass through.
    assert!(
        process_identity_valid(pid, 0),
        "sentinel start_ticks=0 should validate (backward compat)"
    );

    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

#[sinex_test]
async fn test_coordination_state_serializes_process_start_ticks() -> TestResult<()> {
    // Verify the new field serializes and deserializes correctly,
    // including backward-compatible reading of old state files.
    let state = CoordinationState {
        command: "check".to_string(),
        job_id: 42,
        pid: 12345,
        process_start_ticks: 9876543210,
        is_foreground: false,
        tree_fingerprint: "fp".to_string(),
        scope_key: "scope".to_string(),
        started_at: "now".to_string(),
        args: vec![],
        queue: vec![],
    };

    let json = serde_json::to_string(&state)?;
    let roundtripped: CoordinationState = serde_json::from_str(&json)?;
    assert_eq!(roundtripped.process_start_ticks, 9876543210);

    // Old state files (without process_start_ticks) must deserialize as 0.
    let old_json = r#"{"command":"check","job_id":1,"pid":999,"is_foreground":false,"tree_fingerprint":"fp","scope_key":"scope","started_at":"t","args":[],"queue":[]}"#;
    let old_state: CoordinationState = serde_json::from_str(old_json)?;
    assert_eq!(
        old_state.process_start_ticks, 0,
        "old state files without process_start_ticks must deserialize as 0"
    );

    Ok(())
}

// --- coordination_to_result mapping tests ---

fn json_ctx() -> CommandContext {
    CommandContext::new(
        crate::output::OutputWriter::new(crate::output::OutputFormat::Json),
        false,
        None,
        "coordinator",
    )
}

#[sinex_test]
async fn test_coordination_to_result_fresh() -> TestResult<()> {
    let ctx = json_ctx();
    let result = coordination_fresh_result(
        42,
        "success",
        3.5,
        &ctx,
        FreshPackagesProbe {
            packages: vec!["sinex-db".into(), "xtask".into()],
            issue: None,
        },
    );

    assert!(result.is_success());
    let data = result.data.as_ref().expect("should have data");
    assert_eq!(data["action"], "fresh");
    assert_eq!(data["invocation_id"], 42);
    assert_eq!(data["job_id"], serde_json::Value::Null);
    assert_eq!(data["cached_status"], "success");
    assert_eq!(data["cached_duration_secs"], 3.5);
    assert_eq!(
        data["compiled_packages"],
        serde_json::json!(["sinex-db", "xtask"])
    );
    assert_eq!(data["compiled_packages_issue"], serde_json::Value::Null);
    Ok(())
}

#[sinex_test]
async fn test_coordination_fresh_result_surfaces_compiled_package_probe_errors() -> TestResult<()> {
    let ctx = json_ctx();
    let result = coordination_fresh_result(
        42,
        "success",
        3.5,
        &ctx,
        FreshPackagesProbe {
            packages: Vec::new(),
            issue: Some("probe exploded".into()),
        },
    );

    assert!(result.is_success());
    assert_eq!(result.warnings, vec!["probe exploded".to_string()]);
    let data = result.data.as_ref().expect("should have data");
    assert_eq!(data["compiled_packages"], serde_json::json!([]));
    assert_eq!(data["compiled_packages_issue"], "probe exploded");
    Ok(())
}

#[sinex_test]
async fn test_fresh_packages_probe_from_result_reports_errors() -> TestResult<()> {
    let db_path = std::path::Path::new("/tmp/test-history.db");
    let probe = fresh_packages_probe_from_result(
        7,
        db_path,
        Err(color_eyre::eyre::eyre!("history exploded")),
    );
    assert!(probe.packages.is_empty());
    let issue = probe.issue.expect("probe failure should surface");
    assert!(issue.contains("failed to load compiled packages for fresh invocation 7"));
    assert!(issue.contains("/tmp/test-history.db"));
    assert!(issue.contains("history exploded"));
    Ok(())
}

#[sinex_test]
async fn test_coordination_to_result_attached() -> TestResult<()> {
    let ctx = json_ctx();
    let coord = CoordinationResult::Attached { job_id: 99 };
    let result = coordination_to_result(&coord, &ctx);

    assert!(result.is_success());
    let data = result.data.as_ref().expect("should have data");
    assert_eq!(data["action"], "attached");
    assert_eq!(data["job_id"], 99);
    assert!(data["hint"].as_str().unwrap().contains("99"));
    Ok(())
}

#[sinex_test]
async fn test_coordination_to_result_superseded() -> TestResult<()> {
    let ctx = json_ctx();
    let coord = CoordinationResult::Superseded {
        old_job_id: 10,
        new_job_id: 20,
    };
    let result = coordination_to_result(&coord, &ctx);

    assert!(result.is_success());
    let data = result.data.as_ref().expect("should have data");
    assert_eq!(data["action"], "superseded");
    assert_eq!(data["old_job_id"], 10);
    assert_eq!(data["new_job_id"], 20);
    Ok(())
}

#[sinex_test]
async fn test_coordination_to_result_queued() -> TestResult<()> {
    let ctx = json_ctx();
    let coord = CoordinationResult::Queued { current_job_id: 55 };
    let result = coordination_to_result(&coord, &ctx);

    assert!(result.is_success());
    let data = result.data.as_ref().expect("should have data");
    assert_eq!(data["action"], "queued");
    assert_eq!(data["current_job_id"], 55);
    assert_eq!(data["hint"], "Monitor with: xtask jobs status 55");
    Ok(())
}

#[sinex_test]
async fn test_coordination_to_result_queued_pending_assignment() -> TestResult<()> {
    let ctx = json_ctx();
    let coord = CoordinationResult::Queued { current_job_id: -1 };
    let result = coordination_to_result(&coord, &ctx);

    assert!(result.is_success());
    assert_eq!(
        result.message.as_deref(),
        Some("Queued behind an active coordinated slot awaiting job assignment")
    );
    let data = result.data.as_ref().expect("should have data");
    assert_eq!(data["action"], "queued");
    assert_eq!(data["current_job_id"], serde_json::Value::Null);
    assert_eq!(data["current_job_pending_assignment"], true);
    assert_eq!(data["hint"], "Monitor with: xtask jobs active");
    Ok(())
}

#[sinex_test]
async fn test_coordination_to_result_started() -> TestResult<()> {
    let ctx = json_ctx();
    let coord = CoordinationResult::Started { job_id: -1 };
    let result = coordination_to_result(&coord, &ctx);

    assert!(result.is_success());
    let data = result.data.as_ref().expect("should have data");
    assert_eq!(data["action"], "started");
    assert_eq!(data["job_id"], -1);
    Ok(())
}

// --- extract_scope_args edge cases ---

#[sinex_test]
async fn test_extract_scope_args_build_short_combined() -> TestResult<()> {
    // -psinex-db (no space) canonicalizes to the package scope marker.
    let args: Vec<String> = vec!["-psinex-db".into()];
    let scope = extract_scope_args("build", &args);
    assert_eq!(scope, vec!["--scope=packages:sinex-db"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_test_combined_filter() -> TestResult<()> {
    // -Etest(my_test) (no space) canonicalizes to the long filter form.
    let args: Vec<String> = vec!["-Etest(my_test)".into()];
    let scope = extract_scope_args("test", &args);
    assert_eq!(scope, vec!["--filter=test(my_test)"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_test_heavy_flag() -> TestResult<()> {
    let args: Vec<String> = vec!["--heavy".into(), "-p".into(), "sinex-db".into()];
    let scope = extract_scope_args("test", &args);
    assert_eq!(
        scope,
        vec![
            "--scope=packages:sinex-db".to_string(),
            "--heavy".to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_unknown_command() -> TestResult<()> {
    // Unknown commands should return empty scope
    let args: Vec<String> = vec!["-p".into(), "sinex-db".into(), "--release".into()];
    let scope = extract_scope_args("status", &args);
    assert!(scope.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_extract_scope_args_build_all_flag() -> TestResult<()> {
    let args: Vec<String> = vec!["--all".into()];
    let scope = extract_scope_args("build", &args);
    assert!(scope.contains(&"--all".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_coordination_result_serde_roundtrip() -> TestResult<()> {
    let variants = vec![
        CoordinationResult::Started { job_id: 1 },
        CoordinationResult::Attached { job_id: 2 },
        CoordinationResult::Fresh {
            invocation_id: 3,
            status: "success".into(),
            duration_secs: 1.5,
        },
        CoordinationResult::Superseded {
            old_job_id: 4,
            new_job_id: 5,
        },
        CoordinationResult::Queued { current_job_id: 6 },
    ];

    for variant in &variants {
        let json = serde_json::to_string(variant)?;
        let deserialized: CoordinationResult = serde_json::from_str(&json)?;
        // Re-serialize and compare JSON strings for equality
        let json2 = serde_json::to_string(&deserialized)?;
        assert_eq!(json, json2, "Roundtrip failed for: {json}");
    }
    Ok(())
}

#[sinex_test]
async fn test_should_coordinate_test_list_flag() -> TestResult<()> {
    // --list and -l should both exclude coordination
    assert!(!JobCoordinator::should_coordinate(
        "test",
        &["--list".into()]
    ));
    assert!(!JobCoordinator::should_coordinate("test", &["-l".into()]));
    Ok(())
}

#[sinex_test]
async fn test_should_coordinate_test_dry_run() -> TestResult<()> {
    assert!(!JobCoordinator::should_coordinate(
        "test",
        &["--dry-run".into()]
    ));
    let explanation = explain_freshness("test", &["--dry-run".into()])?;
    assert!(!explanation.should_coordinate);
    assert!(!explanation.fresh_reuse_enabled);
    assert_eq!(explanation.proof_kind, "test.nextest.plan");
    Ok(())
}

// --- R1: Per-package fingerprinting ---

#[sinex_test]
async fn test_extract_explicit_packages_p_flag() -> TestResult<()> {
    let args = vec!["-p".into(), "sinex-db".into()];
    let pkgs = extract_explicit_packages("check", &args);
    assert_eq!(pkgs, vec!["sinex-db"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_explicit_packages_long_flag() -> TestResult<()> {
    let args = vec!["--package".into(), "sinexd".into()];
    let pkgs = extract_explicit_packages("check", &args);
    assert_eq!(pkgs, vec!["sinexd"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_explicit_packages_equals_form() -> TestResult<()> {
    let args = vec!["--package=sinex-primitives".into()];
    let pkgs = extract_explicit_packages("check", &args);
    assert_eq!(pkgs, vec!["sinex-primitives"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_explicit_packages_multiple() -> TestResult<()> {
    let args = vec!["-p".into(), "sinex-db".into(), "-p".into(), "sinexd".into()];
    let pkgs = extract_explicit_packages("check", &args);
    assert_eq!(pkgs.len(), 2);
    assert!(pkgs.contains(&"sinex-db".to_string()));
    assert!(pkgs.contains(&"sinexd".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_extract_explicit_packages_none() -> TestResult<()> {
    // No -p flag: returns empty (will use workspace fingerprint)
    let args: Vec<String> = vec!["--lint".into(), "--all".into()];
    let pkgs = extract_explicit_packages("check", &args);
    assert!(pkgs.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_extract_explicit_packages_unknown_command() -> TestResult<()> {
    // Non-coordinated commands return empty.
    let args = vec!["-p".into(), "sinex-db".into()];
    let pkgs = extract_explicit_packages("doctor", &args);
    assert!(pkgs.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_package_to_path_well_known() -> TestResult<()> {
    assert_eq!(package_to_path("sinexctl"), "crate/sinexctl/");
    assert_eq!(package_to_path("xtask"), "xtask/");
    assert_eq!(package_to_path("sinex-e2e-tests"), "tests/e2e/");
    assert_eq!(package_to_path("sinex-workspace-tests"), "tests/workspace/");
    assert_eq!(package_to_path("sinex-vm-test-suite"), "tests/vm-suite/");
    Ok(())
}

#[sinex_test]
async fn test_package_to_path_known_crate() -> TestResult<()> {
    // sinex-primitives should resolve to crate/sinex-primitives/
    let path = package_to_path("sinex-primitives");
    assert!(
        path.starts_with("crate/"),
        "expected crate/ prefix, got: {path}"
    );
    Ok(())
}

#[sinex_test]
async fn test_package_to_path_unknown_falls_back() -> TestResult<()> {
    // Unknown package should fall back to "crate/" (broad, safe)
    let path = package_to_path("nonexistent-package-xyz");
    assert_eq!(path, "crate/");
    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// Property tests — scope_key invariants
// ────────────────────────────────────────────────────────────────────────

use crate::sandbox::sinex_proptest;
use proptest::prelude::*;

sinex_proptest! {
    /// scope_key is deterministic: identical inputs always produce the same hash.
    ///
    /// This is the foundational invariant — the coordinator's dedup logic
    /// relies on the same work always producing the same scope key so that
    /// concurrent agents attach to the same running job rather than spawning
    /// duplicates.
    fn prop_scope_key_is_deterministic(
        pkg in "[a-z][a-z0-9-]{2,15}"
    ) -> TestResult<()> {
        let args: Vec<String> = vec!["-p".to_string(), pkg];
        prop_assert_eq!(scope_key("check", &args), scope_key("check", &args));
        Ok(())
    }

    /// Output/background flags do not change the scope key.
    ///
    /// Flags like --bg and --json change command plumbing, not proof identity.
    /// Verification-mode flags such as --lint and --fmt are intentionally
    /// excluded because they prove a different surface than plain check.
    fn prop_scope_key_ignores_non_scope_flags(
        pkg in "[a-z][a-z0-9-]{2,15}",
        extra in prop_oneof![
            Just("--bg"),
            Just("--json"),
        ]
    ) -> TestResult<()> {
        let base: Vec<String> = vec!["-p".to_string(), pkg.clone()];
        let with_flag = {
            let mut v = base.clone();
            v.push(extra.to_string());
            v
        };
        prop_assert_eq!(
            scope_key("check", &base),
            scope_key("check", &with_flag),
            "non-scope flag '{}' must not change the scope key", extra
        );
        Ok(())
    }

    /// Distinct package names (non-overlapping lengths) produce distinct scope keys.
    ///
    /// Uses length-partitioned strategies — pkg_a is 3–9 chars, pkg_b is 10–15
    /// chars — so they can never be equal, avoiding prop_assume rejection while
    /// still exercising SHA256 collision resistance on distinct inputs.
    fn prop_scope_key_distinct_packages_differ(
        pkg_a in "[a-z][a-z0-9]{2,8}",
        pkg_b in "[a-z][a-z0-9]{9,14}"
    ) -> TestResult<()> {
        let ka = scope_key("check", &["-p".to_string(), pkg_a]);
        let kb = scope_key("check", &["-p".to_string(), pkg_b]);
        prop_assert_ne!(ka, kb, "distinct packages must produce distinct scope keys");
        Ok(())
    }

    /// --all scope key differs from any -p scoped key.
    ///
    /// A workspace-wide check (--all) and a package-scoped check (-p foo)
    /// are genuinely different work units. The coordinator must never attach
    /// an --all job to a -p job or vice versa.
    fn prop_scope_key_all_differs_from_scoped(
        pkg in "[a-z][a-z0-9]{2,8}"
    ) -> TestResult<()> {
        let scoped   = vec!["-p".to_string(), pkg];
        let all_args = vec!["--all".to_string()];
        prop_assert_ne!(
            scope_key("check", &scoped),
            scope_key("check", &all_args),
            "--all scope key must differ from package-scoped key"
        );
        Ok(())
    }
}
