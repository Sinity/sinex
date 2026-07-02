use super::*;
use crate::sandbox::sinex_test;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;

#[sinex_test]
async fn should_exclude_root_local_artifacts() -> ::xtask::sandbox::TestResult<()> {
    for relative in [
        Path::new(".git/config"),
        Path::new(".sinex/run/.s.PGSQL.5432"),
        Path::new("crate/sinex-macros/.sinex/trybuild-target/tests/trybuild/foo/Cargo.toml"),
        Path::new("crate/sinex-primitives/target/debug/deps/libfoo.rmeta"),
        Path::new(".direnv/flake-input"),
        Path::new(".devenv/state"),
        Path::new("target/debug/xtask"),
        Path::new("result"),
        Path::new("result-bin"),
    ] {
        assert!(
            should_exclude(relative),
            "expected {} to be excluded",
            relative.display()
        );
    }

    for relative in [
        Path::new("Cargo.toml"),
        Path::new("crate/sinexd-extra/Cargo.toml"),
        Path::new("docs/README.md"),
    ] {
        assert!(
            !should_exclude(relative),
            "expected {} to stay included",
            relative.display()
        );
    }
    Ok(())
}

#[sinex_test]
async fn stage_checkout_for_flake_filters_runtime_state_and_keeps_new_sources() -> Result<()> {
    let source = tempfile::tempdir()?;
    let output = tempfile::tempdir()?;

    fs::write(source.path().join("flake.nix"), "{ }\n")?;
    fs::create_dir_all(source.path().join("crate/sinexd/src/sources"))?;
    fs::write(
        source.path().join("crate/sinexd/src/sources/new_source.rs"),
        "// source fixture\n",
    )?;

    fs::create_dir_all(source.path().join(".sinex/run"))?;
    let socket_path = source.path().join(".sinex/run/.s.PGSQL.5432");
    let _listener = UnixListener::bind(&socket_path)?;

    let nested_trybuild_manifest = source
        .path()
        .join("crate/sinex-macros/.sinex/trybuild-target/tests/trybuild/sinex-macros/Cargo.toml");
    fs::create_dir_all(
        nested_trybuild_manifest
            .parent()
            .expect("nested trybuild manifest parent should exist"),
    )?;
    fs::write(
        &nested_trybuild_manifest,
        "[package]\nname = \"trybuild-generated\"\nversion = \"0.1.0\"\n",
    )?;

    fs::create_dir_all(source.path().join("target/debug"))?;
    fs::write(source.path().join("target/debug/xtask"), "binary")?;
    symlink("flake.nix", source.path().join("result"))?;

    let report = stage_checkout_for_flake(source.path(), Some(output.path()), true)?;

    assert!(
        output.path().join("flake.nix").is_file(),
        "top-level source file must be copied"
    );
    assert!(
        output
            .path()
            .join("crate/sinexd/src/sources/new_source.rs")
            .is_file(),
        "new source files must survive staging"
    );
    assert!(
        !output.path().join(".sinex").exists(),
        "runtime state directory must be excluded"
    );
    assert!(
        !output.path().join("crate/sinex-macros/.sinex").exists(),
        "nested crate-local runtime state must be excluded"
    );
    assert!(
        !output.path().join("target").exists(),
        "build output must be excluded"
    );
    assert!(
        !output.path().join("result").exists(),
        "checkout-local result symlink must be excluded"
    );
    assert!(
        report
            .excluded_paths
            .iter()
            .any(|path| path == ".sinex" || path.starts_with(".sinex/")),
        "excluded paths should mention runtime state"
    );
    assert!(
        report.excluded_paths.iter().any(|path| {
            path == "crate/sinex-macros/.sinex" || path.starts_with("crate/sinex-macros/.sinex/")
        }),
        "excluded paths should mention nested crate-local runtime state"
    );
    assert!(
        report.flake_uri.starts_with("path:"),
        "staged report should include a flake path URI"
    );

    Ok(())
}

#[sinex_test]
async fn stage_checkout_for_flake_preserves_regular_symlinks() -> Result<()> {
    let source = tempfile::tempdir()?;
    let output = tempfile::tempdir()?;

    fs::create_dir_all(source.path().join("docs"))?;
    fs::write(source.path().join("docs/README.md"), "hello")?;
    symlink("docs/README.md", source.path().join("docs-link"))?;

    let report = stage_checkout_for_flake(source.path(), Some(output.path()), true)?;

    let staged_link = output.path().join("docs-link");
    assert!(fs::symlink_metadata(&staged_link)?.file_type().is_symlink());
    assert_eq!(
        fs::read_link(&staged_link)?,
        PathBuf::from("docs/README.md")
    );
    assert_eq!(report.copied_symlinks, 1);

    Ok(())
}
