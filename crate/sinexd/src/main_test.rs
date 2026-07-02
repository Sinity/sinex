use super::{Cli, Command};
use clap::Parser;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn package_completeness_accepts_authoring_aliases() -> xtask::sandbox::TestResult<()> {
    let cli = Cli::try_parse_from([
        "sinexd",
        "export-package-completeness",
        "--package",
        "terminal.atuin-history",
        "--mode",
        "terminal.atuin-history",
        "--strict",
    ])
    .expect("package completeness authoring aliases should parse");

    let Some(Command::ExportPackageCompleteness {
        package_id,
        mode_id,
        strict,
        ..
    }) = cli.command
    else {
        panic!("expected export-package-completeness command");
    };

    assert_eq!(package_id.as_deref(), Some("terminal.atuin-history"));
    assert_eq!(mode_id.as_deref(), Some("terminal.atuin-history"));
    assert!(strict);
    Ok(())
}

#[sinex_test]
async fn source_skeleton_accepts_package_and_mode_aliases() -> xtask::sandbox::TestResult<()> {
    let cli = Cli::try_parse_from([
        "sinexd",
        "export-source-skeleton",
        "--package",
        "terminal.atuin-history",
        "--mode",
        "terminal.atuin-history",
    ])
    .expect("source skeleton authoring aliases should parse");

    let Some(Command::ExportSourceSkeleton {
        package_id,
        mode_id,
        ..
    }) = cli.command
    else {
        panic!("expected export-source-skeleton command");
    };

    assert_eq!(package_id, "terminal.atuin-history");
    assert_eq!(mode_id, "terminal.atuin-history");
    Ok(())
}
