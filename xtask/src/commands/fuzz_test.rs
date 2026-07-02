use super::*;
use crate::sandbox::sinex_test;
use std::ffi::OsString;

#[sinex_test]
async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = FuzzCommand {
        subcommand: FuzzSubcommand::List,
    };
    assert_eq!(cmd.name(), "fuzz");
    Ok(())
}

#[sinex_test]
async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = FuzzCommand {
        subcommand: FuzzSubcommand::Run {
            target: "test::target".to_string(),
            max_time: 60,
            jobs: None,
        },
    };
    let metadata = cmd.metadata();
    assert_eq!(metadata.category, Some("security"));
    assert!(metadata.timeout.is_some());
    assert!(!metadata.modifies_state);
    Ok(())
}

#[sinex_test]
async fn test_init_modifies_state() -> ::xtask::sandbox::TestResult<()> {
    let cmd = FuzzCommand {
        subcommand: FuzzSubcommand::Init {
            package: "test".to_string(),
        },
    };
    let metadata = cmd.metadata();
    assert!(metadata.modifies_state);
    Ok(())
}

#[sinex_test]
async fn test_list_command() -> ::xtask::sandbox::TestResult<()> {
    let cmd = FuzzCommand {
        subcommand: FuzzSubcommand::List,
    };
    let ctx = crate::command::CommandContext::new(
        crate::output::OutputWriter::new(crate::output::OutputFormat::Silent),
        false,
        None,
        "fuzz",
    );

    // Should not panic even if no fuzz targets exist
    let result = cmd.execute(&ctx).await;
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_invalid_target_format() -> ::xtask::sandbox::TestResult<()> {
    let cmd = FuzzCommand {
        subcommand: FuzzSubcommand::Run {
            target: "invalid_format".to_string(),
            max_time: 60,
            jobs: None,
        },
    };
    let ctx = crate::command::CommandContext::new(
        crate::output::OutputWriter::new(crate::output::OutputFormat::Silent),
        false,
        None,
        "fuzz",
    );

    let result = cmd.execute(&ctx).await?;
    assert!(result.is_failure());
    assert_eq!(result.errors[0].code, "INVALID_TARGET_FORMAT");
    Ok(())
}

#[sinex_test]
async fn test_parse_fuzz_manifest_extracts_fuzz_bins() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let manifest = dir.path().join("Cargo.toml");
    fs::write(
        &manifest,
        r#"[package]
name = "demo-fuzz"

[[bin]]
name = "fuzz_input_validation"

[[bin]]
name = "helper"
"#,
    )?;

    let targets = parse_fuzz_manifest(&manifest)?;
    assert_eq!(
        targets,
        vec![("demo-fuzz".to_string(), "fuzz_input_validation".to_string())]
    );
    Ok(())
}

#[sinex_test]
async fn test_find_fuzz_dir_resolves_manifest_package_name() -> ::xtask::sandbox::TestResult<()>
{
    let dir = tempfile::tempdir()?;
    let fuzz_dir = dir.path().join("fuzz");
    fs::create_dir_all(&fuzz_dir)?;
    let manifest = fuzz_dir.join("Cargo.toml");
    fs::write(
        &manifest,
        r#"[package]
name = "demo-fuzz"

[[bin]]
name = "fuzz_input_validation"
"#,
    )?;

    let resolved = find_fuzz_dir_for_package_in_manifests("demo-fuzz", [manifest.clone()])?;
    assert_eq!(resolved, fuzz_dir);
    Ok(())
}

#[sinex_test]
async fn test_find_fuzz_dir_rejects_unknown_manifest_package()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let fuzz_dir = dir.path().join("fuzz");
    fs::create_dir_all(&fuzz_dir)?;
    let manifest = fuzz_dir.join("Cargo.toml");
    fs::write(
        &manifest,
        r#"[package]
name = "demo-fuzz"
"#,
    )?;

    let error = find_fuzz_dir_for_package_in_manifests("missing-fuzz", [manifest])
        .expect_err("unknown fuzz package should be rejected");
    assert!(error.to_string().contains("Could not find fuzz package"));
    Ok(())
}

#[sinex_test]
async fn test_fuzz_ld_library_path_merges_nix_ldflags_and_existing_path()
-> ::xtask::sandbox::TestResult<()> {
    let path = fuzz_ld_library_path(
        Some("-rpath /ignored -L/nix/store/gcc-lib/lib -L/nix/store/dbus/lib"),
        Some("/nix/store/dbus/lib:/extra/lib"),
        Some("/nix/store/cxx/lib"),
    )
    .expect("library path should be built");

    assert_eq!(
        path,
        "/nix/store/gcc-lib/lib:/nix/store/dbus/lib:/nix/store/cxx/lib:/extra/lib"
    );
    Ok(())
}

#[sinex_test]
async fn test_parse_fuzz_manifest_reports_malformed_toml() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let manifest = dir.path().join("Cargo.toml");
    fs::write(&manifest, "[package\nname = \"broken\"")?;

    let error = parse_fuzz_manifest(&manifest).expect_err("malformed manifest should surface");
    assert!(error.to_string().contains("failed to parse fuzz manifest"));
    Ok(())
}

#[sinex_test]
async fn test_collect_dir_entry_names_reports_entry_failures()
-> ::xtask::sandbox::TestResult<()> {
    let error = collect_dir_entry_names(
        Path::new("/tmp/corpus"),
        [
            Ok(OsString::from("seed-1")),
            Err(std::io::Error::other("entry read failed")),
        ],
    )
    .expect_err("entry failure should surface");

    assert!(error.to_string().contains("failed to read directory entry"));
    Ok(())
}
