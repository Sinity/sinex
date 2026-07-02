use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn new_workspace_declares_isolated_env_and_history_path() -> TestResult<()> {
    let workspace = EphemeralWorkspace::new()?;
    let env = workspace.env_overrides();

    assert_eq!(env.len(), 2);
    assert!(
        env.iter().any(|(key, value)| {
            key == "CARGO_TARGET_DIR" && Path::new(value) == workspace.target_dir()
        }),
        "CARGO_TARGET_DIR must point at the isolated target dir: {env:?}"
    );
    assert!(
        env.iter().any(|(key, value)| {
            key == "SINEX_STATE_DIR" && Path::new(value) == workspace.state_dir()
        }),
        "SINEX_STATE_DIR must point at the isolated state dir: {env:?}"
    );
    assert_eq!(
        workspace.history_db_path(),
        workspace.state_dir().join("xtask-history.db")
    );
    assert_ne!(
        workspace.dir(),
        workspace.target_dir(),
        "workspace and target dirs must not alias"
    );
    assert_ne!(
        workspace.dir(),
        workspace.state_dir(),
        "workspace and state dirs must not alias"
    );
    Ok(())
}

#[sinex_test]
async fn add_member_updates_workspace_manifest_and_creates_crate() -> TestResult<()> {
    let workspace = EphemeralWorkspace::new()?;
    workspace.add_member("ws-extra")?;

    let manifest = std::fs::read_to_string(workspace.dir().join("Cargo.toml"))?;
    assert!(
        manifest.contains(r#"members = ["ws-lib", "ws-extra"]"#),
        "workspace manifest must preserve named member visibility: {manifest}"
    );
    assert!(workspace.crate_cargo_toml("ws-extra").is_file());
    assert!(workspace.crate_src_lib("ws-extra").is_file());
    Ok(())
}

#[sinex_test]
async fn mutation_helpers_write_distinct_fixture_defects() -> TestResult<()> {
    let workspace = EphemeralWorkspace::new()?;
    workspace.inject_compile_error(DEFAULT_CRATE)?;
    workspace.inject_clippy_warning(DEFAULT_CRATE)?;
    workspace.inject_format_error(DEFAULT_CRATE)?;

    let lib_rs = std::fs::read_to_string(workspace.crate_src_lib(DEFAULT_CRATE))?;
    assert!(lib_rs.contains("let _x: i32 = \"not_an_int\";"));
    assert!(lib_rs.contains("v.len() == 0"));
    assert!(lib_rs.contains("fn _fmt_error(){let x=1+2;let _=x;}"));
    Ok(())
}

#[sinex_test]
async fn inject_unused_dep_appends_dependency_section() -> TestResult<()> {
    let workspace = EphemeralWorkspace::new()?;
    workspace.inject_unused_dep(DEFAULT_CRATE, "serde_json", "1")?;

    let manifest = std::fs::read_to_string(workspace.crate_cargo_toml(DEFAULT_CRATE))?;
    assert!(manifest.contains("[dependencies]"));
    assert!(manifest.contains("serde_json = \"1\""));
    Ok(())
}

#[sinex_test]
async fn inject_compile_error_surfaces_unreadable_lib_rs() -> TestResult<()> {
    let workspace = EphemeralWorkspace::new()?;
    let lib_rs = workspace.crate_src_lib(DEFAULT_CRATE);
    std::fs::remove_file(&lib_rs)?;
    std::fs::create_dir(&lib_rs)?;

    let Err(error) = workspace.inject_compile_error(DEFAULT_CRATE) else {
        return Err(color_eyre::eyre::eyre!("directory lib.rs should surface"));
    };
    let message = format!("{error:#}");
    assert!(message.contains("inject_compile_error: read existing"));
    assert!(message.contains(lib_rs.display().to_string().as_str()));
    Ok(())
}

#[sinex_test]
async fn inject_unused_dep_surfaces_unreadable_manifest() -> TestResult<()> {
    let workspace = EphemeralWorkspace::new()?;
    let cargo_toml = workspace.crate_cargo_toml(DEFAULT_CRATE);
    std::fs::remove_file(&cargo_toml)?;
    std::fs::create_dir(&cargo_toml)?;

    let Err(error) = workspace.inject_unused_dep(DEFAULT_CRATE, "serde_json", "1") else {
        return Err(color_eyre::eyre::eyre!(
            "directory Cargo.toml should surface"
        ));
    };
    let message = format!("{error:#}");
    assert!(message.contains("inject_unused_dep: read existing"));
    assert!(message.contains(cargo_toml.display().to_string().as_str()));
    Ok(())
}
