use std::path::{Path, PathBuf};
use std::process::Command;

pub fn xtask_bin() -> color_eyre::eyre::Result<PathBuf> {
    if let Some(bin) = std::env::var_os("CARGO_BIN_EXE_xtask") {
        return Ok(PathBuf::from(bin));
    }

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to resolve workspace root"))?;
    let exe_name = if cfg!(windows) { "xtask.exe" } else { "xtask" };
    let fallback = workspace_root.join(".sinex/target/debug").join(exe_name);
    if fallback.is_file() {
        Ok(fallback)
    } else {
        Err(color_eyre::eyre::eyre!(
            "CARGO_BIN_EXE_xtask is not set and fallback binary was not found at {}",
            fallback.display()
        ))
    }
}

pub fn xtask_command() -> color_eyre::eyre::Result<Command> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to resolve workspace root"))?;
    let mut command = Command::new(xtask_bin()?);
    command.current_dir(workspace_root);
    Ok(command)
}
