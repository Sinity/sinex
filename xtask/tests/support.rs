use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static SUBPROCESS_STATE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn xtask_bin() -> color_eyre::eyre::Result<PathBuf> {
    if let Some(bin) = std::env::var_os("CARGO_BIN_EXE_xtask") {
        return Ok(PathBuf::from(bin));
    }

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to resolve workspace root"))?;
    let exe_name = if cfg!(windows) { "xtask.exe" } else { "xtask" };
    let fallback = xtask::workspace_target_dir_for(workspace_root)
        .join("debug")
        .join(exe_name);
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
    // Subprocess tests often seed a history DB under an explicit SINEX_STATE_DIR.
    // Clear any suite-level XTASK_HISTORY_DB override so children use the state
    // directory unless the caller opts into a specific history DB explicitly.
    command.env_remove("XTASK_HISTORY_DB");
    command.env("SINEX_STATE_DIR", fresh_state_dir()?);
    command.env("NO_COLOR", "1");
    command.env("FORCE_COLOR", "0");
    Ok(command)
}

fn fresh_state_dir() -> color_eyre::eyre::Result<PathBuf> {
    let base = std::env::temp_dir().join("sinex-xtask-test-state");
    fs::create_dir_all(&base)?;

    let state_dir = base.join(format!(
        "{}-{}",
        std::process::id(),
        SUBPROCESS_STATE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&state_dir)?;
    Ok(state_dir)
}
