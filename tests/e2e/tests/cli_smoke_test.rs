use std::path::PathBuf;
use std::process::Command;

use color_eyre::eyre::eyre;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn exo_cli_stays_parseable() -> TestResult<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| eyre!("failed to resolve workspace root"))?;

    let status = Command::new("python3")
        .arg("-m")
        .arg("compileall")
        .arg("-q")
        .arg("cli/exo.py")
        .current_dir(workspace_root)
        .status();

    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(eyre!("python3 -m compileall exited with status {status}")),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("⚠️  python3 not found on PATH; skipping CLI smoke test");
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}
