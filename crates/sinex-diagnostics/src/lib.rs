//! Run installed administrative commands without linking a database client.
use serde_json::Value;
use std::ffi::OsString;
use std::path::Path;

pub async fn run(command: &str, database: &str, store: Option<&Path>) -> Result<Value, String> {
    let mut args: Vec<OsString> = vec![
        "diagnostics".into(),
        command.into(),
        "--database".into(),
        database.into(),
    ];
    if let Some(store) = store {
        args.extend(["--store".into(), store.as_os_str().to_owned()]);
    }
    admin(&args).await
}

pub async fn private_mode(state: &str) -> Result<Value, String> {
    admin(&["private".into(), state.into()]).await
}

async fn admin(args: &[OsString]) -> Result<Value, String> {
    let executable = std::env::current_exe()
        .map_err(|e| e.to_string())?
        .with_file_name("sinexd");
    let mut process = tokio::process::Command::new(&executable);
    process.arg("admin").args(args);
    let output = process
        .output()
        .await
        .map_err(|e| format!("{}: {e}", executable.display()))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().into());
    }
    serde_json::from_slice(&output.stdout).map_err(|e| format!("admin reply: {e}"))
}
