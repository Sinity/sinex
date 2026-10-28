//! Run the installed database diagnostics without linking a database client.
use serde_json::Value;
use std::path::Path;

pub(crate) async fn run(
    command: &str,
    database: &str,
    store: Option<&Path>,
) -> Result<Value, String> {
    let executable = std::env::current_exe()
        .map_err(|e| e.to_string())?
        .with_file_name("sinexd");
    let mut process = tokio::process::Command::new(&executable);
    process.args(["admin", "diagnostics", command, "--database", database]);
    if let Some(store) = store {
        process.arg("--store").arg(store);
    }
    let output = process
        .output()
        .await
        .map_err(|e| format!("{}: {e}", executable.display()))?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        let error = error.trim();
        return Err(if error.is_empty() {
            format!(
                "{}: diagnostics {command} failed ({})",
                executable.display(),
                output.status
            )
        } else {
            error.to_owned()
        });
    }
    serde_json::from_slice(&output.stdout).map_err(|e| format!("diagnostics reply: {e}"))
}
