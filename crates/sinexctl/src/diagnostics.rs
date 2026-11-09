//! Run the installed database diagnostics without linking a database client.
use command_surface::{Failure, FailureKind};
use serde_json::Value;
use std::path::Path;

pub(crate) async fn run(
    command: &str,
    database: &str,
    store: Option<&Path>,
) -> Result<Value, Failure> {
    let broken = |message: String| Failure::new(FailureKind::Io, message);
    let executable = std::env::current_exe()
        .map_err(|e| broken(e.to_string()))?
        .with_file_name("sinexd");
    let mut process = tokio::process::Command::new(&executable);
    process.args(["admin", "diagnostics", command, "--database", database]);
    if let Some(store) = store {
        process.arg("--store").arg(store);
    }
    let output = process
        .output()
        .await
        .map_err(|e| broken(format!("{}: {e}", executable.display())))?;
    if !output.status.success() {
        // A failure the diagnostics reported on standard output keeps its kind.
        if let Some(failure) = serde_json::from_slice::<Value>(&output.stdout)
            .ok()
            .and_then(|reported| serde_json::from_value::<Failure>(reported["error"].clone()).ok())
        {
            return Err(failure);
        }
        let error = String::from_utf8_lossy(&output.stderr);
        let error = error.trim();
        return Err(broken(if error.is_empty() {
            format!(
                "{}: diagnostics {command} failed ({})",
                executable.display(),
                output.status
            )
        } else {
            error.to_owned()
        }));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|e| crate::client::protocol_mismatch(&format!("diagnostics {command}"), e))
}
