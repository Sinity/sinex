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

/// The CLI owns the admin process group until all of its descendants are reaped.
pub(crate) async fn backup_copy(
    job: &str,
    cancelled: impl std::future::Future<Output = Failure>,
) -> Result<Value, Failure> {
    let executable = std::env::current_exe()
        .map_err(|e| Failure::new(FailureKind::Io, e.to_string()))?
        .with_file_name("sinexd");
    let mut command = std::process::Command::new(&executable);
    command.args(["admin", "backup-copy"]);
    backup_process(command, job.to_owned(), cancelled).await
}

async fn backup_process(
    mut command: std::process::Command,
    input: String,
    cancelled: impl std::future::Future<Output = Failure>,
) -> Result<Value, Failure> {
    use std::io::{Read, Write};
    use tokio::signal::unix::{SignalKind, signal};
    let broken = |message: String| Failure::new(FailureKind::Io, message);
    let mut interrupt = signal(SignalKind::interrupt()).map_err(|e| broken(e.to_string()))?;
    let mut terminate = signal(SignalKind::terminate()).map_err(|e| broken(e.to_string()))?;
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let (registered, ready) = tokio::sync::oneshot::channel();
    let mut worker =
        tokio::task::spawn_blocking(move || -> std::io::Result<std::process::Output> {
            let mut child = sinex_process::Child::spawn(&mut command)?;
            if registered.send(child.control()).is_err() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "backup caller stopped",
                ));
            }
            let mut stdout = child.stdout().expect("piped stdout");
            // Reap the leader even if a descendant inherited its stdout pipe.
            let reader = std::thread::spawn(move || {
                let mut output = Vec::new();
                stdout.read_to_end(&mut output).map(|_| output)
            });
            let written = child
                .stdin()
                .expect("piped stdin")
                .write_all(input.as_bytes());
            if written.is_err() {
                child.control().kill();
            }
            let status = child.wait();
            let stdout = reader
                .join()
                .map_err(|_| std::io::Error::other("backup output reader panicked"))?;
            written?;
            Ok(std::process::Output {
                status: status?,
                stdout: stdout?,
                stderr: Vec::new(),
            })
        });
    let control = match ready.await {
        Ok(control) => control,
        Err(_) => {
            return Err(broken(match worker.await {
                Ok(Err(error)) => error.to_string(),
                Err(error) => error.to_string(),
                Ok(Ok(_)) => "backup process ended before ownership registration".into(),
            }));
        }
    };
    struct Cancel(std::sync::Arc<sinex_process::Control>);
    impl Drop for Cancel {
        fn drop(&mut self) {
            self.0.kill();
        }
    }
    let _cancel = Cancel(control.clone());
    tokio::pin!(cancelled);
    let output = tokio::select! {
        result = &mut worker => result.map_err(|e| broken(e.to_string()))?.map_err(|e| broken(e.to_string()))?,
        failure = &mut cancelled => {
            control.kill();
            let reaped = worker.await.map_err(|e| broken(e.to_string()))?;
            reaped.map_err(|e| broken(e.to_string()))?;
            return Err(failure);
        },
        _ = interrupt.recv() => {
            control.kill();
            worker.await.map_err(|e| broken(e.to_string()))?.map_err(|e| broken(e.to_string()))?;
            return Err(Failure::new(FailureKind::Cancelled, "backup interrupted"));
        },
        _ = terminate.recv() => {
            control.kill();
            worker.await.map_err(|e| broken(e.to_string()))?.map_err(|e| broken(e.to_string()))?;
            return Err(Failure::new(FailureKind::Cancelled, "backup stopped"));
        },
    };
    let reported: Value = serde_json::from_slice(&output.stdout).map_err(|e| {
        broken(format!(
            "backup copy answered no JSON ({e}); exit {}",
            output.status
        ))
    })?;
    if let Ok(failure) = serde_json::from_value::<Failure>(reported["error"].clone()) {
        return Err(failure);
    }
    match (output.status.success(), reported.get("manifest")) {
        (true, Some(manifest)) => Ok(manifest.clone()),
        _ => Err(broken(format!("backup copy failed ({})", output.status))),
    }
}

#[cfg(test)]
mod tests;
