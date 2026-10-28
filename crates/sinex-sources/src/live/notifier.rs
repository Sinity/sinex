//! Publishing errors remain in local status and reach the graphical session.
use std::ffi::OsStr;
use std::process::Stdio;

pub(super) async fn raise(binding: &str, reason: &str) {
    eprintln!("{binding}: error: {reason}");
    if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_none() {
        return;
    }
    if !send(OsStr::new("busctl"), binding, reason).await {
        eprintln!("{binding}: could not send capture error to the desktop notifier");
    }
}

async fn send(program: &OsStr, binding: &str, reason: &str) -> bool {
    let mut command = tokio::process::Command::new(program);
    command
        .args([
            "--user",
            "--timeout=2",
            "call",
            "org.freedesktop.Notifications",
            "/org/freedesktop/Notifications",
            "org.freedesktop.Notifications",
            "Notify",
            "susssasa{sv}i",
            "Sinex",
            "0",
            "",
            "Sinex capture error",
            &format!("{binding}: {reason}"),
            "0",
            "0",
            "-1",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    matches!(tokio::time::timeout(std::time::Duration::from_secs(3), command.status()).await, Ok(Ok(status)) if status.success())
}

#[cfg(test)]
mod tests;
