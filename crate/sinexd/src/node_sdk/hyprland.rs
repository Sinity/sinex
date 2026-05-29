//! Typed Hyprland command-socket helpers.
//!
//! This module is intentionally capability-specific: it can dispatch only the
//! `HyprlandWorkspaceCommand` vocabulary from `sinex-primitives`, not arbitrary
//! shell commands.

use std::{
    env,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
};

use sinex_primitives::events::payloads::instruction::HyprlandWorkspaceCommand;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::node_sdk::{NodeResult, SinexError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyprlandCommandSocketResponse {
    pub socket_path: PathBuf,
    pub command: HyprlandWorkspaceCommand,
    pub response: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyprlandCommandSocketProbe {
    pub socket_path: PathBuf,
    pub available: bool,
    pub caveat: Option<String>,
}

impl HyprlandCommandSocketProbe {
    fn available(socket_path: &Path) -> Self {
        Self {
            socket_path: socket_path.to_path_buf(),
            available: true,
            caveat: None,
        }
    }

    fn unavailable(socket_path: &Path, caveat: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.to_path_buf(),
            available: false,
            caveat: Some(caveat.into()),
        }
    }
}

#[must_use]
pub fn resolve_hyprland_command_socket_path(explicit: Option<&str>) -> Option<PathBuf> {
    if let Some(path) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(path));
    }

    let runtime_dir = env::var_os("XDG_RUNTIME_DIR").filter(|value| !value.is_empty())?;
    let instance_signature =
        env::var_os("HYPRLAND_INSTANCE_SIGNATURE").filter(|value| !value.is_empty())?;
    Some(
        PathBuf::from(runtime_dir)
            .join("hypr")
            .join(instance_signature)
            .join(".socket.sock"),
    )
}

pub async fn probe_hyprland_command_socket(
    socket_path: impl AsRef<Path>,
) -> HyprlandCommandSocketProbe {
    let socket_path = socket_path.as_ref();
    let metadata = match tokio::fs::metadata(socket_path).await {
        Ok(metadata) => metadata,
        Err(error) => {
            return HyprlandCommandSocketProbe::unavailable(
                socket_path,
                format!("Hyprland command socket is not visible: {error}"),
            );
        }
    };

    if !metadata.file_type().is_socket() {
        return HyprlandCommandSocketProbe::unavailable(
            socket_path,
            "Hyprland command socket path exists but is not a Unix socket",
        );
    }

    match UnixStream::connect(socket_path).await {
        Ok(_stream) => HyprlandCommandSocketProbe::available(socket_path),
        Err(error) => HyprlandCommandSocketProbe::unavailable(
            socket_path,
            format!("Hyprland command socket is not connectable: {error}"),
        ),
    }
}

pub async fn dispatch_hyprland_workspace_command(
    socket_path: impl AsRef<Path>,
    command: &HyprlandWorkspaceCommand,
) -> NodeResult<HyprlandCommandSocketResponse> {
    let socket_path = socket_path.as_ref();
    let message = command.command_socket_message();
    let mut stream = UnixStream::connect(socket_path).await.map_err(|error| {
        SinexError::io("failed to connect to Hyprland command socket")
            .with_path(socket_path.display().to_string())
            .with_std_error(&error)
    })?;

    stream
        .write_all(message.as_bytes())
        .await
        .map_err(|error| {
            SinexError::io("failed to write Hyprland command socket request")
                .with_path(socket_path.display().to_string())
                .with_std_error(&error)
        })?;
    stream.shutdown().await.map_err(|error| {
        SinexError::io("failed to close Hyprland command socket request")
            .with_path(socket_path.display().to_string())
            .with_std_error(&error)
    })?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(|error| {
            SinexError::io("failed to read Hyprland command socket response")
                .with_path(socket_path.display().to_string())
                .with_std_error(&error)
        })?;

    Ok(HyprlandCommandSocketResponse {
        socket_path: socket_path.to_path_buf(),
        command: command.clone(),
        response,
    })
}

#[cfg(test)]
mod tests {
    use sinex_primitives::events::payloads::instruction::{
        HyprlandDispatch, HyprlandWorkspaceCommand,
    };
    use std::path::PathBuf;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixListener,
    };
    use xtask::sandbox::prelude::*;

    use super::{
        dispatch_hyprland_workspace_command, probe_hyprland_command_socket,
        resolve_hyprland_command_socket_path,
    };

    #[sinex_test]
    async fn hyprland_command_socket_dispatches_typed_workspace_command() -> TestResult<()> {
        let temp = tempfile::Builder::new()
            .prefix("sinex-hypr-")
            .tempdir_in("/tmp")?;
        let socket_path = temp.path().join("hyprland-command.sock");
        let listener = UnixListener::bind(&socket_path)?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = String::new();
            stream.read_to_string(&mut request).await?;
            stream.write_all(b"ok").await?;
            Ok::<_, std::io::Error>(request)
        });

        let command = HyprlandWorkspaceCommand {
            dispatch: HyprlandDispatch::Workspace,
            workspace_id: 4,
        };
        let response = dispatch_hyprland_workspace_command(&socket_path, &command).await?;
        let request = server.await??;

        assert_eq!(request, "dispatch workspace 4");
        assert_eq!(response.response, "ok");
        assert_eq!(response.command, command);
        assert_eq!(response.socket_path, socket_path);
        Ok(())
    }

    #[sinex_test]
    async fn hyprland_command_socket_probe_reports_connectable_socket() -> TestResult<()> {
        let temp = tempfile::Builder::new()
            .prefix("sinex-hypr-")
            .tempdir_in("/tmp")?;
        let socket_path = temp.path().join("hyprland-command.sock");
        let listener = UnixListener::bind(&socket_path)?;
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await?;
            Ok::<_, std::io::Error>(())
        });

        let probe = probe_hyprland_command_socket(&socket_path).await;
        server.await??;

        assert!(probe.available);
        assert_eq!(probe.socket_path, socket_path);
        assert_eq!(probe.caveat, None);
        Ok(())
    }

    #[sinex_test]
    async fn hyprland_command_socket_probe_reports_missing_socket() -> TestResult<()> {
        let temp = tempfile::Builder::new()
            .prefix("sinex-hypr-")
            .tempdir_in("/tmp")?;
        let socket_path = temp.path().join("missing.sock");

        let probe = probe_hyprland_command_socket(&socket_path).await;

        assert!(!probe.available);
        assert_eq!(probe.socket_path, socket_path);
        assert!(
            probe
                .caveat
                .as_deref()
                .is_some_and(|caveat| caveat.contains("not visible"))
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn hyprland_command_socket_resolution_uses_runtime_env() -> TestResult<()> {
        let temp = tempfile::Builder::new()
            .prefix("sinex-hypr-runtime-")
            .tempdir_in("/tmp")?;
        let mut env = EnvGuard::new();
        env.set("XDG_RUNTIME_DIR", temp.path().display().to_string());
        env.set("HYPRLAND_INSTANCE_SIGNATURE", "instance-1");

        assert_eq!(
            resolve_hyprland_command_socket_path(None),
            Some(temp.path().join("hypr/instance-1/.socket.sock"))
        );
        assert_eq!(
            resolve_hyprland_command_socket_path(Some(" /tmp/explicit.sock ")),
            Some(PathBuf::from("/tmp/explicit.sock"))
        );
        Ok(())
    }
}
