//! Typed Hyprland command-socket helpers.
//!
//! This module is intentionally capability-specific: it can dispatch only the
//! `HyprlandWorkspaceCommand` vocabulary from `sinex-primitives`, not arbitrary
//! shell commands.

use std::path::{Path, PathBuf};

use sinex_primitives::events::payloads::instruction::HyprlandWorkspaceCommand;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{NodeResult, SinexError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyprlandCommandSocketResponse {
    pub socket_path: PathBuf,
    pub command: HyprlandWorkspaceCommand,
    pub response: String,
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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixListener,
    };
    use xtask::sandbox::prelude::*;

    use super::dispatch_hyprland_workspace_command;

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
}
