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

use crate::runtime::{RuntimeResult, SinexError};

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
) -> RuntimeResult<HyprlandCommandSocketResponse> {
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
#[path = "hyprland_test.rs"]
mod tests;
