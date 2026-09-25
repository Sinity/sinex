//! Typed Hyprland command-socket helpers.
//!
//! This module is intentionally capability-specific: it can dispatch only the
//! `HyprlandWorkspaceCommand` vocabulary from `sinex-primitives`, not arbitrary
//! shell commands.

use std::{
    env,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    time::Duration,
};

use sinex_primitives::events::payloads::instruction::HyprlandWorkspaceCommand;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::timeout,
};

use crate::runtime::{RuntimeResult, SinexError};

const COMMAND_SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_COMMAND_SOCKET_RESPONSE_BYTES: usize = 4096;

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

pub struct HyprlandCommandSocketConnection {
    socket_path: PathBuf,
    stream: UnixStream,
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
    if let Err(error) = validate_command_socket_path(socket_path) {
        return HyprlandCommandSocketProbe::unavailable(socket_path, error.to_string());
    }
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

    match timeout(COMMAND_SOCKET_IO_TIMEOUT, UnixStream::connect(socket_path)).await {
        Ok(Ok(_stream)) => HyprlandCommandSocketProbe::available(socket_path),
        Ok(Err(error)) => HyprlandCommandSocketProbe::unavailable(
            socket_path,
            format!("Hyprland command socket is not connectable: {error}"),
        ),
        Err(_) => HyprlandCommandSocketProbe::unavailable(
            socket_path,
            "Hyprland command socket connect timed out",
        ),
    }
}

pub async fn dispatch_hyprland_workspace_command(
    socket_path: impl AsRef<Path>,
    command: &HyprlandWorkspaceCommand,
) -> RuntimeResult<HyprlandCommandSocketResponse> {
    let socket_path = socket_path.as_ref();
    let validated_path = validate_command_socket_path(socket_path)?;
    connect_validated_hyprland_command_socket(validated_path)
        .await?
        .dispatch(command)
        .await
}

pub async fn connect_hyprland_command_socket(
    socket_path: impl AsRef<Path>,
) -> RuntimeResult<HyprlandCommandSocketConnection> {
    let validated_path = validate_command_socket_path(socket_path.as_ref())?;
    connect_validated_hyprland_command_socket(validated_path).await
}

async fn connect_validated_hyprland_command_socket(
    validated_path: PathBuf,
) -> RuntimeResult<HyprlandCommandSocketConnection> {
    let stream = timeout(
        COMMAND_SOCKET_IO_TIMEOUT,
        UnixStream::connect(&validated_path),
    )
    .await
    .map_err(|_| SinexError::io("Hyprland command socket connect timed out"))?
    .map_err(|error| {
        SinexError::io("failed to connect to Hyprland command socket")
            .with_path(validated_path.display().to_string())
            .with_std_error(&error)
    })?;
    Ok(HyprlandCommandSocketConnection {
        socket_path: validated_path,
        stream,
    })
}

impl HyprlandCommandSocketConnection {
    pub async fn dispatch(
        mut self,
        command: &HyprlandWorkspaceCommand,
    ) -> RuntimeResult<HyprlandCommandSocketResponse> {
        let message = command.command_socket_message();
        let socket_path = &self.socket_path;
        let io = async {
            let stream = &mut self.stream;
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

            let mut response = Vec::new();
            let mut limited_stream = stream.take((MAX_COMMAND_SOCKET_RESPONSE_BYTES + 1) as u64);
            limited_stream
                .read_to_end(&mut response)
                .await
                .map_err(|error| {
                    SinexError::io("failed to read Hyprland command socket response")
                        .with_path(socket_path.display().to_string())
                        .with_std_error(&error)
                })?;
            if response.len() > MAX_COMMAND_SOCKET_RESPONSE_BYTES {
                return Err(SinexError::validation(
                    "Hyprland command socket response exceeds the size limit",
                ));
            }
            String::from_utf8(response).map_err(|error| {
                SinexError::serialization("Hyprland command socket response is not UTF-8")
                    .with_std_error(&error)
            })
        };
        let response = timeout(COMMAND_SOCKET_IO_TIMEOUT, io)
            .await
            .map_err(|_| SinexError::io("Hyprland command socket I/O timed out"))??;

        Ok(HyprlandCommandSocketResponse {
            socket_path: self.socket_path,
            command: command.clone(),
            response,
        })
    }
}

fn validate_command_socket_path(socket_path: &Path) -> RuntimeResult<PathBuf> {
    let path = socket_path.to_str().ok_or_else(|| {
        SinexError::validation("Hyprland command socket path must be valid UTF-8")
    })?;
    sinex_primitives::validation::validate_path(path).map(PathBuf::from)
}

#[cfg(test)]
#[path = "hyprland_test.rs"]
mod tests;
