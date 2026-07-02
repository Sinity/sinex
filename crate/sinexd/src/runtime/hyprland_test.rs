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
