//! Unix socket fixture.
//!
//! Starts a minimal line-delimited Unix socket server in a temp directory.
//! The server writes each line from `data` to connecting clients then closes.
//! The fixture returns the socket path so the source unit host can connect.

use std::path::PathBuf;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;

use super::{FixtureBinding, FixtureHandle};

/// Build a Unix socket fixture.
///
/// Starts a background Tokio task that binds a Unix socket in a temp dir and
/// writes `data` to the first connecting client, then closes the connection.
///
/// Returns a [`FixtureHandle`] whose `binding` is `FixtureBinding::UnixSocketPath`
/// pointing at the socket file.
///
/// # Errors
///
/// Returns an error if the socket cannot be bound.
pub async fn build(data: &[u8]) -> Result<FixtureHandle, String> {
    let dir = TempDir::new().map_err(|e| format!("failed to create unix socket temp dir: {e}"))?;
    let socket_path: PathBuf = dir.path().join("fixture.sock");

    let listener = UnixListener::bind(&socket_path)
        .map_err(|e| format!("failed to bind unix socket fixture: {e}"))?;

    let data_owned = data.to_vec();
    tokio::spawn(async move {
        // Serve a single connection: write data and close.
        if let Ok((mut stream, _)) = listener.accept().await {
            let _ = stream.write_all(&data_owned).await;
            // Close is implicit on drop.
        }
        // After serving once, the listener goes out of scope.
    });

    Ok(FixtureHandle::with_resource(
        FixtureBinding::UnixSocketPath(socket_path),
        dir,
    ))
}
