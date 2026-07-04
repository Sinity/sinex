//! Adapter for line-delimited Unix domain socket streams.
//!
//! Either connects to a producer-owned socket (e.g., Hyprland IPC at
//! `/run/user/1000/hypr/<instance>/.hyprland/socket2.sock`) or listens on a
//! Sinex-owned socket that producer hooks can connect to. Each newline-
//! terminated message is one [`SourceRecord`].
//!
//! Cursor is `()` — Hyprland IPC is a live stream with no replay.
//! Anchor is [`MaterialAnchor::StreamFrame`] with a monotonic byte/line offset.
//!
//! In connect mode, when the server closes the connection (`EOF`), the stream
//! ends unless `reconnect_on_eof` is `true`; reconnection attempts use a simple
//! exponential back-off (50 ms, 100 ms, 200 ms … capped at 2 s). In listen mode,
//! the adapter keeps accepting producer connections until the stream is dropped.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::FileTypeExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord, TimingEvidence};
use sinex_primitives::temporal::Timestamp;

use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// UnixSocketStreamAdapter
// =============================================================================

/// Adapter for a line-delimited Unix domain socket.
///
/// Suitable for Hyprland IPC (`socket2.sock`) and similar event sockets that
/// emit newline-terminated JSON or plain-text messages, plus local bridge
/// sockets where Sinex owns the receive side.
///
/// Cursor is `()` — no replay; anchor only.
#[derive(Debug, Clone, Default)]
pub struct UnixSocketStreamAdapter;

/// Configuration for [`UnixSocketStreamAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UnixSocketStreamConfig {
    /// Path to the Unix domain socket.
    #[schemars(with = "String")]
    pub socket_path: Utf8PathBuf,

    /// Whether this adapter connects to an existing socket or listens on a
    /// Sinex-owned socket.
    #[serde(default)]
    pub mode: UnixSocketStreamMode,

    /// If true in [`UnixSocketStreamMode::Connect`], reconnect when the
    /// producer-owned socket closes the connection. Listen mode keeps accepting
    /// new producer connections until the stream is dropped.
    #[serde(default)]
    pub reconnect_on_eof: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum UnixSocketStreamMode {
    /// Connect to a producer-owned socket and read frames from it.
    #[default]
    Connect,

    /// Bind a Sinex-owned socket and accept producer connections.
    Listen,
}

/// No cursor for [`UnixSocketStreamAdapter`] — anchor only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnixSocketStreamCursor;

#[async_trait]
impl InputShapeAdapter for UnixSocketStreamAdapter {
    type Config = UnixSocketStreamConfig;
    type Cursor = UnixSocketStreamCursor;
    const KIND: InputShapeKind = InputShapeKind::UnixSocket;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let socket_path = config.socket_path.clone();
        match config.mode {
            UnixSocketStreamMode::Connect => {
                let reconnect = config.reconnect_on_eof;

                // Connect eagerly so we surface errors at open time.
                let stream_conn = UnixStream::connect(socket_path.as_std_path())
                    .await
                    .map_err(|e| {
                        ParserError::Adapter(format!(
                            "failed to connect to unix socket {socket_path}: {e}"
                        ))
                    })?;

                Ok(build_unix_stream(
                    material_id,
                    stream_conn,
                    socket_path,
                    reconnect,
                ))
            }
            UnixSocketStreamMode::Listen => {
                let listener = bind_unix_listener(&socket_path)?;
                Ok(build_unix_listener_stream(material_id, listener))
            }
        }
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(UnixSocketStreamCursor)
    }
}

fn bind_unix_listener(socket_path: &Utf8PathBuf) -> ParserResult<UnixListener> {
    if let Some(parent) = socket_path.as_std_path().parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            ParserError::Adapter(format!(
                "failed to create unix socket parent {}: {error}",
                parent.display()
            ))
        })?;
    }

    match std::fs::symlink_metadata(socket_path.as_std_path()) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            std::fs::remove_file(socket_path.as_std_path()).map_err(|error| {
                ParserError::Adapter(format!(
                    "failed to remove stale unix socket {socket_path}: {error}"
                ))
            })?;
        }
        Ok(_) => {
            return Err(ParserError::Adapter(format!(
                "refusing to replace non-socket path {socket_path}"
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ParserError::Adapter(format!(
                "failed to inspect unix socket path {socket_path}: {error}"
            )));
        }
    }

    UnixListener::bind(socket_path.as_std_path()).map_err(|error| {
        ParserError::Adapter(format!("failed to bind unix socket {socket_path}: {error}"))
    })
}

fn build_unix_listener_stream(
    material_id: Id<SourceMaterial>,
    listener: UnixListener,
) -> BoxStream<'static, ParserResult<SourceRecord>> {
    let stream = async_stream::stream! {
        let mut bytes_read: u64 = 0;
        let mut line_count: u64 = 0;

        loop {
            let (mut conn, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(error) => {
                    yield Err(ParserError::Io(error));
                    return;
                }
            };

            let reader = BufReader::new(&mut conn);
            let mut lines = reader.lines();

            loop {
                match lines.next_line().await {
                    Err(error) => {
                        yield Err(ParserError::Io(error));
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(line)) => {
                        if line.is_empty() {
                            bytes_read += 1;
                            continue;
                        }
                        let line_bytes = line.as_bytes().to_vec();
                        let received_at = Timestamp::now();
                        let len = line_bytes.len() as u64;

                        let anchor = MaterialAnchor::StreamFrame {
                            material_offset: bytes_read,
                            frame_index: line_count,
                        };

                        bytes_read += len + 1;
                        line_count += 1;

                        yield Ok(SourceRecord {
                            material_id,
                            anchor,
                            bytes: line_bytes,
                            logical_path: None,
                            source_ts_hint: Some(TimingEvidence::RealtimeCapture {
                                value: received_at,
                                capture_source: "unix_socket.listen".to_string(),
                            }),
                            metadata: serde_json::json!({
                                "unix_socket_mode": "listen",
                            }),
                        });
                    }
                }
            }
        }
    };

    Box::pin(stream)
}

fn build_unix_stream(
    material_id: Id<SourceMaterial>,
    initial_conn: UnixStream,
    socket_path: Utf8PathBuf,
    reconnect: bool,
) -> BoxStream<'static, ParserResult<SourceRecord>> {
    let stream = async_stream::stream! {
        let mut bytes_read: u64 = 0;
        let mut line_count: u64 = 0;
        let mut conn = initial_conn;

        loop {
            let reader = BufReader::new(&mut conn);
            let mut lines = reader.lines();

            loop {
                match lines.next_line().await {
                    Err(e) => {
                        yield Err(ParserError::Io(e));
                        return; // Non-EOF I/O error — abort.
                    }
                    Ok(None) => {
                        break; // EOF from server.
                    }
                    Ok(Some(line)) => {
                        if line.is_empty() {
                            bytes_read += 1; // just the newline
                            continue;
                        }
                        let line_bytes = line.as_bytes().to_vec();
                        let received_at = Timestamp::now();
                        let len = line_bytes.len() as u64;

                        let anchor = MaterialAnchor::StreamFrame {
                            material_offset: bytes_read,
                            frame_index: line_count,
                        };

                        bytes_read += len + 1; // +1 for the newline
                        line_count += 1;

                        yield Ok(SourceRecord {
                            material_id,
                            anchor,
                            bytes: line_bytes,
                            logical_path: None,
                            source_ts_hint: Some(TimingEvidence::RealtimeCapture {
                                value: received_at,
                                capture_source: "unix_socket.connect".to_string(),
                            }),
                            metadata: serde_json::Value::Null,
                        });
                    }
                }
            }

            // EOF reached.
            if !reconnect {
                break;
            }

            // Exponential backoff reconnection.
            let mut delay_ms = 50u64;
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                match UnixStream::connect(socket_path.as_std_path()).await {
                    Ok(new_conn) => {
                        conn = new_conn;
                        break;
                    }
                    Err(_) => {
                        delay_ms = (delay_ms * 2).min(2_000);
                    }
                }
            }
        }
    };

    Box::pin(stream)
}

// =============================================================================
// Test helper: pipe-based fake socket
// =============================================================================

/// Create an in-process Unix domain socket pair for testing.
///
/// Returns `(server_side, client_side)`. The test writes to the server side;
/// the adapter reads from the client side.
#[cfg(test)]
pub fn make_socket_pair() -> (UnixStream, UnixStream) {
    let (a, b) = UnixStream::pair().unwrap();
    (a, b)
}

#[cfg(test)]
#[path = "unix_socket_stream_test.rs"]
mod tests;
