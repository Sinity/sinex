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
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

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
                            source_ts_hint: None,
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
                            source_ts_hint: None,
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
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    use tokio::time::{Duration, timeout};
    use xtask::sandbox::prelude::sinex_test;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    fn short_socket_tempdir() -> xtask::sandbox::TestResult<tempfile::TempDir> {
        tempfile::Builder::new()
            .prefix("sx")
            .tempdir_in("/tmp")
            .map_err(Into::into)
    }

    #[sinex_test]
    async fn test_unix_socket_yields_one_record_per_line() -> xtask::sandbox::TestResult<()> {
        let (mut server, client) = make_socket_pair();
        server.write_all(b"line1\nline2\nline3\n").await.unwrap();
        drop(server); // Close server side → EOF

        let stream = build_unix_stream(
            dummy_material_id(),
            client,
            Utf8PathBuf::from("/fake/socket"),
            false,
        );

        let records: Vec<_> = stream.collect().await;
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].as_ref().unwrap().bytes, b"line1");
        assert_eq!(records[1].as_ref().unwrap().bytes, b"line2");
        assert_eq!(records[2].as_ref().unwrap().bytes, b"line3");
        Ok(())
    }

    #[sinex_test]
    async fn test_unix_socket_anchor_contains_byte_offset() -> xtask::sandbox::TestResult<()> {
        let (mut server, client) = make_socket_pair();
        server.write_all(b"hello\nworld\n").await.unwrap();
        drop(server);

        let stream = build_unix_stream(
            dummy_material_id(),
            client,
            Utf8PathBuf::from("/fake/socket"),
            false,
        );
        let records: Vec<_> = stream.collect().await;

        assert!(matches!(
            records[0].as_ref().unwrap().anchor,
            MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0
            }
        ));
        // "hello\n" = 6 bytes → next line starts at offset 6
        assert!(matches!(
            records[1].as_ref().unwrap().anchor,
            MaterialAnchor::StreamFrame {
                material_offset: 6,
                frame_index: 1
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_unix_socket_skips_empty_lines() -> xtask::sandbox::TestResult<()> {
        let (mut server, client) = make_socket_pair();
        server.write_all(b"msg1\n\nmsg2\n").await.unwrap();
        drop(server);

        let stream = build_unix_stream(
            dummy_material_id(),
            client,
            Utf8PathBuf::from("/fake/socket"),
            false,
        );
        let records: Vec<_> = stream.collect().await;

        assert_eq!(records.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_unix_socket_frame_index_monotonic() -> xtask::sandbox::TestResult<()> {
        let (mut server, client) = make_socket_pair();
        server.write_all(b"a\nb\nc\n").await.unwrap();
        drop(server);

        let stream = build_unix_stream(
            dummy_material_id(),
            client,
            Utf8PathBuf::from("/fake/socket"),
            false,
        );
        let records: Vec<_> = stream.collect().await;

        let mut indices = Vec::new();
        for record in &records {
            match &record.as_ref().unwrap().anchor {
                MaterialAnchor::StreamFrame { frame_index, .. } => indices.push(*frame_index),
                other => {
                    return Err(color_eyre::eyre::eyre!(
                        "expected stream-frame anchor, got {other:?}"
                    ));
                }
            }
        }

        for w in indices.windows(2) {
            assert!(w[0] < w[1]);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_unix_socket_cursor_after_is_unit() -> xtask::sandbox::TestResult<()> {
        let adapter = UnixSocketStreamAdapter;
        let record = SourceRecord {
            material_id: dummy_material_id(),
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0,
            },
            bytes: b"data".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let cursor = adapter.cursor_after(&record).unwrap();
        assert_eq!(cursor, UnixSocketStreamCursor);
        Ok(())
    }

    #[sinex_test]
    async fn test_unix_socket_connect_to_missing_socket_fails() -> xtask::sandbox::TestResult<()> {
        let adapter = UnixSocketStreamAdapter;
        let config = UnixSocketStreamConfig {
            socket_path: Utf8PathBuf::from("/nonexistent/socket.sock"),
            mode: UnixSocketStreamMode::Connect,
            reconnect_on_eof: false,
        };
        assert!(
            adapter
                .open(dummy_material_id(), &config, None)
                .await
                .is_err()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_kind_is_unix_socket() -> xtask::sandbox::TestResult<()> {
        assert_eq!(UnixSocketStreamAdapter::KIND, InputShapeKind::UnixSocket);
        Ok(())
    }

    #[sinex_test]
    async fn listen_mode_accepts_multiple_producer_connections() -> xtask::sandbox::TestResult<()> {
        let dir = short_socket_tempdir()?;
        let socket_path = dir.path().join("k.sock");
        let config = UnixSocketStreamConfig {
            socket_path: Utf8PathBuf::from_path_buf(socket_path.clone())
                .map_err(|path| color_eyre::eyre::eyre!("non-UTF8 socket path: {path:?}"))?,
            mode: UnixSocketStreamMode::Listen,
            reconnect_on_eof: false,
        };

        let adapter = UnixSocketStreamAdapter;
        let mut stream = adapter.open(dummy_material_id(), &config, None).await?;

        let mut first = UnixStream::connect(&socket_path).await?;
        first.write_all(b"one\n").await?;
        drop(first);
        let one = timeout(Duration::from_secs(1), stream.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("first producer frame missing"))??;

        let mut second = UnixStream::connect(&socket_path).await?;
        second.write_all(b"two\n").await?;
        drop(second);
        let two = timeout(Duration::from_secs(1), stream.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("second producer frame missing"))??;

        assert_eq!(one.bytes, b"one");
        assert_eq!(two.bytes, b"two");
        assert!(matches!(
            one.anchor,
            MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0
            }
        ));
        assert!(matches!(
            two.anchor,
            MaterialAnchor::StreamFrame {
                material_offset: 4,
                frame_index: 1
            }
        ));
        assert_eq!(one.metadata["unix_socket_mode"], "listen");
        assert_eq!(two.metadata["unix_socket_mode"], "listen");
        Ok(())
    }

    #[sinex_test]
    async fn listen_mode_replaces_stale_socket() -> xtask::sandbox::TestResult<()> {
        let dir = short_socket_tempdir()?;
        let socket_path = dir.path().join("stale.sock");
        let stale = UnixListener::bind(&socket_path)?;
        drop(stale);

        let config = UnixSocketStreamConfig {
            socket_path: Utf8PathBuf::from_path_buf(socket_path.clone())
                .map_err(|path| color_eyre::eyre::eyre!("non-UTF8 socket path: {path:?}"))?,
            mode: UnixSocketStreamMode::Listen,
            reconnect_on_eof: false,
        };

        let adapter = UnixSocketStreamAdapter;
        let mut stream = adapter.open(dummy_material_id(), &config, None).await?;
        let mut producer = UnixStream::connect(&socket_path).await?;
        producer.write_all(b"after-stale\n").await?;
        drop(producer);

        let record = timeout(Duration::from_secs(1), stream.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("producer frame missing"))??;
        assert_eq!(record.bytes, b"after-stale");
        Ok(())
    }

    #[sinex_test]
    async fn listen_mode_refuses_non_socket_path() -> xtask::sandbox::TestResult<()> {
        let dir = short_socket_tempdir()?;
        let socket_path = dir.path().join("not-a-socket");
        std::fs::write(&socket_path, b"do not replace")?;
        let config = UnixSocketStreamConfig {
            socket_path: Utf8PathBuf::from_path_buf(socket_path)
                .map_err(|path| color_eyre::eyre::eyre!("non-UTF8 socket path: {path:?}"))?,
            mode: UnixSocketStreamMode::Listen,
            reconnect_on_eof: false,
        };

        let adapter = UnixSocketStreamAdapter;
        let error = match adapter.open(dummy_material_id(), &config, None).await {
            Ok(_) => {
                return Err(color_eyre::eyre::eyre!(
                    "listen mode replaced a non-socket path"
                ));
            }
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("refusing to replace non-socket path")
        );
        Ok(())
    }
}
