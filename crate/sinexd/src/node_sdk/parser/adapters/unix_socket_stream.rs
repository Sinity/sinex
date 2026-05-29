//! Adapter for line-delimited Unix domain socket streams.
//!
//! Connects to a Unix socket (e.g., Hyprland IPC at
//! `/run/user/1000/hypr/<instance>/`.hyprland/socket2.sock`) and reads
//! newline-terminated messages. Each line is one [`SourceRecord`].
//!
//! Cursor is `()` — Hyprland IPC is a live stream with no replay.
//! Anchor is [`MaterialAnchor::StreamFrame`] with a monotonic byte/line offset.
//!
//! When the server closes the connection (`EOF`), the stream ends. If
//! `reconnect_on_eof` is `true`, the adapter reconnects and continues
//! streaming. Reconnection attempts use a simple exponential back-off
//! (50 ms, 100 ms, 200 ms … capped at 2 s).

use async_trait::async_trait;
use camino::Utf8PathBuf;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::node_sdk::parser::{InputShapeAdapter, ParserError, ParserResult};

// =============================================================================
// UnixSocketStreamAdapter
// =============================================================================

/// Adapter for a line-delimited Unix domain socket.
///
/// Suitable for Hyprland IPC (`socket2.sock`) and similar event sockets that
/// emit newline-terminated JSON or plain-text messages.
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

    /// If true, reconnect when the server closes the connection.
    #[serde(default)]
    pub reconnect_on_eof: bool,
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
        let reconnect = config.reconnect_on_eof;

        // Connect eagerly so we surface errors at open time.
        let stream_conn = UnixStream::connect(socket_path.as_std_path())
            .await
            .map_err(|e| {
                ParserError::Adapter(format!(
                    "failed to connect to unix socket {socket_path}: {e}"
                ))
            })?;

        let stream = build_unix_stream(material_id, stream_conn, socket_path, reconnect);
        Ok(stream)
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(UnixSocketStreamCursor)
    }
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
pub async fn make_socket_pair() -> (UnixStream, UnixStream) {
    let (a, b) = UnixStream::pair().unwrap();
    (a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    use xtask::sandbox::prelude::sinex_test;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    #[sinex_test]
    async fn test_unix_socket_yields_one_record_per_line() -> xtask::sandbox::TestResult<()> {
        let (mut server, client) = make_socket_pair().await;
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
        let (mut server, client) = make_socket_pair().await;
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
        let (mut server, client) = make_socket_pair().await;
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
        let (mut server, client) = make_socket_pair().await;
        server.write_all(b"a\nb\nc\n").await.unwrap();
        drop(server);

        let stream = build_unix_stream(
            dummy_material_id(),
            client,
            Utf8PathBuf::from("/fake/socket"),
            false,
        );
        let records: Vec<_> = stream.collect().await;

        let indices: Vec<u64> = records
            .iter()
            .map(|r| match &r.as_ref().unwrap().anchor {
                MaterialAnchor::StreamFrame { frame_index, .. } => *frame_index,
                _ => panic!("wrong anchor"),
            })
            .collect();

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
}
