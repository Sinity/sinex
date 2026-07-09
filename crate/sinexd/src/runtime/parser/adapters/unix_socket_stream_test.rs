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
async fn test_unix_socket_records_carry_realtime_capture_hint()
-> xtask::sandbox::TestResult<()> {
    let (mut server, client) = make_socket_pair();
    server.write_all(b"line1\nline2\n").await.unwrap();
    drop(server);

    let stream = build_unix_stream(
        dummy_material_id(),
        client,
        Utf8PathBuf::from("/fake/socket"),
        false,
    );
    let records: Vec<_> = stream.collect().await;

    let first = records[0].as_ref().unwrap();
    let second = records[1].as_ref().unwrap();
    let first_ts = match first.source_ts_hint.as_ref() {
        Some(TimingEvidence::RealtimeCapture {
            value,
            capture_source,
        }) => {
            assert_eq!(capture_source, "unix_socket.connect");
            *value
        }
        other => {
            return Err(color_eyre::eyre::eyre!(
                "expected realtime capture timing hint, got {other:?}"
            ));
        }
    };
    let second_ts = match second.source_ts_hint.as_ref() {
        Some(TimingEvidence::RealtimeCapture {
            value,
            capture_source,
        }) => {
            assert_eq!(capture_source, "unix_socket.connect");
            *value
        }
        other => {
            return Err(color_eyre::eyre::eyre!(
                "expected realtime capture timing hint, got {other:?}"
            ));
        }
    };

    assert!(second_ts >= first_ts);
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
async fn connect_mode_reconnects_after_producer_closes_mid_stream()
-> xtask::sandbox::TestResult<()> {
    // Exercises the `reconnect_on_eof: true` path end-to-end: the producer
    // (e.g. a Hyprland compositor restart) closes the connection, and the
    // adapter must reconnect with backoff and keep yielding records from the
    // new connection rather than ending the stream.
    let dir = short_socket_tempdir()?;
    let socket_path = dir.path().join("reconnect.sock");
    let listener = UnixListener::bind(&socket_path)?;

    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.expect("first accept");
        first
            .write_all(b"before-reconnect\n")
            .await
            .expect("write before EOF");
        drop(first); // Producer closes — triggers EOF on the adapter side.

        let (mut second, _) = listener.accept().await.expect("second accept");
        second
            .write_all(b"after-reconnect\n")
            .await
            .expect("write after reconnect");
        // Hold the connection open briefly so the client has time to read
        // before the task (and the socket) drops.
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let initial_conn = UnixStream::connect(&socket_path).await?;
    let utf8_path = Utf8PathBuf::from_path_buf(socket_path)
        .map_err(|path| color_eyre::eyre::eyre!("non-UTF8 socket path: {path:?}"))?;
    let mut stream = build_unix_stream(dummy_material_id(), initial_conn, utf8_path, true);

    let first = timeout(Duration::from_secs(3), stream.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("stream ended before reconnect"))??;
    assert_eq!(first.bytes, b"before-reconnect");

    // Backoff starts at 50ms; give ample margin for the reconnect + accept.
    let second = timeout(Duration::from_secs(5), stream.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("stream ended after reconnect"))??;
    assert_eq!(second.bytes, b"after-reconnect");

    // Frame index and byte offset stay monotonic across the reconnect — the
    // adapter has no replay cursor, so this is the only continuity signal.
    let (first_offset, first_frame) = match first.anchor {
        MaterialAnchor::StreamFrame {
            material_offset,
            frame_index,
        } => (material_offset, frame_index),
        other => {
            return Err(color_eyre::eyre::eyre!("expected stream-frame anchor, got {other:?}"));
        }
    };
    let (second_offset, second_frame) = match second.anchor {
        MaterialAnchor::StreamFrame {
            material_offset,
            frame_index,
        } => (material_offset, frame_index),
        other => {
            return Err(color_eyre::eyre::eyre!("expected stream-frame anchor, got {other:?}"));
        }
    };
    assert!(second_offset > first_offset);
    assert!(second_frame > first_frame);

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
