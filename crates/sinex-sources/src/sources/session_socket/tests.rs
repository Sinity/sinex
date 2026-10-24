use super::*;
use std::io::{Read, Write};

fn parse(line: &str) -> Result<Produced, RecordError> {
    let rec: Map<String, Value> = serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_ns(1).unwrap(),
        line.as_bytes(),
    ))
    .unwrap();
    SessionSocketParser
        .parse(&rec, &(), &crate::RecordContext::none())
        .map(|(mut v, ())| v.remove(0))
}

#[test]
fn neovim_and_kitty_lines_become_typed_events() {
    let Produced::Editor(e) = parse(r#"{"source":"neovim","event":"BufWritePost","path":"/a.rs","filetype":"rust","pid":42,"buf":3}"#).unwrap() else { panic!() };
    assert_eq!(
        (e.event.as_str(), e.path.as_deref(), e.pid),
        ("BufWritePost", Some("/a.rs"), Some(42))
    );
    assert_eq!(e.extra["buf"], 3);
    let Produced::Terminal(t) = parse(
        r#"{"source":"kitty","event":"cmd_end","window_id":7,"cmdline":"ls","exit_status":0}"#,
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        (t.window_id, t.exit_status, t.cmdline.as_deref()),
        (Some(7), Some(0), Some("ls"))
    );
}

#[test]
fn unknown_producers_fail_by_name() {
    assert!(
        parse(r#"{"source":"emacs","event":"x"}"#)
            .unwrap_err()
            .to_string()
            .contains("emacs")
    );
    assert!(parse(r#"{"event":"x"}"#).is_err());
}

fn start_capture() -> (
    PathBuf,
    PathBuf,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<std::io::Result<u64>>,
) {
    let dir = std::env::temp_dir().join(format!("sx-sock-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("s.sock");
    let spool = dir.join("spool");
    let listener = UnixListener::bind(&socket).unwrap();
    let writer = Arc::new(Mutex::new(
        SpoolWriter::open(&spool, Rotation::default()).unwrap(),
    ));
    let (stop, stopped) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        capture_connections(listener, writer, || stopped.try_recv().is_ok())
    });
    (socket, spool, stop, handle)
}

#[test]
fn the_socket_spools_what_connections_write() {
    let (socket, spool, stop, handle) = start_capture();
    let mut client = UnixStream::connect(&socket).unwrap();
    client.write_all(b"{\"source\":\"kitty\",\"event\":\"focus\"}\n{\"source\":\"neovim\",\"event\":\"BufEnter\"}\n").unwrap();
    drop(client);
    stop.send(()).unwrap();
    assert_eq!(handle.join().unwrap().unwrap(), 2);
    let material = driver(spool, "h".into())
        .poll(&Value::Null)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(material.records.len(), 2);
    let _ = std::fs::remove_dir_all(socket.parent().unwrap());
}

#[test]
fn shutdown_drains_sent_records_and_a_partial_line_with_idle_clients_open() {
    let (socket, spool, stop, handle) = start_capture();
    let _idle = UnixStream::connect(&socket).unwrap();
    let mut client = UnixStream::connect(&socket).unwrap();
    client
        .write_all(b"first\nsecond\nlast without newline")
        .unwrap();
    stop.send(()).unwrap();
    assert_eq!(handle.join().unwrap().unwrap(), 3);
    let material = driver(spool, "h".into())
        .poll(&Value::Null)
        .unwrap()
        .material
        .unwrap();
    let lines: Vec<Vec<u8>> = material
        .records
        .iter()
        .map(|(start, end)| {
            let record = serde_json::from_slice(&material.bytes[*start..*end]).unwrap();
            live_stream::received(&record).unwrap().1
        })
        .collect();
    assert_eq!(
        lines,
        [
            b"first".to_vec(),
            b"second".to_vec(),
            b"last without newline".to_vec()
        ]
    );
    // Both clients remain open until after capture has finished.
    drop(client);
    let _ = std::fs::remove_dir_all(socket.parent().unwrap());
}

#[test]
fn reader_panics_are_errors() {
    let handler =
        std::thread::spawn(|| -> std::io::Result<u64> { panic!("synthetic reader failure") });
    assert!(
        join_connection(handler)
            .unwrap_err()
            .to_string()
            .contains("panicked")
    );
}

#[test]
fn spool_errors_are_returned_after_readers_finish() {
    let dir = std::env::temp_dir().join(format!("sx-sock-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let listener = UnixListener::bind(dir.join("s.sock")).unwrap();
    let writer = Arc::new(Mutex::new(
        SpoolWriter::open(dir.join("spool"), Rotation::default()).unwrap(),
    ));
    let poison = writer.clone();
    assert!(
        std::thread::spawn(move || {
            let _lock = poison.lock().unwrap();
            panic!("synthetic poisoned spool");
        })
        .join()
        .is_err()
    );
    let mut client = UnixStream::connect(dir.join("s.sock")).unwrap();
    client.write_all(b"record\n").unwrap();
    drop(client);
    let error = capture_connections(listener, writer, || true).unwrap_err();
    assert!(error.to_string().contains("spool lock poisoned"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn read_shutdown_preserves_queued_bytes_and_refuses_future_writes() {
    let (mut reader, mut client) = UnixStream::pair().unwrap();
    client.write_all(b"first\npartial").unwrap();
    reader.shutdown(Shutdown::Read).unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"first\npartial");
    assert!(client.write(b"future").is_err());
}

#[test]
fn continuously_writing_peer_cannot_extend_shutdown() {
    let (socket, spool, stop, handle) = start_capture();
    let mut client = UnixStream::connect(&socket).unwrap();
    let (writing, started) = std::sync::mpsc::channel();
    let producer = std::thread::spawn(move || {
        let mut sent = 0;
        let mut writing = Some(writing);
        loop {
            match client.write(&[b'x'; 64]) {
                Ok(0) => break,
                Ok(n) => {
                    sent += n;
                    if let Some(writing) = writing.take() {
                        writing.send(()).unwrap();
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
                    break;
                }
            }
        }
        sent
    });
    started.recv().unwrap();
    stop.send(()).unwrap();
    assert_eq!(handle.join().unwrap().unwrap(), 1);
    let sent = producer.join().unwrap();
    let material = driver(spool, "h".into())
        .poll(&Value::Null)
        .unwrap()
        .material
        .unwrap();
    let captured: usize = material
        .records
        .iter()
        .map(|(start, end)| {
            let record = serde_json::from_slice(&material.bytes[*start..*end]).unwrap();
            let bytes = live_stream::received(&record).unwrap().1;
            assert!(bytes.iter().all(|b| *b == b'x'));
            bytes.len()
        })
        .sum();
    assert_eq!(captured, sent);
    let _ = std::fs::remove_dir_all(socket.parent().unwrap());
}

#[test]
fn public_capture_removes_its_socket_on_stop_and_setup_error() {
    let dir = std::env::temp_dir().join(format!("sx-sock-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("s.sock");
    assert_eq!(
        capture(&socket, &dir.join("spool"), Some(Duration::ZERO)).unwrap(),
        0
    );
    assert!(!socket.exists());
    let invalid_spool = dir.join("file");
    std::fs::write(&invalid_spool, b"not a directory").unwrap();
    assert!(capture(&socket, &invalid_spool, Some(Duration::ZERO)).is_err());
    assert!(!socket.exists());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn private_records_stay_unwritten_and_do_not_count_as_captured() {
    let dir = std::env::temp_dir().join(format!("sx-private-{}", uuid::Uuid::now_v7()));
    let mode = sinex_transport::private_mode::PrivateMode::new(dir.join("private"));
    mode.set(true).unwrap();
    let writer = Mutex::new(
        SpoolWriter::open(dir.join("spool"), Rotation::default())
            .unwrap()
            .with_private_mode(mode),
    );
    let (reader, mut client) = UnixStream::pair().unwrap();
    client.write_all(b"private record\n").unwrap();
    drop(client);
    assert_eq!(capture_connection(reader, &writer).unwrap(), 0);
    assert_eq!(writer.lock().unwrap().dropped, 1);
    assert!(
        driver(dir.join("spool"), "h".into())
            .poll(&Value::Null)
            .unwrap()
            .material
            .is_none()
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stop_is_checked_between_accept_batches_with_more_clients_pending() {
    let dir = std::env::temp_dir().join(format!("sx-sock-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("s.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let writer = Arc::new(Mutex::new(
        SpoolWriter::open(dir.join("spool"), Rotation::default()).unwrap(),
    ));
    // More arrivals than one batch, without a timing race: all have queued
    // input before capture starts, and stop is already requested.
    let clients: Vec<_> = (0..=ACCEPT_BATCH)
        .map(|_| {
            let mut client = UnixStream::connect(&socket).unwrap();
            client.write_all(b"accepted record\n").unwrap();
            client
        })
        .collect();
    assert_eq!(
        capture_connections(listener, writer, || true).unwrap(),
        ACCEPT_BATCH as u64
    );
    let material = driver(dir.join("spool"), "h".into())
        .poll(&Value::Null)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(material.records.len(), ACCEPT_BATCH);
    drop(clients);
    let _ = std::fs::remove_dir_all(dir);
}
