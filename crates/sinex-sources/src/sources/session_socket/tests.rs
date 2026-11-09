use super::*;
use serde_json::json;
use sinex_source_core::Reader;
use std::io::{Read, Write};

fn parse(line: &str) -> Result<Produced, RecordError> {
    let rec: Map<String, Value> = serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_ns(1).unwrap(),
        line.as_bytes(),
    ))
    .unwrap();
    SessionSocketParser
        .parse(&rec, &(), &sinex_source_core::RecordContext::none())
        .map(|(mut v, ())| v.remove(0))
}

#[devkit::test]
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

#[devkit::test]
fn kitty_command_output_and_scrollback_are_read_as_plain_text() {
    let Produced::Terminal(t) = parse(
        r#"{"source":"kitty","event":"cmd_end","window_id":7,"cmdline":"ls","exit_status":0,"output":"\u001b[32ma\u001b[m b\r\n"}"#,
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(t.output.as_deref(), Some("a b\n"));
    let Produced::Scrollback(c) = parse(
        r#"{"source":"kitty","event":"scrollback","window_id":7,"kitty_pid":9,"host":"box","title":"zsh","cwd":"/h","lines":40,"from_line":38,"text":"\u001b[1mx\u001b[m  \r\ny\r\n"}"#,
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        (c.window_id, c.kitty_pid, c.host.as_deref(), c.lines),
        (7, Some(9), Some("box"), 40)
    );
    assert!(c.continues);
    assert_eq!((c.new_lines, c.text.as_str()), (2, "x\ny"));
    let Produced::Scrollback(first) = parse(
        r#"{"source":"kitty","event":"scrollback","window_id":7,"lines":1,"from_line":0,"text":"z"}"#,
    )
    .unwrap() else {
        panic!()
    };
    assert!(!first.continues);
    assert!(parse(r#"{"source":"kitty","event":"scrollback","lines":1,"from_line":0}"#).is_err());
    assert!(
        parse(r#"{"source":"kitty","event":"scrollback","window_id":7,"from_line":0}"#).is_err()
    );
}

#[devkit::test]
fn shell_integration_lines_become_reported_commands() {
    let started =
        serde_json::to_value(Timestamp::from_unix_ns(1_700_000_000_123_456_789).unwrap()).unwrap();
    let line = json!({
        "source": "shell", "event": "command",
        "execution": "01900000-0000-7000-8000-000000000001", "shell": "zsh", "host": "box",
        "command": "  echo 'a  b'  ", "cwd": "/work", "started_at": started,
        "duration_ns": 2_000_000, "exit_code": -9, "session": "4242", "atuin_id": "row-1",
        "git_identity": {"common_dir": "/work/.git", "root_commits": ["abc"], "head": "def", "branch": "main"},
        "terminal": "kitty"
    });
    let Produced::Command(c) = parse(&line.to_string()).unwrap() else {
        panic!()
    };
    assert_eq!(c.execution, "01900000-0000-7000-8000-000000000001");
    // The command line is kept byte for byte, outer spaces included.
    assert_eq!(c.command, "  echo 'a  b'  ");
    assert_eq!(c.started_at.unix_ns(), 1_700_000_000_123_456_789);
    assert_eq!((c.duration_ns, c.exit_code), (Some(2_000_000), Some(-9)));
    assert_eq!(c.atuin_id.as_deref(), Some("row-1"));
    assert_eq!(c.git_identity.unwrap().root_commits, ["abc"]);
    assert_eq!(c.extra["terminal"], "kitty");
    // A line without its token or start is a record failure, never a guess.
    for missing in ["execution", "started_at", "host", "command"] {
        let mut partial = line.clone();
        partial.as_object_mut().unwrap().remove(missing);
        let error = parse(&partial.to_string()).unwrap_err().to_string();
        assert!(error.contains(missing), "{missing}: {error}");
    }
}

#[devkit::test]
fn a_shell_report_written_by_the_integration_reads_back_as_itself() {
    let report = ShellReport {
        execution: "01900000-0000-7000-8000-000000000002".into(),
        shell: "bash".into(),
        host: "box".into(),
        command: "printf '%s\\n' \"a  b\"".into(),
        cwd: None,
        started_at: Timestamp::from_unix_ns(1_700_000_000_000_000_001).unwrap(),
        duration_ns: None,
        exit_code: Some(1),
        session: Some("77".into()),
        atuin_id: None,
        git_identity: None,
    };
    let Produced::Command(c) = parse(&shell_line(&report).to_string()).unwrap() else {
        panic!()
    };
    assert_eq!(
        (
            c.execution,
            c.shell,
            c.host,
            c.command,
            c.started_at,
            c.exit_code,
            c.session
        ),
        (
            report.execution,
            report.shell,
            report.host,
            report.command,
            report.started_at,
            report.exit_code,
            report.session
        )
    );
    assert!(c.extra.is_empty());
}

#[devkit::test]
fn sending_without_a_listener_fails_fast_and_a_listener_receives_one_line() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("session.sock");
    let line = json!({"source": "shell", "event": "command"});
    assert!(send(&socket, &line, Duration::from_millis(200)).is_err());
    let listener = UnixListener::bind(&socket).unwrap();
    send(&socket, &line, Duration::from_millis(200)).unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    let mut received = String::new();
    stream.read_to_string(&mut received).unwrap();
    assert_eq!(received, format!("{line}\n"));
}

#[devkit::test]
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
    let (socket, spool, stop, handle, _) = start_capture_with_writer();
    (socket, spool, stop, handle)
}

#[allow(clippy::type_complexity)]
fn start_capture_with_writer() -> (
    PathBuf,
    PathBuf,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<std::io::Result<u64>>,
    Arc<Mutex<SpoolWriter>>,
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
    let shared = writer.clone();
    let handle = std::thread::spawn(move || {
        capture_connections(listener, writer, || stopped.try_recv().is_ok())
    });
    (socket, spool, stop, handle, shared)
}

/// A burst of reports is synced as one group, not one sync per line, and
/// every line is in the spool when capture stops.
#[devkit::test]
fn a_burst_of_lines_is_synced_once() {
    let (socket, spool, stop, handle, writer) = start_capture_with_writer();
    let mut client = UnixStream::connect(&socket).unwrap();
    let burst: String = (0..200)
        .map(|n| format!("{{\"source\":\"kitty\",\"event\":\"focus\",\"n\":{n}}}\n"))
        .collect();
    client.write_all(burst.as_bytes()).unwrap();
    drop(client);
    stop.send(()).unwrap();
    assert_eq!(handle.join().unwrap().unwrap(), 200);
    let syncs = writer.lock().unwrap().syncs();
    assert!(
        (1..=2).contains(&syncs),
        "200 lines cost {syncs} syncs; one group (plus at most one timer tick) is expected"
    );
    let material = driver(spool, "h".into())
        .poll(&Value::Null)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(material.records.len(), 200);
    let _ = std::fs::remove_dir_all(socket.parent().unwrap());
}

#[devkit::test]
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

#[devkit::test]
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
            let record =
                serde_json::from_slice(&crate::test_support::body(&material)[*start..*end])
                    .unwrap();
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

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
fn read_shutdown_preserves_queued_bytes_and_refuses_future_writes() {
    let (mut reader, mut client) = UnixStream::pair().unwrap();
    client.write_all(b"first\npartial").unwrap();
    reader.shutdown(Shutdown::Read).unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"first\npartial");
    assert!(client.write(b"future").is_err());
}

#[devkit::test]
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
            let record =
                serde_json::from_slice(&crate::test_support::body(&material)[*start..*end])
                    .unwrap();
            let bytes = live_stream::received(&record).unwrap().1;
            assert!(bytes.iter().all(|b| *b == b'x'));
            bytes.len()
        })
        .sum();
    assert_eq!(captured, sent);
    let _ = std::fs::remove_dir_all(socket.parent().unwrap());
}

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
fn capture_keeps_repository_and_terminal_identity_beside_each_recorded_cwd() {
    for event in ["cmd_start", "scrollback"] {
        let line = serde_json::json!({"source":"kitty","event":event,"cwd":"/captured/worktree",
            "window_id":7,"lines":1,"from_line":0,"text":"example",
            "git_identity":{"common_dir":"/captured/repo/.git","root_commits":["root"],"head":"sha","branch":"branch"},
            "terminal_identity":{"host":"box","adapter":"kitty","process_id":9,"window_id":"7","parent_process_id":8}});
        let parsed = parse(&line.to_string()).unwrap();
        let extra = match &parsed {
            Produced::Terminal(event) => &event.extra,
            Produced::Scrollback(event) => &event.extra,
            _ => panic!("terminal source"),
        };
        assert_eq!(extra["git_identity"]["common_dir"], "/captured/repo/.git");
        assert_eq!(extra["terminal_identity"]["window_id"], "7");
    }
}
