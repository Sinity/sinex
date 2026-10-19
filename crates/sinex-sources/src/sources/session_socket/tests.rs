use super::*;
use std::io::Write;

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

#[test]
fn the_socket_spools_what_connections_write() {
    let dir = std::env::temp_dir().join(format!("sinex-sock-{}", uuid::Uuid::now_v7()));
    let socket = dir.join("s.sock");
    let spool = dir.join("spool");
    let (s2, sp2) = (socket.clone(), spool.clone());
    let h = std::thread::spawn(move || capture(&s2, &sp2, Some(Duration::from_millis(800))));
    let start = Instant::now();
    while !socket.exists() && start.elapsed() < Duration::from_secs(2) {
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut c = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    c.write_all(b"{\"source\":\"kitty\",\"event\":\"focus\"}\n{\"source\":\"neovim\",\"event\":\"BufEnter\"}\n").unwrap();
    drop(c);
    assert_eq!(h.join().unwrap().unwrap(), 2);
    let m = driver(spool, "h".into())
        .poll(&Value::Null)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(m.records.len(), 2);
    let _ = std::fs::remove_dir_all(dir);
}
