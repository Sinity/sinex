use serde_json::{Value, json};

#[devkit::test]
fn status_reports_capture_backlog_when_the_daemon_is_unreachable() {
    let root = std::env::temp_dir().join(format!("capture-health-{}", std::process::id()));
    let state = root.join("sinex-session");
    std::fs::create_dir_all(state.join("positions")).unwrap();
    std::fs::write(root.join("token"), b"synthetic-token").unwrap();
    let report = json!({"observed_at":"2026-01-01T00:00:00Z", "spools":[{"binding":"synthetic", "unpublished":{"bytes":42,"records":3,"frames":2}}]});
    std::fs::write(
        state.join("health.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let missing_socket = format!("unix:{}", root.join("missing.sock").display());
    for bare in [false, true] {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_sinexctl"));
        command
            .arg("--endpoint")
            .arg(&missing_socket)
            .arg("--token-file")
            .arg(root.join("token"))
            .env("XDG_STATE_HOME", &root)
            .env_remove("SINEX_CAPTURE_STATE");
        if !bare {
            command.arg("status").arg("--capture-state").arg(&state);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(6),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let status: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(status["local_capture"], report);
        assert!(status["daemon"]["unavailable"].is_string());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn status_errors_keep_the_local_capture_report() {
    use std::io::{BufRead, Read, Write};
    let root = std::env::temp_dir().join(format!("capture-refusal-{}", std::process::id()));
    let state = root.join("state");
    std::fs::create_dir_all(state.join("positions")).unwrap();
    std::fs::write(root.join("token"), b"synthetic-token").unwrap();
    let report = json!({"observed_at":"2026-01-01T00:00:00Z", "spools":[{"binding":"synthetic", "unpublished":{"bytes":42,"records":3,"frames":2}}]});
    std::fs::write(
        state.join("health.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = std::io::BufReader::new(stream);
        let mut length = 0;
        loop {
            let mut line = String::new();
            assert_ne!(reader.read_line(&mut line).unwrap(), 0);
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap();
            }
        }
        reader.read_exact(&mut vec![0; length]).unwrap();
        let reply = serde_json::to_vec(&sinex_protocol::Reply::Refused {
            reason: "synthetic database stall".into(),
            not_built: false,
        })
        .unwrap();
        let stream = reader.get_mut();
        write!(stream, "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", reply.len()).unwrap();
        stream.write_all(&reply).unwrap();
        stream.flush().unwrap();
    });
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--token-file")
        .arg(root.join("token"))
        .arg("status")
        .arg("--capture-state")
        .arg(&state)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["local_capture"], report);
    assert_eq!(status["daemon"]["unavailable"], "synthetic database stall");
    server.join().unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .arg("status")
        .arg("--offline")
        .arg("--database")
        .arg("not-a-database-url")
        .arg("--capture-state")
        .arg(&state)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["local_capture"], report);
    assert!(status["daemon"]["unavailable"].is_string());
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn status_uses_the_module_capture_state_path() {
    let dir = std::env::temp_dir().join(format!("capture-configured-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let state = dir.join("configured-state");
    std::fs::create_dir(&state).unwrap();
    let report = json!({"spools":[],"observed_at":"2026-01-01T00:00:00Z"});
    std::fs::write(
        state.join("health.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .args(["status", "--offline", "--database", "not-a-database-url"])
        .env("SINEX_CAPTURE_STATE", &state)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["local_capture"], report);
    std::fs::remove_dir_all(dir).unwrap();
}
