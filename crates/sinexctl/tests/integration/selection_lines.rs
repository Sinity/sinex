//! Actual CLI requests and executable continuations over the HTTP boundary.
use super::query_views::request;
use serde_json::{Value, json};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    process::{Command, Stdio},
};

fn reply(stream: &mut TcpStream, body: &Value) {
    let body = body.to_string();
    write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
}
fn answer() -> Value {
    json!({"outcome":"ok","value":{"population":null,"references":[],"steps":[]}})
}
fn cli(endpoint: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sinexctl"));
    command.args(["--endpoint", endpoint, "--token-file", "/dev/null"]);
    command
}

#[devkit::test]
fn query_read_and_save_reach_the_declared_route_with_their_actual_options() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let incoming = request(&mut stream);
        let mut body = answer();
        body["value"]["steps"] = json!([
            {"verb":"read","outcome":{"status":"done","reply":{"kind":"trace","reply":[]}}},
            {"verb":"save","outcome":{"status":"done","reply":{"kind":"written","reply":{"event":"original-event","material":"original-material"}}}}
        ]);
        reply(&mut stream, &body);
        incoming
    });
    let output = cli(&endpoint)
        .args([
            "--json",
            "--request-id",
            "fixture",
            "--deadline",
            "none",
            "--budget",
            "31",
            "--page-size",
            "2",
            "events where source = synthetic",
            "read",
            "--view",
            "trace",
            "--cursor",
            "walk-page",
            "save",
            "read",
            "--parameter",
            "source:text",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        body["value"]["steps"][1]["outcome"]["reply"]["reply"]["event"],
        "original-event"
    );
    let (path, request) = server.join().unwrap();
    assert_eq!(path, "/v1/rows");
    assert_eq!(request["request_id"], "fixture");
    assert_eq!(request["deadline"], Value::Null);
    assert_eq!(request["budget_tokens"], 31);
    assert_eq!(
        request["arguments"]["selection"],
        "events where source = synthetic"
    );
    assert_eq!(request["arguments"]["page_size"], 2);
    assert_eq!(
        request["arguments"]["steps"],
        json!([
            {"verb":"read","view":"trace","cursor":"walk-page"},
            {"verb":"save","name":"read","parameter":["source:text"]}
        ])
    );
}

#[devkit::test]
fn the_printed_continuation_executes_the_exact_read_without_repeating_save() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let selection = "events where text = \"synthetic ' quote; $HOME\"";
    let continued = json!({"operation":"rows","request_id":"original-read","deadline":null,"budget_tokens":17,"cursor":"next-page","format":"json","arguments":{"selection":selection,"accepted":"01900000-0000-7000-8000-000000000001","steps":[{"verb":"read","view":"trace","at":"7","page_size":3,"cursor":"walk-next"}]}});
    let expected = continued.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let initial = request(&mut stream);
        let mut body = answer();
        body["value"]["steps"] = json!([{"verb":"save","outcome":{"status":"done","reply":{"kind":"written","reply":{"event":"original-event"}}}}]);
        body["next"] = json!([{"label":"next read","request":continued}]);
        reply(&mut stream, &body);
        let (mut stream, _) = listener.accept().unwrap();
        let followed = request(&mut stream);
        let mut body = answer();
        body["value"]["steps"] = json!([{"verb":"read","outcome":{"status":"done","reply":{"kind":"trace","reply":[]}}}]);
        reply(&mut stream, &body);
        (initial, followed)
    });
    let first = cli(&endpoint)
        .args([selection, "save", "fixture"])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let text = String::from_utf8(first.stdout).unwrap();
    let printed = text
        .lines()
        .find_map(|line| line.strip_prefix("next: \"next read\": "))
        .expect(&text);
    let tail = printed.strip_prefix("sinexctl ").unwrap();
    let script = format!(
        "{} {tail}",
        command_surface::quote_argument(env!("CARGO_BIN_EXE_sinexctl"))
    );
    let followed = Command::new("sh").args(["-c", &script]).output().unwrap();
    assert!(
        followed.status.success(),
        "{}",
        String::from_utf8_lossy(&followed.stderr)
    );
    let body: Value = serde_json::from_slice(&followed.stdout).unwrap();
    assert_eq!(body["outcome"], "ok");
    let (initial, followed) = server.join().unwrap();
    assert_eq!(initial.1["arguments"]["steps"][0]["verb"], "save");
    assert_eq!(followed.0, "/v1/rows");
    assert_eq!(followed.1, expected);
}

#[devkit::test]
fn stdin_references_preserve_order_and_partial_failures_preserve_the_receipt() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let incoming = request(&mut stream);
        let mut body = answer();
        body["value"]["steps"] = json!([{"verb":"read","outcome":{"status":"done","reply":{"kind":"records","reply":[]}}}]);
        reply(&mut stream, &body);
        let (mut stream, _) = listener.accept().unwrap();
        let mixed = request(&mut stream);
        let mut body = answer();
        body["value"]["steps"] = json!([
            {"verb":"save","outcome":{"status":"done","reply":{"kind":"written","reply":{"event":"committed"}}}},
            {"verb":"read","outcome":{"status":"failed","error":{"kind":"refused","message":"synthetic refusal","retryable":false}}}
        ]);
        reply(&mut stream, &body);
        (incoming, mixed)
    });
    let mut child = cli(&endpoint)
        .args(["--json", "-", "read"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let references = [
        "sinex:ev/01900000-0000-7000-8000-000000000002",
        "sinex:ev/01900000-0000-7000-8000-000000000001",
    ];
    writeln!(
        child.stdin.take().unwrap(),
        "{}\n{}",
        references[0],
        references[1]
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = cli(&endpoint)
        .args(["--json", "events", "save", "fixture", "read"])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        body["value"]["steps"][0]["outcome"]["reply"]["reply"]["event"],
        "committed"
    );
    let ((path, request), mixed) = server.join().unwrap();
    assert_eq!(path, "/v1/rows");
    assert_eq!(request["arguments"]["references"], json!(references));
    assert!(request["arguments"].get("selection").is_none());
    assert_eq!(mixed.1["arguments"]["steps"][0]["verb"], "save");
}

#[devkit::test]
fn invalid_selection_requests_fail_before_loading_credentials() {
    for args in [
        vec!["events", "where", "source", "=", "synthetic"],
        vec![
            "--request",
            r#"{"operation":"rows","request_id":"invalid","arguments":{"selection":"events","unknown":true}}"#,
        ],
        vec![
            "--request",
            r#"{"operation":"stage","request_id":"invalid","arguments":{}}"#,
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
            .args(["--json", "--token-file", "/missing/synthetic-token"])
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let body: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(body["outcome"], "error");
        assert_eq!(body["error"]["kind"], "bad_request");
        assert!(output.stderr.is_empty());
    }
}

#[devkit::test]
fn a_typed_request_from_stdin_keeps_its_original_options() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let expected = json!({"operation":"rows","request_id":"stdin-original","deadline":null,"cursor":"next","format":"json","arguments":{"selection":"events","steps":[{"verb":"read","view":"full"}]}});
    let input = expected.to_string();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let incoming = request(&mut stream);
        let mut body = answer();
        body["value"]["steps"] = json!([{"verb":"read","outcome":{"status":"done","reply":{"kind":"records","reply":[]}}}]);
        reply(&mut stream, &body);
        incoming
    });
    let mut child = cli(&endpoint)
        .args(["--request", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["outcome"], "ok");
    let (path, request) = server.join().unwrap();
    assert_eq!(path, "/v1/rows");
    assert_eq!(request, expected);
}

#[devkit::test]
fn moment_note_keeps_text_context_options_and_explicit_request_identity() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let incoming = request(&mut stream);
        let mut body = answer();
        body["value"]["steps"] = json!([{"verb":"note","outcome":{"status":"done","reply":{"kind":"written","reply":{"event":"original-note"}}}}]);
        reply(&mut stream, &body);
        incoming
    });
    let output = cli(&endpoint)
        .args([
            "--json",
            "--request-id",
            "moment-fixture",
            "note",
            "read",
            "--at",
            "2026-09-14T08:00:00Z",
            "--cwd",
            "/src/tide",
            "--command",
            "printf synthetic",
            "--app",
            "synthetic-terminal",
            "--title",
            "synthetic title",
            "--command-record",
            "synthetic-command",
            "--focus-record",
            "synthetic-focus",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (path, body) = server.join().unwrap();
    assert_eq!(path, "/v1/rows");
    assert_eq!(body["request_id"], "moment-fixture");
    assert_eq!(
        body["arguments"],
        json!({"steps":[{"verb":"note","text":"read",
        "at":"2026-09-14T08:00:00Z","cwd":"/src/tide","command":"printf synthetic",
        "app":"synthetic-terminal","title":"synthetic title", "command_record":"synthetic-command","focus_record":"synthetic-focus"}]})
    );
    let answer: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        answer["value"]["steps"][0]["outcome"]["reply"]["reply"]["event"],
        "original-note"
    );
}
