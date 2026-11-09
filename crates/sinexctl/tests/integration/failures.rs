//! Failures reach the command line and MCP with the shared table's codes: the
//! daemon's typed failure decides the exit code and the tool error's kind,
//! and the CLI classifies its own failures the same way.
use command_surface::{Failure, FailureKind};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};

/// Read one HTTP request and return its JSON body.
fn request(stream: &mut TcpStream) -> Value {
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut length = 0;
    loop {
        let mut line = String::new();
        assert_ne!(
            reader.read_line(&mut line).unwrap(),
            0,
            "request ended early"
        );
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap();
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// A daemon stand-in that answers each connection with the next of
/// `answers` (an HTTP status and a body) and returns the requests it read.
fn daemon(answers: Vec<(u16, String)>) -> (String, std::thread::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, body) in answers {
            let (mut stream, _) = listener.accept().unwrap();
            requests.push(request(&mut stream));
            write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
        requests
    });
    (endpoint, server)
}

fn failed(failure: &Failure) -> (u16, String) {
    let status = failure.kind.http_status().unwrap_or(503);
    let body = json!({"outcome": "failed", "failure": failure}).to_string();
    (status, body)
}

/// One failure of each kind the daemon reports, as `/v1` answers it.
fn daemon_failures() -> Vec<Failure> {
    vec![
        Failure::new(
            FailureKind::InvalidQuery,
            "expected a condition after `where`",
        ),
        Failure::new(FailureKind::UnknownField, "no field `exit_cod`")
            .with_candidates(vec!["exit_code".into()]),
        Failure::new(FailureKind::NotFound, "no event 0190a0b0"),
        Failure::new(
            FailureKind::Refused,
            "this token grants reader access; the request needs agent",
        ),
        Failure::new(
            FailureKind::Timeout,
            "canceling statement due to statement timeout",
        )
        .with_code("statement_timeout"),
        Failure::new(FailureKind::NotRunning, "pool closed").with_code("database_unavailable"),
        sinex_protocol::stopped_early(Failure::new(
            FailureKind::Refused,
            "restore refused: a record has another live reading",
        )),
    ]
}

fn sinexctl(endpoint: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .args(["--endpoint", endpoint, "--token-file", "/dev/null"])
        .args(args)
        .output()
        .unwrap()
}

#[devkit::test]
fn each_failure_kind_reaches_cli_and_mcp() {
    let expected_exit = [2, 2, 4, 3, 7, 6, 5];
    for (failure, exit) in daemon_failures().into_iter().zip(expected_exit) {
        // A stopped operation carries what moved beside its failure.
        let body = if failure.kind == FailureKind::StoppedEarly {
            json!({"outcome": "stopped_early", "failure": failure, "result": {"batches": 1}})
                .to_string()
        } else {
            failed(&failure).1
        };
        let status = failure.kind.http_status().unwrap_or(503);
        let (endpoint, server) = daemon(vec![(status, body.clone()), (status, body.clone())]);

        let output = sinexctl(&endpoint, &["show", "sinex:ev/0190a0b0"]);
        assert_eq!(output.status.code(), Some(exit), "{failure:?}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(&failure.message), "{stderr}");

        let output = sinexctl(&endpoint, &["--json", "show", "sinex:ev/0190a0b0"]);
        assert_eq!(output.status.code(), Some(exit), "{failure:?}: {output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(printed["error"]["kind"], failure.kind.name(), "{printed}");
        server.join().unwrap();

        let (endpoint, server) = daemon(vec![(status, body)]);
        let reply = mcp_call(
            &endpoint,
            "sinex_show",
            json!({"reference": "sinex:ev/0190a0b0"}),
        );
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert_eq!(
            reply["result"]["structuredContent"]["error"]["kind"],
            failure.kind.name(),
            "{reply}"
        );
        let text = reply["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains(&failure.message), "{text}");
        server.join().unwrap();
    }
}

/// Call one tool through `sinexctl mcp` over stdio and return its response.
fn mcp_call(endpoint: &str, tool: &str, arguments: Value) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .args(["--endpoint", endpoint, "--token-file", "/dev/null", "mcp"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut send = |message: Value| {
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    };
    let mut receive = |id: u64| loop {
        let mut line = String::new();
        assert_ne!(stdout.read_line(&mut line).unwrap(), 0, "MCP server closed");
        let message: Value = serde_json::from_str(&line).unwrap();
        if message["id"] == id {
            break message;
        }
    };
    send(
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "failures-test", "version": "0"},
        }}),
    );
    receive(1);
    send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
    send(
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": tool,
            "arguments": arguments,
        }}),
    );
    let reply = receive(2);
    child.kill().unwrap();
    child.wait().unwrap();
    reply
}

#[devkit::test]
fn sinexctl_client_failures_use_shared_table() {
    // A missing token file is configuration the CLI lacks.
    let output = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .args([
            "--endpoint",
            "http://127.0.0.1:9",
            "--token-file",
            "/nonexistent/synthetic/token",
            "--json",
            "query",
            "events",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(printed["error"]["kind"], "not_configured", "{printed}");

    // An endpoint that is not one is a request to fix.
    let output = sinexctl("relative/socket", &["--json", "query", "events"]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(printed["error"]["kind"], "bad_request", "{printed}");

    // Nothing listening: the daemon is not running.
    let unused = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", unused.local_addr().unwrap());
    drop(unused);
    let output = sinexctl(&endpoint, &["--json", "query", "events"]);
    assert_eq!(output.status.code(), Some(6), "{output:?}");
    let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(printed["error"]["code"], "daemon_unreachable", "{printed}");

    // An answer this build cannot read comes from another protocol version.
    let (endpoint, server) = daemon(vec![(200, "{\"outcome\":\"surprising\"}".into())]);
    let output = sinexctl(&endpoint, &["--json", "query", "events"]);
    assert_eq!(output.status.code(), Some(6), "{output:?}");
    let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        printed["error"]["code"], "daemon_protocol_mismatch",
        "{printed}"
    );
    server.join().unwrap();

    // A stage report that does not decode is the same mismatch, not a refusal.
    let (endpoint, server) = daemon(vec![(
        200,
        json!({"outcome": "ok", "result": {"not": "a stage report"}}).to_string(),
    )]);
    let output = sinexctl(
        &endpoint,
        &["stage-show", "0190a0b0-0000-7000-8000-000000000001"],
    );
    assert_eq!(output.status.code(), Some(6), "{output:?}");
    server.join().unwrap();
}

#[devkit::test]
fn empty_query_exits_1() {
    let empty = json!({"outcome": "ok", "result": {
        "table": "events", "query": "events where source = nothing", "read_as": "synthetic query",
        "columns": ["id"], "rows": [], "total": 0, "resolved": {"zone": "UTC"},
        "page": {"size": 50, "cursor": null}
    }})
    .to_string();
    let (endpoint, server) = daemon(vec![(200, empty.clone()), (200, empty)]);
    let output = sinexctl(&endpoint, &["query", "events where source = nothing"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let output = sinexctl(
        &endpoint,
        &["--json", "--all", "query", "events where source = nothing"],
    );
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(printed["rows"], json!([]));
    server.join().unwrap();
}
