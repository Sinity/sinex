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

        let stopped = failure.kind == FailureKind::StoppedEarly;
        let mcp_body = if stopped {
            body
        } else {
            serde_json::to_string(&command_surface::Answer::<Value>::error(failure.clone()))
                .unwrap()
        };
        let (endpoint, server) = daemon(vec![(status, mcp_body)]);
        let reply = if stopped {
            mcp_call(
                &endpoint,
                "sinex_archive",
                json!({"target":"sinex:ev/0190a0b0"}),
            )
        } else {
            mcp_call(
                &endpoint,
                "sinex_read",
                json!({
                    "arguments":{"references":["sinex:ev/0190a0b0"], "steps":[{"verb":"read"}]},
                }),
            )
        };
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert_eq!(
            reply["result"]["structuredContent"]["error"]["kind"],
            failure.kind.name(),
            "{reply}"
        );
        let text = reply["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains(&failure.message), "{text}");
        if stopped {
            assert_eq!(reply["result"]["structuredContent"]["result"]["batches"], 1);
        }
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

#[devkit::test]
fn a_one_shot_request_uses_one_thread() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let child = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .args([
            "--endpoint",
            &endpoint,
            "--token-file",
            "/dev/null",
            "--json",
            "status",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    request(&mut stream);
    let threads = std::fs::read_dir(format!("/proc/{}/task", child.id()))
        .unwrap()
        .count();
    let body = json!({"outcome": "ok", "result": {"daemon": "running"}}).to_string();
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    drop(stream);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        threads, 1,
        "one-shot requests run on the process's main thread"
    );
}

#[devkit::test]
fn unreachable_names_service_and_endpoint() {
    let unused = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = format!("http://{}", unused.local_addr().unwrap());
    drop(unused);
    let root = tempfile::tempdir().unwrap();
    let unix = format!("unix:{}", root.path().join("missing.sock").display());
    for endpoint in [tcp, unix] {
        let output = sinexctl(&endpoint, &["--json", "query", "events"]);
        assert_eq!(output.status.code(), Some(6), "{output:?}");
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .starts_with(&format!("the daemon is not reachable at {endpoint}: ")),
            "{result}"
        );
        let hint = result["error"]["hint"].as_str().unwrap();
        assert!(hint.contains("systemctl start sinexd"), "{hint}");
        assert!(hint.contains("sinexd run"), "{hint}");
    }
}

#[devkit::test]
fn missing_token_names_token_files() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("missing.token");
    let output = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .arg("--token-file")
        .arg(&path)
        .args(["--json", "query", "events"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&path.display().to_string())
    );
    let hint = result["error"]["hint"].as_str().unwrap();
    for name in [
        "read.token",
        "agent.token",
        "operator.token",
        "--token-file",
    ] {
        assert!(hint.contains(name), "{hint}");
    }
}

#[devkit::test]
fn closed_stdout_exits_141_without_panic() {
    let body = json!({"outcome":"ok", "result": {
        "rows":[{"text":"x".repeat(1024 * 1024)}],
        "columns":["text"], "total":1,
        "resolved":{"zone":"UTC"}, "page":{"size":50,"cursor":null}
    }})
    .to_string();
    let (endpoint, server) = daemon(vec![(200, body)]);
    let mut child = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .args([
            "--endpoint",
            &endpoint,
            "--token-file",
            "/dev/null",
            "--json",
            "query",
            "events",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    stdout.read_exact(&mut [0; 1]).unwrap();
    drop(stdout);
    let output = child.wait_with_output().unwrap();
    server.join().unwrap();
    assert_eq!(output.status.code(), Some(141), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
}

#[devkit::test]
fn mcp_page_keeps_its_continuation() {
    let continued = json!({
        "operation":"rows", "arguments":{"selection":"events", "page_size":1},
        "cursor":"next-page", "request_id":"next-read", "deadline":null,
        "budget_tokens":400, "format":"json",
    });
    let body = json!({"outcome":"ok", "value":{
        "population":{
            "table":"events", "query":"events", "read_as":"events", "rows":[{"id":"first"}],
            "columns":["id"], "total":2,
            "resolved":{"now":"2026-09-15T00:00:00Z", "zone":"UTC", "day_start":"00:00", "commit_seq":42},
            "page":{"size":1,"cursor":"next-page"},
        },
        "references":[], "steps":[],
    }, "notes":[sinex_protocol::DATA_NOTICE], "next":[{"label":"next", "request":continued} ]});
    let mut last = body.clone();
    last["value"]["population"]["rows"] = json!([{"id":"second"}]);
    last["value"]["population"]["page"]["cursor"] = Value::Null;
    last.as_object_mut().unwrap().remove("next");
    let (endpoint, server) = daemon(vec![(200, body.to_string()), (200, last.to_string())]);
    let reply = mcp_call(
        &endpoint,
        "sinex_read",
        json!({
            "arguments":{"selection":"events", "page_size":1}, "format":"json", "deadline":null,
        }),
    );
    let next = reply["result"]["structuredContent"]["next"][0]["request"].clone();
    let followed = mcp_call(&endpoint, "sinex_read", next);
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1], continued);
    assert_eq!(
        followed["result"]["structuredContent"]["value"]["population"]["rows"],
        json!([{"id":"second"}])
    );
    assert_eq!(requests[0]["operation"], "rows");
    assert_eq!(
        requests[0]["arguments"],
        json!({"selection":"events", "page_size":1})
    );
    assert_eq!(reply["result"]["isError"], false, "{reply}");
    let answer = &reply["result"]["structuredContent"];
    assert_eq!(answer["value"]["population"]["page"]["cursor"], "next-page");
    assert_eq!(answer["next"][0]["request"], continued);
    assert!(
        reply["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains(sinex_protocol::DATA_NOTICE)
    );
}

#[devkit::test]
fn mcp_failed_step_keeps_prior_output_and_reports_failure() {
    let failure = Failure::new(
        FailureKind::BadRequest,
        "trace needs event references, not grouped rows",
    );
    let body = json!({"outcome":"ok", "value":{
        "population":null, "references":[], "steps":[
            {"verb":"read", "outcome":{"status":"done", "reply":{"kind":"records", "reply":[{"text":"prior result"}]}}},
            {"verb":"read", "outcome":{"status":"failed", "error":failure}},
        ],
    }}).to_string();
    let (endpoint, server) = daemon(vec![(200, body)]);
    let reply = mcp_call(
        &endpoint,
        "sinex_read",
        json!({
            "arguments":{"selection":"events | group by source", "steps":[{"verb":"read"}, {"verb":"read", "view":"trace"}]},
            "format":"json", "deadline":null,
        }),
    );
    server.join().unwrap();
    assert_eq!(reply["result"]["isError"], true, "{reply}");
    let steps = &reply["result"]["structuredContent"]["value"]["steps"];
    assert_eq!(
        steps[0]["outcome"]["reply"]["reply"][0]["text"],
        "prior result"
    );
    assert_eq!(steps[1]["outcome"]["error"]["kind"], "bad_request");
}

#[devkit::test]
fn mcp_save_without_request_id_keeps_retry_identity_and_uses_write_tool() {
    let arguments = json!({"arguments":{"selection":"events where source = $source", "steps":[{"verb":"save","name":"fixture","parameter":["source:text"]}]}, "format":"json", "deadline":null});
    let denied = mcp_call("http://127.0.0.1:1", "sinex_read", arguments.clone());
    assert_eq!(denied["result"]["isError"], true, "{denied}");
    assert_eq!(
        denied["result"]["structuredContent"]["error"]["kind"],
        "refused"
    );
    let body = json!({"outcome":"ok", "value":{"population":null,"references":[],"steps":[{"verb":"save","outcome":{"status":"done","reply":{"kind":"written","reply":{"event":"one","material":"source","citation":"sinex:record/source/0"}}}}]}}).to_string();
    let (endpoint, server) = daemon(vec![(200, body.clone()), (200, body)]);
    let first = mcp_call(&endpoint, "sinex_write", arguments.clone());
    let second = mcp_call(&endpoint, "sinex_write", arguments.clone());
    assert_eq!(first["result"]["isError"], false, "{first}");
    assert_eq!(
        second["result"]["structuredContent"],
        first["result"]["structuredContent"]
    );
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert!(
        requests[0]["request_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(requests[0]["arguments"], arguments["arguments"]);
}

#[devkit::test]
fn mcp_moment_note_uses_act_tool_and_keeps_retry_identity() {
    let arguments = json!({"arguments":{"steps":[{"verb":"note","text":"synthetic moment","cwd":"/src/tide"}]},"format":"json","deadline":null});
    for tool in ["sinex_read", "sinex_write"] {
        let denied = mcp_call("http://127.0.0.1:1", tool, arguments.clone());
        assert_eq!(denied["result"]["isError"], true, "{denied}");
        assert_eq!(
            denied["result"]["structuredContent"]["error"]["kind"],
            "refused"
        );
    }
    let body = json!({"outcome":"ok","value":{"population":null,"references":[],"steps":[{"verb":"note","outcome":{"status":"done","reply":{"kind":"written","reply":{"event":"one-note","material":"source"}}}}]}}).to_string();
    let (endpoint, server) = daemon(vec![(200, body.clone()), (200, body)]);
    let first = mcp_call(&endpoint, "sinex_act", arguments.clone());
    let second = mcp_call(&endpoint, "sinex_act", arguments);
    assert_eq!(first["result"]["isError"], false, "{first}");
    assert_eq!(
        second["result"]["structuredContent"],
        first["result"]["structuredContent"]
    );
    let requests = server.join().unwrap();
    assert_eq!(requests[0], requests[1]);
    assert!(!requests[0]["request_id"].as_str().unwrap().is_empty());
    assert_eq!(
        requests[0]["arguments"]["steps"][0]["text"],
        "synthetic moment"
    );
}
