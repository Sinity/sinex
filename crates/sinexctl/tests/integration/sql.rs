//! SQL's actual command, complete-result output and one-page ready requests.
use super::query_views::request;
use serde_json::{Value, json};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    process::Command,
};
fn cli(endpoint: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sinexctl"));
    command.args(["--endpoint", endpoint, "--token-file", "/dev/null"]);
    command
}
fn reply(stream: &mut TcpStream, body: Value) {
    let body = body.to_string();
    write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
}
#[devkit::test]
fn sql_command_preserves_all_pages_and_utc_output() {
    for json_mode in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for page in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let (path, received) = request(&mut stream);
                assert_eq!(path, "/v1/sql");
                let mut body = json!({"outcome":"ok","value":{"count":1,"rows":[{"when":if page==0 {"2026-09-14T08:00:00Z"} else {"2026-09-14T08:01:00Z"}}],"page":{"size":1,"cursor":if page==0 {Some("next-page")} else {None}}}});
                if page == 0 {
                    let mut next = received.clone();
                    next["cursor"] = json!("next-page");
                    next["request_id"] = json!("read-next");
                    body["next"] = json!([{"label":"next","request":next}]);
                }
                reply(&mut stream, body);
                requests.push(received);
            }
            requests
        });
        let mut command = cli(&endpoint);
        if json_mode {
            command.arg("--json");
        }
        let output = command
            .args([
                "dev",
                "sql",
                "--page-size",
                "1",
                "--deadline",
                "none",
                "SELECT now() AS when",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        if json_mode {
            let body: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(body["count"], 2);
            assert_eq!(body["rows"].as_array().unwrap().len(), 2);
        } else {
            assert!(text.contains("2026-09-14 08:00:00+00:00"), "{text}");
            assert!(text.contains("2026-09-14 08:01:00+00:00"), "{text}");
            assert!(text.ends_with("2 rows; times in UTC\n"), "{text}");
        }
        let requests = server.join().unwrap();
        assert_eq!(requests[0]["operation"], "sql");
        assert_eq!(
            requests[0]["arguments"],
            json!({"sql":"SELECT now() AS when","page_size":1})
        );
        assert_eq!(requests[0]["deadline"], Value::Null);
        assert_eq!(requests[1]["arguments"], requests[0]["arguments"]);
        assert_eq!(requests[1]["cursor"], "next-page");
    }
}
#[devkit::test]
fn exact_sql_request_reads_one_page_and_keeps_its_ready_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let request_body = json!({"operation":"sql","arguments":{"sql":"SELECT n FROM generate_series(1,4) n","page_size":2},"cursor":"page-two","request_id":"sql-ready","deadline":null,"format":"json"});
    let original = request_body.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let incoming = request(&mut stream);
        reply(
            &mut stream,
            json!({"outcome":"ok","value":{"count":2,"rows":[{"n":3},{"n":4}],"page":{"size":2,"cursor":null}}}),
        );
        incoming
    });
    let output = cli(&endpoint)
        .args(["--request", &request_body.to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let answer: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(answer["value"]["rows"], json!([{"n":3},{"n":4}]));
    let (path, received) = server.join().unwrap();
    assert_eq!(path, "/v1/sql");
    assert_eq!(received, original);
}
#[devkit::test]
fn sql_help_comes_from_the_current_declaration() {
    for args in [["dev", "sql", "--help"], ["help", "dev", "sql"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.starts_with("sinexctl dev sql"), "{text}");
        assert!(text.contains("--page-size"), "{text}");
        assert!(text.contains("--token-file"), "{text}");
    }
}

#[devkit::test]
fn sql_command_keeps_permission_failure_code_and_exit_status() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let incoming = request(&mut stream);
        let body=json!({"outcome":"error","error":{"kind":"refused","code":"42501","message":"synthetic permission denied","retryable":false}}).to_string();
        write!(stream,"HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
        incoming
    });
    let output = cli(&endpoint)
        .args([
            "--json",
            "dev",
            "sql",
            "SELECT payload FROM privacy.settings",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["error"]["kind"], "refused");
    assert_eq!(body["error"]["code"], "42501");
    assert_eq!(server.join().unwrap().0, "/v1/sql");
}
