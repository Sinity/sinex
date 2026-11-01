//! Query views page through the public CLI and HTTP request protocol.
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;

fn request(stream: &mut TcpStream) -> Value {
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let mut bytes = Vec::new();
    let (end, length) = loop {
        let mut chunk = [0; 4096];
        let count = stream.read(&mut chunk).unwrap();
        assert_ne!(count, 0, "request ended before its headers");
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
            let length = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:").map(str::trim))
                .unwrap()
                .parse::<usize>()
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < end + length {
        let mut chunk = [0; 4096];
        let count = stream.read(&mut chunk).unwrap();
        assert_ne!(count, 0, "request ended before its body");
        bytes.extend_from_slice(&chunk[..count]);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}

#[devkit::test]
fn context_timeline_and_window_citations_page_through_the_same_query_protocol() {
    for args in [
        vec!["context", "--since", "2h"],
        vec!["timeline", "where source = synthetic"],
        vec![
            "show",
            "sinex:window/2026-09-14T08:00:00Z/2026-09-14T09:00:00Z",
        ],
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for page in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(request(&mut stream));
                let result = json!({"outcome":"ok","result":{
                    "table":"events","query":"events","read_as":"synthetic query",
                    "columns":["id"],"rows":[{"id":page+1}],"total":2,
                    "resolved":{"zone":"UTC"},
                    "page":{"size":1,"cursor":if page == 0 { Some("next") } else { None }}
                }})
                .to_string();
                write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{result}",result.len()).unwrap();
            }
            requests
        });
        let output = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
            .args([
                "--endpoint",
                &endpoint,
                "--token-file",
                "/dev/null",
                "--all",
                "--json",
            ])
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let answer: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(answer["rows"], json!([{"id":1},{"id":2}]));
        let requests = server.join().unwrap();
        assert_eq!(requests[0]["method"], args[0]);
        assert_eq!(requests[1]["method"], "query");
        assert_eq!(requests[1]["query"], "events");
        assert_eq!(requests[1]["cursor"], "next");
    }
}
