use super::*;
use std::os::unix::net::UnixListener;

#[test]
fn readiness_requires_an_http_success_response() {
    let socket = std::env::temp_dir().join(format!("sx-ready-{}.sock", std::process::id()));
    let listener = UnixListener::bind(&socket).unwrap();
    let server = std::thread::spawn(move || {
        for response in [
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let received = stream.read(&mut request).unwrap();
            assert!(received > 0);
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    assert!(!ready(&socket));
    assert!(ready(&socket));
    server.join().unwrap();
    fs::remove_file(&socket).unwrap();
    assert!(!ready(&socket));
}

#[test]
fn daemon_and_session_arguments_preserve_explicit_capture_selection() {
    use std::path::Path;
    let mut daemon = Command::new("sinexd");
    daemon_arguments(
        &mut daemon,
        "postgresql://synthetic",
        Path::new("store"),
        "nats://synthetic",
        Path::new("tokens"),
        Path::new("api.sock"),
        Some(Path::new("bindings.json")),
    );
    let args: Vec<_> = daemon.get_args().map(|arg| arg.to_str().unwrap()).collect();
    assert_eq!(
        args,
        [
            "run",
            "--database",
            "postgresql://synthetic",
            "--store",
            "store",
            "--nats",
            "nats://synthetic",
            "--tokens",
            "tokens",
            "--socket",
            "api.sock",
            "--host",
            "development",
            "--sources",
            "bindings.json"
        ]
    );
    let mut daemon = Command::new("sinexd");
    daemon_arguments(
        &mut daemon,
        "db",
        Path::new("store"),
        "nats",
        Path::new("tokens"),
        Path::new("socket"),
        None,
    );
    assert!(!daemon.get_args().any(|arg| arg == "--sources"));
    for _ in 0..2 {
        let mut session = Command::new("sinex-session");
        session_arguments(
            &mut session,
            "nats://synthetic",
            Path::new("store"),
            Path::new("state"),
            Path::new("session-sources.json"),
        );
        let args: Vec<_> = session
            .get_args()
            .map(|arg| arg.to_str().unwrap())
            .collect();
        let expected = vec![
            "run",
            "--nats",
            "nats://synthetic",
            "--producer",
            "development-session",
            "--host",
            "development",
            "--store",
            "store",
            "--state",
            "state",
            "--sources",
            "session-sources.json",
        ]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();

        assert_eq!(args, expected);
    }
}

#[test]
fn session_exclusions_cover_every_unselected_catalog_capture() {
    let catalog: serde_json::Value =
        serde_json::from_str(include_str!("../../../nix/catalog.json")).unwrap();
    let generated = session_configuration(
        serde_json::json!({"exclude":{"desktop.clipboard":{"reason":"operator exclusion"}}}),
        &catalog,
        &["clipboard".into(), "integrations".into()],
    )
    .unwrap();
    assert_eq!(
        generated["exclude"]["desktop.clipboard"]["reason"],
        "operator exclusion"
    );
    assert!(generated["exclude"].get("session.integrations").is_none());
    for source in [
        "desktop.hyprland",
        "desktop.notifications",
        "media.mpris",
        "session.media",
        "machine.file-watch",
    ] {
        assert!(
            generated["exclude"][source]["reason"].is_string(),
            "{source}"
        );
    }
    let none = session_configuration(serde_json::json!({}), &catalog, &[]).unwrap();
    assert!(none["exclude"]["desktop.clipboard"]["reason"].is_string());
}
