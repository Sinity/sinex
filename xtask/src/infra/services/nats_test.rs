use super::*;
use crate::sandbox::sinex_test;
use std::io::Write;
use std::net::TcpListener;

#[sinex_test]
async fn generated_config_binds_the_lease_port_on_loopback() -> crate::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = NatsManager::new(NatsConfig {
        port: 44308,
        config_file: temp.path().join("nats.conf"),
        data_dir: temp.path().join("data"),
        log_file: temp.path().join("nats.log"),
    });
    manager.generate_config()?;
    let config = std::fs::read_to_string(temp.path().join("nats.conf"))?;
    assert!(config.contains("host = \"127.0.0.1\""));
    assert!(config.contains("port = 44308"));
    assert!(config.contains("max_file = 16GB"));
    Ok(())
}

#[sinex_test]
async fn readiness_requires_the_nats_protocol_greeting() -> crate::sandbox::TestResult<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    let server = std::thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        stream.write_all(b"not nats\r\n")
    });
    let temp = tempfile::tempdir()?;
    let manager = NatsManager::new(NatsConfig {
        port,
        config_file: temp.path().join("nats.conf"),
        data_dir: temp.path().join("data"),
        log_file: temp.path().join("nats.log"),
    });

    assert!(
        !manager.is_ready(),
        "a foreign TCP listener is not NATS readiness"
    );
    server
        .join()
        .expect("test listener thread must not panic")?;
    Ok(())
}
