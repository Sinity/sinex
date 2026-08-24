use super::*;
use crate::sandbox::sinex_test;

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
