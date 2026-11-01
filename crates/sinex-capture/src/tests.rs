use super::*;
#[path = "../../sinex-source-core/tests/support/discovery_configuration.rs"]
mod configuration;

#[test]
fn satellite_uses_the_complete_configured_context_with_cli_identity_and_state() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let config = configuration::write(root);
    let cli = Cli::try_parse_from([
        "sinex-capture",
        "--nats",
        "nats://127.0.0.1:4222",
        "--producer",
        "fixture",
        "--sources",
        config.to_str().unwrap(),
        "--host",
        "cli-host",
        "--state",
        root.join("state").to_str().unwrap(),
        "--store",
        root.join("store").to_str().unwrap(),
        "--watch",
        root.join("cli-watch").to_str().unwrap(),
    ])
    .unwrap();
    configuration::assert_context(&discovery_context(&cli).unwrap(), root);
}

#[test]
fn blank_satellite_host_refuses_before_creating_capture_state() {
    let dir=tempfile::tempdir().unwrap();let root=dir.path();
    let cli=Cli::try_parse_from(["sinex-capture","--nats","nats://127.0.0.1:4222","--producer","fixture","--host"," ",
        "--state",root.join("state").to_str().unwrap(),"--store",root.join("store").to_str().unwrap()]).unwrap();
    assert!(discovery_context(&cli).unwrap_err().contains("nonblank --host"));
    assert_eq!(std::fs::read_dir(root).unwrap().count(),0);
}
