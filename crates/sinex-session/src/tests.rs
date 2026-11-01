use super::*;
#[path = "../../sinex-source-core/tests/support/discovery_configuration.rs"]
mod configuration;

fn parsed(root:&std::path::Path,config:Option<&std::path::Path>,host:Option<&str>)->Run {
    let mut args:Vec<std::ffi::OsString>=["sinex-session","run","--nats","nats://127.0.0.1:4222","--producer","fixture"].into_iter().map(Into::into).collect();
    args.extend(["--state".into(),root.join("state").into_os_string(),"--store".into(),root.join("store").into_os_string()]);
    if let Some(config)=config {args.extend(["--sources".into(),config.as_os_str().to_owned()]);}
    if let Some(host)=host {args.extend(["--host".into(),host.into()]);}
    let Command::Run(run)=Cli::try_parse_from(args).unwrap().command else{panic!("run command")}; run
}

#[test]
fn session_uses_the_complete_configured_context_with_cli_identity_and_state() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let config = configuration::write(root);
    let cli = Cli::try_parse_from([
        "sinex-session",
        "run",
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
    let Command::Run(run) = cli.command else {
        panic!("run command")
    };
    configuration::assert_context(&discovery_context(&run).unwrap(), root);
}

#[test]
fn session_host_comes_from_explicit_cli_then_configuration_without_inference() {
    let dir=tempfile::tempdir().unwrap();let root=dir.path();let config=configuration::write(root);
    assert_eq!(discovery_context(&parsed(root,Some(&config),None)).unwrap().host,"configured-host");
    assert_eq!(discovery_context(&parsed(root,Some(&config),Some("cli-host"))).unwrap().host,"cli-host");
    assert!(discovery_context(&parsed(root,Some(&config),Some(" "))).is_err(),"blank CLI identity does not fall back");
    std::fs::write(&config,br#"{"discovery":{"host":" "}}"#).unwrap();
    assert!(discovery_context(&parsed(root,Some(&config),None)).is_err());
    assert_eq!(discovery_context(&parsed(root,Some(&config),Some("cli-host"))).unwrap().host,"cli-host");
}

#[test]
fn missing_session_host_refuses_before_creating_capture_state() {
    let dir=tempfile::tempdir().unwrap();
    let reason=discovery_context(&parsed(dir.path(),None,None)).unwrap_err();
    assert!(reason.contains("requires --host"),"{reason}");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(),0);
}
