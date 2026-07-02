use super::*;
use crate::sandbox::sinex_test;
#[cfg(unix)]
use std::ffi::{OsStr, OsString};
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;

fn test_manager(root: &tempfile::TempDir) -> NatsManager {
    NatsManager::new(NatsConfig {
        port: 4222,
        config_file: root.path().join("nats.conf"),
        data_dir: root.path().join("data"),
        pid_file: root.path().join("run/nats.pid"),
        log_file: root.path().join("run/nats.log"),
    })
}

#[sinex_test]
async fn generate_config_binds_loopback_only() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);

    manager.generate_config()?;

    let conf = fs::read_to_string(temp.path().join("nats.conf"))?;
    assert!(
        conf.contains(r#"host = "127.0.0.1""#),
        "dev NATS config must pin a loopback bind, got:\n{conf}"
    );
    assert!(
        conf.contains(&format!("max_mem = {NATS_JETSTREAM_MAX_MEM}"))
            && conf.contains(&format!("max_file = {NATS_JETSTREAM_MAX_FILE}")),
        "dev NATS config must carry bounded JetStream budgets, got:\n{conf}"
    );
    Ok(())
}

#[sinex_test]
async fn generate_config_replaces_wildcard_bind_configs() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);

    // Configs written before the loopback fix lack the host line; the
    // content-equality check must regenerate them on next start.
    fs::write(
        temp.path().join("nats.conf"),
        "# sinex-dev isolated NATS configuration\nport = 4222\n",
    )?;
    manager.generate_config()?;

    let conf = fs::read_to_string(temp.path().join("nats.conf"))?;
    assert!(conf.contains(r#"host = "127.0.0.1""#));
    Ok(())
}

#[sinex_test]
async fn parses_ipv4_and_wildcard_listener_ports() -> TestResult<()> {
    assert_eq!(parse_listener_port("*:4321")?, 4321);
    assert_eq!(parse_listener_port("127.0.0.1:4250")?, 4250);
    assert_eq!(parse_listener_port("[::]:4222")?, 4222);
    Ok(())
}

#[sinex_test]
async fn rejects_non_numeric_listener_ports() -> TestResult<()> {
    let missing_separator = parse_listener_port("*").unwrap_err();
    assert!(format!("{missing_separator:#}").contains("missing port separator"));

    let invalid_port = parse_listener_port("localhost:http").unwrap_err();
    assert!(format!("{invalid_port:#}").contains("failed to parse NATS listener port"));
    Ok(())
}

#[sinex_test]
async fn listener_port_for_pid_probe_reports_ss_spawn_failures() -> TestResult<()> {
    let error =
        listener_port_for_pid_probe(123, Err(std::io::Error::other("ss exploded"))).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("failed to inspect NATS listeners with ss"));
    assert!(message.contains("ss exploded"));
    Ok(())
}

#[sinex_test]
async fn listener_port_for_pid_probe_reports_ss_exit_failures() -> TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        let error = listener_port_for_pid_probe(
            123,
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(256),
                stdout: Vec::new(),
                stderr: b"permission denied".to_vec(),
            }),
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("ss -ltnp exited unsuccessfully"));
        assert!(message.contains("permission denied"));
    }
    Ok(())
}

#[sinex_test]
async fn listener_port_for_pid_probe_extracts_matching_port() -> TestResult<()> {
    let port = listener_port_for_pid_probe(
        123,
        Ok(std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: br#"State  Recv-Q Send-Q Local Address:Port Peer Address:PortProcess
LISTEN 0      4096   127.0.0.1:4222      0.0.0.0:*    users:(("nats-server",pid=123,fd=7))
"#
            .to_vec(),
            stderr: Vec::new(),
        }),
    )?;
    assert_eq!(port, Some(4222));
    Ok(())
}

#[sinex_test]
async fn listener_port_for_pid_probe_reports_malformed_listener_rows() -> TestResult<()> {
    let error = listener_port_for_pid_probe(
        123,
        Ok(std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: br#"State  Recv-Q Send-Q Local Address:Port Peer Address:PortProcess
LISTEN 0      4096   malformed-listener   0.0.0.0:*    users:(("nats-server",pid=123,fd=7))
"#
            .to_vec(),
            stderr: Vec::new(),
        }),
    )
    .unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("missing port separator"));
    assert!(message.contains("malformed-listener"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn find_running_nats_pid_for_port_matches_live_server_without_pid_file() -> TestResult<()> {
    use std::os::unix::process::ExitStatusExt;

    let pid = find_running_nats_pid_for_port(
        4308,
        Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: b"111\n222\n".to_vec(),
            stderr: Vec::new(),
        }),
        |candidate| candidate != 111,
        |candidate| {
            Ok(match candidate {
                111 => Some(4308),
                222 => Some(4308),
                _ => None,
            })
        },
    )?;
    assert_eq!(pid, Some(222));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn find_running_nats_pid_for_port_returns_none_when_port_differs() -> TestResult<()> {
    use std::os::unix::process::ExitStatusExt;

    let pid = find_running_nats_pid_for_port(
        4308,
        Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: b"111\n".to_vec(),
            stderr: Vec::new(),
        }),
        |_| true,
        |_| Ok(Some(4222)),
    )?;
    assert_eq!(pid, None);
    Ok(())
}

#[sinex_test]
async fn wait_for_nats_startup_probe_accepts_expected_listener() -> TestResult<()> {
    wait_for_nats_startup_probe(123, 4222, || Ok(None), |_pid| true, |_pid| Ok(Some(4222)))?;
    Ok(())
}

#[sinex_test]
async fn wait_for_nats_startup_probe_rejects_early_exit() -> TestResult<()> {
    let error = wait_for_nats_startup_probe(
        123,
        4222,
        || Ok(Some("exit status: 1".to_string())),
        |_pid| true,
        |_pid| Ok(None),
    )
    .unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("exited before startup completed"));
    assert!(message.contains("exit status: 1"));
    Ok(())
}

#[sinex_test]
async fn wait_for_nats_startup_probe_rejects_unexpected_listener_port() -> TestResult<()> {
    let error =
        wait_for_nats_startup_probe(123, 4222, || Ok(None), |_pid| true, |_pid| Ok(Some(4333)))
            .unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("unexpected port 4333"));
    assert!(message.contains("expected 4222"));
    Ok(())
}

#[sinex_test]
async fn parse_nats_pgrep_output_reports_spawn_failures() -> TestResult<()> {
    let error = parse_nats_pgrep_output(Err(std::io::Error::other("pgrep exploded"))).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("failed to inspect running nats-server processes with pgrep"));
    assert!(message.contains("pgrep exploded"));
    Ok(())
}

#[sinex_test]
async fn parse_nats_pgrep_output_treats_exit_one_as_no_matches() -> TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        let pids = parse_nats_pgrep_output(Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }))?;
        assert!(pids.is_empty());
    }
    Ok(())
}

#[sinex_test]
async fn parse_nats_pgrep_output_reports_invalid_pid_lines() -> TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        let error = parse_nats_pgrep_output(Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: b"123\nnot-a-pid\n".to_vec(),
            stderr: Vec::new(),
        }))
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("produced invalid PID line"));
        assert!(message.contains("not-a-pid"));
    }
    Ok(())
}

#[sinex_test]
async fn parse_nats_pgrep_output_reports_exit_failures() -> TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        let error = parse_nats_pgrep_output(Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(512),
            stdout: Vec::new(),
            stderr: b"permission denied".to_vec(),
        }))
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("pgrep -f nats-server exited unsuccessfully"));
        assert!(message.contains("permission denied"));
    }
    Ok(())
}

#[sinex_test]
async fn test_read_pid_result_reports_malformed_pid_file() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);
    fs::create_dir_all(manager.config.pid_file.parent().unwrap())?;
    fs::write(&manager.config.pid_file, "not-a-pid\n")?;

    let error = manager.read_pid_result().unwrap_err();
    assert!(format!("{error:#}").contains("failed to parse NATS pid"));
    Ok(())
}

#[sinex_test]
async fn test_remove_service_file_reports_remove_failures() -> TestResult<()> {
    let temp = tempfile::tempdir()?;

    let error = remove_service_file(temp.path(), "test pid file").unwrap_err();
    assert!(format!("{error:#}").contains("failed to remove test pid file"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn nats_server_command_preserves_non_utf8_config_path() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let config_file = PathBuf::from(OsString::from_vec(b"/tmp/nats-\xff.conf".to_vec()));
    let manager = NatsManager::new(NatsConfig {
        port: 4222,
        config_file: config_file.clone(),
        data_dir: temp.path().join("data"),
        pid_file: temp.path().join("run/nats.pid"),
        log_file: temp.path().join("run/nats.log"),
    });

    let args: Vec<OsString> = manager
        .nats_server_command()
        .get_args()
        .map(OsStr::to_os_string)
        .collect();
    assert!(args.iter().any(|arg| arg == config_file.as_os_str()));
    Ok(())
}
