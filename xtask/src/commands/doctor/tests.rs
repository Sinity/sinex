use super::*;
use crate::command::CommandContext;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;
use ::xtask::sandbox::EnvGuard;
use sinex_node_sdk::preflight::services::SystemdServiceDetails;
use sinex_primitives::{DeploymentReadinessMode, nats::NatsConnectionConfig};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;

fn write_executable_script(path: &std::path::Path, body: &str) -> ::xtask::sandbox::TestResult<()> {
    fs::write(path, body)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

fn sample_descriptor() -> DeploymentReadinessDescriptor {
    DeploymentReadinessDescriptor {
        version: 1,
        mode: DeploymentReadinessMode::Prepared,
        source: Some("test".to_string()),
        target: Some(sinex_primitives::DeploymentTarget {
            user: "probe-user".to_string(),
            uid: Some(4242),
            home: Some(PathBuf::from("/tmp/probe-home")),
        }),
        ..Default::default()
    }
}

fn sample_nixos_descriptor() -> DeploymentReadinessDescriptor {
    DeploymentReadinessDescriptor {
        source: Some("nixos".to_string()),
        managed_units: vec![
            "sinex-ingestd.service".to_string(),
            "sinex-gateway.service".to_string(),
            "sinex-filesystem-1.service".to_string(),
            "sinex-source@terminal.atuin-history.service".to_string(),
            "sinex-source@terminal.bash-history.service".to_string(),
            "sinex-source@terminal.fish-history.service".to_string(),
            "sinex-source@terminal.zsh-history.service".to_string(),
            "sinex-system-1.service".to_string(),
            "sinex-health-automaton.service".to_string(),
        ],
        ..Default::default()
    }
}

fn systemd_details(active_state: &str, sub_state: &str, load_state: &str) -> SystemdServiceDetails {
    SystemdServiceDetails {
        active_state: active_state.to_string(),
        sub_state: sub_state.to_string(),
        load_state: load_state.to_string(),
        unit_type: None,
        notify_access: None,
        watchdog_usec: None,
    }
}

#[sinex_test]
async fn test_doctor_report_json_shape() -> ::xtask::sandbox::TestResult<()> {
    let report = DoctorReport {
        postgres: DoctorServiceCheck {
            available: true,
            message: None,
        },
        nats: DoctorServiceCheck {
            available: false,
            message: Some("Cannot connect to NATS on port 4222".into()),
        },
        tools: vec![
            ToolCheck {
                name: "rustc".into(),
                available: true,
                version: Some("1.95.0-nightly".into()),
                path: Some("/nix/store/.../rustc".into()),
                message: None,
            },
            ToolCheck {
                name: "ast-grep".into(),
                available: false,
                version: None,
                path: None,
                message: Some("Tool 'ast-grep' not found in PATH".into()),
            },
        ],
        environment: Some(serde_json::json!({
            "hostname": "testhost",
            "in_dev_shell": true,
        })),
        tls: Some(TlsCheck {
            ca_exists: true,
            server_cert_exists: true,
            client_cert_exists: false,
            server_expires_days: None,
            server_expired: None,
            key_matches: None,
            error: None,
        }),
        postgres_extensions: Some(vec!["pgvector".into(), "timescaledb".into()]),
        postgres_extensions_error: None,
        pipeline_smoke: Some(DoctorServiceCheck {
            available: false,
            message: Some("pipeline smoke failed".into()),
        }),
        overall: false,
    };

    let json = serde_json::to_value(&report)?;

    // Postgres/NATS (agents use: .data.postgres.available, .data.nats.available)
    assert_eq!(json["postgres"]["available"], true);
    assert!(json["postgres"]["message"].is_null());
    assert_eq!(json["nats"]["available"], false);
    assert!(json["nats"]["message"].is_string());

    // Tools (agents use: .data.tools[].name, .available, .version)
    assert!(json["tools"].is_array());
    assert_eq!(json["tools"][0]["name"], "rustc");
    assert_eq!(json["tools"][0]["available"], true);
    assert!(json["tools"][0]["version"].is_string());
    assert!(json["tools"][0]["message"].is_null());
    assert_eq!(json["tools"][1]["available"], false);
    // Unavailable tool should have null version and no path
    assert!(json["tools"][1]["version"].is_null());
    assert!(json["tools"][1].get("path").is_none() || json["tools"][1]["path"].is_null());
    assert!(json["tools"][1]["message"].is_string());

    // Overall (agents use: .data.overall)
    assert_eq!(json["overall"], false);

    // TLS (agents use: .data.tls.ca_exists, etc.)
    assert_eq!(json["tls"]["ca_exists"], true);
    assert_eq!(json["tls"]["client_cert_exists"], false);

    // Extensions (agents use: .data.postgres_extensions[])
    assert!(json["postgres_extensions"].is_array());
    assert_eq!(json["postgres_extensions"][0], "pgvector");
    assert!(json.get("postgres_extensions_error").is_none());
    assert_eq!(json["pipeline_smoke"]["available"], false);
    assert_eq!(json["pipeline_smoke"]["message"], "pipeline smoke failed");
    Ok(())
}

#[sinex_test]
async fn test_pipeline_smoke_invocation_uses_xtask_test_harness() -> ::xtask::sandbox::TestResult<()>
{
    let (program, args, filter) = pipeline_smoke_invocation("/tmp/fake-xtask");
    assert_eq!(program, "/tmp/fake-xtask");
    assert_eq!(args, ["test", "--debug", "-p", "sinex-ingestd", "-E"]);
    assert_eq!(filter, "test(test_pipeline_smoke)");
    Ok(())
}

#[sinex_test]
async fn test_build_tool_check_surfaces_probe_errors() -> ::xtask::sandbox::TestResult<()> {
    let check = build_tool_check(
        "ast-grep",
        Err(color_eyre::eyre::eyre!("Tool 'ast-grep' not found in PATH")),
    );
    assert!(!check.available);
    assert!(check.version.is_none());
    assert!(check.path.is_none());
    assert!(
        check
            .message
            .as_deref()
            .is_some_and(|message| message.contains("not found in PATH"))
    );

    let check = build_tool_check(
        "rustc",
        Ok(ToolInfo {
            path: PathBuf::from("/nix/store/.../rustc"),
            version: "unknown".to_string(),
            probe_issue: Some("Failed to run 'rustc --version'".to_string()),
        }),
    );
    assert!(!check.available);
    assert_eq!(check.version.as_deref(), Some("unknown"));
    assert_eq!(check.path.as_deref(), Some("/nix/store/.../rustc"));
    assert!(
        check
            .message
            .as_deref()
            .is_some_and(|message| message.contains("rustc --version"))
    );
    Ok(())
}

#[sinex_test]
async fn test_probe_postgres_extensions_reports_stack_config_failures()
-> ::xtask::sandbox::TestResult<()> {
    let probe = probe_postgres_extensions(false, Ok("stack"), |_: &&str| {
        panic!("probe should not run when Postgres is not ready")
    });
    assert_eq!(
        probe,
        PostgresExtensionsProbe {
            extensions: None,
            error: None,
        }
    );

    let probe =
        probe_postgres_extensions(true, Err("missing stack config".to_string()), |_: &&str| {
            panic!("probe should not run when stack config resolution already failed")
        });
    assert!(probe.extensions.is_none());
    assert!(
        probe
            .error
            .as_deref()
            .is_some_and(|value| value.contains("missing stack config"))
    );
    Ok(())
}

#[sinex_test]
async fn test_probe_postgres_extensions_reports_psql_failures() -> ::xtask::sandbox::TestResult<()>
{
    let probe = probe_postgres_extensions(true, Ok("stack"), |_: &&str| {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "psql missing",
        ))
    });
    assert!(probe.extensions.is_none());
    assert!(
        probe
            .error
            .as_deref()
            .is_some_and(|value| value.contains("psql missing"))
    );

    let probe = probe_postgres_extensions(true, Ok("stack"), |_: &&str| {
        Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"permission denied".to_vec(),
        })
    });
    assert!(probe.extensions.is_none());
    assert!(
        probe
            .error
            .as_deref()
            .is_some_and(|value| value.contains("permission denied"))
    );
    Ok(())
}

#[sinex_test]
async fn test_doctor_service_check_serialization() -> ::xtask::sandbox::TestResult<()> {
    let check = DoctorServiceCheck {
        available: false,
        message: Some("Connection refused".into()),
    };
    let json = serde_json::to_value(&check)?;
    assert_eq!(json["available"], false);
    assert_eq!(json["message"], "Connection refused");

    // When available, message is typically None
    let check_ok = DoctorServiceCheck {
        available: true,
        message: None,
    };
    let json_ok = serde_json::to_value(&check_ok)?;
    assert_eq!(json_ok["available"], true);
    assert!(json_ok["message"].is_null());
    Ok(())
}

#[sinex_test]
async fn test_tls_check_serialization() -> ::xtask::sandbox::TestResult<()> {
    let check = TlsCheck {
        ca_exists: true,
        server_cert_exists: false,
        client_cert_exists: false,
        server_expires_days: None,
        server_expired: None,
        key_matches: None,
        error: None,
    };
    let json = serde_json::to_value(&check)?;
    assert_eq!(json["ca_exists"], true);
    assert_eq!(json["server_cert_exists"], false);
    assert_eq!(json["client_cert_exists"], false);
    Ok(())
}

#[sinex_test]
async fn test_detect_tls_check_prefers_rcgen_cert_names() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let cert = temp.path().join("server.pem");
    let key = temp.path().join("server-key.pem");
    std::fs::write(&cert, "not-a-real-cert")?;
    std::fs::write(&key, "not-a-real-key")?;

    let mut env = EnvGuard::new();
    env.set("SINEX_GATEWAY_TLS_CERT", cert.display().to_string());

    let check = detect_tls_check().expect("TLS check should resolve active directory");
    assert!(check.server_cert_exists);
    Ok(())
}

#[sinex_test]
async fn test_detect_tls_check_reports_validation_errors() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let cert = temp.path().join("server.pem");
    let key = temp.path().join("server-key.pem");
    std::fs::write(&cert, "not-a-real-cert")?;
    std::fs::write(&key, "not-a-real-key")?;

    let mut env = EnvGuard::new();
    env.set("SINEX_GATEWAY_TLS_CERT", cert.display().to_string());

    let check = detect_tls_check().expect("TLS check should resolve active directory");
    assert!(check.server_cert_exists);
    assert!(check.error.is_some());
    assert!(!check.is_healthy());
    Ok(())
}

#[sinex_test]
async fn test_normalize_gateway_base_url_strips_rpc_suffix() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        normalize_gateway_base_url("https://127.0.0.1:9999/rpc"),
        "https://127.0.0.1:9999"
    );
    assert_eq!(
        normalize_gateway_base_url("https://127.0.0.1:9999/"),
        "https://127.0.0.1:9999"
    );
    Ok(())
}

#[sinex_test]
async fn test_interpret_gateway_ready_response_reports_invalid_json()
-> ::xtask::sandbox::TestResult<()> {
    let item = interpret_gateway_ready_response(
        "https://127.0.0.1:9999/ready",
        reqwest::StatusCode::OK,
        "<html>proxy error</html>",
    );

    assert_eq!(item.status, "fail");
    assert!(
        item.description.contains("non-JSON body"),
        "unexpected message: {}",
        item.description
    );
    assert!(item.description.contains("proxy error"));
    Ok(())
}

#[sinex_test]
async fn test_interpret_gateway_ready_response_passes_serving_true()
-> ::xtask::sandbox::TestResult<()> {
    let item = interpret_gateway_ready_response(
        "https://127.0.0.1:9999/ready",
        reqwest::StatusCode::OK,
        r#"{
                "status":"degraded",
                "healthy":false,
                "serving":true,
                "degradation_reasons":["NATS unavailable"],
                "components":{
                    "database":{"status":"healthy","connected":true},
                    "nats":{"status":"unhealthy","connected":false,"latency_ms":42.0,"detail":"timed out"},
                    "replay_control":{"status":"healthy","enabled":true,"connected":true}
                }
            }"#,
    );

    assert_eq!(item.status, "pass");
    assert!(item.description.contains("healthy=false"));
    assert!(item.description.contains("status=degraded"));
    assert!(item.description.contains("NATS unavailable"));
    Ok(())
}

#[sinex_test]
async fn test_interpret_gateway_ready_response_rejects_non_conforming_health_body()
-> ::xtask::sandbox::TestResult<()> {
    let item = interpret_gateway_ready_response(
        "https://127.0.0.1:9999/ready",
        reqwest::StatusCode::OK,
        r#"{"serving":true,"healthy":false}"#,
    );

    assert_eq!(item.status, "fail");
    assert!(
        item.description.contains("non-conforming health body"),
        "unexpected message: {}",
        item.description
    );
    Ok(())
}

#[sinex_test]
async fn test_remediate_stack_services_reports_missing_stack_config()
-> ::xtask::sandbox::TestResult<()> {
    let warnings = remediate_stack_services(
        false,
        true,
        Err("missing stack config".to_string()),
        false,
        |_: &&str, _| Ok(()),
        |_: &&str, _| Ok(()),
    );

    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("missing stack config"));
    Ok(())
}

#[sinex_test]
async fn test_remediate_stack_services_reports_start_failures() -> ::xtask::sandbox::TestResult<()>
{
    let warnings = remediate_stack_services(
        false,
        false,
        Ok("stack"),
        false,
        |_: &&str, _| Err(color_eyre::eyre::eyre!("pg failed")),
        |_: &&str, _| Err(color_eyre::eyre::eyre!("nats failed")),
    );

    assert_eq!(warnings.len(), 2);
    assert!(warnings.iter().any(|warning| warning.contains("pg failed")));
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("nats failed"))
    );
    Ok(())
}

#[sinex_test]
async fn test_deployment_readiness_report_serialization() -> ::xtask::sandbox::TestResult<()> {
    let report = DeploymentReadinessReport {
        items: vec![
            DeploymentReadinessItem::pass("gateway-ready", "ready"),
            DeploymentReadinessItem::fail("inotify-max-user-watches", "too low"),
        ],
        overall: false,
    };

    let json = serde_json::to_value(&report)?;
    assert_eq!(json["overall"], false);
    assert_eq!(json["items"][0]["name"], "gateway-ready");
    assert_eq!(json["items"][1]["status"], "fail");
    Ok(())
}

#[sinex_test]
async fn test_deployment_readiness_overall_rejects_blocking_skips()
-> ::xtask::sandbox::TestResult<()> {
    let items = vec![
        DeploymentReadinessItem::pass("descriptor", "loaded"),
        DeploymentReadinessItem::skip_blocking(
            "realm-accessible",
            "rerun as the deployment target",
        ),
    ];

    assert!(
        !deployment_readiness_overall(&items),
        "blocking skips must keep deployment readiness false"
    );
    Ok(())
}

#[sinex_test]
async fn test_check_realm_accessible_marks_principal_mismatch_as_blocking_skip()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("UID", "1000");
    let target = TargetIdentity {
        user: "probe-user".to_string(),
        uid: 4242,
        home: PathBuf::from("/tmp/probe-home"),
    };

    let item = check_realm_accessible(&target);
    assert_eq!(item.status, "skip");
    assert!(item.blocking);
    assert!(item.description.contains("rerun as probe-user or root"));
    Ok(())
}

#[sinex_test]
async fn test_runtime_assessment_capture_degraded_signals() -> ::xtask::sandbox::TestResult<()> {
    let metrics = crate::runtime_metrics::RuntimeMetrics {
        ingestd_status: crate::runtime_metrics::IngestdStatus::Stale,
        last_heartbeat_age_secs: Some(300),
        consumer_lag_pending: Some(1500.0),
        consumer_lag_age_secs: Some(10),
        last_batch_latency_ms: Some(6000.0),
        last_batch_latency_age_secs: Some(10),
        query_error: None,
    };

    let warnings = metrics.assessment().warnings;
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("ingestd heartbeat is stale"))
    );
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("consumer lag is high"))
    );
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("batch latency is high"))
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_assessment_capture_stale_telemetry() -> ::xtask::sandbox::TestResult<()> {
    let metrics = crate::runtime_metrics::RuntimeMetrics {
        ingestd_status: crate::runtime_metrics::IngestdStatus::Healthy,
        last_heartbeat_age_secs: Some(5),
        consumer_lag_pending: Some(42.0),
        consumer_lag_age_secs: Some(600),
        last_batch_latency_ms: Some(125.0),
        last_batch_latency_age_secs: Some(600),
        query_error: None,
    };

    let warnings = metrics.assessment().warnings;
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("consumer lag telemetry is stale"))
    );
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("batch latency telemetry is stale"))
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_check_skips_honestly_without_database_url() -> ::xtask::sandbox::TestResult<()>
{
    let mut env = EnvGuard::new();
    env.clear("DATABASE_URL");
    env.set("SINEX_DEPLOYMENT_READINESS_CONFIG", "");
    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), false, None, "doctor");

    let report = execute_runtime_check(&ctx).await?;
    assert!(!report.overall);
    assert!(report.skipped);
    assert_eq!(
        report.skip_reason.as_deref(),
        Some("runtime database target not configured")
    );
    assert_eq!(
        report.assessment.status,
        crate::runtime_metrics::RuntimeHealthStatus::Unavailable
    );
    Ok(())
}

#[sinex_test]
async fn test_resolve_target_identity_prefers_explicit_target_env()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_TARGET_USER", "probe-user");
    env.set("SINEX_TARGET_UID", "4242");
    env.set("SINEX_TARGET_HOME", "/tmp/probe-home");
    env.set("USER", "current-user");
    env.set("UID", "1000");
    env.set("HOME", "/tmp/current-home");

    let identity = resolve_target_identity(None)?;
    assert_eq!(identity.user, "probe-user");
    assert_eq!(identity.uid, 4242);
    assert_eq!(identity.home, PathBuf::from("/tmp/probe-home"));
    Ok(())
}

#[sinex_test]
async fn test_resolve_target_identity_prefers_descriptor_target() -> ::xtask::sandbox::TestResult<()>
{
    let mut env = EnvGuard::new();
    env.set("SINEX_TARGET_USER", "env-user");
    env.set("SINEX_TARGET_UID", "1000");
    env.set("SINEX_TARGET_HOME", "/tmp/env-home");

    let identity = resolve_target_identity(Some(&sample_descriptor()))?;
    assert_eq!(identity.user, "probe-user");
    assert_eq!(identity.uid, 4242);
    assert_eq!(identity.home, PathBuf::from("/tmp/probe-home"));
    Ok(())
}

#[sinex_test]
async fn test_resolve_target_identity_rejects_implicit_shell_user()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.clear("SINEX_TARGET_USER");
    env.clear("SINEX_TARGET_UID");
    env.clear("SINEX_TARGET_HOME");
    env.set("USER", "current-user");
    env.set("UID", "1000");
    env.set("HOME", "/tmp/current-home");

    let error = resolve_target_identity(None)
        .expect_err("deployment readiness should not guess the shell user");
    assert!(
        error
            .to_string()
            .contains("refuses to guess the target user")
    );
    Ok(())
}

#[sinex_test]
async fn test_resolve_target_identity_rejects_unknown_target_without_explicit_uid_home()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(
        "SINEX_TARGET_USER",
        "sinex-target-user-that-should-not-exist-for-tests",
    );
    env.clear("SINEX_TARGET_UID");
    env.clear("SINEX_TARGET_HOME");

    let error = resolve_target_identity(None)
        .expect_err("missing passwd target should not fall back to the current process");
    assert!(error.to_string().contains("missing from /etc/passwd"));
    Ok(())
}

#[sinex_test]
async fn test_check_terminal_sources_accepts_atuin_sqlite_history()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(home.join(".local/share/atuin"))?;
    std::fs::write(home.join(".bash_history"), "echo hello\n")?;

    let atuin_db = home.join(".local/share/atuin/history.db");
    let conn = rusqlite::Connection::open(&atuin_db)?;
    conn.execute(
        "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT NOT NULL,
                session TEXT NOT NULL,
                hostname TEXT NOT NULL,
                exit INTEGER NOT NULL,
                duration INTEGER NOT NULL,
                deleted_at INTEGER
            )",
        [],
    )?;

    let item = check_terminal_sources(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        None,
    );
    assert_eq!(item.status, "pass");
    assert!(item.description.contains("atuin:"));
    assert!(item.description.contains("bash:"));
    Ok(())
}

#[sinex_test]
async fn test_check_terminal_sources_ignores_native_fish_history_when_not_configured()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(home.join(".local/share/fish"))?;
    std::fs::write(home.join(".bash_history"), "echo hello\n")?;
    std::fs::write(
        home.join(".local/share/fish/fish_history"),
        "- cmd: echo fish\n  when: 1234567890\n",
    )?;

    let item = check_terminal_sources(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        None,
    );
    assert_eq!(item.status, "pass");
    assert!(item.description.contains("bash:"));
    assert!(!item.description.contains("fish:"));
    Ok(())
}

#[sinex_test]
async fn test_check_terminal_sources_fails_when_enabled_sources_are_missing()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home)?;

    let item = check_terminal_sources(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        Some(&DeploymentReadinessDescriptor {
            terminal: sinex_primitives::TerminalDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                kitty_enabled: false,
                history_sources: vec![sinex_primitives::TerminalHistorySource {
                    path: PathBuf::from("/tmp/probe-home/.bash_history"),
                    shell: "bash".to_string(),
                }],
            },
            ..Default::default()
        }),
    );
    assert_eq!(item.status, "fail");
    assert!(
        item.description
            .contains("No readable terminal history sources")
    );
    Ok(())
}

#[sinex_test]
async fn test_check_terminal_sources_refuses_descriptor_without_declared_sources()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home)?;
    std::fs::write(home.join(".bash_history"), "echo hidden default\n")?;

    let item = check_terminal_sources(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        Some(&DeploymentReadinessDescriptor {
            terminal: sinex_primitives::TerminalDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                kitty_enabled: false,
                history_sources: Vec::new(),
            },
            ..Default::default()
        }),
    );

    assert_eq!(item.status, "fail");
    assert!(
        item.description
            .contains("terminal.history_sources is empty")
    );
    Ok(())
}

#[sinex_test]
async fn test_check_terminal_sources_rejects_descriptor_declared_native_fish_history()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(home.join(".local/share/fish"))?;
    let fish_history = home.join(".local/share/fish/fish_history");
    std::fs::write(&fish_history, "- cmd: echo fish\n  when: 1234567890\n")?;

    let item = check_terminal_sources(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        Some(&DeploymentReadinessDescriptor {
            terminal: sinex_primitives::TerminalDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                kitty_enabled: false,
                history_sources: vec![sinex_primitives::TerminalHistorySource {
                    path: fish_history.clone(),
                    shell: "fish".to_string(),
                }],
            },
            ..Default::default()
        }),
    );
    assert_eq!(item.status, "fail");
    assert!(
        item.description
            .contains("native Fish YAML history is unsupported")
    );
    assert!(
        item.description
            .contains(&fish_history.display().to_string())
    );
    Ok(())
}

#[sinex_test]
async fn test_check_terminal_sources_rejects_descriptor_declared_elvish_history()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(home.join(".config/elvish"))?;
    let elvish_db = home.join(".config/elvish/db");
    std::fs::write(&elvish_db, "not-supported")?;

    let item = check_terminal_sources(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        Some(&DeploymentReadinessDescriptor {
            terminal: sinex_primitives::TerminalDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                kitty_enabled: false,
                history_sources: vec![sinex_primitives::TerminalHistorySource {
                    path: elvish_db.clone(),
                    shell: "elvish".to_string(),
                }],
            },
            ..Default::default()
        }),
    );
    assert_eq!(item.status, "fail");
    assert!(
        item.description
            .contains("native Elvish history database is unsupported")
    );
    assert!(item.description.contains(&elvish_db.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn test_check_activitywatch_db_accepts_valid_sqlite_history()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    let aw_dir = home.join(".local/share/activitywatch/aw-server-rust");
    std::fs::create_dir_all(&aw_dir)?;

    let aw_db = aw_dir.join("sqlite.db");
    let conn = rusqlite::Connection::open(&aw_db)?;
    conn.execute(
        "CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        [],
    )?;
    conn.execute(
        "CREATE TABLE events (
                bucketrow INTEGER NOT NULL,
                starttime INTEGER NOT NULL,
                endtime INTEGER NOT NULL,
                data TEXT
            )",
        [],
    )?;

    let item = check_activitywatch_db(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        None,
    );
    assert_eq!(item.status, "pass");
    assert!(item.description.contains("ActivityWatch SQLite history"));
    Ok(())
}

#[sinex_test]
async fn test_check_activitywatch_db_fails_when_enabled_path_is_missing()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home)?;
    let missing_db = home.join(".local/share/activitywatch/aw-server-rust/sqlite.db");

    let item = check_activitywatch_db(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        Some(&DeploymentReadinessDescriptor {
            desktop: sinex_primitives::DesktopDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                activitywatch_db_path: Some(missing_db.clone()),
                ..Default::default()
            },
            ..Default::default()
        }),
    );
    assert_eq!(item.status, "fail");
    assert!(item.description.contains(&missing_db.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn test_check_activitywatch_db_refuses_descriptor_without_declared_path()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home)?;

    let item = check_activitywatch_db(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home,
        },
        Some(&DeploymentReadinessDescriptor {
            desktop: sinex_primitives::DesktopDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                activitywatch_db_path: None,
                ..Default::default()
            },
            ..Default::default()
        }),
    );
    assert_eq!(item.status, "fail");
    assert!(
        item.description
            .contains("desktop.activitywatch_db_path is unset")
    );
    Ok(())
}

#[sinex_test]
async fn test_check_schema_apply_requires_database_url_when_expected()
-> ::xtask::sandbox::TestResult<()> {
    let item = check_schema_apply(
        None,
        Some(&DeploymentReadinessDescriptor {
            expectations: sinex_primitives::DeploymentExpectations {
                schema_apply: true,
                ..Default::default()
            },
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(item.status, "fail");
    assert!(
        item.description
            .contains("deployment descriptor database runtime")
    );
    Ok(())
}

#[sinex_test]
async fn test_resolve_database_probe_target_uses_descriptor_runtime()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        source: Some("nixos".to_string()),
        database: sinex_primitives::DeploymentDatabaseRuntime {
            enabled: true,
            host: Some("127.0.0.1".to_string()),
            port: Some(5432),
            name: Some("sinex_prod".to_string()),
            user: Some("sinex".to_string()),
            local_auth: Some("scram-sha-256".to_string()),
            password_required: true,
        },
        secrets: sinex_primitives::DeploymentSecrets {
            database_password_file: Some(PathBuf::from("/run/agenix/sinex-local-db")),
            ..Default::default()
        },
        ..Default::default()
    };

    let target = resolve_database_probe_target(None, Some(&descriptor))?
        .expect("descriptor runtime should produce a database probe target");
    assert_eq!(
        target.database_url,
        "postgresql://sinex@127.0.0.1:5432/sinex_prod"
    );
    assert_eq!(
        target.password_file,
        Some(PathBuf::from("/run/agenix/sinex-local-db"))
    );
    assert!(target.password_required);
    assert_eq!(target.source, "nixos");
    Ok(())
}

#[sinex_test]
async fn test_resolve_database_probe_target_does_not_graft_descriptor_secrets_onto_database_url()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        source: Some("nixos".to_string()),
        database: sinex_primitives::DeploymentDatabaseRuntime {
            enabled: true,
            host: Some("127.0.0.1".to_string()),
            port: Some(5432),
            name: Some("sinex_prod".to_string()),
            user: Some("sinex".to_string()),
            local_auth: Some("scram-sha-256".to_string()),
            password_required: true,
        },
        secrets: sinex_primitives::DeploymentSecrets {
            database_password_file: Some(PathBuf::from("/run/agenix/sinex-local-db")),
            ..Default::default()
        },
        ..Default::default()
    };

    let target = resolve_database_probe_target(
        Some("postgresql:///sinex_dev?host=/tmp/sinex-test-run"),
        Some(&descriptor),
    )?
    .expect("explicit DATABASE_URL should produce a probe target");

    assert_eq!(
        target.database_url,
        "postgresql:///sinex_dev?host=/tmp/sinex-test-run"
    );
    assert_eq!(target.password_file, None);
    assert!(!target.password_required);
    assert_eq!(target.source, "DATABASE_URL");
    Ok(())
}

#[sinex_test]
async fn test_resolve_effective_database_probe_url_keeps_socket_database_url_without_descriptor_password()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        source: Some("nixos".to_string()),
        database: sinex_primitives::DeploymentDatabaseRuntime {
            enabled: true,
            host: Some("127.0.0.1".to_string()),
            port: Some(5432),
            name: Some("sinex_prod".to_string()),
            user: Some("sinex".to_string()),
            local_auth: Some("scram-sha-256".to_string()),
            password_required: true,
        },
        secrets: sinex_primitives::DeploymentSecrets {
            database_password_file: Some(PathBuf::from("/run/agenix/sinex-local-db")),
            ..Default::default()
        },
        ..Default::default()
    };

    let (effective_url, source) = resolve_effective_database_probe_url(
        Some("postgresql:///sinex_dev?host=/tmp/sinex-test-run"),
        Some(&descriptor),
        "runtime metrics",
    )?
    .expect("explicit DATABASE_URL should be usable without descriptor password grafting");

    assert_eq!(
        effective_url,
        "postgresql:///sinex_dev?host=/tmp/sinex-test-run"
    );
    assert_eq!(source, "DATABASE_URL");
    Ok(())
}

#[sinex_test]
async fn test_check_hyprland_socket_rejects_multiple_instances_without_signature()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let runtime_dir = temp.path();
    let hypr_dir = runtime_dir.join("hypr");
    std::fs::create_dir_all(hypr_dir.join("one"))?;
    std::fs::create_dir_all(hypr_dir.join("two"))?;
    std::fs::write(hypr_dir.join("one/.socket2.sock"), "")?;
    std::fs::write(hypr_dir.join("two/.socket2.sock"), "")?;

    let mut env = EnvGuard::new();
    env.set(
        "SINEX_HYPRLAND_RUNTIME_DIR",
        runtime_dir.display().to_string(),
    );
    env.clear("SINEX_HYPRLAND_INSTANCE_SIGNATURE");
    env.clear("HYPRLAND_INSTANCE_SIGNATURE");

    let item = check_hyprland_socket(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home: runtime_dir.to_path_buf(),
        },
        None,
    );
    assert_eq!(item.status, "fail");
    assert!(item.description.contains("Multiple Hyprland instances"));
    Ok(())
}

#[sinex_test]
async fn test_collect_hyprland_socket_candidates_reports_entry_failures()
-> ::xtask::sandbox::TestResult<()> {
    let error =
        collect_hyprland_socket_candidates(vec![Err(std::io::Error::other("readdir exploded"))])
            .unwrap_err();

    assert!(error.to_string().contains("readdir exploded"));
    Ok(())
}

#[sinex_test]
async fn test_collect_hyprland_socket_candidates_keeps_only_event_sockets()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let matching = temp.path().join("one");
    let ignored = temp.path().join("two");
    std::fs::create_dir_all(&matching)?;
    std::fs::create_dir_all(&ignored)?;
    std::fs::write(matching.join(".socket2.sock"), "")?;

    let candidates =
        collect_hyprland_socket_candidates(vec![Ok(matching.clone()), Ok(ignored.clone())])?;

    assert_eq!(candidates, vec![matching]);
    Ok(())
}

#[sinex_test]
async fn test_runtime_dir_for_target_ignores_current_xdg_runtime_for_other_uid()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("UID", "4242");
    env.set("XDG_RUNTIME_DIR", "/run/user/4242");

    let runtime_dir = runtime_dir_for_target(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home: PathBuf::from("/home/probe-user"),
        },
        None,
    )?;

    assert_eq!(runtime_dir, PathBuf::from("/run/user/1000"));
    Ok(())
}

#[sinex_test]
async fn test_runtime_dir_for_target_ignores_ambient_env_when_descriptor_present()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_HYPRLAND_RUNTIME_DIR", "/tmp/ambient-runtime");
    env.set("UID", "4242");
    env.set("XDG_RUNTIME_DIR", "/run/user/4242");

    let runtime_dir = runtime_dir_for_target(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home: PathBuf::from("/home/probe-user"),
        },
        Some(&DeploymentReadinessDescriptor {
            target: Some(sinex_primitives::DeploymentTarget {
                user: "probe-user".to_string(),
                uid: Some(1000),
                home: Some(PathBuf::from("/home/probe-user")),
            }),
            desktop: sinex_primitives::DesktopDeploymentSurface::default(),
            ..Default::default()
        }),
    )?;

    assert_eq!(runtime_dir, PathBuf::from("/run/user/1000"));
    Ok(())
}

#[sinex_test]
async fn test_runtime_dir_for_target_rejects_invalid_uid_env() -> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("UID", "not-a-uid");
    env.clear("SINEX_HYPRLAND_RUNTIME_DIR");
    env.clear("XDG_RUNTIME_DIR");

    let error = runtime_dir_for_target(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home: PathBuf::from("/home/probe-user"),
        },
        None,
    )
    .expect_err("invalid UID should fail honestly");

    let detail = format!("{error:#}");
    assert!(detail.contains("failed to resolve current principal for Hyprland runtime selection"));
    assert!(detail.contains("failed to parse UID environment variable"));
    Ok(())
}

#[sinex_test]
async fn test_check_realm_accessible_surfaces_current_principal_resolution_failure()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("UID", "not-a-uid");

    let item = check_realm_accessible(&TargetIdentity {
        user: "probe-user".to_string(),
        uid: 4242,
        home: PathBuf::from("/tmp/probe-home"),
    });

    assert_eq!(item.status, "skip");
    assert!(item.blocking);
    assert!(
        item.description
            .contains("Could not determine the current principal:")
    );
    assert!(
        item.description
            .contains("failed to parse UID environment variable")
    );
    Ok(())
}

#[sinex_test]
async fn test_check_hyprland_socket_fails_when_enabled_runtime_is_missing()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let runtime_dir = temp.path().join("missing-runtime");

    let item = check_hyprland_socket(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home: temp.path().to_path_buf(),
        },
        Some(&DeploymentReadinessDescriptor {
            desktop: sinex_primitives::DesktopDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                runtime_dir: Some(runtime_dir),
                ..Default::default()
            },
            ..Default::default()
        }),
    );
    assert_eq!(item.status, "fail");
    assert!(item.description.contains("Hyprland runtime is unavailable"));
    Ok(())
}

#[sinex_test]
async fn test_check_hyprland_socket_ignores_ambient_env_when_descriptor_present()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let ambient_runtime = temp.path().join("ambient-runtime");
    let ambient_socket = ambient_runtime.join("ambient/.socket2.sock");
    std::fs::create_dir_all(ambient_socket.parent().expect("socket parent"))?;
    std::fs::write(&ambient_socket, "")?;

    let mut env = EnvGuard::new();
    env.set(
        "SINEX_HYPRLAND_EVENT_SOCKET",
        ambient_socket.display().to_string(),
    );

    let item = check_hyprland_socket(
        &TargetIdentity {
            user: "probe-user".to_string(),
            uid: 1000,
            home: temp.path().to_path_buf(),
        },
        Some(&DeploymentReadinessDescriptor {
            desktop: sinex_primitives::DesktopDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                runtime_dir: Some(temp.path().join("missing-runtime")),
                ..Default::default()
            },
            ..Default::default()
        }),
    );

    assert_eq!(item.status, "fail");
    assert!(item.description.contains("Hyprland runtime is unavailable"));
    Ok(())
}

#[sinex_test]
async fn test_build_gateway_probe_client_allows_http_without_ca() -> ::xtask::sandbox::TestResult<()>
{
    let mut env = EnvGuard::new();
    env.clear("SINEX_RPC_CA_CERT");
    env.clear("SINEX_RPC_CLIENT_CERT");
    env.clear("SINEX_RPC_CLIENT_KEY");

    let _client = build_gateway_probe_client("http://127.0.0.1:9999", None).await?;
    Ok(())
}

#[sinex_test]
async fn test_build_gateway_probe_client_requires_readable_ca_for_https()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let missing_ca = temp.path().join("missing-ca.pem");

    let mut env = EnvGuard::new();
    env.set("SINEX_RPC_CA_CERT", missing_ca.display().to_string());
    env.clear("SINEX_RPC_CLIENT_CERT");
    env.clear("SINEX_RPC_CLIENT_KEY");

    let error = build_gateway_probe_client("https://127.0.0.1:9999", None)
        .await
        .expect_err("HTTPS readiness probing should fail without a readable CA");
    assert!(
        error
            .to_string()
            .contains("failed to read RPC CA certificate")
    );
    Ok(())
}

#[sinex_test]
async fn test_build_gateway_probe_client_uses_descriptor_trust_anchor()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    crate::tls::generate_dev_certs(&crate::tls::CertConfig {
        output_dir: temp.path().to_path_buf(),
        san: vec!["127.0.0.1".to_string(), "localhost".to_string()],
        ca_name: "Doctor Test CA".to_string(),
        validity_days: 30,
        force: true,
    })?;

    let mut env = EnvGuard::new();
    env.clear("SINEX_RPC_CA_CERT");
    env.clear("SINEX_RPC_CLIENT_CERT");
    env.clear("SINEX_RPC_CLIENT_KEY");

    let descriptor = DeploymentReadinessDescriptor {
        secrets: sinex_primitives::DeploymentSecrets {
            gateway_tls_trust_anchor_file: Some(temp.path().join("ca.pem")),
            ..Default::default()
        },
        ..Default::default()
    };

    let _client = build_gateway_probe_client("https://127.0.0.1:9999", Some(&descriptor)).await?;
    Ok(())
}

#[sinex_test]
async fn test_resolve_gateway_probe_tls_paths_prefers_descriptor_trust_anchor()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let descriptor_ca = temp.path().join("descriptor-ca.pem");
    let env_ca = temp.path().join("env-ca.pem");
    std::fs::write(&descriptor_ca, "descriptor")?;
    std::fs::write(&env_ca, "env")?;

    let mut env = EnvGuard::new();
    env.set("SINEX_RPC_CA_CERT", env_ca.display().to_string());

    let descriptor = DeploymentReadinessDescriptor {
        secrets: sinex_primitives::DeploymentSecrets {
            gateway_tls_trust_anchor_file: Some(descriptor_ca.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    let paths = resolve_gateway_probe_tls_paths(Some(&descriptor));
    assert_eq!(paths.trust_anchor, Some(descriptor_ca));
    Ok(())
}

#[sinex_test]
async fn test_resolve_gateway_probe_tls_paths_falls_back_when_descriptor_omits_trust_anchor()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let env_ca = temp.path().join("env-ca.pem");
    std::fs::write(&env_ca, "env")?;

    let mut env = EnvGuard::new();
    env.set("SINEX_RPC_CA_CERT", env_ca.display().to_string());

    let descriptor = DeploymentReadinessDescriptor::default();

    let paths = resolve_gateway_probe_tls_paths(Some(&descriptor));
    assert_eq!(paths.trust_anchor, Some(env_ca));
    Ok(())
}

#[sinex_test]
async fn test_required_nats_stream_names_follow_environment() -> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_ENVIRONMENT", "prod");

    let streams = required_nats_stream_names()?;
    assert!(streams.iter().all(|stream| stream.starts_with("PROD_")));
    assert!(streams.contains(&"PROD_SINEX_RAW_EVENTS".to_string()));
    assert!(streams.contains(&"PROD_SOURCE_MATERIAL".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_check_secret_materials_requires_gateway_admin_token()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let cert = temp.path().join("server.pem");
    let key = temp.path().join("server-key.pem");
    let db = temp.path().join("db-password");
    let missing_admin = temp.path().join("missing-gateway-admin-token");
    std::fs::write(&cert, "cert")?;
    std::fs::write(&key, "key")?;
    std::fs::write(&db, "password")?;

    let mut env = EnvGuard::new();
    env.set("SINEX_GATEWAY_TLS_CERT", cert.display().to_string());
    env.set("SINEX_GATEWAY_TLS_KEY", key.display().to_string());
    env.set("SINEX_DATABASE_PASSWORD_FILE", db.display().to_string());
    env.set(
        "SINEX_GATEWAY_ADMIN_TOKEN_FILE",
        missing_admin.display().to_string(),
    );

    let item = check_secret_materials(None);
    assert_eq!(item.status, "fail");
    assert!(item.description.contains("gateway-admin-token"));
    Ok(())
}

#[sinex_test]
async fn test_check_secret_materials_respects_descriptor_declared_paths_only()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("db-password");
    std::fs::write(&db, "password")?;

    let mut env = EnvGuard::new();
    env.set(
        "SINEX_GATEWAY_TLS_CLIENT_CA",
        temp.path()
            .join("ambient-client-ca.pem")
            .display()
            .to_string(),
    );
    env.set("SINEX_GATEWAY_REQUIRE_CLIENT_TLS", "1");

    let descriptor = DeploymentReadinessDescriptor {
        secrets: sinex_primitives::DeploymentSecrets {
            database_password_file: Some(db),
            gateway_admin_token_file: None,
            gateway_tls_cert_file: None,
            gateway_tls_key_file: None,
            gateway_tls_trust_anchor_file: None,
            gateway_tls_client_ca_file: None,
            nats_ca_cert_file: None,
            nats_client_cert_file: None,
            nats_client_key_file: None,
            nats_token_file: None,
            nats_creds_file: None,
            nats_nkey_seed_file: None,
        },
        ..Default::default()
    };

    let item = check_secret_materials(Some(&descriptor));
    assert_eq!(item.status, "pass");
    assert!(item.description.contains("database-password"));
    Ok(())
}

#[sinex_test]
async fn test_check_secret_materials_requires_descriptor_database_password_when_auth_required()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        database: sinex_primitives::DeploymentDatabaseRuntime {
            enabled: true,
            host: Some("127.0.0.1".to_string()),
            port: Some(5432),
            name: Some("sinex_prod".to_string()),
            user: Some("sinex".to_string()),
            local_auth: Some("scram-sha-256".to_string()),
            password_required: true,
        },
        ..Default::default()
    };

    let item = check_secret_materials(Some(&descriptor));
    assert_eq!(item.status, "fail");
    assert!(item.description.contains("database-password missing"));
    Ok(())
}

#[sinex_test]
async fn test_check_secret_materials_reports_descriptor_declared_nats_secret()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let token = temp.path().join("nats-token");
    std::fs::write(&token, "token")?;

    let mut env = EnvGuard::new();
    env.set(
        "SINEX_GATEWAY_TLS_CLIENT_CA",
        temp.path()
            .join("ambient-client-ca.pem")
            .display()
            .to_string(),
    );
    env.set("SINEX_GATEWAY_REQUIRE_CLIENT_TLS", "1");

    let descriptor = DeploymentReadinessDescriptor {
        secrets: sinex_primitives::DeploymentSecrets {
            nats_token_file: Some(token),
            ..Default::default()
        },
        ..Default::default()
    };

    let item = check_secret_materials(Some(&descriptor));
    assert_eq!(item.status, "pass");
    assert!(item.description.contains("nats-token"));
    Ok(())
}

#[sinex_test]
async fn test_descriptor_nats_secrets_backfill_connection_config()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        nats: sinex_primitives::DeploymentNatsRuntime {
            servers: vec!["tls://nats.example:4223".to_string()],
        },
        secrets: sinex_primitives::DeploymentSecrets {
            nats_ca_cert_file: Some(PathBuf::from("/run/agenix/sinex-nats-ca")),
            nats_token_file: Some(PathBuf::from("/run/agenix/sinex-nats-token")),
            ..Default::default()
        },
        ..Default::default()
    };

    let config =
        apply_descriptor_nats_overrides(NatsConnectionConfig::default(), Some(&descriptor));
    assert_eq!(
        config.ca_cert,
        Some(PathBuf::from("/run/agenix/sinex-nats-ca"))
    );
    assert_eq!(config.url, "tls://nats.example:4223");
    assert_eq!(
        config.token_file,
        Some(PathBuf::from("/run/agenix/sinex-nats-token"))
    );
    Ok(())
}

#[sinex_test]
async fn test_descriptor_nats_server_overrides_non_default_base_config()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        nats: sinex_primitives::DeploymentNatsRuntime {
            servers: vec!["nats://127.0.0.1:4222".to_string()],
        },
        ..Default::default()
    };

    let base = NatsConnectionConfig {
        url: "nats://localhost:4308".to_string(),
        ..Default::default()
    };

    let config = apply_descriptor_nats_overrides(base, Some(&descriptor));
    assert_eq!(config.url, "nats://127.0.0.1:4222");
    Ok(())
}

#[sinex_test]
async fn test_deployment_nats_config_prefers_descriptor_server_over_ambient_override()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        nats: sinex_primitives::DeploymentNatsRuntime {
            servers: vec!["nats://127.0.0.1:4222".to_string()],
        },
        ..Default::default()
    };

    let config = resolve_deployment_nats_config(
        NatsConnectionConfig::default(),
        Some("nats://localhost:4308"),
        Some(&descriptor),
    );

    assert_eq!(config.url, "nats://127.0.0.1:4222");
    Ok(())
}

#[sinex_test]
async fn test_check_document_roots_requires_declared_roots() -> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        document: sinex_primitives::DocumentDeploymentSurface {
            surface: sinex_primitives::DeploymentSurface {
                enabled: true,
                instances: None,
            },
            allowed_roots: Vec::new(),
            scan_service_unit: Some("sinex-document-scan.service".to_string()),
            timer_unit: Some("sinex-document-scan.timer".to_string()),
            schedule: Some("hourly".to_string()),
            run_on_boot: true,
        },
        ..Default::default()
    };

    let item = check_document_roots(Some(&descriptor));
    assert_eq!(item.status, "fail");
    assert!(item.description.contains("no allowed roots"));
    Ok(())
}

#[sinex_test]
async fn test_check_document_roots_accepts_readable_root() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("Documents");
    fs::create_dir_all(&root)?;
    fs::write(root.join("note.md"), "hello")?;

    let descriptor = DeploymentReadinessDescriptor {
        document: sinex_primitives::DocumentDeploymentSurface {
            surface: sinex_primitives::DeploymentSurface {
                enabled: true,
                instances: None,
            },
            allowed_roots: vec![root.clone()],
            scan_service_unit: Some("sinex-document-scan.service".to_string()),
            timer_unit: Some("sinex-document-scan.timer".to_string()),
            schedule: Some("hourly".to_string()),
            run_on_boot: true,
        },
        ..Default::default()
    };

    let item = check_document_roots(Some(&descriptor));
    assert_eq!(item.status, "pass");
    assert!(item.description.contains(&root.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn test_evaluate_document_scan_units_requires_active_timer_when_scheduled()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        document: sinex_primitives::DocumentDeploymentSurface {
            surface: sinex_primitives::DeploymentSurface {
                enabled: true,
                instances: None,
            },
            allowed_roots: vec![PathBuf::from("/tmp/Documents")],
            scan_service_unit: Some("sinex-document-scan.service".to_string()),
            timer_unit: Some("sinex-document-scan.timer".to_string()),
            schedule: Some("hourly".to_string()),
            run_on_boot: true,
        },
        ..Default::default()
    };

    let item = evaluate_document_scan_units(
        Some(&descriptor),
        Some(Ok(systemd_details("inactive", "dead", "loaded"))),
        Some(Ok(systemd_details("inactive", "dead", "loaded"))),
    );

    assert_eq!(item.status, "fail");
    assert!(item.description.contains("timer"));
    assert!(item.description.contains("not active"));
    Ok(())
}

#[sinex_test]
async fn test_evaluate_document_scan_units_accepts_loaded_service_and_active_timer()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor = DeploymentReadinessDescriptor {
        document: sinex_primitives::DocumentDeploymentSurface {
            surface: sinex_primitives::DeploymentSurface {
                enabled: true,
                instances: None,
            },
            allowed_roots: vec![PathBuf::from("/tmp/Documents")],
            scan_service_unit: Some("sinex-document-scan.service".to_string()),
            timer_unit: Some("sinex-document-scan.timer".to_string()),
            schedule: Some("hourly".to_string()),
            run_on_boot: true,
        },
        ..Default::default()
    };

    let item = evaluate_document_scan_units(
        Some(&descriptor),
        Some(Ok(systemd_details("inactive", "dead", "loaded"))),
        Some(Ok(systemd_details("active", "waiting", "loaded"))),
    );

    assert_eq!(item.status, "pass");
    assert!(item.description.contains("sinex-document-scan.service"));
    assert!(item.description.contains("sinex-document-scan.timer"));
    Ok(())
}

#[sinex_test]
async fn test_check_node_entrypoints_skips_empty_prepared_descriptor_units()
-> ::xtask::sandbox::TestResult<()> {
    let item = check_node_entrypoints(Some(&DeploymentReadinessDescriptor {
        mode: DeploymentReadinessMode::Prepared,
        managed_units: Vec::new(),
        ..Default::default()
    }))
    .await;
    assert_eq!(item.status, "skip");
    assert!(item.description.contains("managed units"));
    Ok(())
}

#[sinex_test]
async fn test_check_node_entrypoints_requires_watchdog_contract() -> ::xtask::sandbox::TestResult<()>
{
    let temp = tempfile::tempdir()?;
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;

    write_executable_script(
        &bin_dir.join("systemctl"),
        r#"#!/bin/sh
if [ "$1" = "show" ] && [ "$2" = "sinex-ingestd.service" ]; then
  printf 'ActiveState=active\nSubState=running\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=0\n'
  exit 0
fi
printf 'unexpected invocation: %s\n' "$*" >&2
exit 1
"#,
    )?;

    let original_path = std::env::var("PATH").unwrap_or_default();
    let mut env = EnvGuard::new();
    env.set("PATH", bin_dir.display().to_string());

    let item = check_node_entrypoints(Some(&DeploymentReadinessDescriptor {
        mode: DeploymentReadinessMode::Prepared,
        managed_units: vec!["sinex-ingestd.service".to_string()],
        ..Default::default()
    }))
    .await;

    drop(env);
    assert_eq!(std::env::var("PATH").unwrap_or_default(), original_path);
    assert_eq!(item.status, "fail");
    assert!(item.description.contains("watchdog_usec=0"));
    Ok(())
}

#[sinex_test]
async fn test_check_singleton_workstation_topology_flags_fanout() -> ::xtask::sandbox::TestResult<()>
{
    let descriptor = DeploymentReadinessDescriptor {
        filesystem: sinex_primitives::DeploymentSurface {
            enabled: true,
            instances: Some(2),
        },
        terminal: sinex_primitives::TerminalDeploymentSurface {
            surface: sinex_primitives::DeploymentSurface {
                enabled: true,
                instances: Some(1),
            },
            kitty_enabled: false,
            history_sources: Vec::new(),
        },
        ..Default::default()
    };

    let item = check_singleton_workstation_topology(Some(&descriptor));
    assert_eq!(item.status, "fail");
    assert!(item.description.contains("filesystem=2"));
    Ok(())
}

#[sinex_test]
async fn test_check_singleton_workstation_topology_skips_prepared_descriptor_without_target()
-> ::xtask::sandbox::TestResult<()> {
    let item = check_singleton_workstation_topology(Some(&DeploymentReadinessDescriptor {
        mode: DeploymentReadinessMode::Prepared,
        filesystem: sinex_primitives::DeploymentSurface {
            enabled: true,
            instances: Some(2),
        },
        ..Default::default()
    }));

    assert_eq!(item.status, "skip");
    assert!(
        item.description
            .contains("singleton defaults are not expected")
    );
    Ok(())
}

#[sinex_test]
async fn test_nixos_descriptor_managed_units_are_consumed_directly()
-> ::xtask::sandbox::TestResult<()> {
    let units = sample_nixos_descriptor().managed_units;
    assert!(units.contains(&"sinex-ingestd.service".to_string()));
    assert!(units.contains(&"sinex-gateway.service".to_string()));
    assert!(units.contains(&"sinex-filesystem-1.service".to_string()));
    assert!(units.contains(&"sinex-source@terminal.atuin-history.service".to_string()));
    assert!(units.contains(&"sinex-source@terminal.bash-history.service".to_string()));
    assert!(units.contains(&"sinex-source@terminal.fish-history.service".to_string()));
    assert!(units.contains(&"sinex-source@terminal.zsh-history.service".to_string()));
    assert!(units.contains(&"sinex-system-1.service".to_string()));
    assert!(units.contains(&"sinex-health-automaton.service".to_string()));
    assert!(!units.iter().any(|unit| unit == "sinex-desktop-1.service"));
    Ok(())
}
