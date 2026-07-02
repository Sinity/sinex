use super::{
    NatsArgs, RuntimeCli, RuntimeCommand, default_service_name, edge_mode_enabled,
    handle_export_result, parse_checkpoint, render_cli_value, render_optional_cli_timestamp,
    resolve_primary_database_url, validate_identity_token,
};
use crate::runtime::SinexError;
use crate::runtime::stream::Checkpoint;
use sinex_primitives::SanitizedPath;
use std::str::FromStr;
use xtask::sandbox::sinex_serial_test;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn export_result_surfaces_failure_with_path_context() -> TestResult<()> {
    let path =
        SanitizedPath::from_str("/tmp/export.json").expect("test export path should validate");
    let error = handle_export_result(&path, Err(SinexError::io("disk full while exporting")))
        .expect_err("export failures should not be swallowed");

    let message = format!("{error:#}");
    assert!(message.contains("failed to export runtime exploration data"));
    assert!(message.contains("/tmp/export.json"));
    assert!(message.contains("disk full while exporting"));
    Ok(())
}

#[sinex_test]
async fn render_cli_value_is_explicit_on_format_failure() -> TestResult<()> {
    let rendered = render_cli_value::<&str>(Err("bad timestamp field"));

    assert_eq!(rendered, "<format error: bad timestamp field>");
    Ok(())
}

#[sinex_test]
async fn render_optional_cli_timestamp_is_explicit_when_unknown() -> TestResult<()> {
    assert_eq!(render_optional_cli_timestamp(None), "unknown");
    Ok(())
}

fn test_cli_with_database_url(database_url: Option<&str>) -> RuntimeCli {
    RuntimeCli {
        nats: NatsArgs {
            url: "nats://localhost:4222".to_string(),
            name: None,
            require_tls: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            creds_file: None,
            nkey_seed_file: None,
            token: None,
            token_file: None,
        },
        database_url: database_url.map(ToOwned::to_owned),
        service_name: None,
        source: None,
        runner_pack: None,
        work_dir: None,
        namespace: None,
        verbose: 0,
        runtime_config: None,
        command: RuntimeCommand::Service {
            dry_run: true,
            consumer_group: None,
        },
    }
}

#[sinex_test]
async fn parse_checkpoint_rejects_malformed_json_input() -> TestResult<()> {
    let error = parse_checkpoint("{ definitely-not-json")
        .expect_err("JSON-like checkpoint input must not silently fall back to a stream id");

    assert!(format!("{error:#}").contains("Failed to parse checkpoint JSON"));
    Ok(())
}

#[sinex_test]
async fn parse_checkpoint_rejects_invalid_timestamp_like_input() -> TestResult<()> {
    let error = parse_checkpoint("2026-03-28T25:61:61Z").expect_err(
        "timestamp-like checkpoint input must not silently fall back to a stream id",
    );

    assert!(format!("{error:#}").contains("Invalid timestamp format"));
    Ok(())
}

#[sinex_test]
async fn parse_checkpoint_accepts_stream_ids_after_structured_parsers_fail() -> TestResult<()> {
    let checkpoint = parse_checkpoint("1234567890-0")?;
    match checkpoint {
        Checkpoint::Stream { message_id, .. } => {
            assert_eq!(message_id, "1234567890-0");
        }
        other => {
            return Err(SinexError::validation(format!(
                "expected stream checkpoint, got {}",
                other.description()
            ))
            .into());
        }
    }
    Ok(())
}

#[sinex_test]
async fn validate_identity_token_accepts_source_spelling() -> TestResult<()> {
    assert_eq!(
        validate_identity_token("terminal.atuin-history").expect("valid source"),
        "terminal.atuin-history"
    );
    Ok(())
}

#[sinex_test]
async fn validate_identity_token_rejects_shell_syntax() -> TestResult<()> {
    let error = validate_identity_token("terminal;rm -rf")
        .expect_err("identity tokens must not accept shell syntax");
    assert!(error.contains("ASCII letters"));
    Ok(())
}

#[sinex_test]
async fn source_supplies_default_service_name() -> TestResult<()> {
    let mut cli = test_cli_with_database_url(None);
    cli.source = Some("terminal.atuin-history".to_string());

    assert_eq!(
        default_service_name(&cli).as_str(),
        "sinex-terminal.atuin-history"
    );
    Ok(())
}

#[sinex_test]
async fn resolve_primary_database_url_rejects_invalid_namespaced_url() -> TestResult<()> {
    let cli = test_cli_with_database_url(Some("not-a-valid-postgres-url"));
    let error = resolve_primary_database_url(&cli)
        .expect_err("invalid database URLs must not silently bypass namespacing");

    let rendered = format!("{error:#}");
    assert!(rendered.contains("Failed to validate runtime DATABASE_URL"));
    Ok(())
}

#[sinex_serial_test]
async fn edge_mode_requires_truthy_boolean_override() -> xtask::sandbox::TestResult<()> {
    unsafe { std::env::set_var("SINEX_EDGE_MODE", "enabled") };

    assert!(
        !edge_mode_enabled(false),
        "invalid edge-mode override must not silently enable DB-less execution"
    );

    unsafe { std::env::remove_var("SINEX_EDGE_MODE") };
    Ok(())
}
