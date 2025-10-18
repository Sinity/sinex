use camino::Utf8PathBuf;
use sinex_terminal_satellite::unified_processor::TerminalConfig;
use sinex_terminal_satellite::SensdIntegrationConfig;
use sinex_test_utils::sinex_test;
use std::collections::HashMap;
use validator::Validate;

#[sinex_test]
fn valid_configuration_passes_validation() -> color_eyre::eyre::Result<()> {
    let config = TerminalConfig {
        sensd_config: SensdIntegrationConfig::default(),
        enabled_sources: HashMap::new(),
        atuin_db_path: Some(Utf8PathBuf::from(
            "/home/user/.local/share/atuin/history.db",
        )),
        history_files: vec![
            Utf8PathBuf::from("/home/user/.bash_history"),
            Utf8PathBuf::from("/home/user/.zsh_history"),
        ],
        kitty_socket_path: None,
        recording_output_dir: Some(Utf8PathBuf::from("/home/user/recordings")),
        scrollback_capture_enabled: true,
        polling_interval_secs: 30,
        batch_size: 500,
        scanner_batch_size: 2000,
        scanner_max_file_size_mb: 100,
    };

    assert!(config.validate().is_ok());
    assert!(config.validate_config().is_ok());
    Ok(())
}

#[sinex_test]
fn rejects_polling_intervals_above_limit() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.polling_interval_secs = 4000;

    assert!(config.validate().is_err());
    let error_msg = config.validate_config().unwrap_err();
    assert!(error_msg.contains("between 1 and 3600 seconds"));
    Ok(())
}

#[sinex_test]
fn rejects_zero_batch_size() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.batch_size = 0;

    assert!(config.validate().is_err());
    let error_msg = config.validate_config().unwrap_err();
    assert!(error_msg.contains("between 1 and 10000"));
    Ok(())
}

#[sinex_test]
fn rejects_overlarge_scanner_batch_size() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.scanner_batch_size = 200_000;

    assert!(config.validate().is_err());
    Ok(())
}

#[sinex_test]
fn rejects_overlarge_scanner_file_size() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.scanner_max_file_size_mb = 20_000;

    assert!(config.validate().is_err());
    Ok(())
}

#[sinex_test]
fn rejects_path_traversal_inputs() -> color_eyre::eyre::Result<()> {
    let config = TerminalConfig {
        sensd_config: SensdIntegrationConfig::default(),
        enabled_sources: HashMap::new(),
        atuin_db_path: Some(Utf8PathBuf::from("../../../etc/passwd")),
        history_files: vec![],
        kitty_socket_path: None,
        recording_output_dir: None,
        scrollback_capture_enabled: false,
        polling_interval_secs: 30,
        batch_size: 100,
        scanner_batch_size: 1000,
        scanner_max_file_size_mb: 100,
    };

    assert!(config.validate().is_err());
    Ok(())
}

#[sinex_test]
fn multiple_validation_errors_are_reported() -> color_eyre::eyre::Result<()> {
    let config = TerminalConfig {
        sensd_config: SensdIntegrationConfig::default(),
        enabled_sources: HashMap::new(),
        atuin_db_path: Some(Utf8PathBuf::from("")),
        history_files: vec![Utf8PathBuf::from("../invalid")],
        kitty_socket_path: None,
        recording_output_dir: None,
        scrollback_capture_enabled: false,
        polling_interval_secs: 0,
        batch_size: 0,
        scanner_batch_size: 0,
        scanner_max_file_size_mb: 0,
    };

    let error_msg = config.validate_config().unwrap_err();
    assert!(!error_msg.is_empty());
    Ok(())
}
