use camino::Utf8PathBuf;
use sinex_terminal_satellite::unified_processor::{HistorySourceConfig, TerminalConfig};
use sinex_test_utils::sinex_test;

#[sinex_test]
fn valid_configuration_passes_validation() -> color_eyre::eyre::Result<()> {
    let config = TerminalConfig {
        history_sources: vec![
            HistorySourceConfig {
                path: Utf8PathBuf::from("/home/user/.bash_history"),
                shell: "bash".to_string(),
            },
            HistorySourceConfig {
                path: Utf8PathBuf::from("/home/user/.zsh_history"),
                shell: "zsh".to_string(),
            },
        ],
        polling_interval_secs: 30,
        max_capture_bytes: 16 * 1024,
    };

    assert!(config.validate_config().is_ok());
    Ok(())
}

#[sinex_test]
fn rejects_polling_intervals_above_limit() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.polling_interval_secs = 4000;

    let error_msg = config.validate_config().unwrap_err();
    assert!(error_msg.contains("Polling interval"));
    assert!(error_msg.contains("between 1 and 3600"));
    Ok(())
}

#[sinex_test]
fn rejects_zero_batch_size() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.max_capture_bytes = 32; // below minimum

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn rejects_overlarge_capture_size() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.max_capture_bytes = 2 * 1024 * 1024;

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn rejects_path_traversal_inputs() -> color_eyre::eyre::Result<()> {
    let mut config = TerminalConfig::default();
    config.history_sources = vec![HistorySourceConfig {
        path: Utf8PathBuf::from("../invalid"),
        shell: "bash".to_string(),
    }];

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn multiple_validation_errors_are_reported() -> color_eyre::eyre::Result<()> {
    let config = TerminalConfig {
        history_sources: vec![HistorySourceConfig {
            path: Utf8PathBuf::from("../invalid"),
            shell: "".to_string(),
        }],
        polling_interval_secs: 0,
        max_capture_bytes: 0,
    };

    let error_msg = config.validate_config().unwrap_err();
    assert!(!error_msg.is_empty());
    Ok(())
}
