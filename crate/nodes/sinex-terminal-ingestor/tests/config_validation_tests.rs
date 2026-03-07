use camino::Utf8PathBuf;
use sinex_primitives::{Bytes, Seconds};
use sinex_terminal_ingestor::unified_node::{HistorySourceConfig, TerminalConfig};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn valid_configuration_passes_validation() -> TestResult<()> {
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
        polling_interval_secs: Seconds::from_secs(30),
        max_capture_bytes: Bytes::from_bytes(16 * 1024),
    };

    assert!(config.validate_config().is_ok());
    Ok(())
}

#[sinex_test]
async fn rejects_polling_intervals_above_limit() -> TestResult<()> {
    let mut config = TerminalConfig::default();
    config.polling_interval_secs = Seconds::from_secs(4000);

    let error_msg = config.validate_config().unwrap_err().to_string();
    assert!(error_msg.contains("Polling interval"));
    assert!(error_msg.contains("between 1 and 3600"));
    Ok(())
}

#[sinex_test]
async fn rejects_zero_batch_size() -> TestResult<()> {
    let mut config = TerminalConfig::default();
    config.max_capture_bytes = Bytes::from_bytes(32); // below minimum

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
async fn rejects_overlarge_capture_size() -> TestResult<()> {
    let mut config = TerminalConfig::default();
    config.max_capture_bytes = Bytes::from_bytes(2 * 1024 * 1024);

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
async fn rejects_path_traversal_inputs() -> TestResult<()> {
    let mut config = TerminalConfig::default();
    config.history_sources = vec![HistorySourceConfig {
        path: Utf8PathBuf::from("../invalid"),
        shell: "bash".to_string(),
    }];

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
async fn multiple_validation_errors_are_reported() -> TestResult<()> {
    let config = TerminalConfig {
        history_sources: vec![HistorySourceConfig {
            path: Utf8PathBuf::from("../invalid"),
            shell: String::new(),
        }],
        polling_interval_secs: Seconds::from_secs(0),
        max_capture_bytes: Bytes::from_bytes(0),
    };

    let error_msg = config.validate_config().unwrap_err().to_string();
    assert!(!error_msg.is_empty());
    Ok(())
}
