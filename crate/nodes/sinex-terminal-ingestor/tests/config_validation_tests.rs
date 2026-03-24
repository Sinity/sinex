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

    let err = config.validate_config().unwrap_err();
    assert!(
        err.message().contains("3600"),
        "error should cite the 3600s bound, got: {}",
        err.message()
    );
    Ok(())
}

#[sinex_test]
async fn rejects_below_minimum_capture_size() -> TestResult<()> {
    let mut config = TerminalConfig::default();
    config.max_capture_bytes = Bytes::from_bytes(32); // below 64B minimum

    let err = config.validate_config().unwrap_err();
    assert!(
        err.message().contains("64"),
        "error should cite the 64B minimum, got: {}",
        err.message()
    );
    Ok(())
}

#[sinex_test]
async fn rejects_overlarge_capture_size() -> TestResult<()> {
    let mut config = TerminalConfig::default();
    config.max_capture_bytes = Bytes::from_bytes(2 * 1024 * 1024); // above 1MB maximum

    let err = config.validate_config().unwrap_err();
    assert!(
        err.message().contains("1MB") || err.message().contains("1048576"),
        "error should cite the 1MB bound, got: {}",
        err.message()
    );
    Ok(())
}

#[sinex_test]
async fn rejects_path_traversal_inputs() -> TestResult<()> {
    let mut config = TerminalConfig::default();
    config.history_sources = vec![HistorySourceConfig {
        path: Utf8PathBuf::from("../invalid"),
        shell: "bash".to_string(),
    }];

    let err = config.validate_config().unwrap_err();
    assert!(
        err.message().to_lowercase().contains("path") || err.message().contains("Invalid"),
        "error should describe a path problem, got: {}",
        err.message()
    );
    Ok(())
}

/// Invariant: a maximally-broken config fails validation.
/// Note: validate_config bail-fast returns the FIRST error encountered —
/// multiple violations do not produce multiple error messages.
#[sinex_test]
async fn maximally_invalid_config_fails_validation() -> TestResult<()> {
    let config = TerminalConfig {
        history_sources: vec![HistorySourceConfig {
            path: Utf8PathBuf::from("../invalid"),
            shell: String::new(),
        }],
        polling_interval_secs: Seconds::from_secs(0),
        max_capture_bytes: Bytes::from_bytes(0),
    };

    let err = config
        .validate_config()
        .expect_err("config with multiple violations must fail validation");
    // Bail-fast: returns the first error (path validation), which is a configuration error.
    assert!(
        matches!(err, sinex_primitives::SinexError::Configuration(_)),
        "validation error should be a configuration error, got: {err}"
    );
    Ok(())
}
