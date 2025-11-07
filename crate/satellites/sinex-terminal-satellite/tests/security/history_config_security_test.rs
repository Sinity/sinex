//! Security-focused tests for terminal history configuration.

use camino::Utf8PathBuf;
use sinex_terminal_satellite::unified_processor::{HistorySourceConfig, TerminalConfig};
use sinex_test_utils::sinex_test;
use validator::Validate;

#[sinex_test]
fn rejects_dangerous_history_paths() -> color_eyre::eyre::Result<()> {
    let config = TerminalConfig {
        history_sources: vec![HistorySourceConfig {
            path: Utf8PathBuf::from("../../../../etc/passwd"),
            shell: "bash".to_string(),
        }],
        polling_interval_secs: 10,
        max_capture_bytes: 1024,
    };

    assert!(config.validate().is_err());
    Ok(())
}

#[sinex_test]
fn accepts_safe_history_paths() -> color_eyre::eyre::Result<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let history_path = Utf8PathBuf::from_path_buf(temp_dir.path().join(".bash_history")).unwrap();

    let config = TerminalConfig {
        history_sources: vec![HistorySourceConfig {
            path: history_path,
            shell: "bash".to_string(),
        }],
        polling_interval_secs: 5,
        max_capture_bytes: 2048,
    };

    assert!(config.validate().is_ok());
    assert!(config.validate_config().is_ok());
    Ok(())
}
