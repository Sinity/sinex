//! Security-focused tests for terminal history configuration.

use camino::Utf8PathBuf;
use sinex_primitives::{Bytes, Seconds};
use sinex_terminal_ingestor::unified_node::{HistorySourceConfig, TerminalConfig};
use xtask::sandbox::{sinex_test, TestResult};
use validator::Validate;

#[sinex_test]
fn rejects_dangerous_history_paths() -> TestResult<()> {
    let config = TerminalConfig {
        history_sources: vec![HistorySourceConfig {
            path: Utf8PathBuf::from("../../../../etc/passwd"),
            shell: "bash".to_string(),
        }],
        polling_interval_secs: Seconds::from_secs(10),
        max_capture_bytes: Bytes::from_bytes(1024),
    };

    assert!(config.validate().is_err());
    Ok(())
}

#[sinex_test]
fn accepts_safe_history_paths() -> TestResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let history_path = Utf8PathBuf::from_path_buf(temp_dir.path().join(".bash_history")).unwrap();

    let config = TerminalConfig {
        history_sources: vec![HistorySourceConfig {
            path: history_path,
            shell: "bash".to_string(),
        }],
        polling_interval_secs: Seconds::from_secs(5),
        max_capture_bytes: Bytes::from_bytes(2048),
    };

    assert!(config.validate().is_ok());
    assert!(config.validate_config().is_ok());
    Ok(())
}
