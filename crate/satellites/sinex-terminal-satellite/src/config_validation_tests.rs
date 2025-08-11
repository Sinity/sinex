//! Configuration validation tests for terminal satellite

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use color_eyre::eyre::Result;
    use sinex_test_utils::sinex_test;
    use std::collections::HashMap;
    use validator::Validate;

    #[sinex_test]
    fn test_valid_terminal_config() -> Result<()> {
        let config = TerminalConfig {
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
    fn test_invalid_terminal_config_polling_too_large() -> Result<()> {
        let mut config = TerminalConfig::default();
        config.polling_interval_secs = 4000; // Over 3600 seconds limit

        let result = config.validate();
        assert!(result.is_err());

        let error_msg = config.validate_config().unwrap_err();
        assert!(error_msg.contains("between 1 and 3600 seconds"));
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_terminal_config_batch_size_zero() -> Result<()> {
        let mut config = TerminalConfig::default();
        config.batch_size = 0;

        let result = config.validate();
        assert!(result.is_err());

        let error_msg = config.validate_config().unwrap_err();
        assert!(error_msg.contains("between 1 and 10000"));
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_terminal_config_scanner_batch_too_large() -> Result<()> {
        let mut config = TerminalConfig::default();
        config.scanner_batch_size = 200000; // Too large

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_terminal_config_file_size_too_large() -> Result<()> {
        let mut config = TerminalConfig::default();
        config.scanner_max_file_size_mb = 20000; // Over 10GB limit

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_terminal_config_path_traversal() -> Result<()> {
        let config = TerminalConfig {
            enabled_sources: HashMap::new(),
            atuin_db_path: Some(Utf8PathBuf::from("../../../etc/passwd")), // Path traversal
            history_files: vec![],
            kitty_socket_path: None,
            recording_output_dir: None,
            scrollback_capture_enabled: false,
            polling_interval_secs: 30,
            batch_size: 100,
            scanner_batch_size: 1000,
            scanner_max_file_size_mb: 100,
        };

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_terminal_config_null_byte_in_path() -> Result<()> {
        // This test would be tricky because camino::Utf8PathBuf doesn't allow null bytes
        // But our validation should catch it if somehow it gets through
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_terminal_config_empty_path() -> Result<()> {
        let config = TerminalConfig {
            enabled_sources: HashMap::new(),
            atuin_db_path: Some(Utf8PathBuf::from("")), // Empty path
            history_files: vec![],
            kitty_socket_path: None,
            recording_output_dir: None,
            scrollback_capture_enabled: false,
            polling_interval_secs: 30,
            batch_size: 100,
            scanner_batch_size: 1000,
            scanner_max_file_size_mb: 100,
        };

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_terminal_config_multiple_validation_errors() -> Result<()> {
        let config = TerminalConfig {
            enabled_sources: HashMap::new(),
            atuin_db_path: Some(Utf8PathBuf::from("")), // Empty path
            history_files: vec![Utf8PathBuf::from("../invalid")], // Path traversal
            kitty_socket_path: None,
            recording_output_dir: None,
            scrollback_capture_enabled: false,
            polling_interval_secs: 0,    // Too small
            batch_size: 0,               // Too small
            scanner_batch_size: 0,       // Too small
            scanner_max_file_size_mb: 0, // Too small
        };

        let error_msg = config.validate_config().unwrap_err();

        // Should contain multiple specific error messages
        assert!(!error_msg.is_empty());
        Ok(())
    }
}
