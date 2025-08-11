//! Configuration validation tests for filesystem watcher

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::eyre::Result;
    use sinex_test_utils::sinex_test;
    use validator::Validate;

    #[sinex_test]
    fn test_valid_filesystem_config() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec!["**/*.txt".to_string(), "src/**/*.rs".to_string()],
            ignore_patterns: vec!["target/**".to_string(), "**/.git/**".to_string()],
            debounce_ms: 100,
            max_depth: Some(5),
        };

        assert!(config.validate().is_ok());
        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_filesystem_config_empty_patterns() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec![],
            ignore_patterns: vec![],
            debounce_ms: 100,
            max_depth: None,
        };

        let result = config.validate();
        assert!(result.is_err());

        let error_msg = config.validate_config().unwrap_err();
        assert!(error_msg.contains("At least one watch pattern"));
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_filesystem_config_bad_glob() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec!["[invalid".to_string()], // Invalid glob syntax
            ignore_patterns: vec![],
            debounce_ms: 100,
            max_depth: None,
        };

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_filesystem_config_dangerous_pattern() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec!["/".to_string()], // Dangerous pattern
            ignore_patterns: vec![],
            debounce_ms: 100,
            max_depth: None,
        };

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_filesystem_config_debounce_too_large() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec!["**/*.txt".to_string()],
            ignore_patterns: vec![],
            debounce_ms: 70000, // Too large (over 60 seconds)
            max_depth: None,
        };

        let result = config.validate();
        assert!(result.is_err());

        let error_msg = config.validate_config().unwrap_err();
        assert!(error_msg.contains("between 1ms and 60 seconds"));
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_filesystem_config_depth_zero() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec!["**/*.txt".to_string()],
            ignore_patterns: vec![],
            debounce_ms: 100,
            max_depth: Some(0), // Invalid - zero depth
        };

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_filesystem_config_depth_too_large() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec!["**/*.txt".to_string()],
            ignore_patterns: vec![],
            debounce_ms: 100,
            max_depth: Some(500), // Too large
        };

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_filesystem_config_validation_messages() -> Result<()> {
        let config = FilesystemConfig {
            watch_patterns: vec![], // Empty - should fail
            ignore_patterns: vec![],
            debounce_ms: 0,     // Too small
            max_depth: Some(0), // Invalid
        };

        let error_msg = config.validate_config().unwrap_err();

        // Should contain multiple specific error messages
        assert!(error_msg.contains("At least one watch pattern"));
        assert!(error_msg.contains("between 1ms and 60 seconds"));
        Ok(())
    }
}
