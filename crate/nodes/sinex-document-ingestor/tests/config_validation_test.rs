use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_document_config_default_valid() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec!["/tmp".to_string()],
    };

    assert!(config.validate().is_ok());

    Ok(())
}

#[sinex_test]
async fn test_document_config_max_document_size_too_small() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 512, // Less than 1024
        allowed_roots: vec!["/tmp".to_string()],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("must be between 1KB and 512MB")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_max_document_size_too_large() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 513 * 1024 * 1024, // Greater than 512 MB
        allowed_roots: vec!["/tmp".to_string()],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("must be between 1KB and 512MB")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_max_document_size_edge_case_min() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 1024, // Exactly 1KB (minimum)
        allowed_roots: vec!["/tmp".to_string()],
    };

    assert!(config.validate().is_ok());

    Ok(())
}

#[sinex_test]
async fn test_document_config_max_document_size_edge_case_max() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 512 * 1024 * 1024, // Exactly 512MB (maximum)
        allowed_roots: vec!["/tmp".to_string()],
    };

    assert!(config.validate().is_ok());

    Ok(())
}

#[sinex_test]
async fn test_document_config_empty_mime_type_entry() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string(), String::new()],
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec!["/tmp".to_string()],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Supported MIME types cannot contain empty entries")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_whitespace_only_mime_type() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string(), "   ".to_string()],
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec!["/tmp".to_string()],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Supported MIME types cannot contain empty entries")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_empty_allowed_roots() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec![],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Allowed roots must be configured")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_empty_string_in_allowed_roots() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec!["/tmp".to_string(), String::new()],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Allowed roots cannot contain empty entries")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_whitespace_only_root() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec!["   ".to_string()],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Allowed roots cannot contain empty entries")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_invalid_path_in_allowed_roots() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string()],
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec!["\0invalid".to_string()],
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid allowed root")
    );

    Ok(())
}

#[sinex_test]
async fn test_document_config_multiple_valid_roots() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec!["text/plain".to_string(), "application/pdf".to_string()],
        max_document_size: 50 * 1024 * 1024,
        allowed_roots: vec![
            "/home/user/documents".to_string(),
            "/tmp".to_string(),
            "/var/data".to_string(),
        ],
    };

    assert!(config.validate().is_ok());

    Ok(())
}

#[sinex_test]
async fn test_document_config_empty_mime_types_allowed() -> TestResult<()> {
    let config = sinex_document_ingestor::DocumentIngestorConfig {
        supported_mime_types: vec![], // Empty list is valid (means accept all)
        max_document_size: 25 * 1024 * 1024,
        allowed_roots: vec!["/tmp".to_string()],
    };

    assert!(config.validate().is_ok());

    Ok(())
}
