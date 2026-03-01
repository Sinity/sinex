use color_eyre::Report;
use color_eyre::eyre::eyre;

/// Enhance RPC errors with helpful context and suggestions
pub fn enhance_rpc_error(method: &str, err: Report) -> Report {
    let err_str = err.to_string();

    // Check for specific error patterns and enhance
    if is_connection_error(&err_str) {
        return eyre!(
            "Cannot connect to gateway\n\n{}\n\n{}",
            err,
            connection_error_help()
        );
    }

    if is_auth_error(&err_str) {
        return eyre!("Authentication failed\n\n{}\n\n{}", err, auth_error_help());
    }

    if is_not_found_error(&err) {
        return enhance_not_found_error(method, err);
    }

    if is_timeout_error(&err_str) {
        return eyre!("Request timed out\n\n{}\n\n{}", err, timeout_error_help());
    }

    // Return original error if no enhancement applies
    err
}

/// Check if error is a connection error
fn is_connection_error(err_str: &str) -> bool {
    err_str.contains("connection refused")
        || err_str.contains("connection reset")
        || err_str.contains("network unreachable")
        || err_str.contains("host unreachable")
}

/// Check if error is an authentication error
fn is_auth_error(err_str: &str) -> bool {
    err_str.contains("401")
        || err_str.contains("403")
        || err_str.contains("unauthorized")
        || err_str.contains("forbidden")
        || err_str.contains("authentication")
}

/// Check if error is a not found error
pub fn is_not_found_error(err: &Report) -> bool {
    let err_str = err.to_string();
    err_str.contains("404") || err_str.contains("not found")
}

/// Check if error is a timeout
fn is_timeout_error(err_str: &str) -> bool {
    err_str.contains("timeout") || err_str.contains("timed out")
}

/// Enhance not found errors with suggestions
fn enhance_not_found_error(method: &str, err: Report) -> Report {
    let help_text = match method {
        m if m.contains("node") || m.contains("instance") => {
            "Use 'sinexctl node list' to see all available nodes"
        }
        m if m.contains("operation") || m.contains("ops") => {
            "Use 'sinexctl ops list' to see all operations"
        }
        m if m.contains("audit") => "Use 'sinexctl ops list' to see all operations",
        m if m.contains("dlq") => "Use 'sinexctl dlq list' to see all DLQ subjects",
        _ => "Use 'sinexctl --help' to see available commands",
    };

    eyre!("{}\n\n{}", err, help_text)
}

/// Help text for connection errors
fn connection_error_help() -> &'static str {
    "Troubleshooting:\n\
     • Verify gateway is running: systemctl status sinex-gateway\n\
     • Check network connectivity\n\
     • Verify RPC URL: echo $SINEX_RPC_URL\n\
     • Try --insecure if using self-signed certificates (dev only)"
}

/// Help text for authentication errors
fn auth_error_help() -> &'static str {
    "Troubleshooting:\n\
     • Set token: export SINEX_RPC_TOKEN=your-token\n\
     • Or use token file: --token-file ~/.config/sinex/token\n\
     • Verify token is valid: sinexctl gateway ping\n\
     • Check token permissions in gateway config"
}

/// Help text for timeout errors
fn timeout_error_help() -> &'static str {
    "Troubleshooting:\n\
     • Gateway may be under heavy load\n\
     • Try increasing timeout: --timeout 60\n\
     • Check gateway logs for slow queries\n\
     • Verify network latency"
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    fn test_connection_error_detection() -> TestResult<()> {
        assert!(is_connection_error("connection refused"));
        assert!(is_connection_error("connection reset by peer"));
        assert!(is_connection_error("network unreachable"));
        assert!(!is_connection_error("some other error"));
        Ok(())
    }

    #[sinex_test]
    fn test_auth_error_detection() -> TestResult<()> {
        assert!(is_auth_error("HTTP 401"));
        assert!(is_auth_error("HTTP 403 Forbidden"));
        assert!(is_auth_error("authentication failed"));
        assert!(!is_auth_error("HTTP 500"));
        Ok(())
    }

    #[sinex_test]
    fn test_not_found_error_detection() -> TestResult<()> {
        let err = eyre!("HTTP 404: Resource not found");
        assert!(is_not_found_error(&err));

        let err = eyre!("Node not found");
        assert!(is_not_found_error(&err));

        let err = eyre!("Connection timeout");
        assert!(!is_not_found_error(&err));
        Ok(())
    }

    #[sinex_test]
    fn test_timeout_error_detection() -> TestResult<()> {
        assert!(is_timeout_error("request timed out"));
        assert!(is_timeout_error("connection timeout"));
        assert!(!is_timeout_error("connection refused"));
        Ok(())
    }

    #[sinex_test]
    fn test_enhance_connection_error() -> TestResult<()> {
        let original = eyre!("connection refused");
        let enhanced = enhance_rpc_error("test.method", original);
        let enhanced_str = enhanced.to_string();

        assert!(enhanced_str.contains("Cannot connect to gateway"));
        assert!(enhanced_str.contains("Troubleshooting"));
        assert!(enhanced_str.contains("systemctl status"));
        Ok(())
    }

    #[sinex_test]
    fn test_enhance_auth_error() -> TestResult<()> {
        let original = eyre!("HTTP 401: Unauthorized");
        let enhanced = enhance_rpc_error("test.method", original);
        let enhanced_str = enhanced.to_string();

        assert!(enhanced_str.contains("Authentication failed"));
        assert!(enhanced_str.contains("SINEX_RPC_TOKEN"));
        Ok(())
    }

    #[sinex_test]
    fn test_enhance_node_not_found() -> TestResult<()> {
        let original = eyre!("HTTP 404: Node not found");
        let enhanced = enhance_rpc_error("coordination.instance_health", original);
        let enhanced_str = enhanced.to_string();

        assert!(enhanced_str.contains("sinexctl node list"));
        Ok(())
    }

    #[sinex_test]
    fn test_enhance_operation_not_found() -> TestResult<()> {
        let original = eyre!("Operation not found");
        let enhanced = enhance_rpc_error("ops.get", original);
        let enhanced_str = enhanced.to_string();

        // Debug: print the enhanced error
        eprintln!("Enhanced error: {enhanced_str}");

        assert!(
            enhanced_str.contains("sinexctl ops list"),
            "Enhanced error does not contain expected text. Got: {enhanced_str}"
        );
        Ok(())
    }
}
