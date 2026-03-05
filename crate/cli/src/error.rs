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
