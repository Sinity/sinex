use color_eyre::Report;
use color_eyre::eyre::eyre;
use reqwest::StatusCode;
use sinex_primitives::rpc::methods;

use crate::client::gateway::GatewayRpcError;

/// Enhance RPC errors with helpful context and suggestions
pub fn enhance_rpc_error(method: &str, err: Report) -> Report {
    // Check for specific error patterns and enhance
    if is_connection_error(&err) {
        return eyre!(
            "Cannot connect to gateway\n\n{}\n\n{}",
            err,
            connection_error_help()
        );
    }

    if is_auth_error(&err) {
        return eyre!("Authentication failed\n\n{}\n\n{}", err, auth_error_help());
    }

    if is_not_found_error(&err) {
        return enhance_not_found_error(method, err);
    }

    if is_timeout_error(&err) {
        return eyre!("Request timed out\n\n{}\n\n{}", err, timeout_error_help());
    }

    // Return original error if no enhancement applies
    err
}

/// Check if error is a connection error
fn is_connection_error(err: &Report) -> bool {
    if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>()
        && reqwest_err.is_connect()
    {
        return true;
    }

    let err_str = err.to_string().to_ascii_lowercase();
    err_str.contains("connection refused")
        || err_str.contains("connection reset")
        || err_str.contains("network unreachable")
        || err_str.contains("host unreachable")
}

/// Check if error is an authentication error
fn is_auth_error(err: &Report) -> bool {
    if matches!(
        extract_status_code(err),
        Some(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
    ) {
        return true;
    }

    let err_str = err.to_string().to_ascii_lowercase();
    err_str.contains("unauthorized")
        || err_str.contains("forbidden")
        || err_str.contains("authentication")
}

/// Check if error is a not found error
#[must_use]
pub fn is_not_found_error(err: &Report) -> bool {
    if let Some(status) = extract_status_code(err)
        && status == StatusCode::NOT_FOUND
    {
        return true;
    }
    err.to_string().to_ascii_lowercase().contains("not found")
}

/// Check if error is a timeout
fn is_timeout_error(err: &Report) -> bool {
    if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>()
        && reqwest_err.is_timeout()
    {
        return true;
    }

    let err_str = err.to_string().to_ascii_lowercase();
    err_str.contains("timeout") || err_str.contains("timed out")
}

fn extract_status_code(err: &Report) -> Option<StatusCode> {
    if let Some(gateway_err) = err.downcast_ref::<GatewayRpcError>()
        && let GatewayRpcError::HttpStatus { status, .. } = gateway_err
    {
        return Some(*status);
    }

    if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>() {
        return reqwest_err.status();
    }

    None
}

/// Enhance not found errors with suggestions
fn enhance_not_found_error(method: &str, err: Report) -> Report {
    let help_text = match method {
        methods::NODES_LIST
        | methods::NODES_DRAIN
        | methods::NODES_RESUME
        | methods::NODES_SET_HORIZON
        | methods::COORDINATION_LIST_INSTANCES
        | methods::COORDINATION_GET_LEADER
        | methods::COORDINATION_INSTANCE_HEALTH => {
            "Use 'sinexctl node list' to see all available nodes"
        }
        methods::OPS_START | methods::OPS_LIST | methods::OPS_GET | methods::OPS_CANCEL => {
            "Use 'sinexctl ops list' to see all operations"
        }
        methods::AUDIT_GET => "Use 'sinexctl ops list' to see all operations",
        methods::DLQ_LIST | methods::DLQ_PEEK | methods::DLQ_REQUEUE | methods::DLQ_PURGE => {
            "Use 'sinexctl dlq list' to see all DLQ subjects"
        }
        methods::REPLAY_CREATE_OPERATION
        | methods::REPLAY_PREVIEW_OPERATION
        | methods::REPLAY_APPROVE_OPERATION
        | methods::REPLAY_EXECUTE_OPERATION
        | methods::REPLAY_CANCEL_OPERATION
        | methods::REPLAY_OPERATION_STATUS
        | methods::REPLAY_LIST_OPERATIONS => "Use 'sinexctl replay list' to see replay operations",
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
