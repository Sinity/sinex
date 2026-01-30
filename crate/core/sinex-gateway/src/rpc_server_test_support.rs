//! Test-only helpers for gateway auth environment handling.

use axum::http::HeaderMap;
use color_eyre::eyre::{self, eyre, WrapErr};
use serde_json::Value;
use sinex_primitives::{Bytes, Seconds};

use crate::rpc_server::{
    constant_time_eq as constant_time_eq_inner, extract_token as extract_token_inner,
    read_token_from_env as read_token_from_env_inner, validate_jsonrpc_request, JsonRpcRequest,
    RpcServerLimits,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayAuthModeSnapshot {
    StaticToken,
}

#[derive(Debug, Clone, Copy)]
pub struct RpcServerLimitsSnapshot {
    pub concurrency_limit: usize,
    pub request_timeout_secs: Seconds,
    pub max_body_bytes: Bytes,
}

pub fn gateway_auth_mode_from_env() -> eyre::Result<GatewayAuthModeSnapshot> {
    match read_token_from_env_inner()? {
        Some(token) => {
            if token.trim().is_empty() {
                Err(eyre!(
                    "SINEX_RPC_TOKEN (or SINEX_GATEWAY_ADMIN_TOKEN_FILE / SINEX_RPC_TOKEN_FILE) is set but empty; refusing to start without a token"
                ))
            } else {
                Ok(GatewayAuthModeSnapshot::StaticToken)
            }
        }
        None => Err(eyre!(
            "SINEX_RPC_TOKEN is not set. Export a token (or SINEX_GATEWAY_ADMIN_TOKEN_FILE / SINEX_RPC_TOKEN_FILE) so the gateway can authenticate RPC clients."
        )),
    }
}

pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    extract_token_inner(headers)
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    constant_time_eq_inner(a, b)
}

pub fn read_token_from_env() -> eyre::Result<Option<String>> {
    read_token_from_env_inner()
}

pub fn rpc_server_limits_snapshot() -> RpcServerLimitsSnapshot {
    let limits = RpcServerLimits::from_env();
    RpcServerLimitsSnapshot {
        concurrency_limit: limits.concurrency_limit,
        request_timeout_secs: Seconds::from_secs(limits.request_timeout.as_secs()),
        max_body_bytes: limits.max_body_bytes,
    }
}

pub fn validate_jsonrpc_value(value: &Value) -> eyre::Result<()> {
    let request: JsonRpcRequest =
        serde_json::from_value(value.clone()).wrap_err("Invalid JSON-RPC request payload")?;
    validate_jsonrpc_request(&request)
}
