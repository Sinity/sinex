use crate::handlers::parse_default_on_null;
use crate::rpc_server::RpcAuthContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::{
    PrivateModeReasonClass, RuntimePrivateModeState, load_private_mode_state,
    save_private_mode_state,
};
use sinex_primitives::temporal::Timestamp;
use std::path::Path;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PrivateModeStatusRequest {}

#[derive(Debug, Clone, Deserialize)]
pub struct PrivateModeEnableRequest {
    #[serde(default = "default_actor")]
    pub actor: String,

    #[serde(default)]
    pub reason_class: PrivateModeReasonClass,

    #[serde(default)]
    pub source_classes: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PrivateModeDisableRequest {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PrivateModeStateResponse {
    pub state: RuntimePrivateModeState,
}

fn default_actor() -> String {
    "operator".to_string()
}

impl Default for PrivateModeEnableRequest {
    fn default() -> Self {
        Self {
            actor: default_actor(),
            reason_class: PrivateModeReasonClass::default(),
            source_classes: Vec::new(),
        }
    }
}

pub async fn handle_private_mode_status(state_dir: &Path, params: Value) -> Result<Value> {
    let _req: PrivateModeStatusRequest = parse_default_on_null(params)?;
    private_mode_response(load_private_mode_state(state_dir)?)
}

pub async fn handle_private_mode_enable(
    state_dir: &Path,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let req: PrivateModeEnableRequest = parse_default_on_null(params)?;
    let mut state =
        RuntimePrivateModeState::enabled_by(req.actor, req.source_classes, Timestamp::now());
    state.reason_class = req.reason_class;
    save_private_mode_state(state_dir, &state)?;
    private_mode_response(state)
}

pub async fn handle_private_mode_disable(
    state_dir: &Path,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let _req: PrivateModeDisableRequest = parse_default_on_null(params)?;
    let state = load_private_mode_state(state_dir)?.disable();
    save_private_mode_state(state_dir, &state)?;
    private_mode_response(state)
}

fn private_mode_response(state: RuntimePrivateModeState) -> Result<Value> {
    serde_json::to_value(PrivateModeStateResponse { state }).map_err(|err| {
        SinexError::serialization("failed to serialize private-mode response").with_std_error(&err)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn private_mode_status_defaults_disabled() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;

        let value = handle_private_mode_status(dir.path(), Value::Null).await?;

        let response: PrivateModeStateResponse = serde_json::from_value(value)?;
        assert!(!response.state.enabled);
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_enable_and_disable_round_trip() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let auth = RpcAuthContext::system();

        let enabled = handle_private_mode_enable(
            dir.path(),
            json!({
                "actor": "sinity",
                "reason_class": "policy_hold",
                "source_classes": ["desktop"]
            }),
            &auth,
        )
        .await?;
        let enabled: PrivateModeStateResponse = serde_json::from_value(enabled)?;

        assert!(enabled.state.enabled);
        assert_eq!(enabled.state.actor, "sinity");
        assert_eq!(
            enabled.state.reason_class,
            PrivateModeReasonClass::PolicyHold
        );
        assert_eq!(enabled.state.affected_source_classes, vec!["desktop"]);

        let disabled = handle_private_mode_disable(dir.path(), Value::Null, &auth).await?;
        let disabled: PrivateModeStateResponse = serde_json::from_value(disabled)?;

        assert!(!disabled.state.enabled);
        assert_eq!(disabled.state.actor, "sinity");
        assert_eq!(disabled.state.affected_source_classes, vec!["desktop"]);
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_enable_null_params_uses_operator_defaults()
    -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let auth = RpcAuthContext::system();

        let enabled = handle_private_mode_enable(dir.path(), Value::Null, &auth).await?;
        let enabled: PrivateModeStateResponse = serde_json::from_value(enabled)?;

        assert!(enabled.state.enabled);
        assert_eq!(enabled.state.actor, "operator");
        assert_eq!(
            enabled.state.reason_class,
            PrivateModeReasonClass::OperatorPrivate
        );
        assert!(enabled.state.affected_source_classes.is_empty());
        Ok(())
    }
}
