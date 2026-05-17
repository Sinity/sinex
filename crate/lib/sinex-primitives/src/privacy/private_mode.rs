//! Runtime private-mode state.
//!
//! Private mode is an operator-controlled capture suppression state. This
//! module owns the durable wire/file shape so CLI, gateway, and source workers
//! do not grow parallel interpretations.

use crate::error::SinexError;
use crate::temporal::Timestamp;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const PRIVATE_MODE_STATE_RELATIVE_PATH: &str = "private-mode/state.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivateModeReasonClass {
    OperatorPrivate,
    PolicyHold,
    TestFixture,
}

impl Default for PrivateModeReasonClass {
    fn default() -> Self {
        Self::OperatorPrivate
    }
}

impl std::fmt::Display for PrivateModeReasonClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::OperatorPrivate => "operator_private",
            Self::PolicyHold => "policy_hold",
            Self::TestFixture => "test_fixture",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for PrivateModeReasonClass {
    type Err = SinexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "operator_private" => Ok(Self::OperatorPrivate),
            "policy_hold" => Ok(Self::PolicyHold),
            "test_fixture" => Ok(Self::TestFixture),
            other => Err(SinexError::validation(format!(
                "invalid private-mode reason_class '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePrivateModeState {
    pub enabled: bool,
    pub reason_class: PrivateModeReasonClass,
    pub actor: String,
    pub started_at: Option<Timestamp>,
    pub expires_at: Option<Timestamp>,
    pub affected_source_classes: Vec<String>,
    pub updated_by_operation_id: Option<String>,
}

impl RuntimePrivateModeState {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            reason_class: PrivateModeReasonClass::OperatorPrivate,
            actor: "unknown".to_string(),
            started_at: None,
            expires_at: None,
            affected_source_classes: Vec::new(),
            updated_by_operation_id: None,
        }
    }

    #[must_use]
    pub fn enabled_by(
        actor: impl Into<String>,
        affected_source_classes: Vec<String>,
        now: Timestamp,
    ) -> Self {
        Self {
            enabled: true,
            reason_class: PrivateModeReasonClass::OperatorPrivate,
            actor: actor.into(),
            started_at: Some(now),
            expires_at: None,
            affected_source_classes,
            updated_by_operation_id: None,
        }
    }

    #[must_use]
    pub fn disable(mut self) -> Self {
        self.enabled = false;
        self.expires_at = None;
        self
    }
}

impl Default for RuntimePrivateModeState {
    fn default() -> Self {
        Self::disabled()
    }
}

#[must_use]
pub fn private_mode_state_path(state_dir: &Path) -> PathBuf {
    state_dir.join(PRIVATE_MODE_STATE_RELATIVE_PATH)
}

pub fn load_private_mode_state(state_dir: &Path) -> Result<RuntimePrivateModeState, SinexError> {
    let path = private_mode_state_path(state_dir);
    if !path.exists() {
        return Ok(RuntimePrivateModeState::disabled());
    }

    let raw = std::fs::read_to_string(&path).map_err(|error| {
        SinexError::io("failed to read private-mode state")
            .with_path(path.display())
            .with_std_error(&error)
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SinexError::serialization("failed to parse private-mode state")
            .with_path(path.display())
            .with_std_error(&error)
    })
}

pub fn save_private_mode_state(
    state_dir: &Path,
    state: &RuntimePrivateModeState,
) -> Result<(), SinexError> {
    let path = private_mode_state_path(state_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            SinexError::io("failed to create private-mode state directory")
                .with_path(parent.display())
                .with_std_error(&error)
        })?;
    }

    let body = serde_json::to_vec_pretty(state).map_err(|error| {
        SinexError::serialization("failed to serialize private-mode state")
            .with_path(path.display())
            .with_std_error(&error)
    })?;
    std::fs::write(&path, body).map_err(|error| {
        SinexError::io("failed to write private-mode state")
            .with_path(path.display())
            .with_std_error(&error)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn missing_private_mode_state_defaults_disabled() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state = load_private_mode_state(dir.path())?;

        assert!(!state.enabled);
        assert_eq!(state.actor, "unknown");
        assert!(state.affected_source_classes.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_state_round_trips_json_file() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string(), "clipboard".to_string()],
            Timestamp::UNIX_EPOCH,
        );

        save_private_mode_state(dir.path(), &state)?;
        let loaded = load_private_mode_state(dir.path())?;

        assert_eq!(loaded, state);
        assert!(private_mode_state_path(dir.path()).exists());
        Ok(())
    }
}
