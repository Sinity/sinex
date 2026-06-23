use super::DbResult;
use sinex_primitives::Seconds;
use sinex_primitives::error::SinexError;
use std::time::Duration;

const DEFAULT_MODULE_HEARTBEAT_STALE_SECS: Seconds = Seconds::from_secs(120);
pub(super) const SQLSTATE_UNDEFINED_FUNCTION: &str = "42883";
pub const PROJECTION_REBUILD_OPERATION_TYPE: &str = "projection-rebuild";

pub(super) const MANAGED_OPERATION_TYPES: &[&str] = &[
    "replay",
    "archive",
    "restore",
    "purge",
    "tombstone",
    PROJECTION_REBUILD_OPERATION_TYPE,
];

pub(super) fn module_heartbeat_stale_after() -> DbResult<Duration> {
    match std::env::var("SINEX_MODULE_HEARTBEAT_STALE_SECS") {
        Ok(raw) => {
            let value = raw.parse::<u64>().map_err(|error| {
                SinexError::configuration(
                    "SINEX_MODULE_HEARTBEAT_STALE_SECS must be a positive integer",
                )
                .with_std_error(&error)
                .with_context("value", raw.clone())
            })?;

            if value == 0 {
                return Err(SinexError::configuration(
                    "SINEX_MODULE_HEARTBEAT_STALE_SECS must be greater than zero",
                )
                .with_context("value", raw));
            }

            Ok(Duration::from_secs(value))
        }
        Err(std::env::VarError::NotPresent) => Ok(Duration::from_secs(
            DEFAULT_MODULE_HEARTBEAT_STALE_SECS.as_secs(),
        )),
        Err(std::env::VarError::NotUnicode(_)) => Err(SinexError::configuration(
            "SINEX_MODULE_HEARTBEAT_STALE_SECS must be valid UTF-8",
        )),
    }
}

pub(super) fn probe_health<T>(result: DbResult<T>) -> (Option<T>, Option<String>) {
    match result {
        Ok(value) => (Some(value), None),
        Err(error) => {
            let message = error.to_string();
            (None, Some(message))
        }
    }
}

pub(super) fn probe_health_bool(result: DbResult<bool>) -> (bool, Option<String>) {
    match result {
        Ok(value) => (value, None),
        Err(error) => (false, Some(error.to_string())),
    }
}
