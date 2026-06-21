//! Typed runtime-pressure response vocabulary.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Runtime response selected for an observed pressure condition.
///
/// This is an observation/response vocabulary, not a scheduler policy. Health
/// and DLQ DTOs use it so code compares typed values while preserving stable
/// snake_case wire strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePressureAction {
    /// No runtime response is available or required.
    None,
    /// Admit normally.
    Admit,
    /// Admit while surfacing pressure to operators.
    AdmitWithPressure,
    /// Inspect manually before mutation/retry.
    Inspect,
    /// Throttle automatic intake or retry.
    Throttle,
}

impl RuntimePressureAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Admit => "admit",
            Self::AdmitWithPressure => "admit_with_pressure",
            Self::Inspect => "inspect",
            Self::Throttle => "throttle",
        }
    }

    /// Select the strongest response among two observed actions.
    #[must_use]
    pub const fn strongest(self, other: Self) -> Self {
        match (self, other) {
            (Self::Throttle, _) | (_, Self::Throttle) => Self::Throttle,
            (Self::Inspect, _) | (_, Self::Inspect) => Self::Inspect,
            (Self::AdmitWithPressure, _) | (_, Self::AdmitWithPressure) => Self::AdmitWithPressure,
            (Self::Admit, _) | (_, Self::Admit) => Self::Admit,
            (Self::None, Self::None) => Self::None,
        }
    }
}

impl fmt::Display for RuntimePressureAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
