use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;

/// Time horizon defines the scope and mode of scanning operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeHorizon {
    Historical { end_time: Timestamp },
    Continuous,
    Snapshot,
}

impl TimeHorizon {
    pub fn is_continuous(&self) -> bool {
        matches!(self, TimeHorizon::Continuous)
    }

    pub fn is_bounded(&self) -> bool {
        matches!(self, TimeHorizon::Historical { .. } | TimeHorizon::Snapshot)
    }

    pub fn end_time(&self) -> Option<Timestamp> {
        if let TimeHorizon::Historical { end_time } = self {
            Some(*end_time)
        } else {
            None
        }
    }
}
