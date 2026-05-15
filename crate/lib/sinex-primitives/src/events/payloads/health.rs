//! Health-domain event payloads.
//!
//! Hosts personal-health observations from Samsung Health, Sleep As
//! Android, and similar consumer trackers. Per the parser how-to,
//! group by domain not provider — future trackers (Apple Health,
//! Garmin, Oura) land here too.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

/// One sleep session observation from a merged Samsung Health / Sleep As
/// Android export (#1052).
///
/// The `sleep_merged_summary.csv` joins Samsung Health (`sh_*`) data with
/// the Sleep As Android comment (`sa_comment`). Both halves carry useful
/// signal: Samsung exposes heart-rate aggregates + per-stage event counts,
/// Sleep As Android contributes the user's own categorization.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "samsung-health", event_type = "sleep.session")]
pub struct SleepSessionPayload {
    /// Samsung Health row UUID (column `sh_datauuid`). Stable across exports.
    pub sh_data_uuid: String,

    /// Session start time in the local timezone the device recorded
    /// (column `start_local`).
    pub start_at: Timestamp,

    /// Session end time in the local timezone (column `end_local`).
    pub end_at: Timestamp,

    /// Reported duration in minutes (column `sh_duration_minutes`).
    pub duration_minutes: f64,

    /// Per-stage event counts. Samsung exposes these as separate columns:
    /// `events_hr`, `events_light`, `events_deep`, `events_rem`.
    pub events_hr: u32,
    pub events_light: u32,
    pub events_deep: u32,
    pub events_rem: u32,

    /// Total trimmed-event count for the session (column
    /// `trimmed_event_count`).
    pub trimmed_event_count: u32,

    /// Average heart rate (BPM). None when Samsung did not record HR data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hr_avg: Option<f64>,

    /// Minimum heart rate (BPM). None when Samsung did not record HR data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hr_min: Option<f64>,

    /// Maximum heart rate (BPM). None when Samsung did not record HR data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hr_max: Option<f64>,

    /// Free-text Sleep As Android comment (column `sa_comment`). User-edited
    /// tags like `#watch`, `#nap`, sleep notes. None when blank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sa_comment: Option<String>,

    /// Sleep As Android-observed duration vs Samsung Health duration delta
    /// (column `sa_vs_sh_duration_minutes`). Surface for cross-source
    /// reconciliation; small values mean the two sources agree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sa_vs_sh_duration_minutes: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventPayload;

    #[test]
    fn declares_source_and_event_type() {
        assert_eq!(
            SleepSessionPayload::SOURCE.as_static_str(),
            "samsung-health"
        );
        assert_eq!(
            SleepSessionPayload::EVENT_TYPE.as_static_str(),
            "sleep.session"
        );
    }
}
