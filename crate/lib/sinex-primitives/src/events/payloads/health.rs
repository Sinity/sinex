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

/// Timing precision of a manually declared health observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthTimingQuality {
    Exact,
    Approximate,
    DateOnly,
    Unknown,
}

/// Numeric quantity with caller-supplied units and precision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HealthQuantity {
    pub value: f64,
    pub unit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<String>,
}

/// Structured manual declaration of substance intake (#1348).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "manual-health",
    event_type = "health.substance.intake_recorded"
)]
pub struct HealthSubstanceIntakeRecordedPayload {
    pub intake_id: crate::Uuid,
    pub substance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dose: Option<HealthQuantity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<String>,
    pub occurred_at: Timestamp,
    pub timing_quality: HealthTimingQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    pub note_redacted: bool,
}

/// Structured manual observation of an effect or state after intake (#1348).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "manual-health", event_type = "health.effect.observed")]
pub struct HealthEffectObservationRecordedPayload {
    pub observation_id: crate::Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_intake_id: Option<crate::Uuid>,
    pub effect: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    pub observed_at: Timestamp,
    pub timing_quality: HealthTimingQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    pub note_redacted: bool,
}

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
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn declares_source_and_event_type() -> TestResult<()> {
        assert_eq!(
            SleepSessionPayload::SOURCE.as_static_str(),
            "samsung-health"
        );
        assert_eq!(
            SleepSessionPayload::EVENT_TYPE.as_static_str(),
            "sleep.session"
        );
        assert_eq!(
            HealthSubstanceIntakeRecordedPayload::EVENT_TYPE.as_static_str(),
            "health.substance.intake_recorded"
        );
        assert_eq!(
            HealthEffectObservationRecordedPayload::EVENT_TYPE.as_static_str(),
            "health.effect.observed"
        );
        Ok(())
    }
}
