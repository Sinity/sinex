//! Shared liveness evaluation for bounded confirmed-event delivery streams.
//!
//! Confirmed streams are delivery buses, not archives: `DiscardPolicy::Old`
//! may evict an event while a durable automaton consumer is still catching up.
//! This module keeps the retention/gap calculation independent from NATS and
//! from any particular operator surface so every confirmed consumer reports
//! the same evidence and takes the same recovery path.

use time::OffsetDateTime;

/// Default interval between confirmed-stream liveness inspections.
pub const DEFAULT_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
/// Emit an eviction-pressure warning at this fraction of a configured limit.
pub const EVICTION_WARNING_PERCENT: u64 = 80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmedStreamLivenessStatus {
    Nominal,
    ApproachingEviction,
    GapDetected,
}

impl ConfirmedStreamLivenessStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nominal => "nominal",
            Self::ApproachingEviction => "approaching_eviction",
            Self::GapDetected => "gap_detected",
        }
    }

    #[must_use]
    pub const fn health_status(self) -> sinex_primitives::domain::HealthStatus {
        match self {
            Self::Nominal => sinex_primitives::domain::HealthStatus::Healthy,
            Self::ApproachingEviction => sinex_primitives::domain::HealthStatus::Degraded,
            Self::GapDetected => sinex_primitives::domain::HealthStatus::Unhealthy,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConfirmedStreamLivenessSnapshot {
    pub stream_name: String,
    pub consumer_name: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub first_timestamp: OffsetDateTime,
    pub retained_messages: u64,
    pub retained_bytes: u64,
    pub max_messages: u64,
    pub max_bytes: u64,
    pub max_age_secs: u64,
    pub consumer_pending: u64,
    pub consumer_ack_pending: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryObservation {
    pub previous_last_seen: Option<u64>,
    pub stream_sequence: u64,
}

#[derive(Debug, Clone)]
pub struct ConfirmedStreamLivenessAssessment {
    pub stream_name: String,
    pub consumer_name: String,
    pub status: ConfirmedStreamLivenessStatus,
    pub last_seen_sequence: Option<u64>,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub sequence_lag: Option<u64>,
    pub retained_messages: u64,
    pub retained_bytes: u64,
    pub max_messages: u64,
    pub max_bytes: u64,
    pub retained_age_secs: u64,
    pub max_age_secs: u64,
    pub consumer_pending: u64,
    pub consumer_ack_pending: usize,
    pub gap_from_sequence: Option<u64>,
}

impl ConfirmedStreamLivenessAssessment {
    #[must_use]
    pub fn describe(&self) -> String {
        format!(
            "confirmed stream liveness status={} last_seen={:?} retained={}..{} lag={:?} pending={} retained_age_secs={} max_messages={} max_age_secs={} gap_from={:?}; historical catch-up is required before live delivery resumes",
            self.status.as_str(),
            self.last_seen_sequence,
            self.first_sequence,
            self.last_sequence,
            self.sequence_lag,
            self.consumer_pending,
            self.retained_age_secs,
            self.max_messages,
            self.max_age_secs,
            self.gap_from_sequence,
        )
    }
}

#[derive(Debug, Default)]
pub struct ConfirmedStreamLiveness {
    last_seen_sequence: Option<u64>,
}

impl ConfirmedStreamLiveness {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last_seen_sequence: None,
        }
    }

    #[must_use]
    pub const fn last_seen_sequence(&self) -> Option<u64> {
        self.last_seen_sequence
    }

    /// Record the stream sequence from a delivered JetStream message.
    pub fn observe_delivery(&mut self, stream_sequence: u64) -> DeliveryObservation {
        let previous_last_seen = self.last_seen_sequence;
        self.last_seen_sequence = Some(
            self.last_seen_sequence
                .map_or(stream_sequence, |last| last.max(stream_sequence)),
        );
        DeliveryObservation {
            previous_last_seen,
            stream_sequence,
        }
    }

    #[must_use]
    pub fn assess(
        &self,
        snapshot: &ConfirmedStreamLivenessSnapshot,
        now: OffsetDateTime,
    ) -> ConfirmedStreamLivenessAssessment {
        self.assess_from(snapshot, now, self.last_seen_sequence)
    }

    #[must_use]
    pub fn assess_after_delivery(
        &self,
        snapshot: &ConfirmedStreamLivenessSnapshot,
        now: OffsetDateTime,
        observation: DeliveryObservation,
    ) -> ConfirmedStreamLivenessAssessment {
        self.assess_from(snapshot, now, observation.previous_last_seen)
    }

    fn assess_from(
        &self,
        snapshot: &ConfirmedStreamLivenessSnapshot,
        now: OffsetDateTime,
        gap_reference: Option<u64>,
    ) -> ConfirmedStreamLivenessAssessment {
        let sequence_lag = gap_reference.map(|last| snapshot.last_sequence.saturating_sub(last));
        let gap_from_sequence = gap_reference.filter(|last| {
            snapshot.last_sequence > last.saturating_add(1)
                && snapshot.first_sequence > last.saturating_add(1)
        });
        let retained_age_secs = if snapshot.retained_messages == 0 {
            0
        } else {
            (now - snapshot.first_timestamp)
                .whole_seconds()
                .max(0)
                .try_into()
                .unwrap_or(u64::MAX)
        };

        let capacity_warning = near_limit(snapshot.retained_messages, snapshot.max_messages)
            || near_limit(snapshot.retained_bytes, snapshot.max_bytes)
            || near_limit(retained_age_secs, snapshot.max_age_secs);
        let lag_warning = gap_reference.is_some_and(|last| {
            near_limit(
                snapshot.last_sequence.saturating_sub(last),
                snapshot.max_messages,
            )
        });

        let status = if gap_from_sequence.is_some() {
            ConfirmedStreamLivenessStatus::GapDetected
        } else if capacity_warning || lag_warning {
            ConfirmedStreamLivenessStatus::ApproachingEviction
        } else {
            ConfirmedStreamLivenessStatus::Nominal
        };

        ConfirmedStreamLivenessAssessment {
            stream_name: snapshot.stream_name.clone(),
            consumer_name: snapshot.consumer_name.clone(),
            status,
            last_seen_sequence: self.last_seen_sequence,
            first_sequence: snapshot.first_sequence,
            last_sequence: snapshot.last_sequence,
            sequence_lag,
            retained_messages: snapshot.retained_messages,
            retained_bytes: snapshot.retained_bytes,
            max_messages: snapshot.max_messages,
            max_bytes: snapshot.max_bytes,
            retained_age_secs,
            max_age_secs: snapshot.max_age_secs,
            consumer_pending: snapshot.consumer_pending,
            consumer_ack_pending: snapshot.consumer_ack_pending,
            gap_from_sequence,
        }
    }
}

fn near_limit(value: u64, limit: u64) -> bool {
    limit > 0 && u128::from(value) * 100 >= u128::from(limit) * u128::from(EVICTION_WARNING_PERCENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(
        first: u64,
        last: u64,
        messages: u64,
        max_messages: u64,
    ) -> ConfirmedStreamLivenessSnapshot {
        ConfirmedStreamLivenessSnapshot {
            stream_name: "confirmed".to_string(),
            consumer_name: "automaton".to_string(),
            first_sequence: first,
            last_sequence: last,
            first_timestamp: OffsetDateTime::now_utc(),
            retained_messages: messages,
            retained_bytes: 1,
            max_messages,
            max_bytes: 0,
            max_age_secs: 0,
            consumer_pending: 0,
            consumer_ack_pending: 0,
        }
    }

    #[test]
    fn detects_retention_gap_against_the_last_seen_sequence() {
        let mut liveness = ConfirmedStreamLiveness::new();
        liveness.observe_delivery(10);

        let assessment = liveness.assess(&snapshot(15, 20, 6, 100), OffsetDateTime::now_utc());

        assert_eq!(
            assessment.status,
            ConfirmedStreamLivenessStatus::GapDetected
        );
        assert_eq!(assessment.gap_from_sequence, Some(10));
        assert_eq!(assessment.last_seen_sequence, Some(10));
    }

    #[test]
    fn warns_before_retention_message_limit_is_exhausted() {
        let liveness = ConfirmedStreamLiveness::new();
        let assessment = liveness.assess(&snapshot(1, 8, 8, 10), OffsetDateTime::now_utc());

        assert_eq!(
            assessment.status,
            ConfirmedStreamLivenessStatus::ApproachingEviction
        );
        assert!(assessment.describe().contains("approaching_eviction"));
    }

    #[test]
    fn delivery_observation_preserves_the_previous_sequence_for_gap_checks() {
        let mut liveness = ConfirmedStreamLiveness::new();
        assert_eq!(
            liveness.observe_delivery(7),
            DeliveryObservation {
                previous_last_seen: None,
                stream_sequence: 7,
            }
        );
        assert_eq!(
            liveness.observe_delivery(9),
            DeliveryObservation {
                previous_last_seen: Some(7),
                stream_sequence: 9,
            }
        );
    }
}
