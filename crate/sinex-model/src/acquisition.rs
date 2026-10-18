//! Acquisition evidence is separate from event freshness and historical coverage.
use crate::ValidationError;
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcesRequest {
    pub input: Option<Uuid>,
    pub after_input: Option<Uuid>,
    pub before_seq: Option<i64>,
    pub observation: Option<Uuid>,
    pub after_receipt: Option<Uuid>,
}
impl SourcesRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.before_seq.is_some_and(|n| n < 1)
            || (self.input.is_none() && (self.before_seq.is_some() || self.observation.is_some()))
            || (self.after_receipt.is_some() && self.observation.is_none())
            || (self.after_input.is_some() && self.input.is_some())
        {
            return Err(ValidationError(
                "source detail requires input; receipt continuation requires observation; input and after_input conflict",
            ));
        }
        Ok(())
    }
}

/// Supplied by the daemon owner, never by a sources request.
#[derive(Clone, Debug)]
pub struct CollectorScope {
    pub source: String,
    pub locator: String,
    pub run: Uuid,
}

pub fn current_scope(
    active: &[CollectorScope],
    source: &str,
    locator: &str,
    run: Option<Uuid>,
) -> bool {
    active
        .iter()
        .any(|a| a.source == source && a.locator == locator && Some(a.run) == run)
}

/// Constant-size inherited references, not a second history store. A missing legacy
/// baseline stays unknown until that kind of outcome is directly observed.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeRefs {
    pub last_success: Option<Uuid>,
    pub last_failure: Option<Uuid>,
    pub success_known: bool,
    pub failure_known: bool,
}
impl OutcomeRefs {
    pub fn following(prior: Option<(Uuid, &str, Option<Self>)>) -> Self {
        let Some((id, status, refs)) = prior else {
            return Self {
                success_known: true,
                failure_known: true,
                ..Self::default()
            };
        };
        let mut refs = refs.unwrap_or_default();
        match status {
            "completed" => {
                refs.last_success = Some(id);
                refs.success_known = true;
            }
            "unavailable" | "interrupted" => {
                refs.last_failure = Some(id);
                refs.failure_known = true;
            }
            _ => {}
        }
        refs
    }
}

/// Collector-observed settlement after the writer reply, and its selected next start.
/// This does not timestamp the first provider failure or bound a reading poll.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollSchedule {
    pub version: u8,
    pub settled_at: String,
    pub next_start_at: String,
    pub interval_ms: u64,
}
impl PollSchedule {
    pub fn after(now: OffsetDateTime, interval_ms: u64) -> Result<Self, ValidationError> {
        if !(100..=3_600_000).contains(&interval_ms) {
            return Err(ValidationError("poll interval outside supported bounds"));
        }
        let delta = time::Duration::milliseconds(
            i64::try_from(interval_ms).map_err(|_| ValidationError("poll interval overflow"))?,
        );
        let next = now
            .checked_add(delta)
            .ok_or(ValidationError("poll clock overflow"))?;
        Ok(Self {
            version: 1,
            settled_at: now
                .format(&Rfc3339)
                .map_err(|_| ValidationError("poll clock encoding"))?,
            next_start_at: next
                .format(&Rfc3339)
                .map_err(|_| ValidationError("poll clock encoding"))?,
            interval_ms,
        })
    }
    pub fn validate(&self) -> Result<(), ValidationError> {
        let start = OffsetDateTime::parse(&self.settled_at, &Rfc3339)
            .map_err(|_| ValidationError("invalid settlement clock"))?;
        if self.version != 1
            || Self::after(start, self.interval_ms)?.next_start_at != self.next_start_at
        {
            return Err(ValidationError("invalid poll schedule"));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcquisitionState {
    Unknown,
    Reading,
    Completed,
    Unavailable,
    Interrupted,
    Overdue,
}

pub fn state(
    status: Option<&str>,
    schedule: Option<&PollSchedule>,
    now: OffsetDateTime,
) -> AcquisitionState {
    match status {
        Some("reading") => AcquisitionState::Reading,
        Some("unavailable") => AcquisitionState::Unavailable,
        Some("interrupted") => AcquisitionState::Interrupted,
        Some("completed") => match schedule.filter(|s| s.validate().is_ok()) {
            Some(s) => match OffsetDateTime::parse(&s.next_start_at, &Rfc3339) {
                Ok(due) if now > due => AcquisitionState::Overdue,
                _ => AcquisitionState::Completed,
            },
            None => AcquisitionState::Completed,
        },
        _ => AcquisitionState::Unknown,
    }
}
#[cfg(test)]
#[path = "acquisition_tests.rs"]
mod tests;

/// Versioned collector checkpoint. The epoch belongs to one verified open reader,
/// not to occurrence identity. A new reader always performs a real full read.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtuinCheckpoint {
    pub version: u8,
    pub epoch: Uuid,
    pub schema_version: i64,
    pub user_version: i64,
    pub through_rowid: i64,
    pub full_completed_at: String,
    pub full_due_at: String,
    pub needs_full: bool,
}
impl AtuinCheckpoint {
    pub fn valid(&self) -> bool {
        self.version == 1
            && !self.epoch.is_nil()
            && OffsetDateTime::parse(&self.full_completed_at, &Rfc3339).is_ok()
            && OffsetDateTime::parse(&self.full_due_at, &Rfc3339).is_ok_and(|due| {
                OffsetDateTime::parse(&self.full_completed_at, &Rfc3339)
                    .is_ok_and(|done| due > done)
            })
    }
    pub fn reusable(&self, epoch: Uuid, schema: i64, user: i64, now: OffsetDateTime) -> bool {
        self.valid()
            && self.epoch == epoch
            && self.schema_version == schema
            && self.user_version == user
            && !self.needs_full
            && OffsetDateTime::parse(&self.full_due_at, &Rfc3339).is_ok_and(|due| now < due)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtuinReadPlan {
    pub version: u8,
    pub kind: AtuinReadKind,
    pub checkpoint: AtuinCheckpoint,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtuinReadKind {
    FullValueComparison,
    NewCoordinatesAndPriorUnfinished,
}

/// Where an Atuin read starts: the input, the commit sequence it reads as of, and the last checkpoint.
#[derive(Clone, Debug)]
pub struct AtuinReadStart {
    pub input: Uuid,
    pub snapshot_seq: i64,
    pub checkpoint: Option<AtuinCheckpoint>,
}
