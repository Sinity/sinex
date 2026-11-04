//! What became of every record a material framed, and of every reading those
//! records made. Staging reports it per product, status sums it per input,
//! and a stage refuses to report `ok` when it does not balance: a mismatch is
//! a defect in the pipeline, never a status.
use serde::{Deserialize, Serialize};

/// Counts for one binding, one product or one material, added as reading
/// goes. Each side of an identity is counted at a different point of the
/// pipeline: `framed` by the driver, the record outcomes by the drain, the
/// reading outcomes by admission. So a record or reading lost between them
/// shows as an imbalance.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accounting {
    /// Records the driver framed in the material's bytes.
    pub framed: u64,
    /// Records the input held before and no longer holds. They are in no
    /// new material; each one's live reading is archived as removed at the
    /// source.
    pub removed: u64,
    /// Records the parser read into one or more readings.
    pub read: u64,
    /// Records the parser read into no reading: a header row, a comment, a
    /// declaration the source keeps no event for.
    pub headers: u64,
    /// Records the parser could not read; their bytes stay in the material.
    pub failures: u64,
    /// Readings offered to admission.
    pub readings: u64,
    /// Readings admitted as the first live reading of their record.
    pub new: u64,
    /// Readings equal to the live reading of the same record, from this or an
    /// earlier export: suppressed.
    pub same: u64,
    /// Newer readings that replaced the live reading of their record.
    pub revised: u64,
    /// Readings older than the live reading of their record: suppressed.
    pub older: u64,
    /// Readings superseded by a newer reading of the same record in the same
    /// batch.
    pub superseded: u64,
    /// The first record failure's reason, so the first unreadable form is
    /// shown without a query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failure: Option<String>,
}

impl Accounting {
    pub fn add(&mut self, other: &Accounting) {
        self.framed += other.framed;
        self.removed += other.removed;
        self.read += other.read;
        self.headers += other.headers;
        self.failures += other.failures;
        self.readings += other.readings;
        self.new += other.new;
        self.same += other.same;
        self.revised += other.revised;
        self.older += other.older;
        self.superseded += other.superseded;
        if self.first_failure.is_none() {
            self.first_failure.clone_from(&other.first_failure);
        }
    }

    /// What the parser made of each of a batch's records.
    pub fn records(&mut self, records: &[crate::RecordOutcome]) {
        for record in records {
            match record {
                crate::RecordOutcome::Events { events, .. } if events.is_empty() => {
                    self.headers += 1;
                }
                crate::RecordOutcome::Events { .. } => self.read += 1,
                crate::RecordOutcome::Failure { reason, .. } => {
                    self.failures += 1;
                    if self.first_failure.is_none() {
                        self.first_failure = Some(reason.clone());
                    }
                }
            }
        }
    }

    /// The two identities: every framed record is read, a header or a
    /// failure; every reading offered is new, the same, revised, older or
    /// superseded. `Err` names each identity that does not hold.
    pub fn balanced(&self) -> Result<(), String> {
        let mut problems = Vec::new();
        let outcomes = self.read + self.headers + self.failures;
        if self.framed != outcomes {
            problems.push(format!(
                "{} records framed, but {} read + {} headers + {} failures = {outcomes}",
                self.framed, self.read, self.headers, self.failures
            ));
        }
        let decided = self.new + self.same + self.revised + self.older + self.superseded;
        if self.readings != decided {
            problems.push(format!(
                "{} readings offered, but {} new + {} same + {} revised + {} older + {} superseded = {decided}",
                self.readings, self.new, self.same, self.revised, self.older, self.superseded
            ));
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems.join("; "))
        }
    }
}

#[cfg(test)]
mod tests;
