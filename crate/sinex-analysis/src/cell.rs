//! Observation states (spec `analysis.md` §3.1). Every value reported at a
//! unit carries one, so "nothing happened" and "nothing was observed" never
//! look alike. There is no `Default` and no conversion to the bare value: code
//! that wants a number matches on the state.
use serde::{Deserialize, Serialize};

/// A value at a unit, with what Sinex could see of that unit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Cell<T> {
    /// The sources could observe the whole unit and did; the value may be zero.
    Observed { value: T },
    /// The sources could observe part of the unit; the value covers that part.
    Partial { value: T, observed_fraction: f64 },
    /// Recorded rows exist, but whether the source saw the whole unit is
    /// unknown. A lower bound, never treated as observed.
    Recorded { value: T },
    /// No value, and why.
    Missing { reason: Missing },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Missing {
    /// No source feeding the metric was running or exported for the unit.
    NoOpportunity,
    /// A recorded gap covers the unit.
    Gap,
    /// Coverage cannot be established and nothing was recorded.
    CoverageUnknown,
    /// The metric is undefined here (a ratio with a zero denominator).
    NotApplicable,
    /// A key the metric's relation depends on is being recomputed.
    Recomputing,
}

impl Missing {
    /// Rank used when a paired method drops a unit for both sides: the worse
    /// reason wins (`analysis.md` §6.6).
    pub fn severity(self) -> u8 {
        match self {
            Missing::NoOpportunity => 6,
            Missing::Gap => 5,
            Missing::CoverageUnknown => 4,
            Missing::Recomputing => 3,
            Missing::NotApplicable => 1,
        }
    }
}

/// Why a cell was left out of a method's population.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exclusion {
    NoOpportunity,
    Gap,
    CoverageUnknown,
    Recomputing,
    Recorded,
    Partial,
    NotApplicable,
}

impl Exclusion {
    fn severity(self) -> u8 {
        match self {
            Exclusion::NoOpportunity => 7,
            Exclusion::Gap => 6,
            Exclusion::CoverageUnknown => 5,
            Exclusion::Recomputing => 4,
            Exclusion::Recorded => 3,
            Exclusion::Partial => 2,
            Exclusion::NotApplicable => 1,
        }
    }

    /// The worse of two reasons, for a unit dropped on both sides of a pair.
    pub fn worse(self, other: Self) -> Self {
        if other.severity() > self.severity() {
            other
        } else {
            self
        }
    }
}

impl From<Missing> for Exclusion {
    fn from(m: Missing) -> Self {
        match m {
            Missing::NoOpportunity => Exclusion::NoOpportunity,
            Missing::Gap => Exclusion::Gap,
            Missing::CoverageUnknown => Exclusion::CoverageUnknown,
            Missing::NotApplicable => Exclusion::NotApplicable,
            Missing::Recomputing => Exclusion::Recomputing,
        }
    }
}

/// Which cells a method may use (`analysis.md` §6.1): observed cells, partial
/// ones only at or above a stated fraction, recorded ones only when admitted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Eligibility {
    pub min_observed_fraction: Option<f64>,
    pub admit_recorded: bool,
}

impl<T: Copy> Cell<T> {
    /// The value if the cell is eligible, else why it is not.
    pub fn eligible(&self, rule: Eligibility) -> Result<T, Exclusion> {
        match *self {
            Cell::Observed { value } => Ok(value),
            Cell::Partial {
                value,
                observed_fraction,
            } => match rule.min_observed_fraction {
                Some(min) if observed_fraction >= min => Ok(value),
                _ => Err(Exclusion::Partial),
            },
            Cell::Recorded { value } if rule.admit_recorded => Ok(value),
            Cell::Recorded { .. } => Err(Exclusion::Recorded),
            Cell::Missing { reason } => Err(reason.into()),
        }
    }
}

impl<T> Cell<T> {
    pub fn state_name(&self) -> &'static str {
        match self {
            Cell::Observed { .. } => "observed",
            Cell::Partial { .. } => "partial",
            Cell::Recorded { .. } => "recorded",
            Cell::Missing {
                reason: Missing::Gap,
            } => "gap",
            Cell::Missing {
                reason: Missing::NotApplicable,
            } => "not_applicable",
            Cell::Missing { .. } => "unknown",
        }
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Cell::Observed { value } | Cell::Partial { value, .. } | Cell::Recorded { value } => {
                Some(value)
            }
            Cell::Missing { .. } => None,
        }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Cell<U> {
        match self {
            Cell::Observed { value } => Cell::Observed { value: f(value) },
            Cell::Partial {
                value,
                observed_fraction,
            } => Cell::Partial {
                value: f(value),
                observed_fraction,
            },
            Cell::Recorded { value } => Cell::Recorded { value: f(value) },
            Cell::Missing { reason } => Cell::Missing { reason },
        }
    }
}

#[cfg(test)]
mod tests;
