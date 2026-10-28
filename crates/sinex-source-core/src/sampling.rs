//! Event selection after parsing. Raw records and parser carry are unchanged.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplingRule {
    pub version: u32,
    pub every: u64,
    pub reason: String,
}

impl SamplingRule {
    pub fn new(every: u64, reason: String) -> Result<Self, String> {
        let rule = Self {
            version: 1,
            every,
            reason,
        };
        rule.validate()?;
        Ok(rule)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err("unknown sampling rule version".into());
        }
        if self.every < 2 || self.reason.trim().is_empty() {
            return Err("sampling needs every >= 2 and a reason".into());
        }
        Ok(())
    }

    /// Discrete changes, gaps and records with typed outputs are always kept.
    /// Callers parse the complete record before asking for event selection.
    pub fn selects(
        &self,
        capture_uuid: Uuid,
        sequence: u64,
        continuous: bool,
        typed: bool,
    ) -> Result<bool, String> {
        self.validate()?;
        if !continuous || typed {
            return Ok(true);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"sinex.event_sampling.v1\0");
        hash.update(capture_uuid.as_bytes());
        hash.update(&sequence.to_le_bytes());
        let number = u64::from_le_bytes(
            hash.finalize().as_bytes()[..8]
                .try_into()
                .expect("eight bytes"),
        );
        let threshold = (1u128 << 64) / u128::from(self.every);
        Ok(u128::from(number) < threshold)
    }
}

#[cfg(test)]
mod tests;
