//! The tail a restart rebuilds from completed history: record digests and
//! where each was read, never the record bodies.
use super::*;

#[derive(Default)]
pub struct AppendTail {
    newest_first: Vec<Member>,
    bytes: usize,
}
struct Member {
    location: RecordLocation,
    length: usize,
    digest: [u8; 32],
}
impl AppendTail {
    fn accepts(&self, length: usize) -> bool {
        self.newest_first.len() < STATE_RECORDS
            && (self.newest_first.is_empty() || self.bytes.saturating_add(length) <= STATE_TAIL)
    }
    /// Call newest to oldest. A single large record is kept, as during poll.
    pub fn push_raw(&mut self, location: RecordLocation, raw: &[u8]) -> Result<bool, DriverError> {
        if raw.is_empty() || location.anchor.checked_add(raw.len() as u64).is_none() {
            return Err(DriverError::InvalidMaterial(
                "invalid completed append coordinates".into(),
            ));
        }
        if !self.accepts(raw.len()) {
            return Ok(false);
        }
        self.newest_first.push(Member {
            location,
            length: raw.len(),
            digest: *blake3::hash(raw).as_bytes(),
        });
        self.bytes = self.bytes.saturating_add(raw.len());
        Ok(true)
    }
    pub fn into_state(self) -> Result<AppendState, DriverError> {
        let mut state = AppendState::default();
        for member in self.newest_first.into_iter().rev() {
            state.insert_digest(
                member.digest,
                member.length,
                member.location.material,
                usize::try_from(member.location.anchor).map_err(|_| {
                    DriverError::InvalidMaterial("append anchor exceeds address space".into())
                })?,
            );
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests;
