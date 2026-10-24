//! Completed history carries digests and scoped erased membership, never bodies.
use super::*;

#[derive(Default)]
pub struct AppendTail {
    newest_first: Vec<Member>,
    bytes: usize,
}
enum Member {
    Raw(RecordLocation, usize, [u8; 32]),
    Erased(ErasedMember),
}
impl AppendTail {
    fn accepts(&self, length: usize) -> bool {
        self.newest_first.len() < STATE_RECORDS
            && (self.newest_first.is_empty() || self.bytes.saturating_add(length) <= STATE_TAIL)
    }
    /// Call newest to oldest. A single large record is retained, as during poll.
    pub fn push_raw(&mut self, location: RecordLocation, raw: &[u8]) -> Result<bool, DriverError> {
        if raw.is_empty() || location.anchor.checked_add(raw.len() as u64).is_none() {
            return Err(DriverError::InvalidMaterial(
                "invalid completed append coordinates".into(),
            ));
        }
        if !self.accepts(raw.len()) {
            return Ok(false);
        }
        self.newest_first.push(Member::Raw(
            location,
            raw.len(),
            *blake3::hash(raw).as_bytes(),
        ));
        self.bytes = self.bytes.saturating_add(raw.len());
        Ok(true)
    }
    pub fn push_erased(&mut self, member: ErasedMember) -> bool {
        let length = member.coordinates().1;
        if !self.accepts(length) {
            return false;
        }
        self.newest_first.push(Member::Erased(member));
        self.bytes = self.bytes.saturating_add(length);
        true
    }
    pub fn into_state(self) -> Result<AppendState, DriverError> {
        let mut state = AppendState::default();
        for member in self.newest_first.into_iter().rev() {
            match member {
                Member::Raw(location, length, digest) => state.insert_digest(
                    digest,
                    length,
                    location.material,
                    usize::try_from(location.anchor).map_err(|_| {
                        DriverError::InvalidMaterial("append anchor exceeds address space".into())
                    })?,
                ),
                Member::Erased(member) => state.remember_erased(member)?,
            }
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests;
