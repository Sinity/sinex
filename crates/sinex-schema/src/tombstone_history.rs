//! A tombstone records its commit position, its reading's original position
//! and its class. Tombstones written before those columns existed lack them
//! and stay as they are: NOT VALID leaves existing rows unchecked while
//! PostgreSQL checks every later write.

pub(crate) const COMPLETE: &str = "ALTER TABLE core.event_tombstones
  ADD CONSTRAINT tombstone_history_complete CHECK (
    commit_seq IS NOT NULL AND original_commit_seq IS NOT NULL AND class IS NOT NULL) NOT VALID";

#[cfg(test)]
mod tests;
