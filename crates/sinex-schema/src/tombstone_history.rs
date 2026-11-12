//! The grandfathered incomplete history is closed by PostgreSQL itself:
//! NOT VALID leaves prior rows untouched and checks every later write.

pub(crate) const COMPLETE: &str = "ALTER TABLE core.event_tombstones
  ADD CONSTRAINT tombstone_history_complete CHECK (
    commit_seq IS NOT NULL AND original_commit_seq IS NOT NULL AND class IS NOT NULL) NOT VALID";

pub(crate) fn statements() -> Vec<String> {
    vec![COMPLETE.into()]
}

pub(crate) fn previous(after_tables: &mut Vec<String>) {
    after_tables.retain(|statement| statement != COMPLETE);
}

#[cfg(test)]
mod tests;
