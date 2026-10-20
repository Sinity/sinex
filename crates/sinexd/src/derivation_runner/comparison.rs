//! What a recompute would change, computed without writing.
use serde_json::Value;
use sinex_model::Event;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Added,
    Changed,
    Removed,
}

/// What a recompute would change.
#[derive(Debug, Default)]
pub struct Comparison {
    /// The stored comparison's ID.
    pub id: Option<i64>,
    pub keys: usize,
    pub same: usize,
    pub added: usize,
    pub changed: usize,
    pub removed: usize,
    /// (kind, key, old payload, new payload) for the first few of each kind.
    pub examples: Vec<(Change, Value, Option<Value>, Option<Value>)>,
}

impl Comparison {
    pub(super) fn note(
        &mut self,
        kind: Change,
        key: &Value,
        old: Option<&Event>,
        new: Option<&Event>,
        examples: usize,
    ) {
        let count = match kind {
            Change::Added => &mut self.added,
            Change::Changed => &mut self.changed,
            Change::Removed => &mut self.removed,
        };
        *count += 1;
        if self.examples.iter().filter(|e| e.0 == kind).count() < examples {
            self.examples.push((
                kind,
                key.clone(),
                old.map(|e| e.payload.clone()),
                new.map(|e| e.payload.clone()),
            ));
        }
    }
}

impl std::fmt::Display for Comparison {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(id) = self.id {
            writeln!(f, "comparison {id}")?;
        }
        writeln!(
            f,
            "keys {} | outputs unchanged {} | added {} | changed {} | removed {}",
            self.keys, self.same, self.added, self.changed, self.removed
        )?;
        for (kind, key, old, new) in &self.examples {
            writeln!(f, "{kind:?} at {key}")?;
            if let Some(old) = old {
                writeln!(f, "  old {old}")?;
            }
            if let Some(new) = new {
                writeln!(f, "  new {new}")?;
            }
        }
        Ok(())
    }
}
