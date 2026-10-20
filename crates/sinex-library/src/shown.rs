//! Which value is shown for an item's property. An operator override
//! wins, an explicit empty one included; otherwise the imported value;
//! otherwise a model's proposal. Two different values at the deciding level
//! are a conflict; no value at all is unknown. Nothing is ever picked by
//! timestamp.
use sinex_model::event_types::library::{Assignment, Level};
use std::collections::BTreeMap;

/// The shown value of one property of one target, with where it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shown {
    Unknown,
    /// Set to empty by the assignment named.
    Empty {
        by: String,
    },
    Value {
        value: String,
        by: String,
        level: Level,
    },
    /// Several values at the deciding level: (value, assignment).
    Conflict {
        level: Level,
        values: Vec<(String, String)>,
    },
}

fn live(a: &&Assignment) -> bool {
    a.withdrawn_at.is_none()
}

/// The shown value of a single-choice property, from every assignment of
/// that field to the target (live or withdrawn; withdrawn ones are ignored).
pub fn property(assignments: &[Assignment]) -> Shown {
    for level in [Level::Override, Level::Import, Level::Proposal] {
        let at: Vec<&Assignment> = assignments
            .iter()
            .filter(live)
            .filter(|a| a.level == level)
            .collect();
        if at.is_empty() {
            continue;
        }
        let mut values: BTreeMap<Option<&str>, &str> = BTreeMap::new();
        for a in &at {
            values.entry(a.value.as_deref()).or_insert(&a.assignment);
        }
        if values.len() > 1 {
            return Shown::Conflict {
                level,
                values: values
                    .into_iter()
                    .map(|(v, by)| (v.unwrap_or("").to_owned(), by.to_owned()))
                    .collect(),
            };
        }
        let (value, by) = values.into_iter().next().expect("one value");
        return match value {
            None => Shown::Empty { by: by.to_owned() },
            Some(v) => Shown::Value {
                value: v.to_owned(),
                by: by.to_owned(),
                level,
            },
        };
    }
    Shown::Unknown
}

/// A tag that applies, with the assignments supporting it and the highest
/// level among them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tag {
    pub tag: String,
    pub level: Level,
    pub supported_by: Vec<String>,
}

/// The tags that apply: each while at least one live assignment supports it.
/// An operator's explicit empty (`value = None` at override) removes every
/// tag below override, so "no tags" can be stated; an override of `-name`
/// removes that one tag below override (an imported tag the operator
/// disagrees with).
pub fn tags(assignments: &[Assignment]) -> Vec<Tag> {
    let overrides = || {
        assignments
            .iter()
            .filter(live)
            .filter(|a| a.level == Level::Override)
    };
    let cleared = overrides().any(|a| a.value.is_none());
    let denied: Vec<&str> = overrides()
        .filter_map(|a| a.value.as_deref()?.strip_prefix('-'))
        .collect();
    let mut out: BTreeMap<&str, Tag> = BTreeMap::new();
    for a in assignments.iter().filter(live) {
        let Some(tag) = a.value.as_deref() else {
            continue;
        };
        if tag.starts_with('-') {
            continue;
        }
        if (cleared || denied.contains(&tag)) && a.level != Level::Override {
            continue;
        }
        let entry = out.entry(tag).or_insert_with(|| Tag {
            tag: tag.to_owned(),
            level: a.level,
            supported_by: Vec::new(),
        });
        entry.level = entry.level.min(a.level);
        entry.supported_by.push(a.assignment.clone());
    }
    out.into_values().collect()
}

#[cfg(test)]
mod tests;
