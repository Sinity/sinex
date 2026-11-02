//! The value the library shows for a target's property or tag, chosen from
//! its current curation records by level: the operator's authored value,
//! then an agent's, then imported values, then proposals. An explicit empty
//! property stops fallback; two different values at the deciding level are a
//! conflict; no value at all is unknown. Nothing is picked by time. Which
//! records are current is the `authored` currency function's answer; this
//! module only ranks them.
use serde::Serialize;
use serde_json::Value;
use sinex_model::event_types::library::{Curation, CurationKind, Target, ValueOrigin};
use std::collections::BTreeMap;

use crate::curation::OPERATOR;

/// How high a record's value ranks, highest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    /// The operator's own authored value.
    Operator,
    /// An agent's (or any other author's) authored value.
    Agent,
    Imported,
    Proposed,
}

pub fn level(record: &Curation) -> Level {
    match record.origin {
        ValueOrigin::Authored if record.author == OPERATOR => Level::Operator,
        ValueOrigin::Authored => Level::Agent,
        ValueOrigin::Imported => Level::Imported,
        ValueOrigin::Proposed => Level::Proposed,
    }
}

/// One current record supporting a shown value.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Supporter {
    /// `<root>@<rev>`.
    pub record: String,
    pub author: String,
    pub level: Level,
    pub cites: Option<String>,
}

impl Supporter {
    fn of(record: &Curation) -> Self {
        Self {
            record: format!("{}@{}", record.root, record.rev),
            author: record.author.clone(),
            level: level(record),
            cites: record.cites.clone(),
        }
    }
}

/// The shown value of one property of one target.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Shown {
    Unknown,
    /// Set to empty at the deciding level.
    Empty {
        level: Level,
        supporters: Vec<Supporter>,
    },
    Value {
        value: Value,
        level: Level,
        supporters: Vec<Supporter>,
    },
    /// Several values at the deciding level, each with its supporters.
    Conflict {
        level: Level,
        values: Vec<(Value, Vec<Supporter>)>,
    },
}

/// The shown value from the current records of one property of one target.
pub fn property(records: &[&Curation]) -> Shown {
    let Some(deciding) = records.iter().map(|r| level(r)).min() else {
        return Shown::Unknown;
    };
    let mut values: BTreeMap<String, (Value, Vec<Supporter>)> = BTreeMap::new();
    for record in records.iter().filter(|r| level(r) == deciding) {
        let value = record.value.clone().unwrap_or(Value::Null);
        values
            .entry(value.to_string())
            .or_insert_with(|| (value, Vec::new()))
            .1
            .push(Supporter::of(record));
    }
    if values.len() > 1 {
        return Shown::Conflict {
            level: deciding,
            values: values.into_values().collect(),
        };
    }
    let (value, supporters) = values.into_values().next().expect("one value");
    if value.is_null() {
        Shown::Empty {
            level: deciding,
            supporters,
        }
    } else {
        Shown::Value {
            value,
            level: deciding,
            supporters,
        }
    }
}

/// Whether a tag is shown on a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TagState {
    Applies,
    /// Rejected at the deciding level: an imported or proposed tag the
    /// operator or an agent says is wrong.
    Rejected,
    /// Applied and rejected at the same level.
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Tag {
    pub tag: String,
    pub state: TagState,
    pub level: Level,
    /// Every current record applying the tag.
    pub applied_by: Vec<Supporter>,
    /// Every current record rejecting it.
    pub rejected_by: Vec<Supporter>,
}

fn rejects(record: &Curation) -> bool {
    record.value == Some(Value::Bool(false))
}

/// The tags of one target from its current tag records. A tag applies while
/// its deciding level applies it; withdrawing the operator's application
/// leaves an agent's or an import's.
pub fn tags(records: &[&Curation]) -> Vec<Tag> {
    let mut by_tag: BTreeMap<&str, Vec<&Curation>> = BTreeMap::new();
    for record in records {
        if let Some(tag) = record.key.as_deref() {
            by_tag.entry(tag).or_default().push(record);
        }
    }
    by_tag
        .into_iter()
        .map(|(tag, statements)| {
            let deciding = statements
                .iter()
                .map(|r| level(r))
                .min()
                .expect("a statement");
            let (rejecting, applying): (Vec<&&Curation>, Vec<&&Curation>) = statements
                .iter()
                .filter(|r| level(r) == deciding)
                .partition(|r| rejects(r));
            let state = match (applying.is_empty(), rejecting.is_empty()) {
                (false, true) => TagState::Applies,
                (true, false) => TagState::Rejected,
                _ => TagState::Conflict,
            };
            let (rejected_by, applied_by): (Vec<&Curation>, Vec<&Curation>) =
                statements.iter().partition(|r| rejects(r));
            Tag {
                tag: tag.to_owned(),
                state,
                level: deciding,
                applied_by: applied_by.into_iter().map(Supporter::of).collect(),
                rejected_by: rejected_by.into_iter().map(Supporter::of).collect(),
            }
        })
        .collect()
}

/// One rater's current rating.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Rating {
    pub scale: String,
    pub value: Value,
    pub supporter: Supporter,
}

/// One current note.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Note {
    pub text: Value,
    pub supporter: Supporter,
}

/// Everything shown for one target.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Values {
    pub target: Target,
    pub tags: Vec<Tag>,
    pub properties: BTreeMap<String, Shown>,
    /// Several raters coexist; each keeps their own.
    pub ratings: Vec<Rating>,
    pub notes: Vec<Note>,
}

/// The shown values of every target the current records describe, from the
/// current records alone.
pub fn values(current: &[Curation]) -> Vec<Values> {
    let mut by_target: BTreeMap<&Target, Vec<&Curation>> = BTreeMap::new();
    for record in current {
        if matches!(
            record.kind,
            CurationKind::Tag | CurationKind::Property | CurationKind::Rating | CurationKind::Note
        ) && let Some(target) = &record.target
        {
            by_target.entry(target).or_default().push(record);
        }
    }
    by_target
        .into_iter()
        .map(|(target, records)| {
            let of = |kind| {
                records
                    .iter()
                    .copied()
                    .filter(move |r| r.kind == kind)
                    .collect::<Vec<_>>()
            };
            let mut properties: BTreeMap<&str, Vec<&Curation>> = BTreeMap::new();
            for record in of(CurationKind::Property) {
                properties
                    .entry(record.key.as_deref().unwrap_or_default())
                    .or_default()
                    .push(record);
            }
            Values {
                target: target.clone(),
                tags: tags(&of(CurationKind::Tag)),
                properties: properties
                    .into_iter()
                    .map(|(name, records)| (name.to_owned(), property(&records)))
                    .collect(),
                ratings: of(CurationKind::Rating)
                    .into_iter()
                    .map(|r| Rating {
                        scale: r.key.clone().unwrap_or_default(),
                        value: r.value.clone().unwrap_or(Value::Null),
                        supporter: Supporter::of(r),
                    })
                    .collect(),
                notes: of(CurationKind::Note)
                    .into_iter()
                    .map(|r| Note {
                        text: r.value.clone().unwrap_or(Value::Null),
                        supporter: Supporter::of(r),
                    })
                    .collect(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests;
