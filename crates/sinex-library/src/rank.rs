//! Rankings from comparison records, fitted by the shared `ranking` crate
//! (Plackett–Luce with Laplace intervals). A comparison orders k items best
//! first with ties as groups; it is one judgment, never expanded into pairs.
//! The judge set chooses whose comparisons count; the default is the
//! operator's alone.
use ranking::{JudgeSet, Judgment};
use serde_json::Value;
use sinex_model::event_types::library::{Curation, CurationKind, Target};

/// The items of a comparison record, best first, as tie groups of item IDs.
pub fn order(record: &Curation) -> Option<Vec<Vec<String>>> {
    let groups: Vec<Vec<Target>> =
        serde_json::from_value(record.value.as_ref()?.get("order")?.clone()).ok()?;
    Some(
        groups
            .into_iter()
            .map(|group| group.into_iter().map(|target| target.item).collect())
            .collect(),
    )
}

pub fn blind(record: &Curation) -> bool {
    record
        .value
        .as_ref()
        .and_then(|v| v.get("blind"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// The judgments the current comparison records on `scale` state, one per
/// record, judged by the record's author.
pub fn judgments(current: &[Curation], scale: &str) -> Vec<Judgment> {
    current
        .iter()
        .filter(|r| r.kind == CurationKind::Comparison && r.key.as_deref() == Some(scale))
        .filter_map(|r| {
            Some(Judgment {
                judge: r.author.clone(),
                order: order(r)?,
            })
        })
        .collect()
}

/// A judge set from its spelling: `all`, or authors separated by commas.
/// No spelling means the operator's judgments alone.
pub fn judge_set(text: Option<&str>) -> JudgeSet {
    match text.map(str::trim) {
        None | Some("") => JudgeSet::named([crate::curation::OPERATOR]),
        Some("all") => JudgeSet::All,
        Some(names) => JudgeSet::named(
            names
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty()),
        ),
    }
}

#[cfg(test)]
mod tests;
