//! Library items and renditions computed from visible scans and previews.
use crate::derived_event::derived;
use crate::{Computed, Env, Input, InputQuery, KeyState, KeyView, Keyed, PerEvent};
use serde_json::Value;
use sinex_model::event_types::library::{FileCopy, FilenameHints, Item, Rendition, RenditionRole};
use sinex_model::{Event, RecordState, Timestamp};
use std::collections::BTreeMap;

pub struct LibraryItems;

impl Keyed for LibraryItems {
    const NAME: &'static str = "library.items";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["library.copy", "library.preview"];
    const OUTPUTS: &'static [&'static str] = &["library.item", "library.rendition"];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        input.payload.get("item").cloned().into_iter().collect()
    }

    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        key.as_str()?;
        Some(
            InputQuery::window(
                Self::INPUTS,
                Timestamp::from_unix_secs(-62_135_596_800).expect("year 1"),
                Timestamp::from_unix_secs(253_402_300_799).expect("year 9999"),
            )
            .only("item", key.clone()),
        )
    }

    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }

    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        let mut copies: Vec<(&Event, FileCopy)> = inputs
            .iter()
            .filter(|event| event.event_type == "library.copy")
            .filter_map(|event| {
                serde_json::from_value(event.payload.clone())
                    .ok()
                    .map(|copy| (event, copy))
            })
            .collect();
        copies.sort_by_key(|(event, copy)| {
            (
                copy.part_index,
                event
                    .location()
                    .map(|(location, index)| (location.anchor, index))
                    .unwrap_or_default(),
            )
        });
        // Removal describes availability, never loss of the stored original.
        let present: Vec<_> = copies
            .iter()
            .filter(|(_, copy)| copy.vanished_at.is_none())
            .collect();
        let Some((_, first)) = present.first().copied() else {
            return Computed::default();
        };
        let (_, latest) = present.last().copied().expect("present scan");
        let mut revisions: BTreeMap<&str, (&FileCopy, Vec<&Event>)> = BTreeMap::new();
        for (event, copy) in &present {
            revisions
                .entry(&copy.content)
                .or_insert_with(|| (copy, Vec::new()))
                .1
                .push(event);
        }
        let mut outputs = Vec::new();
        for (copy, evidence) in revisions.values() {
            let original = Rendition {
                rendition: format!("{}:{}", first.item, copy.content),
                item: first.item.clone(),
                revision: copy.content.clone(),
                part_index: copy.part_index,
                role: RenditionRole::Original,
                content: copy.content.clone(),
                media_type: copy.media_type.clone(),
                width: copy.width,
                height: copy.height,
                duration_ms: copy.duration_ms,
                made_from: None,
                made_with: Default::default(),
                by: Self::NAME.into(),
                at: copy.scanned_at,
                withdrawn_at: None,
            };
            outputs.push(derived(&original, Self::NAME, evidence.iter().copied()));
        }
        for preview in inputs
            .iter()
            .filter(|event| event.event_type == "library.preview")
        {
            let Some((copy, evidence)) = preview.payload["revision"]
                .as_str()
                .and_then(|revision| revisions.get(revision))
            else {
                continue;
            };
            let mut payload = preview.payload.clone();
            payload["part_index"] = Value::from(copy.part_index);
            if let Ok(rendition) = serde_json::from_value::<Rendition>(payload) {
                outputs.push(derived(
                    &rendition,
                    Self::NAME,
                    evidence.iter().copied().chain(std::iter::once(preview)),
                ));
            }
        }
        let mut paths = BTreeMap::new();
        for (_, copy) in &copies {
            paths.insert(&copy.path, copy);
        }
        let replaces = present
            .iter()
            .rev()
            .find(|(_, copy)| copy.content != latest.content)
            .map(|(_, copy)| copy.content.clone())
            .or_else(|| latest.previous_content.clone());
        let item = Item {
            item: first.item.clone(),
            revision: latest.content.clone(),
            replaces,
            content: latest.content.clone(),
            media_type: latest.media_type.clone(),
            copies: paths
                .values()
                .filter(|copy| copy.vanished_at.is_none())
                .map(|copy| format!("{}:{}", copy.host, copy.path))
                .collect(),
            label: first.path.rsplit('/').next().map(str::to_owned),
            by: Self::NAME.into(),
            at: latest.scanned_at,
            withdrawn_at: None,
        };
        outputs.push(derived(
            &item,
            Self::NAME,
            copies.iter().map(|(event, _)| *event),
        ));
        Computed {
            outputs,
            waiting: false,
        }
    }
}

#[cfg(test)]
mod tests;

pub struct Hints;
impl PerEvent for Hints {
    const NAME: &'static str = "library.filename-hints";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["library.copy"];
    const OUTPUTS: &'static [&'static str] = &["library.filename_hints"];
    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let event = input.event;
        if event.record_state == RecordState::Removed {
            return Ok(vec![]);
        }
        let path = event
            .payload
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or("library copy has no path")?;
        let content = event
            .payload
            .get("content")
            .and_then(serde_json::Value::as_str)
            .ok_or("library copy has no content")?;
        let name = path.rsplit('/').next().unwrap_or(path);
        let stem = name.rsplit_once('.').map_or(name, |(stem, _)| stem);
        let year = isolated(stem, 4, |c| c.is_ascii_digit()).and_then(|s| s.parse().ok());
        let external = isolated(stem, 32, |c| c.is_ascii_hexdigit()).map(str::to_owned);
        let (author, title) = stem.split_once(" - ").map_or((None, None), |(a, t)| {
            let title = t
                .rfind(" (")
                .filter(|&i| {
                    t.ends_with(')')
                        && t[i + 2..t.len() - 1].len() == 4
                        && t[i + 2..t.len() - 1].bytes().all(|b| b.is_ascii_digit())
                })
                .map_or(t, |i| &t[..i]);
            (nonempty(a), nonempty(title))
        });
        let payload = FilenameHints {
            copy: event.id,
            path: path.into(),
            content: content.into(),
            author_hint: author,
            title_hint: title,
            year_hint: year,
            external_id_hint: external,
            rule_version: Self::VERSION,
        };
        Ok(vec![crate::derived_event::derived(
            &payload,
            Self::NAME,
            [event],
        )])
    }
}
fn nonempty(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_owned())
}
fn isolated(text: &str, length: usize, accept: impl Fn(u8) -> bool) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        if !accept(bytes[start]) {
            start += 1;
            continue;
        }
        let mut end = start + 1;
        while end < bytes.len() && accept(bytes[end]) {
            end += 1;
        }
        if end - start == length {
            return text.get(start..end);
        }
        start = end;
    }
    None
}


#[cfg(test)]
#[path = "library/filename_hints_tests.rs"]
mod filename_hints_tests;
