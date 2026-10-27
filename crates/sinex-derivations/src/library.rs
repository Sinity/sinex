//! Library items and renditions computed from visible scans and previews.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::Value;
use sinex_model::event_types::library::{FileCopy, Item, Rendition, RenditionRole};
use sinex_model::{Event, Timestamp};
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
        let copies: Vec<(&Event, FileCopy)> = inputs
            .iter()
            .filter(|event| event.event_type == "library.copy")
            .filter_map(|event| {
                serde_json::from_value(event.payload.clone())
                    .ok()
                    .map(|copy| (event, copy))
            })
            .collect();
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
            if let Some((_, evidence)) = preview.payload["revision"]
                .as_str()
                .and_then(|revision| revisions.get(revision))
                && let Ok(rendition) = serde_json::from_value::<Rendition>(preview.payload.clone())
            {
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
