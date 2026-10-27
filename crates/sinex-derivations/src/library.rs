//! Library items and their original renditions, computed from visible scans.
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
        let mut revisions: Vec<(&FileCopy, Vec<&Event>)> = Vec::new();
        for (event, copy) in present {
            if let Some((last, evidence)) = revisions.last_mut()
                && last.content == copy.content
            {
                evidence.push(event);
            } else {
                revisions.push((copy, vec![event]));
            }
        }
        let mut outputs = Vec::new();
        for (copy, evidence) in &revisions {
            let revision = copy.revision;
            let original = Rendition {
                rendition: format!("{}:{}:{}", first.item, copy.revision, copy.content),
                item: first.item.clone(),
                revision,
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
            if let Some((_, (_, evidence))) = revisions.iter().enumerate().find(|(index, _)| {
                preview.payload["revision"].as_u64()
                    == Some(u64::from(revisions[*index].0.revision))
            }) && let Ok(rendition) =
                serde_json::from_value::<Rendition>(preview.payload.clone())
            {
                outputs.push(derived(
                    &rendition,
                    Self::NAME,
                    evidence.iter().copied().chain(std::iter::once(preview)),
                ));
            }
        }
        let (latest, _) = revisions.last().expect("at least one revision");
        let mut paths = BTreeMap::new();
        for (_, copy) in &copies {
            paths.insert(&copy.path, copy);
        }
        let item = Item {
            item: first.item.clone(),
            revision: latest.revision,
            replaces: (latest.revision > 1).then_some(latest.revision.saturating_sub(1)),
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
        // Path removals and moves also affect the current item's reading.
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
