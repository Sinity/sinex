//! Which item an acquisition belongs to. An acquisition is one copy of a file
//! the library scan saw at a path; an item is the continuing thing those
//! copies are of. Equal bytes never merge items by themselves, and a path
//! alone never shows continuity:
//!
//! - an acquisition that already belongs to an item, with the bytes the item
//!   holds, changes nothing;
//! - a rename (the scan saw the same file, device and inode, with the same
//!   bytes at the old path) moves the item's acquisition to the new path;
//! - the same file changing in place (`previous_content` is the item's
//!   bytes) is a new revision of that item;
//! - anything else is a new item, with its original rendition.
use sinex_model::Timestamp;
use sinex_model::event_types::library::{
    Acquisition, Item, LibraryRecord, Rendition, RenditionRole,
};

/// An acquisition's key in an item's `acquisitions`.
pub fn acquisition_key(host: &str, path: &str) -> String {
    format!("{host}:{path}")
}

/// A new identifier for a library record (a UUIDv7).
pub fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn label(path: &str) -> Option<String> {
    path.rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn original(item: &Item, a: &Acquisition, actor: &str, at: Timestamp) -> Rendition {
    Rendition {
        rendition: new_id(),
        item: item.item.clone(),
        revision: item.revision,
        role: RenditionRole::Original,
        content: a.content.clone(),
        media_type: a.media_type.clone(),
        width: a.width,
        height: a.height,
        duration_ms: a.duration_ms,
        made_from: None,
        made_with: Default::default(),
        by: actor.to_owned(),
        at,
        withdrawn_at: None,
    }
}

/// The library records an acquisition calls for. `item_at` finds the item an
/// acquisition key (`host:path`) belongs to, if any.
pub fn records_for(
    a: &Acquisition,
    item_at: impl Fn(&str) -> Option<Item>,
    actor: &str,
    at: Timestamp,
) -> Vec<LibraryRecord> {
    if a.vanished_at.is_some() {
        // Availability is read from acquisitions; a missing copy changes no
        // item, and an inaccessible root never means deletion.
        return Vec::new();
    }
    let here = acquisition_key(&a.host, &a.path);
    if let Some(existing) = item_at(&here) {
        if existing.content == a.content {
            return Vec::new();
        }
        if a.previous_content.as_deref() == Some(existing.content.as_str()) {
            let item = Item {
                revision: existing.revision + 1,
                replaces: Some(existing.revision),
                content: a.content.clone(),
                media_type: a.media_type.clone(),
                by: actor.to_owned(),
                at,
                ..existing.clone()
            };
            let r = original(&item, a, actor, at);
            return vec![LibraryRecord::Item(item), LibraryRecord::Rendition(r)];
        }
        // Different bytes at a path that belongs to an item without evidence of continuity:
        // the old item no longer has a copy here, and these bytes are a new
        // item.
        let mut old = existing.clone();
        old.acquisitions.retain(|k| *k != here);
        old.by = actor.to_owned();
        old.at = at;
        let mut out = vec![LibraryRecord::Item(old)];
        out.extend(new_item(a, actor, at));
        return out;
    }
    if let Some(from) = &a.moved_from
        && let Some(existing) = item_at(&acquisition_key(&a.host, from))
        && existing.content == a.content
    {
        let mut item = existing.clone();
        let old = acquisition_key(&a.host, from);
        item.acquisitions.retain(|k| *k != old);
        item.acquisitions.push(here);
        item.by = actor.to_owned();
        item.at = at;
        return vec![LibraryRecord::Item(item)];
    }
    new_item(a, actor, at)
}

fn new_item(a: &Acquisition, actor: &str, at: Timestamp) -> Vec<LibraryRecord> {
    let item = Item {
        item: new_id(),
        revision: 1,
        replaces: None,
        content: a.content.clone(),
        media_type: a.media_type.clone(),
        acquisitions: vec![acquisition_key(&a.host, &a.path)],
        label: label(&a.path),
        by: actor.to_owned(),
        at,
        withdrawn_at: None,
    };
    let r = original(&item, a, actor, at);
    vec![LibraryRecord::Item(item), LibraryRecord::Rendition(r)]
}

#[cfg(test)]
mod tests;
