//! Which item an acquisition belongs to (spec `library.md` §2). Equal bytes
//! never merge items by themselves, and a path alone never shows continuity:
//!
//! - an acquisition an item already adopts, with the bytes the item holds,
//!   changes nothing;
//! - a rename (the scan saw the same file, device and inode, with the same
//!   bytes at the old path) moves the item's acquisition to the new path;
//! - the same file changing in place (`previous_content` is the item's
//!   bytes) is a new revision of that item;
//! - anything else is a new item, with its original rendition.
use sinex_model::Timestamp;
use sinex_model::kinds::library::{Acquisition, Item, LibraryRecord, Rendition, RenditionRole};

/// What the library knows about the item adopting an acquisition.
#[derive(Clone, Debug, PartialEq)]
pub struct Owner {
    pub item: Item,
}

/// An acquisition's key in an item's `acquisitions`.
pub fn key(host: &str, path: &str) -> String {
    format!("{host}:{path}")
}

pub fn mint() -> String {
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
        rendition: mint(),
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

/// The library records an acquisition calls for. `owner_of` finds the item
/// adopting a `host:path` key, if any.
pub fn adopt(
    a: &Acquisition,
    owner_of: impl Fn(&str) -> Option<Owner>,
    actor: &str,
    at: Timestamp,
) -> Vec<LibraryRecord> {
    if a.vanished_at.is_some() {
        // Availability is read from acquisitions; a missing copy changes no
        // item, and an inaccessible root never means deletion.
        return Vec::new();
    }
    let here = key(&a.host, &a.path);
    if let Some(owner) = owner_of(&here) {
        if owner.item.content == a.content {
            return Vec::new();
        }
        if a.previous_content.as_deref() == Some(owner.item.content.as_str()) {
            let item = Item {
                revision: owner.item.revision + 1,
                replaces: Some(owner.item.revision),
                content: a.content.clone(),
                media_type: a.media_type.clone(),
                by: actor.to_owned(),
                at,
                ..owner.item.clone()
            };
            let r = original(&item, a, actor, at);
            return vec![LibraryRecord::Item(item), LibraryRecord::Rendition(r)];
        }
        // Different bytes at an adopted path without evidence of continuity:
        // the old item no longer has a copy here, and these bytes are a new
        // item.
        let mut old = owner.item.clone();
        old.acquisitions.retain(|k| *k != here);
        old.by = actor.to_owned();
        old.at = at;
        let mut out = vec![LibraryRecord::Item(old)];
        out.extend(new_item(a, actor, at));
        return out;
    }
    if let Some(from) = &a.moved_from
        && let Some(owner) = owner_of(&key(&a.host, from))
        && owner.item.content == a.content
    {
        let mut item = owner.item.clone();
        let old = key(&a.host, from);
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
        item: mint(),
        revision: 1,
        replaces: None,
        content: a.content.clone(),
        media_type: a.media_type.clone(),
        acquisitions: vec![key(&a.host, &a.path)],
        label: label(&a.path),
        by: actor.to_owned(),
        at,
        withdrawn_at: None,
    };
    let r = original(&item, a, actor, at);
    vec![LibraryRecord::Item(item), LibraryRecord::Rendition(r)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acq(path: &str, content: &str) -> Acquisition {
        Acquisition {
            host: "h".into(),
            root: "/r".into(),
            path: path.into(),
            content: content.into(),
            size: 3,
            sha256: String::new(),
            md5: String::new(),
            media_type: "image/png".into(),
            width: Some(2),
            height: Some(2),
            duration_ms: None,
            stored: true,
            device: Some(1),
            inode: Some(9),
            modified_at: None,
            moved_from: None,
            previous_content: None,
            vanished_at: None,
        }
    }

    fn at() -> Timestamp {
        Timestamp::from_unix_secs(100).unwrap()
    }

    fn items(r: &[LibraryRecord]) -> Vec<&Item> {
        r.iter()
            .filter_map(|x| match x {
                LibraryRecord::Item(i) => Some(i),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_new_copy_is_a_new_item_with_its_original() {
        let r = adopt(&acq("/r/a.png", "blake3:a"), |_| None, "intake", at());
        assert_eq!(r.len(), 2);
        let i = items(&r)[0];
        assert_eq!(i.revision, 1);
        assert_eq!(i.acquisitions, ["h:/r/a.png"]);
        assert_eq!(i.label.as_deref(), Some("a.png"));
        assert!(
            matches!(&r[1], LibraryRecord::Rendition(x) if x.role == RenditionRole::Original && x.content == "blake3:a")
        );
    }

    #[test]
    fn equal_bytes_at_an_unrelated_path_are_another_item() {
        let first = adopt(&acq("/r/a.png", "blake3:a"), |_| None, "intake", at());
        let owner = items(&first)[0].clone();
        let r = adopt(
            &acq("/r/copy.png", "blake3:a"),
            |k| {
                (k == "h:/r/a.png").then(|| Owner {
                    item: owner.clone(),
                })
            },
            "intake",
            at(),
        );
        assert_ne!(items(&r)[0].item, owner.item);
    }

    #[test]
    fn a_rename_moves_the_acquisition_and_an_edit_makes_a_revision() {
        let first = adopt(&acq("/r/a.png", "blake3:a"), |_| None, "intake", at());
        let owner = items(&first)[0].clone();
        let mut moved = acq("/r/b.png", "blake3:a");
        moved.moved_from = Some("/r/a.png".into());
        let r = adopt(
            &moved,
            |k| {
                (k == "h:/r/a.png").then(|| Owner {
                    item: owner.clone(),
                })
            },
            "intake",
            at(),
        );
        let i = items(&r)[0];
        assert_eq!(i.item, owner.item);
        assert_eq!(i.acquisitions, ["h:/r/b.png"]);
        assert_eq!(i.revision, 1);

        let mut edited = acq("/r/a.png", "blake3:a2");
        edited.previous_content = Some("blake3:a".into());
        let r = adopt(
            &edited,
            |k| {
                (k == "h:/r/a.png").then(|| Owner {
                    item: owner.clone(),
                })
            },
            "intake",
            at(),
        );
        let i = items(&r)[0];
        assert_eq!(
            (i.item.as_str(), i.revision, i.replaces),
            (owner.item.as_str(), 2, Some(1))
        );
        assert_eq!(i.content, "blake3:a2");

        // Replaced by an unrelated file at the same path: no continuity.
        let r = adopt(
            &acq("/r/a.png", "blake3:z"),
            |k| {
                (k == "h:/r/a.png").then(|| Owner {
                    item: owner.clone(),
                })
            },
            "intake",
            at(),
        );
        let is = items(&r);
        assert_eq!(is[0].item, owner.item);
        assert!(is[0].acquisitions.is_empty());
        assert_ne!(is[1].item, owner.item);

        // Unchanged and vanished copies change nothing.
        assert!(
            adopt(
                &acq("/r/a.png", "blake3:a"),
                |_| Some(Owner {
                    item: owner.clone()
                }),
                "intake",
                at()
            )
            .is_empty()
        );
        let mut gone = acq("/r/a.png", "blake3:a");
        gone.vanished_at = Some(at());
        assert!(adopt(&gone, |_| None, "intake", at()).is_empty());
    }
}
