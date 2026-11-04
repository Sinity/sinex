//! Reading items, copies, renditions and views from live events. These
//! records are ordinary events, so "current" is the live reading of each
//! record's key; nothing is stored beside them. Curation records follow the
//! authored rules instead ([`crate::curation`]). The statements are the store's
//! generic payload reads (`sinex_store::payloads`); this module names the
//! library's event types and fields.
use serde::de::DeserializeOwned;
use sinex_model::event_types::library::{FileCopy, Item, Rendition, View};
use sinex_store::Connection as Conn;
use sinex_store::payloads::{self, RecordStates};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] sinex_store::Error),
    #[error("stored {0} payload does not decode: {1}")]
    Decode(&'static str, String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn latest_copies(rows: Vec<serde_json::Value>) -> Result<Vec<FileCopy>> {
    let mut latest = std::collections::BTreeMap::new();
    for copy in decode::<FileCopy>(COPY, rows)? {
        let key = (copy.host.clone(), copy.root.clone(), copy.path.clone());
        if latest
            .get(&key)
            .is_none_or(|old: &FileCopy| copy.part_index >= old.part_index)
        {
            latest.insert(key, copy);
        }
    }
    Ok(latest.into_values().collect())
}

fn decode<T: DeserializeOwned>(kind: &'static str, rows: Vec<serde_json::Value>) -> Result<Vec<T>> {
    rows.into_iter()
        .map(|v| serde_json::from_value(v).map_err(|e| Error::Decode(kind, e.to_string())))
        .collect()
}

const ITEM: &str = "library.item";
const COPY: &str = "library.copy";

/// Live items holding any of the copy `keys` (`host:path`), present
/// (not withdrawn).
pub async fn items_at(conn: &mut Conn, keys: &[String]) -> Result<Vec<Item>> {
    let rows =
        payloads::live_containing(conn, ITEM, &["copies"], keys, RecordStates::Present).await?;
    decode(ITEM, rows)
}

/// Live copies under a root, present or vanished.
pub async fn copies(conn: &mut Conn, host: &str, root: &str) -> Result<Vec<FileCopy>> {
    let rows =
        payloads::live_where(conn, COPY, &["root"], &[root.to_owned()], RecordStates::Any).await?;
    Ok(latest_copies(rows)?
        .into_iter()
        .filter(|a| a.host == host)
        .collect())
}

/// Live copies at these `host:path` keys.
pub async fn copies_at(conn: &mut Conn, keys: &[String]) -> Result<Vec<FileCopy>> {
    let paths: Vec<String> = keys
        .iter()
        .filter_map(|k| k.split_once(':').map(|(_, p)| p.to_owned()))
        .collect();
    let rows = payloads::live_where(conn, COPY, &["path"], &paths, RecordStates::Any).await?;
    Ok(latest_copies(rows)?
        .into_iter()
        .filter(|a| keys.contains(&format!("{}:{}", a.host, a.path)))
        .collect())
}

/// Live copies of these bytes anywhere, present ones first.
pub async fn copies_of(conn: &mut Conn, content: &str) -> Result<Vec<FileCopy>> {
    let rows = payloads::live_where(
        conn,
        COPY,
        &["content"],
        &[content.to_owned()],
        RecordStates::Any,
    )
    .await?;
    let keys: Vec<_> = decode::<FileCopy>(COPY, rows)?
        .into_iter()
        .map(|copy| format!("{}:{}", copy.host, copy.path))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut all = copies_at(conn, &keys).await?;
    all.retain(|copy| copy.content == content);
    all.sort_by_key(|a| a.vanished_at.is_some());
    Ok(all)
}

/// The current revision of an item, withdrawn or not.
pub async fn item(conn: &mut Conn, item: &str) -> Result<Option<Item>> {
    let rows =
        payloads::live_where(conn, ITEM, &["item"], &[item.to_owned()], RecordStates::Any).await?;
    Ok(decode(ITEM, rows)?.into_iter().next())
}

/// Live, present items holding these bytes.
pub async fn items_with(conn: &mut Conn, content: &str) -> Result<Vec<Item>> {
    let rows = payloads::live_where(
        conn,
        ITEM,
        &["content"],
        &[content.to_owned()],
        RecordStates::Present,
    )
    .await?;
    decode(ITEM, rows)
}

/// Visible content transitions of an item, oldest first.
pub async fn revisions(conn: &mut Conn, item: &str) -> Result<Vec<Item>> {
    let Some(current) = self::item(conn, item).await? else {
        return Ok(Vec::new());
    };
    let copies: Vec<FileCopy> = about(conn, COPY, &["item"], item).await?;
    Ok(revision_history(current, copies))
}

fn revision_history(current: Item, mut copies: Vec<FileCopy>) -> Vec<Item> {
    copies.sort_by(|a, b| (a.part_index, &a.path).cmp(&(b.part_index, &b.path)));
    let mut previous = None;
    let mut all = Vec::new();
    for copy in copies.into_iter().filter(|copy| copy.vanished_at.is_none()) {
        if previous.as_ref() == Some(&copy.content) {
            continue;
        }
        let revision = Item {
            revision: copy.content.clone(),
            replaces: previous,
            content: copy.content.clone(),
            media_type: copy.media_type,
            at: copy.scanned_at,
            ..current.clone()
        };
        previous = Some(copy.content);
        all.push(revision);
    }
    all
}

async fn about<T: DeserializeOwned>(
    conn: &mut Conn,
    kind: &'static str,
    field: &[&str],
    item: &str,
) -> Result<Vec<T>> {
    let rows =
        payloads::live_where(conn, kind, field, &[item.to_owned()], RecordStates::Any).await?;
    decode(kind, rows)
}

/// Live renditions of an item (any state).
pub async fn renditions(conn: &mut Conn, item: &str) -> Result<Vec<Rendition>> {
    about(conn, "library.rendition", &["item"], item).await
}

/// Views of an item.
pub async fn views(conn: &mut Conn, item: &str) -> Result<Vec<View>> {
    about(conn, "library.view", &["target", "item"], item).await
}

#[cfg(test)]
mod tests;
