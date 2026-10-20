//! Reading the library's current state from live events. Library records
//! are ordinary events, so "current" is the live reading of each record's
//! key; nothing is stored beside them. The statements are the store's
//! generic payload reads (`sinex_store::payloads`); this module names the
//! library's event types and fields.
use serde::de::DeserializeOwned;
use sinex_model::event_types::library::{
    Acquisition, Assignment, Collection, Comparison, Entry, Item, Rating, Rendition, View,
};
use sinex_store::Connection as Conn;
use sinex_store::payloads::{self, State};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] sinex_store::Error),
    #[error("stored {0} payload does not decode: {1}")]
    Decode(&'static str, String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn decode<T: DeserializeOwned>(kind: &'static str, rows: Vec<serde_json::Value>) -> Result<Vec<T>> {
    rows.into_iter()
        .map(|v| serde_json::from_value(v).map_err(|e| Error::Decode(kind, e.to_string())))
        .collect()
}

const ITEM: &str = "library.item";
const ACQUISITION: &str = "library.acquisition";

/// Live items holding any of the acquisition `keys` (`host:path`), present
/// (not withdrawn).
pub async fn items_at(conn: &mut Conn, keys: &[String]) -> Result<Vec<Item>> {
    let rows =
        payloads::live_containing(conn, ITEM, &["acquisitions"], keys, State::Present).await?;
    decode(ITEM, rows)
}

/// Live acquisitions under a root, present or vanished.
pub async fn acquisitions(conn: &mut Conn, host: &str, root: &str) -> Result<Vec<Acquisition>> {
    let rows =
        payloads::live_where(conn, ACQUISITION, &["root"], &[root.to_owned()], State::Any).await?;
    Ok(decode::<Acquisition>(ACQUISITION, rows)?
        .into_iter()
        .filter(|a| a.host == host)
        .collect())
}

/// Live acquisitions at these `host:path` keys.
pub async fn acquisitions_at(conn: &mut Conn, keys: &[String]) -> Result<Vec<Acquisition>> {
    let paths: Vec<String> = keys
        .iter()
        .filter_map(|k| k.split_once(':').map(|(_, p)| p.to_owned()))
        .collect();
    let rows = payloads::live_where(conn, ACQUISITION, &["path"], &paths, State::Any).await?;
    Ok(decode::<Acquisition>(ACQUISITION, rows)?
        .into_iter()
        .filter(|a| keys.contains(&format!("{}:{}", a.host, a.path)))
        .collect())
}

/// Live acquisitions of these bytes anywhere, present ones first.
pub async fn acquisitions_of(conn: &mut Conn, content: &str) -> Result<Vec<Acquisition>> {
    let rows = payloads::live_where(
        conn,
        ACQUISITION,
        &["content"],
        &[content.to_owned()],
        State::Any,
    )
    .await?;
    let mut all: Vec<Acquisition> = decode(ACQUISITION, rows)?;
    all.sort_by_key(|a| a.vanished_at.is_some());
    Ok(all)
}

/// The current revision of an item, withdrawn or not.
pub async fn item(conn: &mut Conn, item: &str) -> Result<Option<Item>> {
    let rows = payloads::live_where(conn, ITEM, &["item"], &[item.to_owned()], State::Any).await?;
    Ok(decode(ITEM, rows)?.into_iter().next())
}

/// Live, present items holding these bytes.
pub async fn items_with(conn: &mut Conn, content: &str) -> Result<Vec<Item>> {
    let rows = payloads::live_where(
        conn,
        ITEM,
        &["content"],
        &[content.to_owned()],
        State::Present,
    )
    .await?;
    decode(ITEM, rows)
}

/// Every revision an item ever had, oldest first.
pub async fn revisions(conn: &mut Conn, item: &str) -> Result<Vec<Item>> {
    let rows = payloads::history(conn, ITEM, &["item"], item).await?;
    let mut all: Vec<Item> = decode(ITEM, rows)?;
    all.dedup_by(|a, b| a.revision == b.revision && a.content == b.content);
    Ok(all)
}

async fn about<T: DeserializeOwned>(
    conn: &mut Conn,
    kind: &'static str,
    field: &[&str],
    item: &str,
) -> Result<Vec<T>> {
    let rows = payloads::live_where(conn, kind, field, &[item.to_owned()], State::Any).await?;
    decode(kind, rows)
}

/// Live renditions of an item (any state).
pub async fn renditions(conn: &mut Conn, item: &str) -> Result<Vec<Rendition>> {
    about(conn, "library.rendition", &["item"], item).await
}

/// Live assignments to an item (any state).
pub async fn assignments(conn: &mut Conn, item: &str) -> Result<Vec<Assignment>> {
    about(conn, "library.assignment", &["target", "item"], item).await
}

/// Live ratings of an item (any state).
pub async fn ratings(conn: &mut Conn, item: &str) -> Result<Vec<Rating>> {
    about(conn, "library.rating", &["item"], item).await
}

/// Views of an item.
pub async fn views(conn: &mut Conn, item: &str) -> Result<Vec<View>> {
    about(conn, "library.view", &["target", "item"], item).await
}

/// Collection entries for an item (any state).
pub async fn entries_of(conn: &mut Conn, item: &str) -> Result<Vec<Entry>> {
    about(conn, "library.entry", &["target", "item"], item).await
}

/// A live, present collection by name.
pub async fn collection_named(conn: &mut Conn, name: &str) -> Result<Option<Collection>> {
    let rows = payloads::live_where(
        conn,
        "library.collection",
        &["name"],
        &[name.to_owned()],
        State::Present,
    )
    .await?;
    Ok(decode("library.collection", rows)?.into_iter().next())
}

/// A collection's present entries, in order.
pub async fn entries(conn: &mut Conn, collection: &str) -> Result<Vec<Entry>> {
    let rows = payloads::live_where(
        conn,
        "library.entry",
        &["collection"],
        &[collection.to_owned()],
        State::Present,
    )
    .await?;
    let mut all: Vec<Entry> = decode("library.entry", rows)?;
    all.sort_by(|a, b| {
        a.position
            .partial_cmp(&b.position)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(all)
}

/// Present collections, by name.
pub async fn collections(conn: &mut Conn) -> Result<Vec<Collection>> {
    let rows = payloads::live_all(conn, "library.collection", State::Present).await?;
    let mut all: Vec<Collection> = decode("library.collection", rows)?;
    all.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(all)
}

/// Present pairwise judgements on a scale.
pub async fn comparisons(conn: &mut Conn, scale: &str) -> Result<Vec<Comparison>> {
    let rows = payloads::live_where(
        conn,
        "library.comparison",
        &["scale"],
        &[scale.to_owned()],
        State::Present,
    )
    .await?;
    decode("library.comparison", rows)
}
