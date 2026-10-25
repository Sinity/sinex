//! The media library's operations, shared by the API and
//! `sinexd admin library-*`. Every change is a library request: its records
//! are stored as the request's own bytes and admitted through the one writer
//! (`sinex-sources`'s `library.request`), attributed to the caller. Reads
//! come from live events through `sinex_library::reads`.
use crate::writer::WriterHandle;
use serde_json::{Value, json};
use sinex_library::{items, reads};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{Assignment, Item, Level, LibraryRecord, Target};
use sinex_protocol::Request;
use sinex_sources::sources::library as library_source;
use sinex_store::Blobs;
use sqlx::PgConnection;

mod curation;
mod intake;
mod media;
mod views;

use views::{collection_view, item_view, rank_view};

/// Who intake's own records are attributed to.
pub const INTAKE: &str = "library-intake";

/// What a library operation writes with.
pub struct Library<'a> {
    pub writer: &'a WriterHandle,
    pub blobs: &'a Blobs,
    pub host: &'a str,
}

type R<T> = Result<T, String>;

fn e(err: impl std::fmt::Display) -> String {
    err.to_string()
}

impl Library<'_> {
    /// Store and admit one library request. All its records commit together
    /// or none do (one material, admitted in one batch when it fits).
    pub async fn write(
        &self,
        conn: &mut PgConnection,
        records: Vec<LibraryRecord>,
        actor: &str,
        action: &str,
    ) -> R<Value> {
        self.write_citing(conn, records, actor, action, Vec::new())
            .await
    }

    /// [`Library::write`] for records that cite stored bytes beyond their
    /// material (a preview), already written to the content store.
    async fn write_citing(
        &self,
        conn: &mut PgConnection,
        records: Vec<LibraryRecord>,
        actor: &str,
        action: &str,
        cited: Vec<sinex_model::OwnedBlob>,
    ) -> R<Value> {
        if records.is_empty() {
            return Ok(json!({"records":0}));
        }
        let n = records.len();
        let binding = format!("library.request:{actor}@{}", self.host);
        let mut acquired = library_source::request_material(&records, actor, self.host, action);
        if !cited.is_empty() {
            acquired
                .material
                .as_mut()
                .expect("request has material")
                .manifest
                .details
                .insert("owned_blobs".into(), serde_json::to_value(cited).map_err(e)?);
        }
        let result = crate::authored::submit(
            conn,
            self.writer,
            self.blobs,
            std::sync::Arc::new(library_source::request_source()),
            &binding,
            acquired,
        )
        .await?;
        if result.outcome.failures > 0 {
            return Err(format!(
                "{} of {n} library records failed to read back",
                result.outcome.failures
            ));
        }
        Ok(
            json!({"records":n, "admitted":result.outcome.admitted, "replaced":result.outcome.replaced,
            "material":result.material}),
        )
    }

    async fn item_or(&self, conn: &mut PgConnection, item: &str) -> R<Item> {
        match reads::item(conn, item).await.map_err(e)? {
            Some(i) if i.withdrawn_at.is_none() => Ok(i),
            Some(_) => Err(format!("item {item} was withdrawn")),
            None => Err(format!("no library item {item}")),
        }
    }

    fn assignment(
        item: &str,
        field: &str,
        value: Option<String>,
        level: Level,
        source_ref: Option<String>,
    ) -> Assignment {
        Assignment {
            assignment: items::new_id(),
            target: Target {
                item: item.to_owned(),
                ..Target::default()
            },
            field: field.to_owned(),
            value,
            level,
            source_ref,
            by: String::new(),
            at: Timestamp::now(),
            withdrawn_at: None,
        }
    }

    fn withdraw(mut a: Assignment) -> LibraryRecord {
        a.withdrawn_at = Some(Timestamp::now());
        LibraryRecord::Assignment(a)
    }
}

/// Run one library command: reads need only `conn`; writes need the writer.
pub async fn dispatch(
    conn: &mut PgConnection,
    writer: Option<&WriterHandle>,
    blobs: &Blobs,
    host: &str,
    request: Request,
    actor: &str,
) -> R<Value> {
    let reads_only = matches!(
        request,
        Request::LibraryItem { .. }
            | Request::LibraryCollection { .. }
            | Request::LibraryRank { .. }
    );
    let writer = match (writer, reads_only) {
        (Some(w), _) => w,
        (None, true) => {
            return match request {
                Request::LibraryItem { item } => item_view(conn, &item).await,
                Request::LibraryCollection { collection } => {
                    collection_view(conn, collection.as_deref()).await
                }
                Request::LibraryRank { scale } => {
                    rank_view(conn, scale.as_deref().unwrap_or("overall")).await
                }
                _ => unreachable!("reads only"),
            };
        }
        (None, false) => {
            return Err("this daemon has no writer; writes go through `sinexd run`".into());
        }
    };
    let library = Library {
        writer,
        blobs,
        host,
    };
    let number = |s: &str, what: &str| {
        s.trim()
            .parse::<f64>()
            .map_err(|_| format!("{what} must be a number, not `{s}`"))
    };
    match request {
        Request::LibraryAdd { path, reference } => {
            library
                .add(
                    conn,
                    std::path::Path::new(&path),
                    reference.unwrap_or(false),
                )
                .await
        }
        Request::LibraryItem { item } => item_view(conn, &item).await,
        Request::LibraryTag { item, tag } => library.tag(conn, &item, &tag, actor).await,
        Request::LibraryUntag { item, tag } => library.untag(conn, &item, &tag, actor).await,
        Request::LibrarySet {
            item,
            field,
            value,
            empty,
        } => {
            let value = match (value, empty.unwrap_or(false)) {
                (Some(_), true) => return Err("give a value or --empty, not both".into()),
                (None, false) => return Err("give a value, or --empty".into()),
                (v, _) => v,
            };
            library.set(conn, &item, &field, value, actor).await
        }
        Request::LibraryCollect { collection, item } => {
            library.collect(conn, &collection, &item, actor).await
        }
        Request::LibraryUncollect { collection, item } => {
            library.uncollect(conn, &collection, &item, actor).await
        }
        Request::LibraryCollection { collection } => {
            collection_view(conn, collection.as_deref()).await
        }
        Request::LibraryRate {
            item,
            value,
            scale,
            of,
        } => {
            let value = number(&value, "a rating")?;
            let of = of
                .as_deref()
                .map(|o| number(o, "--of"))
                .transpose()?
                .unwrap_or(5.0);
            library
                .rate(
                    conn,
                    &item,
                    value,
                    scale.as_deref().unwrap_or("stars"),
                    of,
                    actor,
                )
                .await
        }
        Request::LibraryCompare {
            winner,
            loser,
            scale,
        } => {
            library
                .compare(
                    conn,
                    &winner,
                    &loser,
                    scale.as_deref().unwrap_or("overall"),
                    actor,
                )
                .await
        }
        Request::LibraryRank { scale } => {
            rank_view(conn, scale.as_deref().unwrap_or("overall")).await
        }
        Request::LibraryOpen { item, rendition } => {
            library.open(conn, &item, rendition.as_deref()).await
        }
        Request::LibraryViewed {
            item,
            seconds,
            player,
        } => {
            let seconds = seconds
                .as_deref()
                .map(|s| number(s, "--seconds"))
                .transpose()?;
            library
                .viewed(
                    conn,
                    &item,
                    seconds,
                    player.as_deref().unwrap_or("unknown"),
                    actor,
                )
                .await
        }
        Request::LibraryPreview { item } => library.preview(conn, &item, actor).await,
        Request::LibraryImport { path } => {
            library
                .import(conn, std::path::Path::new(&path), actor)
                .await
        }
        _ => unreachable!("only library requests reach here"),
    }
}

#[cfg(test)]
mod tests;
