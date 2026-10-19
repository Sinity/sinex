//! The media library's operations, shared by the API and
//! `sinexd admin library-*`. Every change is a library request: its records
//! are stored as the request's own bytes and admitted through the one writer
//! (`sinex-sources`'s `library.request`), attributed to the caller. Reads
//! come from live events through `sinex_library::reads`.
use crate::context::Sink;
use crate::writer::{Priority, WriterHandle};
use serde_json::{Value, json};
use sinex_library::{items, reads};
use sinex_model::event_types::library::{Assignment, Item, Level, LibraryRecord, Target};
use sinex_model::{RunId, Timestamp};
use sinex_protocol::Request;
use sinex_sources::sources::library as library_source;
use sinex_sources::{Limits, drivers};
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
    pub async fn write(&self, records: Vec<LibraryRecord>, actor: &str, action: &str) -> R<Value> {
        if records.is_empty() {
            return Ok(json!({ "records": 0 }));
        }
        let n = records.len();
        let host = self.host.to_owned();
        let binding = format!("library.request:{actor}@{host}");
        let sink = Sink(self.blobs.clone());
        let actor_owned = actor.to_owned();
        let action = action.to_owned();
        let batches = tokio::task::spawn_blocking(move || {
            let acquired = library_source::request_material(&records, &actor_owned, &host, &action);
            drivers::batches(
                &library_source::request_source(),
                &binding,
                RunId::new(),
                acquired,
                Value::Null,
                &sink,
                Limits {
                    max_events: usize::MAX,
                    max_bytes: usize::MAX,
                },
            )
        })
        .await
        .map_err(e)?
        .map_err(e)?;
        let (mut admitted, mut replaced, mut failures) = (0, 0, 0);
        for b in batches {
            let o = self.writer.submit(Priority::Live, b).await.map_err(e)?;
            admitted += o.admitted;
            replaced += o.replaced;
            failures += o.failures;
        }
        if failures > 0 {
            return Err(format!(
                "{failures} of {n} library records failed to read back"
            ));
        }
        Ok(json!({ "records": n, "admitted": admitted, "replaced": replaced }))
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
