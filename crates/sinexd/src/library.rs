//! Media library commands. Scans and previews feed the library derivation;
//! curation goes through request intake as planned curation records. Reads
//! use live events.
use crate::writer::WriterHandle;
use serde_json::{Value, json};
use sinex_library::reads;
use sinex_model::Timestamp;
use sinex_model::event_types::library::{Item, LibraryRecord};
use sinex_protocol::Request;
use sinex_sources::sources::library as library_source;
use sinex_store::Blobs;
use sqlx::PgConnection;

mod curation;
mod intake;
mod media;
mod views;

use views::{collection_view, item_view, rank_view};

/// What a library operation writes with.
pub(crate) struct Library<'a> {
    pub writer: &'a WriterHandle,
    pub blobs: &'a Blobs,
    pub host: &'a str,
    pub roots: &'a [std::path::PathBuf],
    pub time_settings: sinex_model::time_settings::TimeSettings,
}

type R<T> = Result<T, String>;

fn e(err: impl std::fmt::Display) -> String {
    err.to_string()
}

/// The channel every library request arrives on: the daemon's API. The
/// API does not yet report whether the CLI, MCP or the web called it.
const VIA_API: &str = "api";

/// How one library request is stored and admitted.
struct Stored<'a> {
    action: &'a str,
    via: &'a str,
    at: Timestamp,
    /// Stored bytes the records cite beyond their material (a preview).
    cited: Vec<sinex_model::OwnedBlob>,
    /// Planned curation records, verified against the stored ones inside
    /// the admitting transaction.
    curation: bool,
}

impl Library<'_> {
    /// Store and admit one library request. All its records commit together
    /// or none do (one material, admitted in one batch).
    async fn write(
        &self,
        conn: &mut PgConnection,
        records: Vec<LibraryRecord>,
        actor: &str,
        stored: Stored<'_>,
    ) -> R<Value> {
        if records.is_empty() {
            return Ok(json!({"records":0}));
        }
        let n = records.len();
        let binding = format!("library.request:{actor}@{}", self.host);
        let mut acquired = library_source::request_material(
            &records,
            library_source::RequestWriter {
                author: actor,
                via: stored.via,
            },
            self.host,
            stored.action,
            stored.at,
        );
        if !stored.cited.is_empty() {
            acquired
                .material
                .as_mut()
                .expect("request has material")
                .manifest
                .details
                .insert(
                    "owned_blobs".into(),
                    serde_json::to_value(stored.cited).map_err(e)?,
                );
        }
        // Boxed: the intake future is the bulk of every library write, and
        // inline it gave each caller's debug-build poll frame a ~15 KiB slot
        // per await site, enough to overflow a 2 MiB thread stack.
        let source = std::sync::Arc::new(library_source::request_source());
        let result = match stored.curation {
            true => {
                Box::pin(crate::intake::request_curation(
                    conn,
                    self.writer,
                    self.blobs,
                    source,
                    &binding,
                    self.host,
                    acquired,
                ))
                .await?
            }
            false => {
                Box::pin(crate::intake::request(
                    conn,
                    self.writer,
                    self.blobs,
                    source,
                    &binding,
                    self.host,
                    acquired,
                ))
                .await?
            }
        };
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
        reads::item(conn, item)
            .await
            .map_err(e)?
            .ok_or_else(|| format!("no library item {item}"))
    }
}

/// Run one library command: reads need only `conn`; writes need the writer.
pub(crate) async fn dispatch(
    conn: &mut PgConnection,
    writer: Option<&WriterHandle>,
    blobs: &Blobs,
    host: &str,
    roots: &[std::path::PathBuf],
    time_settings: sinex_model::time_settings::TimeSettings,
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
        (_, true) => {
            return match request {
                Request::LibraryItem { item } => item_view(conn, &item).await,
                Request::LibraryCollection { collection } => {
                    collection_view(conn, collection.as_deref()).await
                }
                Request::LibraryRank {
                    scale,
                    judges,
                    collection,
                    k,
                    rule,
                } => {
                    rank_view(
                        conn,
                        &scale,
                        judges.as_deref(),
                        collection.as_deref(),
                        k,
                        rule.as_deref(),
                    )
                    .await
                }
                _ => unreachable!("reads only"),
            };
        }
        (Some(w), false) => w,
        (None, false) => {
            return Err("this daemon has no writer; writes go through `sinexd run`".into());
        }
    };
    let library = Library {
        writer,
        blobs,
        host,
        roots,
        time_settings,
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
        Request::LibraryCurate {
            request: id,
            writes,
        } => library.curate(conn, id, &writes, actor).await,
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
