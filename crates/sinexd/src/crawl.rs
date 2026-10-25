//! The `crawl` command: fetch pages from stored browser history whose host is
//! on the allow list and that have not been fetched yet. The allow list is the
//! opt-in: nothing is fetched without one.
use crate::backfill::{Totals, drive};
use crate::material_sink::Sink;
use crate::writer::WriterHandle;
use sinex_sources::sources::page_crawl::{self, Crawl};
use sinex_store::Blobs;
use sinex_store::payloads::{self, RecordStates};
use sinex_store::runs;
use sqlx::PgConnection;
use std::collections::BTreeSet;
use std::time::Duration;

/// URLs the browser sources stored, newest reading order first deduplicated.
async fn history_urls(read_conn: &mut PgConnection) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for event_type in ["browser.url", "browser.history_entry", "browser.visit"] {
        let rows = payloads::live_all(read_conn, event_type, RecordStates::Present)
            .await
            .map_err(|e| e.to_string())?;
        for p in rows {
            if let Some(url) = p.get("url").and_then(serde_json::Value::as_str)
                && seen.insert(url.to_owned())
            {
                out.push(url.to_owned());
            }
        }
    }
    Ok(out)
}

/// Crawl with its parts: a read connection, the content store and the writer.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    read_conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    host: &str,
    allow: Vec<String>,
    urls: Vec<String>,
    limit: Option<usize>,
    pause: Duration,
) -> Result<Totals, String> {
    if allow.is_empty() {
        return Err("nothing is crawled without an allow list (--allow DOMAIN)".into());
    }
    let fetched: BTreeSet<String> =
        payloads::live_all(read_conn, "page.fetched", RecordStates::Any)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter_map(|p| {
                p.get("url")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect();
    let wanted = if urls.is_empty() {
        history_urls(read_conn).await?
    } else {
        urls
    };
    let mut todo: Vec<String> = wanted
        .into_iter()
        .filter(|u| !fetched.contains(u))
        .filter(|u| page_crawl::split(u).is_some_and(|(_, h, _)| page_crawl::allowed(&h, &allow)))
        .collect();
    if let Some(limit) = limit {
        todo.truncate(limit);
    }
    println!("crawl: {} pages to fetch", todo.len());
    let manifest = runs::Manifest {
        name: "sinexd crawl",
        kind: "source",
        version: env!("CARGO_PKG_VERSION"),
        build: option_env!("SINEX_BUILD").unwrap_or("dev"),
        emits: &["page.fetched"],
    };
    let run = runs::start(read_conn, &manifest, host)
        .await
        .map_err(|e| e.to_string())?;
    let mut crawl =
        sinex_sources::binding::Stateless(Crawl::new(todo, allow, pause, host.to_owned()));
    let source = page_crawl::source();
    let sink = Sink(blobs.clone());
    // Each crawl is its own list, so its binding starts from the beginning.
    let binding = format!("page-crawl@{host}/{}", run.0);
    sinex_store::bindings::save(
        read_conn,
        &sinex_store::bindings::Stored {
            binding: binding.clone(),
            source: sinex_sources::Source::spec(&source).name.into(),
            input: "page-crawl".into(),
            settings: serde_json::json!({"host":host}),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let totals = drive(
        read_conn,
        writer,
        run,
        &source,
        &binding,
        &sink,
        Default::default(),
        sinex_model::Class::Open,
        &mut crawl,
        tokio::sync::watch::channel(false).1,
    )
    .await?;
    runs::end(read_conn, run).await.map_err(|e| e.to_string())?;
    Ok(totals)
}
