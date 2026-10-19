//! The `crawl` command: fetch pages from stored browser history whose host is
//! on the allow list and that have not been fetched yet (architecture §5.6).
//! The allow list is the opt-in: nothing is fetched without one.
use crate::backfill::{Totals, drive};
use crate::context::Sink;
use crate::writer::WriterHandle;
use sinex_sources::sources::page_crawl::{self, Crawl};
use sinex_store::Blobs;
use sinex_store::payloads::{self, State};
use sinex_store::runs;
use sqlx::PgConnection;
use std::collections::BTreeSet;
use std::time::Duration;

/// URLs the browser sources stored, newest reading order first deduplicated.
async fn history_urls(aux: &mut PgConnection) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for event_type in ["browser.url", "browser.history_entry", "browser.visit"] {
        let rows = payloads::live_all(aux, event_type, State::Present)
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
pub async fn run(
    aux: &mut PgConnection,
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
    let fetched: BTreeSet<String> = payloads::live_all(aux, "page.fetched", State::Any)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter_map(|p| {
            p.get("url")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    let candidates = if urls.is_empty() {
        history_urls(aux).await?
    } else {
        urls
    };
    let mut todo: Vec<String> = candidates
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
    let run = runs::start(aux, &manifest, host)
        .await
        .map_err(|e| e.to_string())?;
    let crawl = Crawl::new(todo, allow, pause, host.to_owned());
    let source = page_crawl::source();
    let sink = Sink(blobs.clone());
    // Each crawl is its own list, so its binding starts from the beginning.
    let binding = format!("page-crawl@{host}/{}", run.0);
    let totals = drive(
        aux,
        writer,
        run,
        &source,
        &binding,
        &sink,
        |c| crawl.poll(c),
    )
    .await?;
    runs::end(aux, run)
        .await
        .map_err(|e| e.to_string())?;
    Ok(totals)
}
