//! `sinexd admin crawl`: fetch pages from stored browser history whose host is
//! on the allow list and that have not been fetched yet (architecture §5.6).
//! The allow list is the opt-in: nothing is fetched without one.
use crate::backfill::{Totals, drive};
use crate::context::{Context, Sink};
use sinex_sources::sources::page_crawl::{self, Crawl};
use sinex_store::payloads::{self, State};
use sinex_store::runs;
use std::collections::BTreeSet;
use std::time::Duration;

/// URLs the browser sources stored, newest reading order first deduplicated.
async fn history_urls(ctx: &mut Context) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for event_type in ["browser.url", "browser.history_entry", "browser.visit"] {
        let rows = payloads::live_all(&mut ctx.aux, event_type, State::Present)
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

pub async fn run(
    ctx: &mut Context,
    host: &str,
    allow: Vec<String>,
    urls: Vec<String>,
    limit: Option<usize>,
    pause: Duration,
) -> Result<Totals, String> {
    if allow.is_empty() {
        return Err("nothing is crawled without an allow list (--allow DOMAIN)".into());
    }
    let fetched: BTreeSet<String> = payloads::live_all(&mut ctx.aux, "page.fetched", State::Any)
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
        history_urls(ctx).await?
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
    let run = runs::start(&mut ctx.aux, &manifest, host)
        .await
        .map_err(|e| e.to_string())?;
    let crawl = Crawl::new(todo, allow, pause, host.to_owned());
    let source = page_crawl::source();
    let sink = Sink(ctx.blobs.clone());
    // Each crawl is its own list, so its binding starts from the beginning.
    let binding = format!("page-crawl@{host}/{}", run.0);
    let totals = drive(
        &mut ctx.aux,
        &ctx.writer,
        run,
        &source,
        &binding,
        &sink,
        |c| crawl.poll(c),
    )
    .await?;
    runs::end(&mut ctx.aux, run)
        .await
        .map_err(|e| e.to_string())?;
    Ok(totals)
}
