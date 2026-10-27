//! The `crawl` command: fetch pages from stored browser history whose host is
//! on the allow list and that have not been fetched yet. The allow list is the
//! opt-in: nothing is fetched without one.
use crate::backfill::Totals;
use crate::writer::WriterHandle;
use sinex_source_core::{Binding, Settings};
use sinex_sources::sources;
use sinex_store::Blobs;
use sinex_store::payloads::{self, RecordStates};
use sqlx::PgConnection;
use std::collections::BTreeSet;
use std::time::Duration;

// One binding has one committed cursor, so requests cannot interleave URL lists.
static CRAWL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// URLs from live browser records, deduplicated in commit order.
async fn history_urls(read_conn: &mut PgConnection) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    let rows = payloads::live_all(read_conn, "visit.fused", RecordStates::Present)
        .await
        .map_err(|e| e.to_string())?;
    for payload in rows {
        if let Some(url) = payload.get("url").and_then(serde_json::Value::as_str)
            && seen.insert(url.to_owned())
        {
            out.push(url.to_owned());
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
    let _crawl = CRAWL_LOCK.lock().await;
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
    let todo = select_urls(wanted, &fetched, &allow, limit);
    let format = sources::format("page-crawl").ok_or("page-crawl format is not registered")?;
    let mut settings = Settings::new(host);
    settings.options = serde_json::json!({"allow":allow, "pause":pause.as_secs(), "limit":limit});
    let spec = (format.specs)()
        .into_iter()
        .next()
        .ok_or("crawl format has no source")?;
    let reader =
        sinex_sources::sources::page_crawl::Crawl::new(todo, allow, pause, host.to_owned());
    let binding = Binding::of(spec, std::path::Path::new("page-crawl"), settings);
    crate::binding_reader::once(
        read_conn,
        blobs,
        writer,
        host,
        vec![crate::binding_reader::BindingReader::with_reading(
            format,
            binding,
            sinex_source_core::binding::polled(reader),
        )?],
    )
    .await
}

fn select_urls(
    urls: Vec<String>,
    fetched: &BTreeSet<String>,
    allow: &[String],
    limit: Option<usize>,
) -> Vec<String> {
    let mut todo: Vec<String> = urls
        .into_iter()
        .filter(|u| !fetched.contains(u))
        .filter(|url| {
            sinex_sources::sources::page_crawl::split(url).is_some_and(|(_, host, _)| {
                sinex_sources::sources::page_crawl::allowed(&host, allow)
            })
        })
        .collect();
    if let Some(limit) = limit {
        todo.truncate(limit);
    }
    todo
}

#[cfg(test)]
mod tests;
