use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{WrapErr, eyre};
use serde::Serialize;
use sinex_db::{DbPoolExt, create_pool};
use sinex_node_sdk::content_store::{
    ContentStoreConfig, MaterialContentStore, UnusedContentEntry,
};

use crate::Result;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum BlobCommands {
    /// Reclaim unused content-store keys that no longer have a matching `core.blobs` row.
    SweepOrphans(BlobSweepOrphansCommand),
}

impl BlobCommands {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            Self::SweepOrphans(cmd) => cmd.execute(format).await,
        }
    }
}

#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Show content-store keys that are unused and have no DB blob row
    sinexctl blob sweep-orphans

    # Actually drop those orphaned keys from the large-object backend
    sinexctl blob sweep-orphans --apply
")]
pub struct BlobSweepOrphansCommand {
    /// Content-store root path.
    #[arg(long, env = "SINEX_CONTENT_STORE_PATH")]
    pub content_store_path: Utf8PathBuf,

    /// Drop orphaned keys instead of only reporting them.
    #[arg(long)]
    pub apply: bool,
}

#[derive(Debug, Serialize)]
struct BlobSweepSummary {
    content_store_path: String,
    mode: &'static str,
    total_unused_entries: usize,
    db_backed_entries: usize,
    orphaned_entries: usize,
    dropped_entries: usize,
    orphaned_keys: Vec<BlobOrphanEntry>,
}

#[derive(Debug, Serialize)]
struct BlobOrphanEntry {
    number: u32,
    key: String,
    size_bytes: u64,
}

impl BlobSweepOrphansCommand {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        let database_url = std::env::var("DATABASE_URL").map_err(|_| {
            eyre!(
                "DATABASE_URL not set. Set it in your environment before running direct blob maintenance commands."
            )
        })?;
        let pool = create_pool(&database_url)
            .await
            .wrap_err("connect database for blob orphan sweep")?;

        let content_store = MaterialContentStore::new(ContentStoreConfig {
            root_path: self.content_store_path.clone(),
            num_copies: None,
            large_files: None,
        })
        .wrap_err_with(|| format!("open content-store root {}", self.content_store_path))?;
        let unused_entries = content_store
            .list_unused()
            .await
            .wrap_err("list content-store unused entries")?;

        let mut db_backed_entries = 0usize;
        let mut orphaned_unused = Vec::new();
        for entry in unused_entries {
            let size_bytes = i64::try_from(entry.key.size)
                .wrap_err_with(|| format!("content-store key size does not fit i64: {}", entry.key.key))?;
            if pool
                .blobs()
                .get_by_content(entry.key.storage_backend(), &entry.key.digest, size_bytes)
                .await
                .wrap_err_with(|| format!("lookup blob row for content-store key {}", entry.key.key))?
                .is_some()
            {
                db_backed_entries += 1;
            } else {
                orphaned_unused.push(entry);
            }
        }

        let dropped_entries = if self.apply && !orphaned_unused.is_empty() {
            let numbers = orphaned_unused
                .iter()
                .map(|entry| entry.number)
                .collect::<Vec<_>>();
            content_store
                .drop_unused(&numbers, true)
                .await
                .wrap_err("drop orphaned content-store unused entries")?;
            numbers.len()
        } else {
            0
        };

        let summary = BlobSweepSummary {
            content_store_path: self.content_store_path.to_string(),
            mode: if self.apply { "apply" } else { "dry-run" },
            total_unused_entries: db_backed_entries + orphaned_unused.len(),
            db_backed_entries,
            orphaned_entries: orphaned_unused.len(),
            dropped_entries,
            orphaned_keys: orphaned_unused.into_iter().map(blob_orphan_entry).collect(),
        };

        CommandOutput::single(summary, format_blob_sweep_summary).display(&format)
    }
}

fn blob_orphan_entry(entry: UnusedContentEntry) -> BlobOrphanEntry {
    BlobOrphanEntry {
        number: entry.number,
        key: entry.key.key,
        size_bytes: entry.key.size,
    }
}

fn format_blob_sweep_summary(summary: &BlobSweepSummary) -> String {
    let mut output = String::new();
    output.push_str("Blob Orphan Sweep\n");
    output.push_str(&format!(
        "  Content Store: {}\n",
        summary.content_store_path
    ));
    output.push_str(&format!("  Mode: {}\n", summary.mode));
    output.push_str(&format!(
        "  Total Unused Entries: {}\n",
        summary.total_unused_entries
    ));
    output.push_str(&format!(
        "  DB-backed Entries: {}\n",
        summary.db_backed_entries
    ));
    output.push_str(&format!(
        "  Orphaned Entries: {}\n",
        summary.orphaned_entries
    ));
    output.push_str(&format!("  Dropped Entries: {}\n", summary.dropped_entries));
    if !summary.orphaned_keys.is_empty() {
        output.push_str("  Orphaned Keys:\n");
        for orphan in &summary.orphaned_keys {
            output.push_str(&format!(
                "    {}  {}  ({} bytes)\n",
                orphan.number, orphan.key, orphan.size_bytes
            ));
        }
    }
    output
}
