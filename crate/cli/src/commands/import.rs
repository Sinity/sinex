use clap::{Args, Subcommand};
use color_eyre::Result;
use console::style;
use sinex_primitives::rpc::ingest::EventIngestRequest;

use crate::client::GatewayClient;

#[derive(Debug, Args)]
pub struct ImportCommand {
    #[command(subcommand)]
    pub cmd: ImportCommands,
}

impl ImportCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        self.cmd.execute(client).await
    }
}

#[derive(Debug, Subcommand)]
pub enum ImportCommands {
    /// Import Spotify streaming history JSON files
    Spotify(SpotifyImportArgs),
}

impl ImportCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::Spotify(args) => import_spotify(client, args).await,
        }
    }
}

#[derive(Debug, Args)]
pub struct SpotifyImportArgs {
    /// Path to Spotify streaming history JSON file(s) or directory
    pub paths: Vec<String>,

    /// Dry run (show what would be imported without sending)
    #[arg(long)]
    pub dry_run: bool,

    /// Maximum events per batch (controls gateway request rate)
    #[arg(long, default_value_t = 100)]
    pub batch_size: usize,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotifyStreamEntry {
    end_time: String,
    artist_name: String,
    track_name: String,
    ms_played: u64,
}

async fn import_spotify(client: &GatewayClient, args: &SpotifyImportArgs) -> Result<()> {
    let mut files = Vec::new();

    for path_str in &args.paths {
        let path = std::path::Path::new(path_str);
        if path.is_dir() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let p = entry.path();
                if p.extension().is_some_and(|e| e == "json")
                    && p.file_name()
                        .is_some_and(|n| n.to_string_lossy().contains("StreamingHistory"))
                {
                    files.push(p);
                }
            }
        } else if path.is_file() {
            files.push(path.to_path_buf());
        } else {
            eprintln!("{} Path not found: {path_str}", style("⚠").yellow());
        }
    }

    files.sort();

    if files.is_empty() {
        println!("{} No Spotify streaming history files found", style("✗").red());
        return Ok(());
    }

    println!(
        "{} Found {} file(s) to import",
        style("→").cyan(),
        files.len()
    );

    let mut total_events = 0u64;
    let mut total_skipped = 0u64;

    for file in &files {
        let filename = file
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let content = std::fs::read_to_string(file)?;
        let entries: Vec<SpotifyStreamEntry> = serde_json::from_str(&content)?;

        println!(
            "  {} {} ({} entries)",
            style("📄").dim(),
            filename,
            entries.len()
        );

        if args.dry_run {
            total_events += entries.len() as u64;
            continue;
        }

        for entry in &entries {
            if entry.ms_played < 30_000 {
                total_skipped += 1;
                continue;
            }

            let ts_orig = format!("{}:00Z", entry.end_time.replace(' ', "T"));

            let payload = serde_json::json!({
                "artist_name": entry.artist_name,
                "track_name": entry.track_name,
                "ms_played": entry.ms_played,
                "duration_seconds": entry.ms_played / 1000,
            });

            let req = EventIngestRequest {
                source: "spotify.history".to_string(),
                event_type: "media.play".to_string(),
                payload,
                ts_orig,
                host: None,
            };

            client.ingest_event(req).await?;
            total_events += 1;

            if total_events % 500 == 0 {
                eprint!("\r  {} events ingested...", total_events);
            }
        }
    }

    println!();
    println!(
        "{} Imported {} events ({} skipped < 30s)",
        style("✓").green(),
        style(total_events).bold(),
        total_skipped,
    );

    Ok(())
}
