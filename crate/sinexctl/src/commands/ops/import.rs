use super::*;
use sinex_primitives::rpc::sources::ImportProgressEntry;
use tabled::{builder::Builder, settings::Style};

/// Live rate/position/ETA/backlog for in-flight paced historical imports
/// (sinex-2n9). Backed by the same live snapshot the scan loop's
/// `ScanPacer` publishes — entries disappear when a scan completes or goes
/// stale, so an empty list means "nothing currently importing", not "never
/// imported".
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    sinexctl ops import list
    sinexctl ops import list --format json
")]
pub enum ImportCommands {
    /// List in-flight paced historical imports.
    #[command(alias = "ls")]
    List,
}

impl ImportCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List => {
                let response = client.sources_import_progress().await?;
                let envelope =
                    ViewEnvelope::new("sinexctl.ops.import.list", response.imports.clone());
                if let Some(output) = render_envelope(&envelope, &response.imports, format)? {
                    print_machine_output(&output);
                    return Ok(());
                }
                println!("{}", format_import_progress_table(&response.imports));
            }
        }
        Ok(())
    }
}

fn format_import_progress_table(imports: &[ImportProgressEntry]) -> String {
    if imports.is_empty() {
        return "No historical imports currently in flight.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE", "PACED", "RATE(ev/s)", "EVENTS", "POSITION", "ETA", "BACKLOG",
    ]);
    for entry in imports {
        builder.push_record([
            entry.module_name.clone(),
            if entry.paced {
                "yes".to_string()
            } else {
                "UNLIMITED".to_string()
            },
            format!("{:.1}", entry.rate_events_per_sec),
            entry.events_processed.to_string(),
            entry.position.clone().unwrap_or_else(|| "-".to_string()),
            entry
                .eta_seconds
                .map_or_else(|| "-".to_string(), format_eta),
            entry
                .backlog_pending
                .map_or_else(|| "-".to_string(), |pending| pending.to_string()),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_eta(seconds: f64) -> String {
    if seconds < 60.0 {
        format!("{seconds:.0}s")
    } else if seconds < 3600.0 {
        format!("{:.0}m", seconds / 60.0)
    } else {
        format!("{:.1}h", seconds / 3600.0)
    }
}
