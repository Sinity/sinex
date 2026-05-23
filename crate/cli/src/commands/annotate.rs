//! `sinexctl annotate` — top-level event-annotation verb (#1172 AC-9).
//!
//! This is the *event* annotation surface (`core.event_annotations`). It is
//! intentionally distinct from `sinexctl sources annotate <uuid>`, which
//! annotates source materials. The two operate on different tables and
//! different RPC methods.

use clap::Args;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use sinex_primitives::rpc::events::EventsAnnotateRequest;

use crate::client::GatewayClient;
use crate::fmt::format_json;
use crate::model::OutputFormat;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl annotate 0193... --note 'investigated, ok'
    sinexctl annotate 0193... --kind correction --note 'ts_orig was wrong'
")]
pub struct AnnotateCommand {
    /// Event UUID to annotate.
    pub event_id: String,

    /// Annotation note (the textual content). Required.
    #[arg(long, short = 'n')]
    pub note: String,

    /// Annotation kind / type. Defaults to `note`.
    #[arg(long, default_value = "note")]
    pub kind: String,
}

impl AnnotateCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        if self.note.trim().is_empty() {
            return Err(eyre!("annotation --note must not be empty"));
        }
        if self.kind.trim().is_empty() {
            return Err(eyre!("annotation --kind must not be empty"));
        }

        let response = client
            .events_annotate(EventsAnnotateRequest {
                event_id: self.event_id.clone(),
                annotation_type: self.kind.clone(),
                content: self.note.clone(),
                metadata: None,
            })
            .await?;

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&response)?);
            }
            OutputFormat::Yaml => {
                println!("{}", crate::fmt::format_yaml(&response)?);
            }
            OutputFormat::Table => {
                println!("Annotation recorded for event {}.", self.event_id);
            }
        }
        Ok(())
    }
}
