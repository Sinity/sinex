use clap::{Args, ValueEnum};
use serde_json::Value;
use serde_json::json;
use sinex_primitives::events::Event;
use sinex_primitives::ids::Id;
use sinex_primitives::query::{StructuralJoinKind, StructuralJoinQuery};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::render_finite_envelope;
use crate::model::OutputFormat;
use sinex_primitives::views::ViewEnvelope;

#[derive(Debug, Args)]
pub struct StructuralJoinCommand {
    /// Event ID to use as the structural-join root.
    event_id: Id<Event<Value>>,
    /// Structural composite to evaluate.
    #[arg(long, value_enum, default_value = "provenance-pack")]
    kind: StructuralJoinKindArg,
    /// Maximum evidence events to return.
    #[arg(long, default_value = "100")]
    limit: i64,
}

#[derive(Debug, Clone, Copy, ValueEnum, serde::Serialize)]
enum StructuralJoinKindArg {
    CaptureCoincidence,
    ProvenancePack,
}

impl From<StructuralJoinKindArg> for StructuralJoinKind {
    fn from(value: StructuralJoinKindArg) -> Self {
        match value {
            StructuralJoinKindArg::CaptureCoincidence => Self::CaptureCoincidence,
            StructuralJoinKindArg::ProvenancePack => Self::ProvenancePack,
        }
    }
}

impl StructuralJoinCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let result = client
            .structural_join(StructuralJoinQuery {
                event_id: self.event_id,
                kind: self.kind.into(),
                limit: self.limit,
            })
            .await?;
        if let Some(output) = render_finite_envelope(
            &ViewEnvelope::new("sinexctl.events.structural_join", &result)
                .with_query_echo(json!({"event_id": self.event_id, "kind": self.kind})),
            format,
        )? {
            print!("{output}");
        }
        Ok(())
    }
}
