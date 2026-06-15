use clap::{Args, Parser, Subcommand};
use sinex_primitives::query::EventQuery;
use sinex_primitives::relations::{EventRelationExpr, EvidenceRef, EvidenceWindow, SameField};
use sinex_primitives::rpc::events::EventsRelationEvidenceRequest;
use sinex_primitives::views::ViewEnvelope;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::render_envelope;
use crate::model::OutputFormat;

/// Evaluate event relations over live events.
#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Find events within five minutes of seed hits
    sinexctl relations within --within-secs 300 --seed-query-json '{\"event_types\":[\"command.executed\"],\"limit\":10}'

    # Compare seed hits to an explicit candidate query
    sinexctl relations overlaps --seed-query-json '{\"sources\":[\"terminal.atuin-history\"]}' --candidate-query-json '{\"sources\":[\"desktop.hyprland\"]}'

    # Match candidates that share a payload field with seeds
    sinexctl relations same --field payload:project --seed-query-json '{\"limit\":20}'
")]
pub struct RelationsCommand {
    #[command(subcommand)]
    subcommand: RelationsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RelationsSubcommand {
    /// Include candidates within N seconds of any seed event.
    Within {
        #[command(flatten)]
        query: RelationQueryArgs,
        /// Maximum gap in seconds between seed and candidate ranges.
        #[arg(long)]
        within_secs: i64,
    },
    /// Include candidates whose observed ranges overlap any seed event.
    Overlaps {
        #[command(flatten)]
        query: RelationQueryArgs,
    },
    /// Include candidates before any seed event within a bounded gap.
    Before {
        #[command(flatten)]
        query: RelationQueryArgs,
        /// Maximum gap in seconds between candidate and seed.
        #[arg(long)]
        max_gap_secs: i64,
    },
    /// Include candidates after any seed event within a bounded gap.
    After {
        #[command(flatten)]
        query: RelationQueryArgs,
        /// Maximum gap in seconds between seed and candidate.
        #[arg(long)]
        max_gap_secs: i64,
    },
    /// Include candidates sharing a field value with any seed event.
    Same {
        #[command(flatten)]
        query: RelationQueryArgs,
        /// Field to compare: source, scope_key, equivalence_key, or payload:<key>.
        #[arg(long, value_parser = parse_same_field)]
        field: SameField,
    },
    /// Treat seed hits as an ordered sequence within N seconds.
    Sequence {
        #[command(flatten)]
        query: RelationQueryArgs,
        /// Maximum span in seconds across the ordered sequence.
        #[arg(long)]
        within_secs: i64,
    },
}

#[derive(Debug, Clone, Args)]
pub struct RelationQueryArgs {
    /// JSON-encoded EventQuery used to select seed events.
    #[arg(long)]
    seed_query_json: String,

    /// Optional JSON-encoded EventQuery used to select candidate events.
    #[arg(long)]
    candidate_query_json: Option<String>,
}

impl RelationsCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let request = self.subcommand.request()?;
        let envelope = client.relation_evidence(request).await?;
        print_relation_envelope(&envelope, format)
    }
}

impl RelationsSubcommand {
    fn request(&self) -> Result<EventsRelationEvidenceRequest> {
        let (query, relation) = match self {
            Self::Within { query, within_secs } => (
                query,
                EventRelationExpr::Within {
                    within_secs: *within_secs,
                },
            ),
            Self::Overlaps { query } => (query, EventRelationExpr::Overlaps),
            Self::Before {
                query,
                max_gap_secs,
            } => (
                query,
                EventRelationExpr::Before {
                    max_gap_secs: *max_gap_secs,
                },
            ),
            Self::After {
                query,
                max_gap_secs,
            } => (
                query,
                EventRelationExpr::After {
                    max_gap_secs: *max_gap_secs,
                },
            ),
            Self::Same { query, field } => (
                query,
                EventRelationExpr::Same {
                    field: field.clone(),
                },
            ),
            Self::Sequence { query, within_secs } => (
                query,
                EventRelationExpr::Sequence {
                    within_secs: *within_secs,
                },
            ),
        };

        Ok(EventsRelationEvidenceRequest {
            seed_query: parse_event_query_json(&query.seed_query_json)?,
            candidate_query: query
                .candidate_query_json
                .as_deref()
                .map(parse_event_query_json)
                .transpose()?,
            relation,
        })
    }
}

fn print_relation_envelope(
    envelope: &ViewEnvelope<EvidenceWindow>,
    format: OutputFormat,
) -> Result<()> {
    if let Some(output) = render_envelope(envelope, &envelope.payload.support_refs, format)? {
        print!("{output}");
        if !output.is_empty() && !output.ends_with('\n') {
            println!();
        }
        return Ok(());
    }

    println!("{}", format_relation_table(envelope));
    Ok(())
}

fn format_relation_table(envelope: &ViewEnvelope<EvidenceWindow>) -> String {
    let window = &envelope.payload;
    let mut lines = vec![
        "Relation evidence".to_string(),
        format!("  Seeds:          {}", window.seed_refs.len()),
        format!("  Support:        {}", window.support_refs.len()),
        format!("  Contradictions: {}", window.contradiction_refs.len()),
        format!("  Caveats:        {}", window.caveats.len()),
        format!(
            "  Observed range: {} -> {}",
            window
                .observed_range
                .start
                .map_or_else(|| "-".to_string(), |ts| ts.to_string()),
            window
                .observed_range
                .end
                .map_or_else(|| "-".to_string(), |ts| ts.to_string())
        ),
    ];

    if !window.support_refs.is_empty() {
        lines.push(String::new());
        lines.push("Support refs:".to_string());
        for evidence in window.support_refs.iter().take(20) {
            lines.push(format_evidence_ref(evidence));
        }
        if window.support_refs.len() > 20 {
            lines.push(format!("  ... {} more", window.support_refs.len() - 20));
        }
    }

    if !window.caveats.is_empty() {
        lines.push(String::new());
        lines.push("Caveats:".to_string());
        for caveat in &window.caveats {
            lines.push(format!("  {}: {}", caveat.id, caveat.message));
        }
    }

    lines.join("\n")
}

fn format_evidence_ref(evidence: &EvidenceRef) -> String {
    format!(
        "  {:?} {} - {}",
        evidence.object.kind, evidence.object.id, evidence.rationale
    )
}

fn parse_event_query_json(input: &str) -> Result<EventQuery> {
    let mut query: EventQuery = serde_json::from_str(input)?;
    query.validate()?;
    Ok(query)
}

fn parse_same_field(input: &str) -> std::result::Result<SameField, String> {
    match input {
        "source" => Ok(SameField::Source),
        "scope_key" => Ok(SameField::ScopeKey),
        "equivalence_key" => Ok(SameField::EquivalenceKey),
        _ => input
            .strip_prefix("payload:")
            .filter(|key| !key.is_empty())
            .map(|key| SameField::Payload(key.to_string()))
            .ok_or_else(|| {
                "field must be source, scope_key, equivalence_key, or payload:<key>".to_string()
            }),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use clap::CommandFactory;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn within_command_builds_relation_evidence_request() -> xtask::sandbox::TestResult<()> {
        let command = RelationsCommand::parse_from([
            "relations",
            "within",
            "--within-secs",
            "300",
            "--seed-query-json",
            r#"{"event_types":["command.executed"],"limit":5}"#,
        ]);

        let request = command.subcommand.request()?;
        assert_eq!(request.seed_query.limit, 5);
        assert!(request.candidate_query.is_none());
        assert!(matches!(
            request.relation,
            EventRelationExpr::Within { within_secs: 300 }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn same_field_parser_supports_payload_fields() -> xtask::sandbox::TestResult<()> {
        assert_eq!(
            parse_same_field("payload:project").map_err(|error| color_eyre::eyre::eyre!(error))?,
            SameField::Payload("project".to_string())
        );
        assert!(parse_same_field("payload:").is_err());
        Ok(())
    }

    #[test]
    fn relations_help_includes_seed_query_json_flag() {
        let help = RelationsCommand::command().render_long_help().to_string();
        assert!(
            help.contains("--seed-query-json"),
            "relations command must expose the seed query input"
        );
    }
}
