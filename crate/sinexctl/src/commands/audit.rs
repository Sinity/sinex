use clap::Args;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::rpc::audit::AuditGetResponse;
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};

use crate::Result;
use crate::client::GatewayClient;
use crate::error::is_not_found_error;
use crate::fmt::print_finite_envelope;
use crate::model::OutputFormat;

/// Get audit trail for an operation
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # View audit trail for an operation
    sinexctl ops audit 01HQ2KM...

    # Output as JSON for processing
    sinexctl ops audit 01HQ2KM... -f json

    # Output as YAML
    sinexctl ops audit 01HQ2KM... -f yaml
")]
pub struct AuditCommand {
    /// Operation ID
    operation_id: String,
}

impl AuditCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        // Try to fetch audit trail, handle 404 gracefully
        let response: AuditGetResponse = match client.audit_get(&self.operation_id).await {
            Ok(resp) => resp,
            Err(e) if is_not_found_error(&e) => {
                eprintln!("Operation '{}' not found", self.operation_id);
                eprintln!();
                eprintln!("Use 'sinexctl ops list' to see available operations");
                std::process::exit(1);
            }
            Err(e) => return Err(e),
        };

        let envelope = audit_envelope(response.clone(), &self.operation_id);
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }

        match format {
            OutputFormat::Table => {
                println!("Audit Trail for Operation: {}", self.operation_id);
                println!("{}", "─".repeat(80));

                let op = &response.audit_trail.operation;
                println!("Operation:");
                println!("  Type: {}", op.operation_type);
                println!("  Status: {}", op.result_status);
                println!("  Operator: {}", op.operator);
                if let Some(msg) = &op.result_message {
                    println!("  Message: {msg}");
                }
                if let Some(duration) = op.duration_ms {
                    println!("  Duration: {duration}ms");
                }

                let events = &response.audit_trail.affected_events;
                if events.is_empty() {
                    println!("\nNo affected events recorded.");
                } else {
                    println!("\nAffected Events ({}):", events.len());
                    for (i, event) in events.iter().enumerate() {
                        println!(
                            "  {}. {} / {} ({})",
                            i + 1,
                            event.source,
                            event.event_type,
                            event.id
                        );
                    }
                    if response.has_more
                        && let Some(ref cursor) = response.next_cursor
                    {
                        println!(
                            "\n  … more results available. Use --after-id {cursor} to fetch the next page."
                        );
                    }
                }
            }
            OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Ndjson | OutputFormat::Dot => {}
        }

        Ok(())
    }
}

fn audit_envelope(
    response: AuditGetResponse,
    operation_id: &str,
) -> ViewEnvelope<AuditGetResponse> {
    let mut envelope = ViewEnvelope::new("sinexctl.ops.audit", response).with_query_echo(
        serde_json::json!({
            "operation_id": operation_id,
        }),
    );
    envelope.caveats = audit_caveats(&envelope.payload, operation_id);
    envelope
}

fn audit_caveats(response: &AuditGetResponse, operation_id: &str) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    if response.audit_trail.affected_events.is_empty() {
        caveats.push(audit_caveat(
            ReadinessCaveatId::SourceAbsent,
            "audit trail has no affected events recorded; this only proves the operation audit slice is empty",
            operation_id,
        ));
    }
    if response.has_more {
        caveats.push(audit_caveat(
            ReadinessCaveatId::WindowPartial,
            "audit trail is paginated; this response is a partial affected-event window",
            operation_id,
        ));
    }
    match response.audit_trail.operation.result_status {
        OperationStatus::Failed | OperationStatus::Cancelled => caveats.push(audit_caveat(
            ReadinessCaveatId::WindowPartial,
            "audited operation did not complete successfully; downstream state may reflect a partial or aborted change",
            operation_id,
        )),
        OperationStatus::Running | OperationStatus::Pending => caveats.push(audit_caveat(
            ReadinessCaveatId::WindowPartial,
            "audited operation is not terminal yet; audit trail may still be incomplete",
            operation_id,
        )),
        OperationStatus::Success => {}
    }
    caveats
}

fn audit_caveat(
    id: ReadinessCaveatId,
    message: impl Into<String>,
    operation_id: &str,
) -> CaveatView {
    CaveatView {
        id: id.as_str().to_string(),
        message: message.into(),
        ref_: Some(
            SinexObjectRef::new(SinexObjectKind::Operation, operation_id)
                .with_label(operation_id)
                .with_command_hint(format!("sinexctl ops audit {operation_id}"))
                .with_rpc_method("audit.get"),
        ),
    }
}

#[cfg(test)]
#[path = "audit_test.rs"]
mod tests;
