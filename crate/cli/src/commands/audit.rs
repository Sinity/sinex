use clap::Args;
use sinex_primitives::rpc::audit::AuditGetResponse;

use crate::client::GatewayClient;
use crate::error::is_not_found_error;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::Result;

/// Get audit trail for an operation
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # View audit trail for an operation
    sinexctl audit 01HQ2KM...

    # Output as JSON for processing
    sinexctl audit 01HQ2KM... -f json

    # Output as YAML
    sinexctl audit 01HQ2KM... -f yaml
")]
pub struct AuditCommand {
    /// Operation ID
    operation_id: String,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl AuditCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
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

        match self.format {
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
                if !events.is_empty() {
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
                } else {
                    println!("\nNo affected events recorded.");
                }
            }
            OutputFormat::Json => {
                println!("{}", format_json(&response)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&response)?);
            }
        }

        Ok(())
    }
}
