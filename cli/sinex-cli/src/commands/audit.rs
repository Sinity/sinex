use clap::Args;

use crate::client::GatewayClient;
use crate::error::is_not_found_error;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::util::json::get_str;
use crate::Result;

/// Get audit trail for an operation
#[derive(Debug, Args)]
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
        let audit = match client.audit_get(&self.operation_id).await {
            Ok(audit) => audit,
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

                if let Some(operation) = audit.get("operation") {
                    println!("Operation:");
                    println!("  Type: {}", get_str(operation, "operation_type"));
                    println!("  Status: {}", get_str(operation, "status"));
                    println!("  Operator: {}", get_str(operation, "operator"));
                    println!("  Started: {}", get_str(operation, "started_at"));
                }

                if let Some(events) = audit.get("events").and_then(|v| v.as_array()) {
                    println!("\nAudit Events:");
                    for (i, event) in events.iter().enumerate() {
                        println!("  {}. {}", i + 1, serde_json::to_string_pretty(event)?);
                    }
                }
            }
            OutputFormat::Json => {
                println!("{}", format_json(&audit)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&audit)?);
            }
        }

        Ok(())
    }
}
