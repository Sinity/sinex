use super::*;
use sinex_primitives::rpc::sources::{
    ImportProgressEntry, SourcesImportReportRequest, SourcesImportReportResponse,
};
use sinex_primitives::views::CaveatView;
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
    sinexctl ops import report <operation-id>
    sinexctl ops import report <operation-id> --format json
")]
pub enum ImportCommands {
    /// List in-flight paced historical imports.
    #[command(alias = "ls")]
    List,
    /// Render durable new/suppressed/superseded/failure outcomes for one import operation.
    Report {
        /// Replay or import operation UUID returned by the operation control plane.
        operation_id: String,
    },
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
            Self::Report { operation_id } => {
                let response = client
                    .sources_import_report(SourcesImportReportRequest {
                        operation_id: operation_id.clone(),
                    })
                    .await?;
                let mut envelope =
                    ViewEnvelope::new("sinexctl.ops.import.report", response.clone());
                if abnormal_suppression_rate(&response) {
                    envelope.caveats.push(CaveatView {
                        id: "import.abnormal_suppression_rate".to_string(),
                        message: "Most candidates were suppressed. Inspect the per-source/material breakdown and examples before treating this import as a complete no-op.".to_string(),
                        ref_: None,
                    });
                }
                if print_finite_envelope(&envelope, format)? {
                    return Ok(());
                }
                println!("{}", format_import_report_table(&response));
            }
        }
        Ok(())
    }
}

fn abnormal_suppression_rate(report: &SourcesImportReportResponse) -> bool {
    report.attempted >= 10 && report.suppressed.saturating_mul(2) >= report.attempted
}

fn format_import_report_table(report: &SourcesImportReportResponse) -> String {
    let mut output = format!(
        "Import idempotence: {} new, {} suppressed, {} superseded, {} failures, {} DLQ, {} unresolved\nOperation: {} ({})\n",
        report.new,
        report.suppressed,
        report.superseded,
        report.failures,
        report.dlq,
        report.unresolved,
        report.operation_id,
        report.operation_status,
    );
    if let Some(source) = &report.source {
        output.push_str(&format!("Source: {source}\n"));
    }
    output.push_str(&format!("Attempted: {}\n", report.attempted));
    if !report.source_material_ids.is_empty() {
        output.push_str(&format!(
            "Source materials: {}\n",
            report.source_material_ids.join(", ")
        ));
    }

    if !report.breakdown.is_empty() {
        let mut builder = Builder::new();
        builder.push_record([
            "SOURCE",
            "EVENT TYPE",
            "MATERIAL",
            "NEW",
            "SUPPRESSED",
            "SUPERSEDED",
            "FAILURES",
            "DLQ",
        ]);
        for row in &report.breakdown {
            builder.push_record([
                row.source.clone(),
                row.event_type.clone(),
                row.source_material_id
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                row.new.to_string(),
                row.suppressed.to_string(),
                row.superseded.to_string(),
                row.failures.to_string(),
                row.dlq.to_string(),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        output.push_str(&table.to_string());
    }
    output
}

fn format_import_progress_table(imports: &[ImportProgressEntry]) -> String {
    if imports.is_empty() {
        return "No historical imports currently in flight.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE",
        "PACED",
        "RATE(ev/s)",
        "EVENTS",
        "POSITION",
        "ETA",
        "BACKLOG",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn report(attempted: u64, suppressed: u64) -> SourcesImportReportResponse {
        SourcesImportReportResponse {
            operation_id: "01JIMPORTTEST".to_string(),
            operation_type: "replay".to_string(),
            operation_status: "success".to_string(),
            scope: serde_json::Value::Null,
            source: None,
            source_material_ids: Vec::new(),
            attempted,
            new: attempted.saturating_sub(suppressed),
            suppressed,
            superseded: 0,
            failures: 0,
            dlq: 0,
            unresolved: 0,
            breakdown: Vec::new(),
            examples: Vec::new(),
        }
    }

    #[test]
    fn suppression_caveat_requires_a_material_attempt_set() {
        assert!(!abnormal_suppression_rate(&report(9, 9)));
        assert!(!abnormal_suppression_rate(&report(10, 4)));
        assert!(abnormal_suppression_rate(&report(10, 5)));
    }
}
