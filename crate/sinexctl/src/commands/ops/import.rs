use super::*;
use sinex_primitives::rpc::curation::CurationListDuplicateCandidatesRequest;
use sinex_primitives::rpc::sources::{
    ImportProgressEntry, SourcesImportReportRequest, SourcesImportReportResponse,
};
use sinex_primitives::views::CaveatView;
use tabled::{builder::Builder, settings::Style};

const ADJUDICATION_CANDIDATE_LIMIT: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdjudicationQueueSummary {
    Available {
        clusters: usize,
        events: i64,
        partial: bool,
    },
    Unavailable,
}

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
                let adjudication =
                    load_adjudication_queue_summary(client, response.source.clone()).await;
                println!(
                    "{}",
                    format_import_report_table_with_adjudication(&response, adjudication)
                );
            }
        }
        Ok(())
    }
}

async fn load_adjudication_queue_summary(
    client: &GatewayClient,
    source: Option<String>,
) -> AdjudicationQueueSummary {
    let response = client
        .curation_duplicate_candidates_list(CurationListDuplicateCandidatesRequest {
            source,
            limit: ADJUDICATION_CANDIDATE_LIMIT,
            events_per_cluster: 1,
            ..Default::default()
        })
        .await;
    match response {
        Ok(response) => AdjudicationQueueSummary::Available {
            clusters: response.clusters.len(),
            events: response.clusters.iter().map(|cluster| cluster.event_count).sum(),
            partial: response.clusters.len() as i64 >= ADJUDICATION_CANDIDATE_LIMIT,
        },
        Err(_) => AdjudicationQueueSummary::Unavailable,
    }
}

fn abnormal_suppression_rate(report: &SourcesImportReportResponse) -> bool {
    report.attempted >= 10 && report.suppressed.saturating_mul(2) >= report.attempted
}

fn format_import_report_table_with_adjudication(
    report: &SourcesImportReportResponse,
    adjudication: AdjudicationQueueSummary,
) -> String {
    let adjudication_count = match adjudication {
        AdjudicationQueueSummary::Available {
            clusters, partial, ..
        } if partial => format!("{clusters}+"),
        AdjudicationQueueSummary::Available { clusters, .. } => clusters.to_string(),
        AdjudicationQueueSummary::Unavailable => "unavailable".to_string(),
    };
    let mut output = format!(
        "Import idempotence: {} new, {} suppressed, {} superseded, {} failures, {} DLQ, {} unresolved, {} adjudication candidates\nOperation: {} ({})\n",
        report.new,
        report.suppressed,
        report.superseded,
        report.failures,
        report.dlq,
        report.unresolved,
        adjudication_count,
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
    if !report.examples.is_empty() {
        let mut builder = Builder::new();
        builder.push_record([
            "OUTCOME",
            "EVENT",
            "SOURCE",
            "EVENT TYPE",
            "MATERIAL",
            "REASON",
            "SUPERSEDES",
        ]);
        for example in &report.examples {
            builder.push_record([
                example.outcome.clone(),
                example.event_id.clone(),
                example.source.clone(),
                example.event_type.clone(),
                example
                    .source_material_id
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                example.reason.clone().unwrap_or_else(|| "-".to_string()),
                example
                    .superseded_event_id
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        output.push_str("\nExamples:\n");
        output.push_str(&table.to_string());
    }
    if let AdjudicationQueueSummary::Available {
        clusters,
        events,
        partial,
    } = adjudication
    {
        let qualifier = if partial { "at least " } else { "" };
        output.push_str(&format!(
            "\nAdjudication queue: {qualifier}{clusters} pending candidate cluster(s), {events} candidate event(s) in the current {} scope.\n",
            report.source.as_deref().unwrap_or("all-source")
        ));
    } else {
        output.push_str("\nAdjudication queue: unavailable; import outcomes remain complete.\n");
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
    use sinex_primitives::rpc::sources::ImportReportExample;

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

    #[test]
    fn import_report_table_keeps_outcome_examples_and_adjudication_visible() {
        let mut report = report(2, 1);
        report.examples.push(ImportReportExample {
            outcome: "suppressed".to_string(),
            event_id: "event-ref".to_string(),
            source: "fixture".to_string(),
            event_type: "fixture.event".to_string(),
            source_material_id: Some("material-ref".to_string()),
            reason: Some("equivalence key already admitted".to_string()),
            superseded_event_id: Some("prior-event-ref".to_string()),
        });

        let table = format_import_report_table_with_adjudication(
            &report,
            AdjudicationQueueSummary::Available {
                clusters: 3,
                events: 7,
                partial: false,
            },
        );

        assert!(table.contains("1 suppressed"));
        assert!(table.contains("3 adjudication candidates"));
        assert!(table.contains("event-ref"));
        assert!(table.contains("material-ref"));
        assert!(table.contains("prior-event-ref"));
        assert!(table.contains(
            "3 pending candidate cluster(s), 7 candidate event(s)"
        ));
    }
}
