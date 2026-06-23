use super::*;

/// Read-only operation job surface (rendered through ViewEnvelope)
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List recent operations (all kinds)
    sinexctl ops jobs list

    # List only replay jobs
    sinexctl ops jobs list -t replay

    # List failed jobs, JSON output
    sinexctl ops jobs list -s failed --format json

    # Show a specific operation
    sinexctl ops jobs show 01HQ2KM...
")]
pub enum JobsCommands {
    /// List operations as a ViewEnvelope (all kinds, or filtered)
    #[command(alias = "ls")]
    List {
        /// Filter by operation kind (replay, archive, restore, purge, tombstone)
        #[arg(long, short = 't')]
        kind: Option<String>,

        /// Filter by result status (running, success, failed, cancelled, pending)
        #[arg(long, short = 's')]
        status: Option<String>,

        /// Maximum number of results
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Show a single operation as a ViewEnvelope
    Show {
        /// Operation ID
        operation_id: String,
    },
}

impl JobsCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List {
                kind,
                status,
                limit,
            } => {
                let operations = client
                    .ops_list(kind.clone(), status.clone(), Some(*limit))
                    .await?;

                let views = operations_to_views(&operations);

                let envelope = ViewEnvelope::new(
                    "sinexctl.ops.jobs.list",
                    OperationJobListView::new(views.clone()),
                )
                .with_query_echo(serde_json::json!({
                    "kind": kind,
                    "status": status,
                    "limit": limit,
                }));

                if let Some(output) = render_envelope(&envelope, &views, format)? {
                    print_machine_output(&output);
                    return Ok(());
                }
                // Table format — human rendering
                if envelope.payload.jobs.is_empty() {
                    println!("No operations found.");
                } else {
                    println!("{}", format_jobs_list_table(&envelope.payload.jobs));
                }
            }
            Self::Show { operation_id } => {
                let operation = client.ops_get(operation_id).await?;
                let view = operation_to_view(&operation);

                let envelope = ViewEnvelope::new("sinexctl.ops.jobs.show", view.clone());

                if print_finite_envelope(&envelope, format)? {
                    return Ok(());
                }
                // Table format — human rendering
                println!("{}", format_job_show_table(&view));
            }
        }
        Ok(())
    }
}
