//! Prediction calibration report command.
use crate::{
    client::GatewayClient,
    fmt::{format_json, format_yaml},
    model::OutputFormat,
};
use clap::Args;
use color_eyre::Result;
use sinex_primitives::rpc::predictions::PredictionReportRequest;
#[derive(Debug, Args)]
pub struct PredictionsCommand {
    #[arg(long)]
    pub predictor: Option<String>,
}
impl PredictionsCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let report = client
            .predictions_report(PredictionReportRequest {
                predictor: self.predictor.clone(),
            })
            .await?;
        match format {
            OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => {
                println!("{}", format_json(&report)?)
            }
            OutputFormat::Yaml => println!("{}", format_yaml(&report)?),
            OutputFormat::Table => {
                println!(
                    "Predictions: {} resolved, {} unresolved",
                    report.resolved_count, report.unresolved_count
                );
                for row in report.calibration {
                    println!(
                        "  {}: Brier {:.4} ({} resolved)",
                        row.predictor, row.brier_score, row.resolved_count
                    );
                }
            }
        }
        Ok(())
    }
}
