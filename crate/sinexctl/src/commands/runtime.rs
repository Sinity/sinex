use clap::Subcommand;
use serde::{Deserialize, Serialize};
use sinex_primitives::rpc::coordination::{InstanceInfo, InstanceHealthResponse};
use sinex_primitives::views::ViewEnvelope;

use crate::Result;
use crate::client::GatewayClient;
use crate::commands::{AutomataCommand, RuntimePresenceCommand};
use crate::fmt::{CommandOutput, format_table_runtime, render_envelope, with_spinner_result};
use crate::model::{OutputFormat, RuntimeModuleRole};

/// Schema version for the runtime module list view payload.
const RUNTIME_MODULE_LIST_SCHEMA_VERSION: &str = "sinex.runtime-module-list/v1";

/// Payload carried inside a [`ViewEnvelope`] for `sinexctl runtime list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeModuleListView {
    pub schema_version: String,
    pub count: usize,
    pub modules: Vec<InstanceInfo>,
}

impl RuntimeModuleListView {
    fn new(modules: Vec<InstanceInfo>) -> Self {
        let count = modules.len();
        Self {
            schema_version: RUNTIME_MODULE_LIST_SCHEMA_VERSION.to_string(),
            count,
            modules,
        }
    }
}

/// Runtime module operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List all registered modules
    sinexctl runtime list

    # List only source modules
    sinexctl runtime list --role source

    # List running modules with health/staleness enrichment
    sinexctl runtime modules

    # Check status of a specific runtime module
    sinexctl runtime status terminal-source

    # Show automata runtime status
    sinexctl runtime automata

    # Drain a runtime module for maintenance
    sinexctl runtime drain terminal-source

    # Resume a drained runtime module
    sinexctl runtime resume terminal-source

    # Set horizon to replay last 24 hours
    sinexctl runtime set-horizon terminal-source 24h
")]
pub enum RuntimeCommands {
    /// List all modules
    List {
        /// Filter by role
        #[arg(long)]
        role: Option<RuntimeModuleRole>,
    },

    /// List running modules with status, health, and uptime
    Modules(RuntimePresenceCommand),

    /// Show automata runtime status
    Automata(AutomataCommand),

    /// Show runtime module status
    Status {
        /// Runtime module ID or name
        module: String,
    },

    /// Drain a runtime module for maintenance
    Drain {
        /// Runtime module ID or name
        module: String,
        /// Reason for draining
        #[arg(long, short)]
        reason: Option<String>,
    },

    /// Resume a drained runtime module
    Resume {
        /// Runtime module ID or name
        module: String,
    },

    /// Set runtime module horizon (cutoff time for event processing)
    SetHorizon {
        /// Runtime module ID or name
        module: String,

        /// Horizon timestamp (RFC3339 format or relative like "1h")
        horizon: String,
    },
}

impl RuntimeCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List { role } => {
                let modules = client.list_runtime(*role).await?;
                let envelope = ViewEnvelope::new(
                    "sinexctl.runtime.list",
                    RuntimeModuleListView::new(modules),
                )
                .with_query_echo(serde_json::json!({
                    "role": role,
                }));

                if let Some(output) =
                    render_envelope(&envelope, &envelope.payload.modules, format)?
                {
                    // Empty ndjson (zero modules) must stay empty — a blank line
                    // is not a valid NDJSON record (Codex review, PR #1766).
                    print!("{output}");
                    if !output.is_empty() && !output.ends_with('\n') {
                        println!();
                    }
                    return Ok(());
                }
                // OutputFormat::Table — fall through to human rendering
                if envelope.payload.modules.is_empty() {
                    println!("No modules found.");
                } else {
                    println!("{}", format_table_runtime(&envelope.payload.modules));
                }
            }
            Self::Modules(cmd) => {
                cmd.execute(client, format).await?;
            }
            Self::Automata(cmd) => {
                cmd.execute(client, format).await?;
            }
            Self::Status { module } => {
                let response = client.runtime_status(module).await?;
                CommandOutput::single(response, format_runtime_status_table).display(&format)?;
            }
            Self::Drain { module, reason } => {
                with_spinner_result(
                    format!("Draining runtime module {module}..."),
                    format!("Runtime module {module} drained"),
                    client.drain_runtime(module, reason.as_deref()),
                )
                .await?;
            }
            Self::Resume { module } => {
                with_spinner_result(
                    format!("Resuming runtime module {module}..."),
                    format!("Runtime module {module} resumed"),
                    client.resume_runtime(module),
                )
            .await?;
            }
            Self::SetHorizon { module, horizon } => {
                with_spinner_result(
                    format!("Setting horizon for {module}..."),
                    format!("Runtime module {module} horizon set to {horizon}"),
                    client.set_runtime_horizon(module, horizon),
                )
                .await?;
            }
        }
        Ok(())
    }
}

/// Format runtime module status as table
fn format_runtime_status_table(response: &InstanceHealthResponse) -> String {
    let mut output = String::new();
    output.push_str("Runtime Module Status:\n");
    output.push_str(&format!(
        "  Instance ID: {}\n",
        response.instance.instance_id
    ));
    output.push_str(&format!("  Type: {}\n", response.instance.module_kind));
    if let Some(ref hostname) = response.instance.hostname {
        output.push_str(&format!("  Hostname: {hostname}\n"));
    }
    output.push_str(&format!(
        "  Status: {}\n",
        if response.healthy {
            "✓ Healthy"
        } else {
            "✗ Unhealthy"
        }
    ));
    if let Some(ref heartbeat) = response.instance.last_heartbeat {
        output.push_str(&format!("  Last Heartbeat: {heartbeat}\n"));
    }
    output.push_str(&format!(
        "  Leader: {}\n",
        if response.instance.is_leader {
            "Yes"
        } else {
            "No"
        }
    ));
    if let Some(ref err) = response.last_error {
        output.push_str(&format!("  Last Error: {err}\n"));
    }
    output
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use super::RUNTIME_MODULE_LIST_SCHEMA_VERSION;
    use sinex_primitives::domain::{HostName, InstanceId, ModuleKind};
    use sinex_primitives::temporal::Timestamp;
    use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
    use xtask::sandbox::sinex_test;

    fn make_module(id: &str, kind: ModuleKind, is_leader: bool) -> InstanceInfo {
        InstanceInfo {
            instance_id: InstanceId::new(id),
            module_kind: kind,
            hostname: Some(HostName::from_static("testhost")),
            last_heartbeat: Some(Timestamp::now()),
            is_leader,
        }
    }

    fn fixture_modules(count: usize) -> Vec<InstanceInfo> {
        (0..count)
            .map(|i| make_module(&format!("instance-{i:04}"), ModuleKind::Source, i == 0))
            .collect()
    }

    fn fixture_envelope(count: usize) -> ViewEnvelope<RuntimeModuleListView> {
        ViewEnvelope::new(
            "sinexctl.runtime.list",
            RuntimeModuleListView::new(fixture_modules(count)),
        )
        .with_query_echo(serde_json::json!({ "role": null }))
    }

    /// `json` format: one finite document equal to the full envelope — parametric over count.
    #[sinex_test]
    async fn json_renders_one_finite_envelope_across_counts() -> xtask::TestResult<()> {
        for count in [0_usize, 1, 3, 10] {
            let envelope = fixture_envelope(count);
            let items = envelope.payload.modules.clone();

            let output = render_envelope(&envelope, &items, OutputFormat::Json)?
                .expect("json must return Some");

            let parsed: serde_json::Value = serde_json::from_str(&output).map_err(|e| {
                color_eyre::eyre::eyre!("json output did not parse (count={count}): {e}")
            })?;

            assert_eq!(
                parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION,
                "json must include envelope schema_version (count={count})"
            );
            assert_eq!(
                parsed["source_surface"], "sinexctl.runtime.list",
                "json must include source_surface (count={count})"
            );
            assert_eq!(
                parsed["payload"]["count"], count,
                "json must embed payload count (count={count})"
            );
            assert_eq!(
                parsed["payload"]["schema_version"], RUNTIME_MODULE_LIST_SCHEMA_VERSION,
                "json must include payload schema_version (count={count})"
            );
        }
        Ok(())
    }

    /// `ndjson` format: exactly N lines for N modules, each line independently parseable.
    #[sinex_test]
    async fn ndjson_line_count_equals_module_count() -> xtask::TestResult<()> {
        for count in [0_usize, 1, 4, 8] {
            let envelope = fixture_envelope(count);
            let items = envelope.payload.modules.clone();

            let output = render_envelope(&envelope, &items, OutputFormat::Ndjson)?
                .expect("ndjson must return Some");

            if count == 0 {
                assert!(output.is_empty(), "ndjson with 0 modules must produce empty output");
                continue;
            }

            assert!(output.ends_with('\n'), "ndjson output must end with a newline");

            let lines: Vec<&str> = output.trim_end_matches('\n').split('\n').collect();
            assert_eq!(
                lines.len(),
                count,
                "ndjson line count must equal module count (count={count})"
            );

            for (i, line) in lines.iter().enumerate() {
                let parsed: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                    color_eyre::eyre::eyre!(
                        "ndjson line {i} did not parse (count={count}): {e}"
                    )
                })?;
                assert!(
                    parsed.get("instance_id").is_some(),
                    "each ndjson line must be a standalone InstanceInfo object (line={i}, count={count})"
                );
                assert!(
                    !parsed.to_string().contains("\x1b["),
                    "ndjson line must not contain ANSI escape sequences (line={i})"
                );
            }
        }
        Ok(())
    }

    /// `dot` format: returns a typed error for non-graph views.
    #[sinex_test]
    async fn dot_returns_error_for_runtime_list_view() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(0);
        let items: Vec<InstanceInfo> = vec![];

        let result = render_envelope(&envelope, &items, OutputFormat::Dot);
        assert!(result.is_err(), "dot must return Err for a non-graph view");

        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("dot"), "error must name the rejected format: {msg}");
        assert!(msg.contains("graph"), "error must explain why dot is rejected: {msg}");
        Ok(())
    }

    /// `table` format: returns `None` so the caller owns table rendering.
    #[sinex_test]
    async fn table_returns_none_for_runtime_list() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(2);
        let items = envelope.payload.modules.clone();

        let result = render_envelope(&envelope, &items, OutputFormat::Table)?;
        assert!(result.is_none(), "table must return None");
        Ok(())
    }

    /// All machine formats must not contain ANSI escape sequences in envelope output.
    #[sinex_test]
    async fn machine_formats_contain_no_ansi_sequences() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(2);
        let items = envelope.payload.modules.clone();

        for format in [OutputFormat::Json, OutputFormat::Ndjson, OutputFormat::Yaml] {
            let output = render_envelope(&envelope, &items, format)?
                .expect("machine format must return Some");
            assert!(
                !output.contains("\x1b["),
                "format {format:?} must not contain ANSI escape sequences"
            );
        }
        Ok(())
    }
}
