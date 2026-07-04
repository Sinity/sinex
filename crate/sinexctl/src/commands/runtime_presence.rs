use clap::Args;
use color_eyre::Result;
use console::style;
use serde::Serialize;
use sinex_primitives::domain::ModuleKind;
use sinex_primitives::rpc::runtime::RuntimeInfo;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};

use crate::client::GatewayClient;
use crate::fmt::{format_heartbeat_age, print_finite_envelope};
use crate::model::{OutputFormat, RuntimeModuleRole};

const RUNTIME_MODULE_PRESENCE_SCHEMA_VERSION: &str = "sinex.runtime-module-presence/v1";

/// List running modules with status, health, and uptime.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # List all running modules
    sinexctl runtime modules

    # Filter by capture modules
    sinexctl runtime modules --role capture

    # Only derived (automata) modules
    sinexctl runtime modules --role derived
")]
pub struct RuntimePresenceCommand {
    /// Filter by role
    #[arg(long)]
    role: Option<RuntimeModuleRole>,
}

impl RuntimePresenceCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let modules = client.runtime_list_active(300).await?.modules;

        let enriched: Vec<EnrichedRuntimeInfo> = modules
            .into_iter()
            .filter(|info| role_matches(info.module_kind, self.role))
            .map(|info| {
                let now = Timestamp::now();
                let healthy = info
                    .last_heartbeat_at
                    .is_some_and(|hb| (now - hb).whole_seconds() < 60);
                let stale = info.last_heartbeat_at.is_some() && !healthy;
                EnrichedRuntimeInfo {
                    info,
                    healthy,
                    stale,
                }
            })
            .collect();

        let envelope = runtime_modules_envelope(&enriched, self.role);
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        render_modules_table(&enriched);

        Ok(())
    }
}

fn role_matches(kind: ModuleKind, role: Option<RuntimeModuleRole>) -> bool {
    match role {
        None => true,
        Some(RuntimeModuleRole::Capture) => kind == ModuleKind::Source,
        Some(RuntimeModuleRole::Derived) => kind == ModuleKind::Automaton,
        Some(RuntimeModuleRole::Core | RuntimeModuleRole::Gateway) => kind == ModuleKind::Service,
    }
}

// ─────────────────────────────────────────────────────────────
// Enriched runtime-module wrapper
// ─────────────────────────────────────────────────────────────

pub(crate) struct EnrichedRuntimeInfo {
    pub(crate) info: RuntimeInfo,
    pub(crate) healthy: bool,
    pub(crate) stale: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RuntimeModulePresenceView {
    schema_version: &'static str,
    count: usize,
    modules: Vec<RuntimeModulePresenceRow>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeModulePresenceRow {
    module_name: String,
    module_kind: String,
    service_name: Option<String>,
    instance_id: Option<String>,
    module_run_id: Option<String>,
    host: Option<String>,
    status: String,
    healthy: bool,
    stale: bool,
    last_heartbeat: Option<String>,
    started_at: Option<String>,
    heartbeat_source: RuntimeHeartbeatSourceView,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum RuntimeHeartbeatSourceView {
    Run,
    Manifest,
    Output,
}

impl From<sinex_primitives::rpc::runtime::RuntimeHeartbeatSource>
    for RuntimeHeartbeatSourceView
{
    fn from(value: sinex_primitives::rpc::runtime::RuntimeHeartbeatSource) -> Self {
        match value {
            sinex_primitives::rpc::runtime::RuntimeHeartbeatSource::Run => Self::Run,
            sinex_primitives::rpc::runtime::RuntimeHeartbeatSource::Manifest => Self::Manifest,
            sinex_primitives::rpc::runtime::RuntimeHeartbeatSource::Output => Self::Output,
        }
    }
}

pub(crate) fn runtime_modules_envelope(
    modules: &[EnrichedRuntimeInfo],
    role: Option<RuntimeModuleRole>,
) -> ViewEnvelope<RuntimeModulePresenceView> {
    let rows = modules
        .iter()
        .map(runtime_module_presence_row)
        .collect::<Vec<_>>();
    let mut envelope = ViewEnvelope::new(
        "sinexctl.runtime.modules",
        RuntimeModulePresenceView {
            schema_version: RUNTIME_MODULE_PRESENCE_SCHEMA_VERSION,
            count: rows.len(),
            modules: rows,
        },
    )
    .with_query_echo(serde_json::json!({
        "role": role.map(|role| role.to_string()),
        "stale_after_seconds": 60,
    }));
    envelope.caveats = runtime_modules_caveats(modules, role);
    envelope
}

fn runtime_module_presence_row(module: &EnrichedRuntimeInfo) -> RuntimeModulePresenceRow {
    RuntimeModulePresenceRow {
        module_name: module.info.module_name.as_str().to_string(),
        module_kind: module.info.module_kind.to_string(),
        service_name: module.info.service_name.clone(),
        instance_id: module.info.instance_id.clone(),
        module_run_id: module.info.module_run_id.map(|id| id.to_string()),
        host: module.info.host.clone(),
        status: module.info.status.clone(),
        healthy: module.healthy,
        stale: module.stale,
        last_heartbeat: module
            .info
            .last_heartbeat_at
            .map(|hb| hb.format_rfc3339()),
        started_at: module.info.started_at.map(|ts| ts.format_rfc3339()),
        heartbeat_source: module.info.heartbeat_source.into(),
    }
}

fn runtime_modules_caveats(
    modules: &[EnrichedRuntimeInfo],
    role: Option<RuntimeModuleRole>,
) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    if modules.is_empty() {
        let role_label = role.map_or_else(|| "any role".to_string(), |role| role.to_string());
        caveats.push(runtime_module_caveat(
            ReadinessCaveatId::SourceAbsent,
            format!(
                "runtime modules returned no active modules for {role_label}; this is a live runtime observation, not proof that no modules are configured"
            ),
            "runtime.modules.empty",
            "sinexctl runtime modules",
        ));
        return caveats;
    }

    for module in modules {
        let id = module
            .info
            .service_name
            .as_deref()
            .or(module.info.instance_id.as_deref())
            .unwrap_or_else(|| module.info.module_name.as_str());
        if module.stale {
            caveats.push(runtime_module_caveat(
                ReadinessCaveatId::WindowPartial,
                format!(
                    "runtime module `{id}` has a stale heartbeat; live runtime coverage may be partial"
                ),
                id,
                &format!("sinexctl runtime modules --role {}", role_hint(module.info.module_kind)),
            ));
        } else if module.info.last_heartbeat_at.is_none() {
            caveats.push(runtime_module_caveat(
                ReadinessCaveatId::CoverageUnmeasurable,
                format!(
                    "runtime module `{id}` has no heartbeat timestamp; freshness cannot be measured"
                ),
                id,
                &format!("sinexctl runtime modules --role {}", role_hint(module.info.module_kind)),
            ));
        }
    }

    caveats
}

fn role_hint(kind: ModuleKind) -> &'static str {
    match kind {
        ModuleKind::Source => "capture",
        ModuleKind::Automaton => "derived",
        ModuleKind::Service => "core",
    }
}

fn runtime_module_caveat(
    id: ReadinessCaveatId,
    message: impl Into<String>,
    ref_id: &str,
    command_hint: &str,
) -> CaveatView {
    CaveatView {
        id: id.as_str().to_string(),
        message: message.into(),
        ref_: Some(
            SinexObjectRef::new(SinexObjectKind::RuntimeModule, ref_id)
                .with_label(ref_id)
                .with_command_hint(command_hint)
                .with_rpc_method("runtime.list_active"),
        ),
    }
}

// ─────────────────────────────────────────────────────────────
// Terminal table rendering
// ─────────────────────────────────────────────────────────────

fn render_modules_table(modules: &[EnrichedRuntimeInfo]) {
    if modules.is_empty() {
        println!("{}", style("No modules found.").dim());
        return;
    }

    let total = modules.len();
    let healthy_count = modules.iter().filter(|module| module.healthy).count();
    let stale_count = modules.iter().filter(|module| module.stale).count();
    let unknown_count = total - healthy_count - stale_count;

    // Summary header
    let mut summary_parts = Vec::new();
    if healthy_count > 0 {
        summary_parts.push(format!("{} healthy", style(healthy_count).green()));
    }
    if stale_count > 0 {
        summary_parts.push(format!("{} stale", style(stale_count).yellow()));
    }
    if unknown_count > 0 {
        summary_parts.push(format!("{} unknown", style(unknown_count).dim()));
    }
    println!(
        "{} module{}: {}",
        style(total).bold(),
        if total == 1 { "" } else { "s" },
        summary_parts.join(", ")
    );

    println!("{}", style("─".repeat(90)).dim());

    // Column headers
    println!(
        "  {:<32} {:<14} {:<10} {:<12} {:<10} STATUS",
        "NAME", "TYPE", "HEALTH", "LAST SEEN", "HOST"
    );
    println!(
        "  {:-<32} {:-<14} {:-<10} {:-<12} {:-<10} {:-<10}",
        "", "", "", "", "", ""
    );

    for module in modules {
        let name = module
            .info
            .service_name
            .as_deref()
            .or(module.info.instance_id.as_deref())
            .unwrap_or_else(|| module.info.module_name.as_str());
        let module_kind = module.info.module_kind.to_string().to_lowercase();

        let health = if module.healthy {
            style("healthy").green()
        } else if module.stale {
            style("stale").yellow()
        } else {
            style("unknown").dim()
        };

        let last_seen = module
            .info
            .last_heartbeat_at
            .as_ref()
            .map_or_else(|| "never".to_string(), format_heartbeat_age);

        let host = module.info.host.as_deref().unwrap_or("—");
        let status = module.info.status.as_str();

        println!("  {name:<32} {module_kind:<14} {health:<16} {last_seen:<12} {host:<10} {status}");
    }
}
