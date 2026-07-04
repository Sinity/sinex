#![allow(clippy::unwrap_used)]

use super::RUNTIME_MODULE_LIST_SCHEMA_VERSION;
use super::*;
use std::collections::BTreeMap;

use sinex_primitives::domain::{HealthStatus, InstanceId, ModuleKind, ModuleName};
use sinex_primitives::rpc::coordination::{ErrorInfo, InstanceInfo};
use sinex_primitives::rpc::runtime::RuntimeHeartbeatSource;
use sinex_primitives::rpc::system::{
    ComponentHealthReport, ComponentsHealth, ReplayControlHealth, SystemHealthResponse,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use crate::fmt::render_finite_envelope;
use xtask::sandbox::sinex_test;

fn make_module(id: &str, kind: ModuleKind) -> RuntimeInfo {
    RuntimeInfo {
        module_name: ModuleName::new(id),
        module_kind: kind,
        version: "test-version".to_string(),
        description: None,
        service_name: Some(id.to_string()),
        instance_id: Some(format!("{id}-instance")),
        module_run_id: None,
        host: Some("testhost".to_string()),
        status: "running".to_string(),
        last_heartbeat_at: Some(Timestamp::now()),
        started_at: Some(Timestamp::now()),
        heartbeat_source: RuntimeHeartbeatSource::Run,
    }
}

fn fixture_modules(count: usize) -> Vec<RuntimeInfo> {
    (0..count)
        .map(|i| make_module(&format!("instance-{i:04}"), ModuleKind::Source))
        .collect()
}

fn fixture_envelope(count: usize) -> ViewEnvelope<RuntimeModuleListView> {
    ViewEnvelope::new(
        "sinexctl.runtime.list",
        RuntimeModuleListView::new(fixture_modules(count)),
    )
    .with_query_echo(serde_json::json!({ "role": null }))
}

fn healthy_component() -> ComponentHealthReport {
    ComponentHealthReport {
        status: HealthStatus::Healthy,
        connected: true,
        latency_ms: None,
        detail: None,
        attributes: BTreeMap::new(),
    }
}

fn fixture_system_health() -> SystemHealthResponse {
    SystemHealthResponse {
        status: HealthStatus::Healthy,
        healthy: true,
        serving: true,
        degradation_reasons: Vec::new(),
        components: ComponentsHealth {
            database: healthy_component(),
            nats: healthy_component(),
            raw_ingest_dlq: healthy_component(),
            replay_control: ReplayControlHealth {
                status: HealthStatus::Healthy,
                enabled: true,
                connected: true,
                last_error: None,
            },
            sse_confirmation: healthy_component(),
        },
    }
}

fn fixture_instance_health() -> InstanceHealthResponse {
    InstanceHealthResponse {
        instance: InstanceInfo {
            instance_id: InstanceId::from("terminal-source"),
            module_kind: ModuleKind::Source,
            hostname: None,
            last_heartbeat: Some(Timestamp::now()),
            is_leader: false,
        },
        healthy: true,
        last_error: None,
    }
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

#[sinex_test]
async fn runtime_status_json_renders_finite_view_envelope() -> xtask::TestResult<()> {
    let envelope = runtime_status_envelope(fixture_instance_health());
    let output = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.runtime.status");
    assert_eq!(
        parsed["payload"]["schema_version"],
        RUNTIME_STATUS_SCHEMA_VERSION
    );
    assert_eq!(
        parsed["payload"]["status"]["instance"]["instance_id"],
        "terminal-source"
    );
    assert!(
        parsed.get("caveats").is_none(),
        "healthy runtime status with heartbeat should not emit readiness caveats"
    );
    Ok(())
}

#[sinex_test]
async fn runtime_status_caveats_name_unhealthy_and_unmeasurable_freshness()
-> xtask::TestResult<()> {
    let mut status = fixture_instance_health();
    status.healthy = false;
    status.instance.last_heartbeat = None;
    status.last_error = Some(ErrorInfo {
        message: "heartbeat missed".to_string(),
        code: Some("stale".to_string()),
        timestamp: None,
    });

    let envelope = runtime_status_envelope(status);
    let caveat_ids = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect::<Vec<_>>();

    assert!(
        caveat_ids.contains(&ReadinessCaveatId::WindowPartial.as_str()),
        "unhealthy runtime status must be surfaced as window.partial"
    );
    assert!(
        caveat_ids.contains(&ReadinessCaveatId::CoverageUnmeasurable.as_str()),
        "missing heartbeat must be surfaced as coverage.unmeasurable"
    );
    assert!(
        envelope
            .caveats
            .iter()
            .any(|caveat| caveat.message.contains("terminal-source")),
        "runtime status caveats must name the module"
    );
    assert!(
        envelope.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref())
            == Some("sinexctl runtime status terminal-source")),
        "runtime status caveats must preserve a command hint"
    );
    Ok(())
}

#[sinex_test]
async fn runtime_health_json_renders_finite_view_envelope() -> xtask::TestResult<()> {
    let envelope = runtime_health_envelope(fixture_system_health());
    let output = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.runtime.health");
    assert_eq!(
        parsed["payload"]["schema_version"],
        RUNTIME_HEALTH_SCHEMA_VERSION
    );
    assert_eq!(parsed["payload"]["health"]["healthy"], true);
    assert!(
        parsed.get("caveats").is_none(),
        "healthy runtime view should not emit readiness caveats"
    );
    Ok(())
}

#[sinex_test]
async fn runtime_health_caveats_name_absent_and_partial_components() -> xtask::TestResult<()> {
    let mut health = fixture_system_health();
    health.status = HealthStatus::Degraded;
    health.healthy = false;
    health
        .degradation_reasons
        .push("NATS unavailable".to_string());
    health.components.nats = ComponentHealthReport {
        status: HealthStatus::Unhealthy,
        connected: false,
        latency_ms: None,
        detail: Some("connection refused".to_string()),
        attributes: BTreeMap::new(),
    };
    health.components.raw_ingest_dlq = ComponentHealthReport {
        status: HealthStatus::Degraded,
        connected: true,
        latency_ms: None,
        detail: Some("pressure high".to_string()),
        attributes: BTreeMap::new(),
    };

    let envelope = runtime_health_envelope(health);
    let caveat_ids = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect::<Vec<_>>();

    assert!(
        caveat_ids.contains(&"source.absent"),
        "disconnected NATS component must be surfaced as source.absent"
    );
    assert!(
        caveat_ids.contains(&"window.partial"),
        "overall degraded health and connected degraded components must be surfaced as window.partial"
    );
    assert!(
        envelope.caveats.iter().any(|caveat| caveat
            .message
            .contains("runtime health component `nats`")),
        "component caveat must name the disconnected component"
    );
    assert!(
        envelope.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref())
            == Some("sinexctl runtime health")),
        "component caveats must preserve a command hint"
    );
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
            assert!(
                output.is_empty(),
                "ndjson with 0 modules must produce empty output"
            );
            continue;
        }

        assert!(
            output.ends_with('\n'),
            "ndjson output must end with a newline"
        );

        let lines: Vec<&str> = output.trim_end_matches('\n').split('\n').collect();
        assert_eq!(
            lines.len(),
            count,
            "ndjson line count must equal module count (count={count})"
        );

        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                color_eyre::eyre::eyre!("ndjson line {i} did not parse (count={count}): {e}")
            })?;
            assert!(
                parsed.get("module_name").is_some(),
                "each ndjson line must be a standalone RuntimeInfo object (line={i}, count={count})"
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
    let items: Vec<RuntimeInfo> = vec![];

    let result = render_envelope(&envelope, &items, OutputFormat::Dot);
    assert!(result.is_err(), "dot must return Err for a non-graph view");

    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("dot"),
        "error must name the rejected format: {msg}"
    );
    assert!(
        msg.contains("graph"),
        "error must explain why dot is rejected: {msg}"
    );
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
