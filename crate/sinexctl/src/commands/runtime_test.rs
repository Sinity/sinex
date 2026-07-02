#![allow(clippy::unwrap_used)]

use super::RUNTIME_MODULE_LIST_SCHEMA_VERSION;
use super::*;
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
