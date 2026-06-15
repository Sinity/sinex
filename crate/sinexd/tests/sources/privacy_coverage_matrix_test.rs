use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use sinex_primitives::utils::{InvalidUrlPolicy, redact_url_password_for_diagnostics};
use sinexd::sources::privacy_coverage::{
    PRIVACY_COVERAGE_ARTIFACT_PATH, render_privacy_coverage_matrix,
};
use tracing_subscriber::fmt::MakeWriter;
use xtask::sandbox::prelude::*;

#[derive(Clone, Default)]
struct CapturedLogs {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct CapturedLogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn output(&self) -> String {
        let bytes = self.bytes.lock().expect("captured log mutex poisoned");
        String::from_utf8(bytes.clone()).expect("tracing output should be UTF-8")
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

impl std::io::Write for CapturedLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes
            .lock()
            .expect("captured log mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn rendered_matrix() -> TestResult<Value> {
    Ok(serde_json::from_str(&render_privacy_coverage_matrix()?)?)
}

fn entry<'a>(matrix: &'a Value, source_id: &str) -> &'a Value {
    matrix["entries"]
        .as_array()
        .expect("entries array")
        .iter()
        .find(|entry| entry["source_id"] == source_id)
        .unwrap_or_else(|| panic!("missing privacy coverage entry for {source_id}"))
}

#[sinex_test]
async fn privacy_coverage_matrix_includes_source_contract_privacy_tiers() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let weechat = entry(&matrix, "weechat.message");

    assert_eq!(weechat["source_contract"]["privacy_tier"], "sensitive");
    assert_eq!(weechat["runtime_binding"]["privacy_context"], "command");
    assert_eq!(
        weechat["surface_behaviors"]["privacy_export"],
        "metadata_only_export_with_field_hints"
    );
    assert_eq!(
        weechat["surface_behaviors"]["query_recent_tui_logs"],
        "operator_authorized_raw_read_not_safe_export"
    );
    assert!(
        !matrix["caveats"]
            .to_string()
            .contains("intentionally not claimed"),
        "matrix must not silently omit operator surface coverage"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_includes_declarative_field_metadata() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let weechat = entry(&matrix, "weechat.message");

    assert_eq!(weechat["field_metadata_status"], "available");
    let fields = weechat["field_privacy_metadata"]
        .as_array()
        .expect("declarative field rows");
    assert!(
        fields.iter().any(|field| {
            field["field_name"] == "message"
                && field["field_type"] == "string"
                && field["field_class"] == "column_index"
                && field["effective_privacy_context"] == "command"
        }),
        "weechat.message field metadata should include message column"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_includes_sensitive_fixture_source() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let fixture = entry(&matrix, "privacy.fixture.sensitive-record");

    assert_eq!(fixture["source_contract"]["privacy_tier"], "sensitive");
    assert_eq!(fixture["runtime_binding"]["proposed"], true);
    assert_eq!(fixture["field_metadata_status"], "available");
    assert_eq!(
        fixture["surface_behaviors"]["privacy_export"],
        "metadata_only_export_with_field_hints"
    );
    assert_eq!(
        fixture["surface_behaviors"]["query_recent_tui_logs"],
        "operator_authorized_raw_read_not_safe_export"
    );

    let fields = fixture["field_privacy_metadata"]
        .as_array()
        .expect("fixture field rows");
    let field = |name: &str| {
        fields
            .iter()
            .find(|field| field["field_name"] == name)
            .unwrap_or_else(|| panic!("missing fixture field {name}"))
    };

    assert_eq!(
        field("source_path")["sensitivity_hints"],
        serde_json::json!(["source_path"])
    );
    assert_eq!(
        field("free_text")["sensitivity_hints"],
        serde_json::json!(["free_text", "potentially_sensitive"])
    );
    assert_eq!(
        field("credential_material")["sensitivity_hints"],
        serde_json::json!(["credential_bearing"])
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_marks_imperative_field_metadata_unavailable() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let bash = entry(&matrix, "terminal.bash-history");

    assert_eq!(bash["field_metadata_status"], "unavailable");
    assert_eq!(bash["field_metadata_behavior"], "unclassified");
    assert!(
        bash["caveats"]
            .to_string()
            .contains("field-level metadata unavailable"),
        "imperative parser entries must carry an explicit caveat"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_records_operator_surface_audit_rows() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let surfaces = matrix["surface_audit"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("surface_audit must be an array"))?;
    let surface = |name: &str| {
        surfaces
            .iter()
            .find(|row| row["surface"] == name)
            .unwrap_or_else(|| panic!("missing privacy surface audit row for {name}"))
    };

    assert_eq!(
        surface("privacy_export")["behavior"],
        "metadata_only_payloads_and_snippets_omitted"
    );
    assert_eq!(
        surface("public_rpc_errors")["behavior"],
        "public_payload_fields_only"
    );
    assert_eq!(
        surface("mcp_read_only_tools")["behavior"],
        "fixture_raw_samples_disabled_and_redacted"
    );
    assert_eq!(
        surface("completion_scripts")["behavior"],
        "formatless_static_command_metadata"
    );
    assert_eq!(
        surface("tui_privacy_actions")["behavior"],
        "static_operator_action_metadata"
    );
    assert_eq!(
        surface("logs_and_diagnostics")["behavior"],
        "fixture_password_url_redacted_in_tracing_output"
    );
    assert_eq!(
        surface("query_recent_watch")["behavior"],
        "operator_authorized_raw_read_not_safe_export"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_surface_rows_carry_evidence_and_caveats() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let surfaces = matrix["surface_audit"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("surface_audit must be an array"))?;

    for row in surfaces {
        let surface = row["surface"].as_str().unwrap_or("<missing>");
        assert!(
            row["evidence"].as_array().is_some_and(|items| !items.is_empty()),
            "surface audit row {surface} must cite evidence"
        );
        assert!(
            row["caveats"].as_array().is_some(),
            "surface audit row {surface} must carry explicit caveats"
        );
    }
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_log_diagnostic_omits_fixture_secret() -> TestResult<()> {
    let fixture_secret = "fixture-secret-password";
    let diagnostic_url = format!("postgresql://operator:{fixture_secret}@db.local/sinex");
    let redacted =
        redact_url_password_for_diagnostics(&diagnostic_url, InvalidUrlPolicy::RedactedMarker);
    let captured = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(captured.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            database_url = %redacted,
            "privacy fixture diagnostic"
        );
    });

    let output = captured.output();
    assert!(
        output.contains("privacy fixture diagnostic"),
        "test must capture the diagnostic log line: {output}"
    );
    assert!(
        output.contains("operator:***@db.local"),
        "diagnostic log should preserve useful URL context with a redacted password: {output}"
    );
    assert!(
        !output.contains(fixture_secret),
        "diagnostic log leaked fixture secret: {output}"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_artifact_matches_inventory() -> TestResult<()> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    let artifact = workspace_root.join(PRIVACY_COVERAGE_ARTIFACT_PATH);

    let rendered = render_privacy_coverage_matrix().expect("render privacy coverage matrix");
    let committed = std::fs::read_to_string(&artifact).unwrap_or_else(|e| {
        panic!(
            "failed to read committed privacy coverage matrix at {}: {e}\n\
             run `sinexd export-privacy-coverage-matrix` to generate it",
            artifact.display()
        )
    });

    assert_eq!(
        committed,
        rendered,
        "privacy coverage matrix artifact is stale — run `sinexd export-privacy-coverage-matrix` to regenerate ({})",
        artifact.display()
    );
    Ok(())
}
