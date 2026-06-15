//! Static privacy coverage matrix for compiled source metadata.
//!
//! This is an auditable inventory, not a runtime redaction policy. It joins the
//! source contract inventory, runtime bindings, parser manifests, and optional
//! parser-declared field rows. Missing field rows are reported explicitly.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;
use sinex_primitives::parser::{ParserFieldPrivacyMetadata, ParserManifest};
use sinex_primitives::source_contracts::{
    SourceContract, SourceRuntimeBinding, all_source_contracts, source_runtime_bindings,
};

use crate::sources::dispatch::parser_inventory_records;

/// Repo-relative path of the committed privacy coverage matrix artifact.
pub const PRIVACY_COVERAGE_ARTIFACT_PATH: &str =
    "crate/sinexd/docs/sources/privacy-coverage.generated.json";

/// Bumped when the matrix shape changes.
const PRIVACY_COVERAGE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
pub struct PrivacyCoverageMatrix {
    schema_version: u32,
    caveats: Vec<&'static str>,
    surface_audit: Vec<SurfaceAuditCoverage>,
    entries: Vec<PrivacyCoverageEntry>,
}

#[derive(Debug, Serialize)]
pub struct PrivacyCoverageEntry {
    source_id: String,
    source_contract: Value,
    runtime_binding: Option<Value>,
    parser_manifest: Option<ParserManifest>,
    field_metadata_status: &'static str,
    field_metadata_behavior: &'static str,
    field_privacy_metadata: Vec<ParserFieldPrivacyMetadata>,
    surface_behaviors: SurfaceBehaviors,
    caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct SurfaceBehaviors {
    privacy_export: &'static str,
    public_rpc_errors: &'static str,
    mcp_search_fixture: &'static str,
    query_recent_tui_logs: &'static str,
}

#[derive(Debug, Serialize)]
pub struct SurfaceAuditCoverage {
    surface: &'static str,
    behavior: &'static str,
    evidence: &'static [&'static str],
    caveats: &'static [&'static str],
}

/// Build the privacy coverage matrix from link-time inventories.
#[must_use]
pub fn build_privacy_coverage_matrix() -> PrivacyCoverageMatrix {
    let bindings = source_runtime_bindings()
        .map(|binding| (binding.source_id, binding))
        .collect::<BTreeMap<&'static str, &'static SourceRuntimeBinding>>();
    let parsers = parser_inventory_records()
        .into_iter()
        .map(|record| (record.source_id.clone(), record))
        .collect::<BTreeMap<String, _>>();

    let mut contracts: Vec<&'static SourceContract> = all_source_contracts().collect();
    contracts.sort_by(|a, b| a.id.cmp(b.id));

    let entries = contracts
        .into_iter()
        .map(|contract| {
            let binding = bindings.get(contract.id).copied();
            let parser = parsers.get(contract.id);
            let fields = parser
                .map(|record| record.field_privacy_metadata.clone())
                .unwrap_or_default();
            let has_fields = !fields.is_empty();
            let mut caveats = Vec::new();

            if parser.is_none() {
                caveats.push("no parser factory is registered for this source in sinexd");
            } else if !has_fields {
                caveats.push(
                    "field-level metadata unavailable; imperative parsers must declare rows before coverage can be inferred",
                );
            }

            PrivacyCoverageEntry {
                source_id: contract.id.to_string(),
                source_contract: to_json_value(contract),
                runtime_binding: binding.map(to_json_value),
                parser_manifest: parser.map(|record| record.manifest.clone()),
                field_metadata_status: if has_fields {
                    "available"
                } else {
                    "unavailable"
                },
                field_metadata_behavior: if has_fields {
                    "metadata_only_export"
                } else {
                    "unclassified"
                },
                field_privacy_metadata: fields,
                surface_behaviors: SurfaceBehaviors {
                    privacy_export: if has_fields {
                        "metadata_only_export_with_field_hints"
                    } else {
                        "metadata_only_export_source_level_only"
                    },
                    public_rpc_errors: "public_error_details_only",
                    mcp_search_fixture: "fixture_redacted",
                    query_recent_tui_logs: "operator_authorized_raw_read_not_safe_export",
                },
                caveats,
            }
        })
        .collect();

    PrivacyCoverageMatrix {
        schema_version: PRIVACY_COVERAGE_SCHEMA_VERSION,
        caveats: vec![
            "static matrix only; does not apply DB/user privacy policy",
            "declarative parser field rows are metadata-only and do not prove runtime redaction",
            "imperative parser field coverage is unavailable until explicitly declared",
            "operator raw-read commands remain operator-authorized views; this artifact records safe-surface evidence, not a global payload-redaction guarantee",
        ],
        surface_audit: surface_audit_coverage(),
        entries,
    }
}

fn surface_audit_coverage() -> Vec<SurfaceAuditCoverage> {
    vec![
        SurfaceAuditCoverage {
            surface: "privacy_export",
            behavior: "metadata_only_payloads_and_snippets_omitted",
            evidence: &[
                "crate/sinexctl/src/commands/privacy.rs::privacy_export_renderers_omit_payload_and_snippet_material",
                "crate/sinexctl/src/model/format_registry.rs::privacy export note",
            ],
            caveats: &[
                "uses events.query for selection only; raw payload and snippet fields are omitted from the export report",
            ],
        },
        SurfaceAuditCoverage {
            surface: "public_rpc_errors",
            behavior: "public_payload_fields_only",
            evidence: &[
                "crate/sinexctl/tests/error_public_test.rs::test_format_public_rpc_error_details_omits_sensitive_context",
                "crate/sinexd/src/api/rpc_server.rs::sinex_error_to_rpc_code tests",
            ],
            caveats: &[
                "stable kind/status/error_id fields are exposed; private nested diagnostic context is intentionally omitted",
            ],
        },
        SurfaceAuditCoverage {
            surface: "mcp_read_only_tools",
            behavior: "fixture_raw_samples_disabled_and_redacted",
            evidence: &[
                "crate/sinexctl/tests/validation_test.rs::mcp_search_events_call_uses_gateway_fixture",
                "crate/sinexctl/tests/validation_test.rs::mcp_trace_lineage_call_uses_gateway_fixture",
                "crate/sinexctl/tests/validation_test.rs::mcp_privacy_status_call_uses_gateway_fixture",
                "crate/sinexctl/tests/validation_test.rs::mcp_document_chunks_call_uses_gateway_fixture",
            ],
            caveats: &[
                "MCP is read-only and typed-client backed; raw sample leakage is pinned by gateway fixtures",
            ],
        },
        SurfaceAuditCoverage {
            surface: "completion_scripts",
            behavior: "formatless_static_command_metadata",
            evidence: &[
                "crate/sinexctl/src/commands/completions.rs::completion_dynamic_vocab_omits_sensitive_fixture_material",
                "crate/sinexctl/src/commands/completions.rs::zsh_postprocessor_injects_dynamic_source_and_event_type_lists",
            ],
            caveats: &[
                "completion dynamic vocab is restricted to source and event-type identifiers from payload inventory",
            ],
        },
        SurfaceAuditCoverage {
            surface: "tui_privacy_actions",
            behavior: "static_operator_action_metadata",
            evidence: &[
                "crate/sinexctl/src/commands/tui.rs::privacy export/delete/redact authority panel",
                "crate/sinexctl/src/commands/tui.rs::redacted fixture surface label",
            ],
            caveats: &[
                "TUI action cards are static command affordances; live event rendering remains an operator raw-read surface",
            ],
        },
        SurfaceAuditCoverage {
            surface: "logs_and_diagnostics",
            behavior: "fixture_password_url_redacted_in_tracing_output",
            evidence: &[
                "crate/sinex-primitives/src/utils/url_redaction.rs",
                "crate/sinexd/src/runtime/preflight/database.rs::redact_password",
                "crate/sinexd/src/runtime/preflight/services.rs::redact_password",
                "crate/sinexd/tests/sources/privacy_coverage_matrix_test.rs::privacy_coverage_log_diagnostic_omits_fixture_secret",
            ],
            caveats: &[
                "covers diagnostic paths that route credentials through URL password redaction before logging; arbitrary tracing payloads remain outside this guarantee",
            ],
        },
        SurfaceAuditCoverage {
            surface: "query_recent_watch",
            behavior: "operator_authorized_raw_read_not_safe_export",
            evidence: &[
                "crate/sinexctl/src/model/format_registry.rs::query/recent/watch command effects",
                "crate/sinexctl/src/commands/privacy.rs::privacy_export_requires_explicit_scope",
            ],
            caveats: &[
                "query/recent/watch are intentionally raw operator views; use privacy export for metadata-only sharing",
            ],
        },
    ]
}

/// Render deterministic pretty JSON with a trailing newline.
pub fn render_privacy_coverage_matrix() -> serde_json::Result<String> {
    Ok(serde_json::to_string_pretty(&build_privacy_coverage_matrix())? + "\n")
}

/// Write or compare the generated matrix artifact.
pub fn export_privacy_coverage_matrix(output: &Path, check_only: bool) -> std::io::Result<bool> {
    let rendered = render_privacy_coverage_matrix()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let current = std::fs::read_to_string(output).ok();
    let changed = current.as_deref() != Some(rendered.as_str());

    if changed && !check_only {
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output, &rendered)?;
    }

    Ok(changed)
}

fn to_json_value<T: Serialize>(value: T) -> Value {
    serde_json::to_value(value).expect("source inventory metadata serializes")
}
