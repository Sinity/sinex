//! Reviewed source/package skeleton renderer (#1737).
//!
//! Skeletons are generated from the package-completeness inventory so authoring
//! starts from the same SourceContract/SourceRuntimeBinding/EventContract/
//! AdmissionPolicy evidence that the #1792 gate checks.

use std::fmt::Write as _;

use crate::sources::package_completeness::{
    PackageCompletenessMode, RequirementDiagnostic, build_package_completeness_report,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceSkeletonError {
    PackageNotFound(String),
    ModeNotFound { package_id: String, mode_id: String },
    Render,
}

impl std::fmt::Display for SourceSkeletonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PackageNotFound(package_id) => write!(
                f,
                "package `{package_id}` not found in package completeness report"
            ),
            Self::ModeNotFound {
                package_id,
                mode_id,
            } => write!(f, "mode `{mode_id}` not found for package `{package_id}`"),
            Self::Render => write!(f, "failed to render source skeleton"),
        }
    }
}

impl std::error::Error for SourceSkeletonError {}

/// Render a reviewed Rust skeleton for one package/mode row.
pub fn render_source_skeleton(
    package_id: &str,
    mode_id: &str,
) -> Result<String, SourceSkeletonError> {
    let report = build_package_completeness_report();
    let package = report
        .packages
        .get(package_id)
        .ok_or_else(|| SourceSkeletonError::PackageNotFound(package_id.to_string()))?;
    let mode = package
        .modes
        .get(mode_id)
        .ok_or_else(|| SourceSkeletonError::ModeNotFound {
            package_id: package_id.to_string(),
            mode_id: mode_id.to_string(),
        })?;

    render_mode_skeleton(mode)
}

fn render_mode_skeleton(mode: &PackageCompletenessMode) -> Result<String, SourceSkeletonError> {
    let module_name = rust_ident(&mode.package_id);
    let type_name = rust_type_name(&mode.package_id);
    let mut out = String::new();

    writeln!(
        out,
        "//! Reviewed source/package skeleton generated from `sinexd export-source-skeleton`."
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "//! Package `{}` mode `{}`.",
        mode.package_id, mode.mode_id
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "use sinex_macros::SourceMeta;").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "use sinex_primitives::privacy::ProcessingContext;")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "use sinex_primitives::source_contracts::{{AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile, RetentionPolicy, RunnerPack, RuntimeShape, SourceContract, SourceRuntimeBinding}};"
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "compile_error!(\"complete parser, disclosure, fixtures, coverage/debt, operations, and deployment fields before registering `{}` `{}`\");",
        mode.package_id, mode.mode_id
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "pub mod {module_name} {{").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "    use super::*;").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "    #[derive(Debug, Clone, Default, SourceMeta)]")
        .map_err(|_| SourceSkeletonError::Render)?;
    let primary_event = mode.event_pairs.first();
    let event_source = primary_event
        .map(|event| event.source.as_str())
        .unwrap_or("replace.event.source");
    let event_type = primary_event
        .map(|event| event.event_type.as_str())
        .unwrap_or("replace.event_type");
    let additional_event_types = mode
        .event_pairs
        .iter()
        .skip(1)
        .map(|event| event.event_type.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let binding = mode.sources.runtime_binding.as_ref();
    let adapter = binding
        .map(|binding| binding.adapter.as_str())
        .unwrap_or("ReplaceAdapter");
    let implementation = binding
        .map(|binding| binding.implementation.as_str())
        .unwrap_or("replace-implementation");
    let source_contract = &mode.sources.source_contract;
    let privacy_tier = privacy_tier_expr(&source_contract.privacy_tier);
    let horizons = horizons_expr(&source_contract.horizons);
    let retention = retention_expr(&source_contract.retention);
    let occurrence_identity = occurrence_identity_expr(&source_contract.occurrence_identity);
    let access_scope = access_scope_expr(&source_contract.access_scope);
    let privacy_context = binding.map_or_else(
        || "ProcessingContext::Command".to_string(),
        |binding| privacy_context_expr(&binding.privacy_context),
    );
    let resource_profile = binding.map_or_else(
        || "ResourceProfile::BoundedFile".to_string(),
        |binding| resource_profile_expr(&binding.resource_profile),
    );
    let runner_pack = binding.map_or_else(
        || "RunnerPack::Staged".to_string(),
        |binding| runner_pack_expr(&binding.runner_pack),
    );
    let checkpoint_family = binding.map_or_else(
        || "CheckpointFamily::AppendStream".to_string(),
        |binding| checkpoint_family_expr(&binding.checkpoint_family),
    );
    let runtime_shape = binding.map_or_else(
        || "RuntimeShape::OnDemand".to_string(),
        |binding| runtime_shape_expr(&binding.runtime_shape),
    );
    let capabilities = binding
        .map(|binding| binding.capabilities.as_slice())
        .unwrap_or(&mode.coverage_debt_refs);
    writeln!(out, "    #[source_meta(").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        id = \"{}\",", mode.package_id)
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "        namespace = \"{}\",",
        mode.sources.source_contract.namespace
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        event_source = \"{event_source}\",")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        event_type = \"{event_type}\",")
        .map_err(|_| SourceSkeletonError::Render)?;
    if !additional_event_types.is_empty() {
        writeln!(out, "        event_types = \"{additional_event_types}\",")
            .map_err(|_| SourceSkeletonError::Render)?;
    }
    writeln!(out, "        adapter = \"{adapter}\",").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        privacy_tier = {privacy_tier},")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        horizons({horizons}),").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        retention = {retention},").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        implementation = \"{implementation}\",")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        occurrence_identity = {occurrence_identity},")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        access_scope = {access_scope},")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        privacy_context = {privacy_context},")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        resource_profile = {resource_profile},")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        runner_pack = {runner_pack},")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        checkpoint_family = {checkpoint_family},")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        runtime_shape = {runtime_shape},")
        .map_err(|_| SourceSkeletonError::Render)?;
    if !capabilities.is_empty() {
        writeln!(
            out,
            "        capabilities = \"{}\",",
            capabilities.join(", ")
        )
        .map_err(|_| SourceSkeletonError::Render)?;
    }
    if binding.is_some_and(|binding| binding.proposed) {
        writeln!(out, "        proposed = true,").map_err(|_| SourceSkeletonError::Render)?;
    }
    writeln!(out, "        factory = \"none\"").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "    )]").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "    pub struct {type_name}SourceMeta;")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "    // Contract references observed by the #1792 gate:"
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "    // SourceContract: {}",
        mode.sources.source_contract.id
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    match &mode.sources.runtime_binding {
        Some(binding) => writeln!(
            out,
            "    // SourceRuntimeBinding: {} subject {}",
            binding.id, binding.subject
        ),
        None => writeln!(out, "    // SourceRuntimeBinding: MISSING"),
    }
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "    // EventContract refs: {}",
        comma_list(&mode.event_contract_refs)
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "    // AdmissionPolicy refs: {}",
        comma_list(&mode.admission_policy_refs)
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "    // Acquisition: {}; operator enablement: {}",
        mode.acquisition_kind, mode.operator_enablement
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "    // Blocking requirements from the package-completeness row:"
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    for requirement in blocking_requirements(&mode.requirements) {
        writeln!(out, "    // - {}: {}", requirement.id, requirement.detail)
            .map_err(|_| SourceSkeletonError::Render)?;
    }
    if !mode.caveats.is_empty() {
        writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
        writeln!(out, "    // Caveats that must remain visible in review:")
            .map_err(|_| SourceSkeletonError::Render)?;
        for caveat in &mode.caveats {
            writeln!(out, "    // - {caveat}").map_err(|_| SourceSkeletonError::Render)?;
        }
    }
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(
        out,
        "    pub fn review_checklist() -> &'static [&'static str] {{"
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "        &[").map_err(|_| SourceSkeletonError::Render)?;
    for field in [
        "source_contract",
        "runtime_binding",
        "parser_or_producer",
        "event_contract_refs",
        "admission_policy_refs",
        "operator_controlled_disclosure_refs",
        "resource_budget",
        "occurrence_identity",
        "fixtures_and_tests",
        "coverage_and_debt_views",
        "operations",
        "catalog_projection",
    ] {
        writeln!(out, "            \"{field}\",").map_err(|_| SourceSkeletonError::Render)?;
    }
    writeln!(out, "        ]").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "    }}").map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "}}").map_err(|_| SourceSkeletonError::Render)?;

    Ok(out)
}

fn blocking_requirements(
    requirements: &[RequirementDiagnostic],
) -> impl Iterator<Item = &RequirementDiagnostic> {
    requirements
        .iter()
        .filter(|requirement| requirement.blocking)
}

fn comma_list(values: &[String]) -> String {
    if values.is_empty() {
        "MISSING".to_string()
    } else {
        values.join(", ")
    }
}

fn string_field<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

fn pascal_variant(value: &str) -> String {
    let mut out = String::new();
    let mut uppercase_next = true;
    for ch in value.chars() {
        if ch == '_' || ch == '-' {
            uppercase_next = true;
        } else if uppercase_next {
            out.push(ch.to_ascii_uppercase());
            uppercase_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

fn quoted(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"replace\"".to_string())
}

fn enum_path(type_name: &str, value: &serde_json::Value, fallback: &str) -> String {
    value.as_str().map_or_else(
        || fallback.to_string(),
        |variant| format!("{type_name}::{}", pascal_variant(variant)),
    )
}

fn privacy_tier_expr(value: &serde_json::Value) -> String {
    enum_path("PrivacyTier", value, "PrivacyTier::Sensitive")
}

fn privacy_context_expr(value: &serde_json::Value) -> String {
    enum_path("ProcessingContext", value, "ProcessingContext::Command")
}

fn resource_profile_expr(value: &serde_json::Value) -> String {
    enum_path("ResourceProfile", value, "ResourceProfile::BoundedFile")
}

fn runner_pack_expr(value: &serde_json::Value) -> String {
    enum_path("RunnerPack", value, "RunnerPack::Staged")
}

fn runtime_shape_expr(value: &serde_json::Value) -> String {
    enum_path("RuntimeShape", value, "RuntimeShape::OnDemand")
}

fn horizons_expr(value: &serde_json::Value) -> String {
    let Some(values) = value.as_array() else {
        return "Horizon::Continuous".to_string();
    };
    let rendered = values
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(|variant| format!("Horizon::{}", pascal_variant(variant)))
        .collect::<Vec<_>>();
    if rendered.is_empty() {
        "Horizon::Continuous".to_string()
    } else {
        rendered.join(", ")
    }
}

fn retention_expr(value: &serde_json::Value) -> String {
    match string_field(value, "kind") {
        Some("forever") => "RetentionPolicy::Forever".to_string(),
        Some("days") => format!(
            "RetentionPolicy::Days {{ days: {} }}",
            value
                .get("days")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        ),
        Some("tiered") => format!(
            "RetentionPolicy::Tiered {{ hot_days: {}, warm_days: {} }}",
            value
                .get("hot_days")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
            value
                .get("warm_days")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        ),
        _ => "RetentionPolicy::Forever".to_string(),
    }
}

fn occurrence_identity_expr(value: &serde_json::Value) -> String {
    match string_field(value, "kind") {
        Some("uuid5_from") => format!(
            "OccurrenceIdentity::Uuid5From({})",
            quoted(
                string_field(value, "key").unwrap_or("replace-with-object-level-occurrence-key")
            )
        ),
        Some("natural") => "OccurrenceIdentity::Natural".to_string(),
        Some("anchor") => "OccurrenceIdentity::Anchor".to_string(),
        _ => "OccurrenceIdentity::Uuid5From(\"replace-with-object-level-occurrence-key\")"
            .to_string(),
    }
}

fn checkpoint_family_expr(value: &serde_json::Value) -> String {
    match string_field(value, "kind") {
        Some("append_stream") => "CheckpointFamily::AppendStream".to_string(),
        Some("mutable_snapshot") => format!(
            "CheckpointFamily::MutableSnapshot {{ backing_store_kind: {}, occurrence_anchor: {} }}",
            quoted(string_field(value, "backing_store_kind").unwrap_or("replace-backing-store")),
            quoted(string_field(value, "occurrence_anchor").unwrap_or("replace-occurrence-anchor")),
        ),
        Some("journal") => "CheckpointFamily::Journal".to_string(),
        Some("polling") => "CheckpointFamily::Polling".to_string(),
        Some("live_observation") => "CheckpointFamily::LiveObservation".to_string(),
        _ => "CheckpointFamily::AppendStream".to_string(),
    }
}

fn access_scope_expr(value: &serde_json::Value) -> String {
    match string_field(value, "scope") {
        Some("internal") => "AccessScope::Internal".to_string(),
        Some("staged_export") => "AccessScope::StagedExport".to_string(),
        Some("target_home") => format!(
            "AccessScope::TargetHome {{ path: {} }}",
            quoted(string_field(value, "path").unwrap_or("replace-path"))
        ),
        Some("target_data") => format!(
            "AccessScope::TargetData {{ path: {} }}",
            quoted(string_field(value, "path").unwrap_or("replace-path"))
        ),
        Some("runtime_bridge") => format!(
            "AccessScope::RuntimeBridge {{ surface: {} }}",
            quoted(string_field(value, "surface").unwrap_or("replace-surface"))
        ),
        Some("systemd_journal") => "AccessScope::SystemdJournal".to_string(),
        Some("kernel_uevents") => "AccessScope::KernelUevents".to_string(),
        Some("session_bus") => "AccessScope::SessionBus".to_string(),
        Some("system_bus") => "AccessScope::SystemBus".to_string(),
        Some("configured_roots") => "AccessScope::ConfiguredRoots".to_string(),
        Some("library_root") => "AccessScope::LibraryRoot".to_string(),
        _ => "AccessScope::Internal".to_string(),
    }
}

fn rust_ident(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn rust_type_name(value: &str) -> String {
    let mut out = String::new();
    let mut uppercase_next = true;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            if uppercase_next {
                out.push(ch.to_ascii_uppercase());
                uppercase_next = false;
            } else {
                out.push(ch);
            }
        } else {
            uppercase_next = true;
        }
    }
    if out.is_empty() {
        "Generated".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skeleton_uses_package_completeness_contract_fields() {
        let rendered =
            render_source_skeleton("terminal.atuin-history", "terminal.atuin-history").unwrap();

        assert!(rendered.contains("SourceMeta"));
        assert!(rendered.contains("EventContract refs:"));
        assert!(rendered.contains("AdmissionPolicy refs:"));
        assert!(rendered.contains("resource_budget"));
        assert!(rendered.contains("coverage_and_debt_views"));
        assert!(rendered.contains("compile_error!"));
        assert!(!rendered.contains("pilot"));
        assert!(rendered.contains("event_source ="));
        assert!(rendered.contains("privacy_tier = PrivacyTier::"));
        assert!(rendered.contains("horizons(Horizon::"));
        assert!(rendered.contains("retention = RetentionPolicy::"));
        assert!(rendered.contains("occurrence_identity = OccurrenceIdentity::"));
        assert!(rendered.contains("privacy_context = ProcessingContext::"));
        assert!(rendered.contains("runtime_shape = RuntimeShape::"));
    }

    #[test]
    fn skeleton_renders_runtime_binding_metadata_when_available() {
        let rendered =
            render_source_skeleton("terminal.kitty-osc-live", "terminal.kitty-osc-live").unwrap();

        assert!(
            rendered
                .contains("access_scope = AccessScope::RuntimeBridge { surface: \"kitty_osc\" }")
        );
        assert!(rendered.contains("resource_profile = ResourceProfile::LiveWatcher"));
        assert!(rendered.contains("runner_pack = RunnerPack::Live"));
        assert!(rendered.contains("checkpoint_family = CheckpointFamily::LiveObservation"));
        assert!(rendered.contains("runtime_shape = RuntimeShape::Continuous"));
        assert!(rendered.contains("capabilities = \"coverage:source-coverage, debt:unified-debt-view, operation:terminal.activity.check"));
        assert!(rendered.contains("operation:terminal.activity.inspect"));
    }

    #[test]
    fn missing_package_reports_requested_id() {
        let err = render_source_skeleton("missing.package", "local").unwrap_err();
        assert_eq!(
            err.to_string(),
            "package `missing.package` not found in package completeness report"
        );
    }
}
