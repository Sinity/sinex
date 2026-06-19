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
    writeln!(
        out,
        "use sinex_primitives::source_contracts::{{RunnerPack, SourceContract, SourceRuntimeBinding}};"
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
    writeln!(
        out,
        "    #[source_meta(id = \"{}\", namespace = \"{}\", mode = \"{}\")]",
        mode.package_id, mode.package_id, mode.mode_id
    )
    .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "    pub struct {type_name}SourceMeta;")
        .map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out).map_err(|_| SourceSkeletonError::Render)?;
    writeln!(out, "    // Contract references observed by the #1792 gate:")
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
    writeln!(out, "    // Blocking requirements from the package-completeness row:")
    .map_err(|_| SourceSkeletonError::Render)?;
    for requirement in blocking_requirements(&mode.requirements) {
        writeln!(
            out,
            "    // - {}: {}",
            requirement.id, requirement.detail
        )
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
    requirements.iter().filter(|requirement| requirement.blocking)
}

fn comma_list(values: &[String]) -> String {
    if values.is_empty() {
        "MISSING".to_string()
    } else {
        values.join(", ")
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
