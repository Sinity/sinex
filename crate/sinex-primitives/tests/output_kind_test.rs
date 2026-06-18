use std::collections::HashSet;
use std::path::PathBuf;

use sinex_primitives::{
    OUTPUT_KIND_DECLARATIONS, OutputKind, declared_output_kind,
    task_domain::TASK_REDUCER_SPEC,
};
use xtask::sandbox::prelude::*;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate/sinex-primitives has a repository root")
        .to_path_buf()
}

#[sinex_test]
async fn registry_covers_each_output_kind() -> TestResult<()> {
    let kinds: HashSet<OutputKind> = OUTPUT_KIND_DECLARATIONS
        .iter()
        .map(|declaration| declaration.kind)
        .collect();

    assert!(kinds.contains(&OutputKind::CanonicalEvent));
    assert!(kinds.contains(&OutputKind::ProjectionRow));
    assert!(kinds.contains(&OutputKind::Artifact));
    assert!(kinds.contains(&OutputKind::Proposal));
    assert!(kinds.contains(&OutputKind::Judgment));
    assert!(kinds.contains(&OutputKind::OperationRecord));
    assert!(kinds.contains(&OutputKind::EphemeralView));
    Ok(())
}

#[sinex_test]
async fn registry_output_ids_are_unique() -> TestResult<()> {
    let mut seen = HashSet::new();
    for declaration in OUTPUT_KIND_DECLARATIONS {
        assert!(
            seen.insert(declaration.output_id),
            "duplicate output-kind declaration for {}",
            declaration.output_id
        );
    }
    Ok(())
}

#[sinex_test]
async fn derived_outputs_are_not_classified_as_canonical_events() -> TestResult<()> {
    for output_id in [
        "source.coverage",
        "domain.current_objects",
        "artifacts.source_catalog",
        "curation.proposal",
        "curation.judgment",
        "operations_log",
        "relations.evidence_window",
        "views.view_envelope",
    ] {
        let kind = declared_output_kind(output_id)
            .unwrap_or_else(|| panic!("missing output-kind declaration for {output_id}"));
        assert!(
            !kind.is_canonical_event(),
            "{output_id} must not be routed through core.events as canonical truth"
        );
    }
    Ok(())
}

#[sinex_test]
async fn projection_specs_declare_projection_row_outputs() -> TestResult<()> {
    assert_eq!(TASK_REDUCER_SPEC.output_kind, OutputKind::ProjectionRow);
    assert_eq!(
        declared_output_kind("domain.current_objects"),
        Some(OutputKind::ProjectionRow)
    );
    Ok(())
}

#[sinex_test]
async fn repo_templates_require_output_kind_classification() -> TestResult<()> {
    let root = repo_root();
    let pr_template = std::fs::read_to_string(root.join(".github/pull_request_template.md"))?;
    let issue_template =
        std::fs::read_to_string(root.join(".github/ISSUE_TEMPLATE/01-feature-or-change.yml"))?;

    assert!(pr_template.contains("## Output Kind"));
    assert!(pr_template.contains("sinex_primitives::output_kind"));
    assert!(pr_template.contains("A new canonical event must explain why"));
    assert!(pr_template.contains("New output-producing boundaries declare or reference"));

    assert!(issue_template.contains("label: Output kind"));
    assert!(issue_template.contains("CanonicalEvent, ProjectionRow, Artifact"));
    assert!(issue_template.contains("explain canonical-event choices"));
    Ok(())
}
