use super::*;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    CheckpointFamily, ResourceProfile, RuntimeShape, SourceBuildImpact, SubjectRef,
};
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn filtered_report_recomputes_package_summary() -> xtask::sandbox::TestResult<()> {
    let rendered =
        render_filtered_package_completeness_report(Some("terminal.kitty-osc-live"), None)
            .unwrap();
    let report: Value = serde_json::from_str(&rendered).unwrap();

    assert_eq!(report["summary"]["package_count"], 1);
    let packages = report["packages"].as_object().unwrap();
    assert_eq!(packages.len(), 1);
    let package = packages.get("terminal.kitty-osc-live").unwrap();
    let mode_count = package["modes"].as_object().unwrap().len();
    assert_eq!(report["summary"]["mode_count"], mode_count);
    Ok(())
}

#[sinex_test]
async fn filtered_report_recomputes_package_mode_summary() -> xtask::sandbox::TestResult<()> {
    let rendered = render_filtered_package_completeness_report(
        Some("terminal.kitty-osc-live"),
        Some("terminal.kitty-osc-live"),
    )
    .unwrap();
    let report: Value = serde_json::from_str(&rendered).unwrap();

    assert_eq!(report["summary"]["package_count"], 1);
    assert_eq!(report["summary"]["mode_count"], 1);
    let package = &report["packages"]["terminal.kitty-osc-live"];
    let modes = package["modes"].as_object().unwrap();
    assert_eq!(modes.len(), 1);
    assert!(modes.contains_key("terminal.kitty-osc-live"));
    Ok(())
}

#[sinex_test]
async fn mode_filter_requires_package_id() -> xtask::sandbox::TestResult<()> {
    let err =
        render_filtered_package_completeness_report(None, Some("terminal.kitty-osc-live"))
            .unwrap_err();

    assert!(matches!(
        err,
        PackageCompletenessFilterError::ModeRequiresPackage
    ));
    Ok(())
}

#[sinex_test]
async fn capability_report_refs_are_filtered_through_typed_parser()
-> xtask::sandbox::TestResult<()> {
    static CAPABILITIES: &[&str] = &[
        "coverage:source-coverage",
        "debt:unified-debt-view",
        "operation:fixture.source.check",
        "operation:",
        "package:fixture.source",
    ];
    let binding = SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:fixture.source"),
        "fixture.source",
        "fixture",
    )
    .implementation("test")
    .adapter("static")
    .output_event_type("fixture.event")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EmbeddedEmitter)
    .capabilities(CAPABILITIES)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .build_impact(SourceBuildImpact::ZERO)
    .build();

    assert_eq!(
        capability_refs(
            Some(&binding),
            &[SourceCapabilityKind::Coverage, SourceCapabilityKind::Debt]
        ),
        vec![
            "coverage:source-coverage".to_string(),
            "debt:unified-debt-view".to_string()
        ]
    );
    assert_eq!(
        capability_refs(Some(&binding), &[SourceCapabilityKind::Operation]),
        vec!["operation:fixture.source.check".to_string()]
    );
    Ok(())
}
