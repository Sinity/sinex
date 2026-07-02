use super::*;
use xtask::sandbox::prelude::sinex_test;

fn remove_authoring_blocker(rendered: &str) -> String {
    rendered
        .lines()
        .filter(|line| !line.trim_start().starts_with("compile_error!("))
        .collect::<Vec<_>>()
        .join("\n")
}

#[sinex_test]
async fn skeletons_parse_as_rust_after_authoring_blocker_is_removed()
-> xtask::sandbox::TestResult<()> {
    for (package_id, mode_id) in [
        ("terminal.atuin-history", "terminal.atuin-history"),
        ("terminal.kitty-osc-live", "terminal.kitty-osc-live"),
        ("email.mailbox", "email.mailbox.gmail-api-scheduled-sync"),
        ("weechat.message", "weechat.message"),
    ] {
        let rendered = render_source_skeleton(package_id, mode_id).unwrap();
        let editable_rust = remove_authoring_blocker(&rendered);
        syn::parse_file(&editable_rust).unwrap_or_else(|error| {
            panic!("generated skeleton for {package_id}/{mode_id} must parse as Rust: {error}")
        });
    }
    Ok(())
}

#[sinex_test]
async fn skeleton_uses_package_completeness_contract_fields() -> xtask::sandbox::TestResult<()> {
    let rendered =
        render_source_skeleton("terminal.atuin-history", "terminal.atuin-history").unwrap();

    assert!(rendered.contains("SourceMeta"));
    assert!(rendered.contains("EventContract refs:"));
    assert!(rendered.contains("AdmissionPolicy refs:"));
    assert!(rendered.contains("resource_budget"));
    assert!(rendered.contains("coverage_and_debt_views"));
    assert!(rendered.contains("compile_error!"));
    assert!(rendered.contains("event_source ="));
    assert!(rendered.contains("privacy_tier = PrivacyTier::"));
    assert!(rendered.contains("horizons(Horizon::"));
    assert!(rendered.contains("retention = RetentionPolicy::"));
    assert!(rendered.contains("occurrence_identity = OccurrenceIdentity::"));
    assert!(rendered.contains("privacy_context = ProcessingContext::"));
    assert!(rendered.contains("runtime_shape = RuntimeShape::"));
    assert!(rendered.contains("factory = \"adapter_parser\""));
    Ok(())
}

#[sinex_test]
async fn skeleton_renders_runtime_binding_metadata_when_available() -> xtask::sandbox::TestResult<()>
{
    let rendered =
        render_source_skeleton("terminal.kitty-osc-live", "terminal.kitty-osc-live").unwrap();

    assert!(
        rendered.contains("access_scope = AccessScope::RuntimeBridge { surface: \"kitty_osc\" }")
    );
    assert!(rendered.contains("resource_profile = ResourceProfile::LiveWatcher"));
    assert!(rendered.contains("runner_pack = RunnerPack::Live"));
    assert!(rendered.contains("checkpoint_family = CheckpointFamily::LiveObservation"));
    assert!(rendered.contains("runtime_shape = RuntimeShape::Continuous"));
    assert!(rendered.contains("capabilities = \"coverage:source-coverage, debt:unified-debt-view, operation:terminal.activity.check"));
    assert!(rendered.contains("operation:terminal.activity.inspect"));
    assert!(rendered.contains("factory = \"adapter_parser\""));
    Ok(())
}

#[sinex_test]
async fn skeleton_renders_package_mode_binding_metadata() -> xtask::sandbox::TestResult<()> {
    let rendered =
        render_source_skeleton("email.mailbox", "email.mailbox.gmail-api-scheduled-sync").unwrap();

    assert!(rendered.contains("subject = \"source:email.mailbox.gmail-api-scheduled-sync\""));
    assert!(rendered.contains("event_type = \"email.sync_cursor.observed\""));
    assert!(rendered.contains("implementation = \"gmail-api-scheduled-sync\""));
    assert!(rendered.contains("adapter = \"GmailApiCursorAdapter\""));
    assert!(rendered.contains("resource_profile = ResourceProfile::BoundedStream"));
    assert!(rendered.contains("runner_pack = RunnerPack::SinexdSource"));
    assert!(rendered.contains("checkpoint_family = CheckpointFamily::Journal"));
    assert!(rendered.contains("runtime_shape = RuntimeShape::Scheduled"));
    assert!(rendered.contains("operation:email.mailbox.authorize"));
    assert!(rendered.contains("operation:email.mailbox.sync"));
    assert!(rendered.contains("operation:email.mailbox.replay"));
    assert!(!rendered.contains("ReplaceAdapter"));
    assert!(!rendered.contains("binding("));
    Ok(())
}

#[sinex_test]
async fn skeleton_preserves_parser_only_manual_factory_modes() -> xtask::sandbox::TestResult<()> {
    let rendered = render_source_skeleton("weechat.message", "weechat.message").unwrap();

    assert!(rendered.contains("factory = \"parser\""));
    assert!(rendered.contains("SourceRuntimeBinding:"));
    Ok(())
}

#[sinex_test]
async fn missing_package_reports_requested_id() -> xtask::sandbox::TestResult<()> {
    let err = render_source_skeleton("missing.package", "local").unwrap_err();
    assert_eq!(
        err.to_string(),
        "package `missing.package` not found in package completeness report"
    );
    Ok(())
}
