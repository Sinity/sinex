use super::*;
use xtask::sandbox::prelude::*;

async fn response(line: &str) -> CompletionResponseView {
    let cmd = CompletionEndpointCommand {
        line: line.to_string(),
        cursor: line.len(),
    };
    cmd.complete(None).await
}

#[sinex_test]
async fn source_completion_uses_inventory_without_gateway() -> TestResult<()> {
    let response = response("sinexctl query events where source = wm").await;
    let candidate = response
        .candidates
        .iter()
        .find(|candidate| candidate.value == "\"wm.hyprland\"")
        .expect("wm source should be available from static payload inventory");
    assert_eq!(candidate.insert, "\"wm.hyprland\"");
    assert_eq!(
        candidate.replace_start,
        "sinexctl query events where source = ".len()
    );
    assert_eq!(
        candidate.replace_end,
        "sinexctl query events where source = wm".len()
    );
    assert_eq!(candidate.source.as_deref(), Some("wm.hyprland"));
    assert!(
        candidate.stale,
        "static inventory candidates are stale fallback data"
    );
    assert_eq!(candidate.danger, "none");
    assert!(
        candidate
            .preview
            .as_deref()
            .is_some_and(|preview| preview.contains("source = \"wm.hyprland\""))
    );
    Ok(())
}

#[sinex_test]
async fn event_type_completion_is_narrowed_by_source() -> TestResult<()> {
    let response =
        response("sinexctl query events where source = \"wm.hyprland\" and event_type = win").await;
    assert!(
        response
            .candidates
            .iter()
            .any(|candidate| candidate.value == "\"window.focused\"")
    );
    assert!(
        response
            .candidates
            .iter()
            .all(|candidate| candidate.value != "\"file.created\""),
        "source-filtered type completion must not include unrelated event types"
    );
    Ok(())
}

#[sinex_test]
async fn grammar_completion_suggests_canonical_root_groups() -> TestResult<()> {
    let response = response("sinexctl ").await;
    let values: BTreeSet<&str> = response
        .candidates
        .iter()
        .map(|candidate| candidate.value.as_str())
        .collect();
    for root in [
        "query", "events", "sources", "show", "runtime", "metrics", "ops", "privacy", "tasks",
        "record", "docs", "semantic", "tui", "config",
    ] {
        assert!(
            values.contains(root),
            "canonical root `{root}` must be suggested: {response:#?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn query_completion_is_descriptor_driven() -> TestResult<()> {
    let unit_response = response("sinexctl query source-").await;
    assert!(
        unit_response
            .candidates
            .iter()
            .any(|candidate| candidate.value == "source-drivers"),
        "query unit completions must come from descriptor registry: {unit_response:#?}"
    );

    let field_response = response("sinexctl query events where s").await;
    assert!(
        field_response
            .candidates
            .iter()
            .any(|candidate| candidate.value == "source"),
        "query field completions must come from descriptor registry: {field_response:#?}"
    );

    let sort_response = response("sinexctl query operations sort ").await;
    assert!(
        sort_response
            .candidates
            .iter()
            .any(|candidate| candidate.value == "operation_id"),
        "query sort completions must come from descriptor registry: {sort_response:#?}"
    );

    let direction_response = response("sinexctl query operations sort status ").await;
    assert!(
        direction_response
            .candidates
            .iter()
            .any(|candidate| candidate.value == "desc"),
        "query sort direction completions must be exposed: {direction_response:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn query_completion_exposes_event_cursor_clauses() -> TestResult<()> {
    let response = response("sinexctl query events ").await;
    let values = response
        .candidates
        .iter()
        .map(|candidate| candidate.value.as_str())
        .collect::<BTreeSet<_>>();

    assert!(values.contains("where"));
    assert!(values.contains("since"));
    assert!(values.contains("limit"));
    assert!(values.contains("after"));
    assert!(values.contains("before"));
    assert!(
        response
            .candidates
            .iter()
            .filter(|candidate| candidate.value == "after" || candidate.value == "before")
            .all(|candidate| candidate.kind == "query-cursor"),
        "event cursor clauses should be explicitly marked: {response:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn query_completion_does_not_offer_event_cursors_for_other_units() -> TestResult<()> {
    let response = response("sinexctl query operations ").await;
    let values = response
        .candidates
        .iter()
        .map(|candidate| candidate.value.as_str())
        .collect::<BTreeSet<_>>();

    assert!(values.contains("where"));
    assert!(values.contains("sort"));
    assert!(values.contains("limit"));
    assert!(!values.contains("since"));
    assert!(!values.contains("after"));
    assert!(!values.contains("before"));
    Ok(())
}

#[sinex_test]
async fn grammar_completion_includes_record_root() -> TestResult<()> {
    let response = response("sinexctl rec").await;
    assert!(
        response
            .candidates
            .iter()
            .any(|candidate| candidate.value == "record"),
        "canonical record root must be suggested: {response:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn ops_dlq_completion_marks_destructive_actions() -> TestResult<()> {
    let response = response("sinexctl ops dlq p").await;
    let purge = response
        .candidates
        .iter()
        .find(|candidate| candidate.value == "purge")
        .expect("DLQ purge should be suggested in ops dlq context");
    assert_eq!(purge.kind, "subcommand");
    assert_eq!(purge.danger, "destructive");
    assert_eq!(purge.replace_start, "sinexctl ops dlq ".len());
    assert_eq!(purge.replace_end, "sinexctl ops dlq p".len());
    assert_eq!(purge.preview.as_deref(), Some("sinexctl ops dlq purge"));
    Ok(())
}

#[sinex_test]
async fn ops_dlq_completion_suggests_cleanup_plan_as_read_only() -> TestResult<()> {
    let response = response("sinexctl ops dlq c").await;
    let cleanup_plan = response
        .candidates
        .iter()
        .find(|candidate| candidate.value == "cleanup-plan")
        .expect("DLQ cleanup-plan should be suggested in ops dlq context");
    assert_eq!(cleanup_plan.kind, "subcommand");
    assert_eq!(cleanup_plan.danger, "none");
    assert_eq!(
        cleanup_plan.preview.as_deref(),
        Some("sinexctl ops dlq cleanup-plan")
    );
    Ok(())
}

#[sinex_test]
async fn ops_dlq_completion_suggests_all_retained_for_cleanup_plan() -> TestResult<()> {
    let response = response("sinexctl ops dlq cleanup-plan --").await;
    let all_retained = response
        .candidates
        .iter()
        .find(|candidate| candidate.value == "--all-retained")
        .expect("--all-retained should be suggested for cleanup-plan");
    assert_eq!(all_retained.kind, "option");
    assert_eq!(all_retained.danger, "none");
    assert_eq!(
        all_retained.preview.as_deref(),
        Some("sinexctl ops dlq cleanup-plan --all-retained")
    );
    Ok(())
}

#[sinex_test]
async fn ops_dlq_completion_suggests_all_retained_for_triage() -> TestResult<()> {
    let response = response("sinexctl ops dlq triage --").await;
    let all_retained = response
        .candidates
        .iter()
        .find(|candidate| candidate.value == "--all-retained")
        .expect("--all-retained should be suggested for triage");
    assert_eq!(all_retained.kind, "option");
    assert_eq!(all_retained.danger, "none");
    Ok(())
}

#[sinex_test]
async fn ops_dlq_completion_marks_purge_options_destructive() -> TestResult<()> {
    let response = response("sinexctl ops dlq purge --").await;
    let confirm = response
        .candidates
        .iter()
        .find(|candidate| candidate.value == "--confirm")
        .expect("--confirm should be suggested for purge");
    assert_eq!(confirm.kind, "option");
    assert_eq!(confirm.danger, "destructive");
    Ok(())
}
