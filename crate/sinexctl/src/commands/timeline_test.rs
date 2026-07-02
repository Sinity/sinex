use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn timeline_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
    let timeline = EventCardListView::from_query_events(&[]);
    let output = render_timeline_machine_output(
        &timeline,
        25,
        Some("shell.atuin"),
        None,
        OutputFormat::Json,
    )?
    .expect("json should render");
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(value["source_surface"], "sinexctl.events.timeline");
    assert_eq!(value["payload"]["count"], 0);
    assert_eq!(value["query_echo"]["limit"], 25);
    assert_eq!(value["query_echo"]["source"], "shell.atuin");
    Ok(())
}

#[sinex_test]
async fn timeline_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
    let timeline = EventCardListView::from_query_events(&[]);
    let result =
        render_timeline_machine_output(&timeline, 100, None, None, OutputFormat::Ndjson);
    assert!(result.is_err(), "timeline is a finite view");
    Ok(())
}
