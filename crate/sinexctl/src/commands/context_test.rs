use super::*;
use sinex_primitives::testing::event_fixture;
use sinex_primitives::views::{
    CONTEXT_SUMMARY_SCHEMA_VERSION, CaveatView, EVENT_CARD_LIST_SCHEMA_VERSION,
    VIEW_ENVELOPE_SCHEMA_VERSION,
};
use xtask::sandbox::prelude::sinex_test;

fn context_event(source: &'static str, event_type: &'static str) -> EventCardView {
    EventCardView::from_query_event(&QueryResultEvent {
        event: event_fixture(
            sinex_primitives::EventSource::from_static(source),
            sinex_primitives::EventType::from_static(event_type),
            json!({ "message": "context fixture" }),
        ),
        relevance_score: None,
        snippet: Some("context fixture".to_string()),
    })
}

fn context_event_with_ref(
    source: &'static str,
    event_type: &'static str,
    ref_id: impl Into<String>,
) -> EventCardView {
    let mut card = context_event(source, event_type);
    card.ref_.id = ref_id.into();
    card
}

#[sinex_test]
async fn context_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
    let mut shell_card = context_event("shell.atuin", "command.executed");
    shell_card.caveats.push(CaveatView {
        id: "policy.disclosure_applied".to_string(),
        message: "payload field redacted by fixture policy".to_string(),
        ref_: None,
    });
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 2,
        cards: vec![shell_card, context_event("wm.hyprland", "window.focused")],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let window = build_context_window("2h", None, Timestamp::now())?;
    let output =
        render_context_machine_output(&event_cards, &sources, &window, OutputFormat::Json)?
            .ok_or_else(|| color_eyre::eyre::eyre!("json output expected"))?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.context");
    assert_eq!(value["query_echo"]["since"], "2h");
    assert_eq!(
        value["payload"]["schema_version"],
        CONTEXT_SUMMARY_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["since"], "2h");
    assert_eq!(value["payload"]["total_events"], 2);
    assert_eq!(value["payload"]["source_count"], 2);
    assert_eq!(
        value["payload"]["sources"][0]["latest_event"]["summary"],
        "context fixture"
    );
    let source_views = value["payload"]["sources"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("context sources must be an array"))?;
    assert!(
        source_views
            .iter()
            .filter_map(|source| source["latest_event"]["caveats"].as_array())
            .flatten()
            .any(|caveat| caveat["id"] == "policy.disclosure_applied"),
        "context cards must preserve disclosure caveats: {source_views:?}"
    );
    Ok(())
}

#[sinex_test]
async fn context_window_accepts_absolute_since_and_until() -> xtask::sandbox::TestResult<()> {
    let now = Timestamp::parse_rfc3339("2026-07-02T20:00:00Z")?;
    let window = build_context_window(
        "2026-07-02T18:00:00Z",
        Some("2026-07-02T19:00:00Z"),
        now,
    )?;

    assert_eq!(
        window.time_range.start(),
        Some(Timestamp::parse_rfc3339("2026-07-02T18:00:00Z")?)
    );
    assert_eq!(
        window.time_range.end(),
        Some(Timestamp::parse_rfc3339("2026-07-02T19:00:00Z")?)
    );
    assert_eq!(window.query_echo()["since"], "2026-07-02T18:00:00Z");
    assert_eq!(window.query_echo()["until"], "2026-07-02T19:00:00Z");
    Ok(())
}

#[sinex_test]
async fn context_window_measures_duration_since_from_until_bound()
-> xtask::sandbox::TestResult<()> {
    let now = Timestamp::parse_rfc3339("2026-07-02T20:00:00Z")?;
    let window = build_context_window("30m", Some("2026-07-02T19:00:00Z"), now)?;

    assert_eq!(
        window.time_range.start(),
        Some(Timestamp::parse_rfc3339("2026-07-02T18:30:00Z")?)
    );
    assert_eq!(
        window.time_range.end(),
        Some(Timestamp::parse_rfc3339("2026-07-02T19:00:00Z")?)
    );
    Ok(())
}

#[sinex_test]
async fn context_diversity_merge_adds_missing_sources_once() -> xtask::sandbox::TestResult<()> {
    let mut event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 1,
        cards: vec![context_event("sinexd.event_engine", "batch.persisted")],
        next_cursor: None,
        total_estimate: None,
    };

    merge_context_diversity_cards(
        &mut event_cards,
        vec![
            context_event("shell.atuin", "command.executed"),
            context_event("shell.atuin", "command.executed"),
        ],
    );

    let sources = grouped_context_sources(&event_cards.cards);
    assert_eq!(event_cards.count, 2);
    assert_eq!(sources.len(), 2);
    assert!(sources
        .iter()
        .any(|(source, _)| source.as_str() == "shell.atuin"));
    assert_eq!(
        sources
            .iter()
            .filter(|(source, _)| source.as_str() == "shell.atuin")
            .count(),
        1
    );
    Ok(())
}

#[sinex_test]
async fn context_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 1,
        cards: vec![context_event("shell.atuin", "command.executed")],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let window = build_context_window("2h", None, Timestamp::now())?;
    let result =
        render_context_machine_output(&event_cards, &sources, &window, OutputFormat::Ndjson);
    assert!(result.is_err(), "context must remain a finite view");
    Ok(())
}

#[sinex_test]
async fn desktop_context_json_uses_typed_view_with_missing_inputs()
-> xtask::sandbox::TestResult<()> {
    let mut terminal_card = context_event("shell.atuin", "command.executed");
    terminal_card.caveats.push(CaveatView {
        id: "policy.disclosure_applied".to_string(),
        message: "terminal command hidden by fixture disclosure policy".to_string(),
        ref_: None,
    });
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 2,
        cards: vec![
            context_event("wm.hyprland", "window.focused"),
            terminal_card,
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        false,
        false,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.events.context.desktop");
    assert_eq!(value["payload"]["output_kind"], "current_view");
    assert_eq!(
        value["payload"]["derivation_ref"],
        sinex_primitives::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID
    );

    let inputs = value["payload"]["inputs"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("desktop inputs must be an array"))?;
    assert!(
        inputs
            .iter()
            .any(|input| input["family"] == "desktop" && input["state"] == "included")
    );
    assert!(
        inputs
            .iter()
            .any(|input| input["family"] == "terminal" && input["state"] == "redacted")
    );
    assert!(
        inputs
            .iter()
            .any(|input| input["family"] == "browser" && input["state"] == "missing")
    );
    assert!(
        inputs
            .iter()
            .any(|input| input["family"] == "notification" && input["state"] == "missing")
    );
    assert!(
        inputs.iter().any(
            |input| input["actions"].as_array().is_some_and(|actions| actions
                .iter()
                .any(|action| action["id"] == "sources.browser.check"))
        ),
        "missing browser evidence should surface an operator action"
    );
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "context.inputs_missing")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_context_classifies_activitywatch_browser_events()
-> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 2,
        cards: vec![
            context_event("activitywatch", "browser.tab.active"),
            context_event("wm.hyprland", "workspace.switched"),
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        false,
        false,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;
    let inputs = value["payload"]["inputs"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("desktop inputs must be an array"))?;

    assert!(
        inputs
            .iter()
            .any(|input| input["family"] == "browser" && input["state"] == "included"),
        "ActivityWatch browser observations should satisfy the browser input family"
    );
    assert!(
        value["payload"]["active_window_ref"].is_null(),
        "workspace events are desktop evidence but not active-window evidence"
    );
    Ok(())
}

#[sinex_test]
async fn desktop_context_candidates_are_evidence_backed_view_output()
-> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 3,
        cards: vec![
            context_event_with_ref("wm.hyprland", "window.focused", "event:desktop"),
            context_event_with_ref("shell.atuin", "command.executed", "event:terminal"),
            context_event_with_ref("activitywatch", "browser.tab.active", "event:browser"),
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        false,
        false,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    let candidates = value["payload"]["candidates"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("desktop candidates must be an array"))?;
    assert!(
        candidates.iter().any(|candidate| candidate["label"]
            .as_str()
            .is_some_and(|label| label.starts_with("active window:"))
            && candidate["evidence_refs"]
                .as_array()
                .is_some_and(|refs| refs.len() == 1)
            && candidate["proposal_ref"].is_null()),
        "active-window candidate should be evidence-backed view output: {candidates:?}"
    );
    assert!(
        candidates.iter().any(|candidate| candidate["label"]
            == "current activity from 3 evidence refs"
            && candidate["evidence_refs"]
                .as_array()
                .is_some_and(|refs| refs.len() == 3)
            && candidate["proposal_ref"].is_null()),
        "multi-signal activity candidate should cite each evidence ref without claiming authority: {candidates:?}"
    );
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "context.candidates_ranked_view")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_context_table_shows_candidate_evidence_counts()
-> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 2,
        cards: vec![
            context_event("wm.hyprland", "window.focused"),
            context_event("shell.atuin", "command.executed"),
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Table,
        false,
        false,
        false,
        false,
    )?;

    assert!(output.contains("candidates"));
    assert!(output.contains("active window: context fixture (1 refs)"));
    assert!(output.contains("current activity from 2 evidence refs (2 refs)"));
    Ok(())
}

#[sinex_test]
async fn desktop_context_explain_returns_evidence_window() -> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 3,
        cards: vec![
            context_event_with_ref("wm.hyprland", "window.focused", "event:desktop"),
            context_event_with_ref("shell.atuin", "command.executed", "event:terminal"),
            context_event_with_ref("activitywatch", "browser.tab.active", "event:browser"),
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        true,
        false,
        false,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        value["source_surface"],
        "sinexctl.events.context.desktop.explain"
    );
    assert_eq!(
        value["query_echo"]["mode"],
        "desktop_context_evidence_window"
    );
    assert_eq!(value["payload"]["query"]["relation"], "sequence");
    let support_refs = value["payload"]["support_refs"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("support_refs must be an array"))?;
    for expected_id in ["event:desktop", "event:terminal", "event:browser"] {
        assert!(
            support_refs
                .iter()
                .any(|support| support["object"]["id"] == expected_id),
            "explain output should cite {expected_id}: {support_refs:?}"
        );
    }
    assert!(
        value["payload"]["contradiction_refs"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "desktop context explain must not invent contradictions"
    );
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "context.candidates_ranked_view")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_context_explain_surfaces_missing_input_caveats()
-> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 1,
        cards: vec![context_event("wm.hyprland", "window.focused")],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        true,
        false,
        false,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert!(
        value["payload"]["expansion_trace"]["steps"]
            .as_array()
            .is_some_and(|steps| steps.iter().any(|step| step["kind"]
                == "coverage_gap_caveat"
                && step["detail"]
                    .as_str()
                    .is_some_and(|detail| detail.contains("browser input caveat"))))
    );
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "input.browser.missing")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_notification_pressure_counts_notification_evidence()
-> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 4,
        cards: vec![
            context_event_with_ref("desktop.notification", "notification.sent", "event:sent"),
            context_event_with_ref(
                "desktop.notification.action",
                "notification.action_invoked",
                "event:action",
            ),
            context_event_with_ref(
                "desktop.notification.closed",
                "notification.closed",
                "event:closed",
            ),
            context_event("wm.hyprland", "window.focused"),
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        true,
        false,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        value["source_surface"],
        "sinexctl.events.context.desktop.notification_pressure"
    );
    assert_eq!(
        value["payload"]["derivation_ref"],
        sinex_primitives::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID
    );
    assert_eq!(
        value["payload"]["output_kind"],
        "notification_pressure_projection"
    );
    assert_eq!(value["payload"]["sent_count"], 1);
    assert_eq!(value["payload"]["action_count"], 1);
    assert_eq!(value["payload"]["closed_count"], 1);
    assert_eq!(value["payload"]["total_notification_events"], 3);
    let refs = value["payload"]["evidence_refs"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("evidence_refs must be an array"))?;
    for expected_id in ["event:sent", "event:action", "event:closed"] {
        assert!(
            refs.iter().any(|ref_| ref_["id"] == expected_id),
            "notification-pressure view should cite {expected_id}: {refs:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn desktop_notification_pressure_bounds_evidence_refs() -> xtask::sandbox::TestResult<()>
{
    let mut cards = Vec::new();
    for index in 0..(MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS + 3) {
        cards.push(context_event_with_ref(
            "desktop.notification",
            "notification.sent",
            format!("event:notification:{index}"),
        ));
    }
    cards[0].caveats.push(CaveatView {
        id: "policy.disclosure_applied".to_string(),
        message: "notification body hidden by fixture disclosure policy".to_string(),
        ref_: None,
    });

    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: cards.len(),
        cards,
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        true,
        false,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        value["payload"]["total_notification_events"],
        MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS + 3
    );
    assert_eq!(
        value["payload"]["evidence_refs"].as_array().map(Vec::len),
        Some(MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS)
    );
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "notification_pressure.evidence_truncated")
            && caveats
                .iter()
                .any(|caveat| caveat["id"] == "policy.disclosure_applied")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_focus_sessions_project_recent_activity_evidence()
-> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 4,
        cards: vec![
            context_event_with_ref("wm.hyprland", "window.focused", "event:desktop"),
            context_event_with_ref("shell.atuin", "command.executed", "event:terminal"),
            context_event_with_ref("activitywatch", "browser.tab.active", "event:browser"),
            context_event_with_ref(
                "desktop.notification",
                "notification.sent",
                "event:notification",
            ),
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        false,
        true,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        value["source_surface"],
        "sinexctl.events.context.desktop.focus_sessions"
    );
    assert_eq!(
        value["payload"]["derivation_ref"],
        sinex_primitives::DESKTOP_FOCUS_SESSION_DERIVATION_ID
    );
    assert_eq!(value["payload"]["output_kind"], "focus_session_projection");
    assert_eq!(value["payload"]["session_count"], 1);
    let session = &value["payload"]["sessions"][0];
    assert_eq!(session["event_count"], 3);
    assert!(
        session["input_families"]
            .as_array()
            .is_some_and(|families| ["browser", "desktop", "terminal"]
                .iter()
                .all(|family| families.iter().any(|value| value == family))),
        "focus-session projection should classify desktop, terminal, and browser evidence: {session:?}"
    );
    let refs = session["evidence_refs"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("evidence_refs must be an array"))?;
    for expected_id in ["event:desktop", "event:terminal", "event:browser"] {
        assert!(
            refs.iter().any(|ref_| ref_["id"] == expected_id),
            "focus-session view should cite {expected_id}: {refs:?}"
        );
    }
    assert!(
        refs.iter().all(|ref_| ref_["id"] != "event:notification"),
        "notification pressure is a sibling projection, not focus-session evidence"
    );
    Ok(())
}

#[sinex_test]
async fn desktop_focus_sessions_bound_evidence_refs_and_preserve_caveats()
-> xtask::sandbox::TestResult<()> {
    let mut cards = Vec::new();
    for index in 0..(MAX_FOCUS_SESSION_EVIDENCE_REFS + 3) {
        cards.push(context_event_with_ref(
            "wm.hyprland",
            "window.focused",
            format!("event:window:{index}"),
        ));
    }
    cards[0].caveats.push(CaveatView {
        id: "policy.disclosure_applied".to_string(),
        message: "window title hidden by fixture disclosure policy".to_string(),
        ref_: None,
    });

    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: cards.len(),
        cards,
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        false,
        true,
        false,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    let session = &value["payload"]["sessions"][0];
    assert_eq!(session["event_count"], MAX_FOCUS_SESSION_EVIDENCE_REFS + 3);
    assert_eq!(
        session["evidence_refs"].as_array().map(Vec::len),
        Some(MAX_FOCUS_SESSION_EVIDENCE_REFS)
    );
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "focus_session.evidence_truncated")
            && caveats
                .iter()
                .any(|caveat| caveat["id"] == "policy.disclosure_applied")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_project_contexts_project_ranked_activity_evidence()
-> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 4,
        cards: vec![
            context_event_with_ref("wm.hyprland", "window.focused", "event:desktop"),
            context_event_with_ref("shell.atuin", "command.executed", "event:terminal"),
            context_event_with_ref("activitywatch", "browser.tab.active", "event:browser"),
            context_event_with_ref(
                "desktop.notification",
                "notification.sent",
                "event:notification",
            ),
        ],
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        false,
        false,
        true,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        value["source_surface"],
        "sinexctl.events.context.desktop.project_contexts"
    );
    assert_eq!(
        value["payload"]["derivation_ref"],
        sinex_primitives::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID
    );
    assert_eq!(
        value["payload"]["output_kind"],
        "project_context_projection"
    );
    assert_eq!(value["payload"]["row_count"], 1);
    let row = &value["payload"]["rows"][0];
    assert!(
        row["label"]
            .as_str()
            .is_some_and(|label| { label.starts_with("terminal activity:") })
    );
    assert!(
        row["input_families"].as_array().is_some_and(|families| [
            "browser", "desktop", "terminal"
        ]
        .iter()
        .all(|family| families.iter().any(|value| value == family))),
        "project-context projection should classify desktop, terminal, and browser evidence: {row:?}"
    );
    let refs = row["evidence_refs"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("evidence_refs must be an array"))?;
    for expected_id in ["event:desktop", "event:terminal", "event:browser"] {
        assert!(
            refs.iter().any(|ref_| ref_["id"] == expected_id),
            "project-context view should cite {expected_id}: {refs:?}"
        );
    }
    assert!(
        refs.iter().all(|ref_| ref_["id"] != "event:notification"),
        "notification pressure remains a sibling projection, not project-context evidence"
    );
    assert!(row["proposal_ref"].is_null());
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "project_context.ranked_view_only")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_project_contexts_bound_evidence_refs_and_preserve_caveats()
-> xtask::sandbox::TestResult<()> {
    let mut cards = Vec::new();
    for index in 0..(MAX_PROJECT_CONTEXT_EVIDENCE_REFS + 3) {
        cards.push(context_event_with_ref(
            "shell.atuin",
            "command.executed",
            format!("event:terminal:{index}"),
        ));
    }
    cards[0].caveats.push(CaveatView {
        id: "policy.disclosure_applied".to_string(),
        message: "terminal command hidden by fixture disclosure policy".to_string(),
        ref_: None,
    });

    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: cards.len(),
        cards,
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let output = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Json,
        false,
        false,
        false,
        true,
    )?;
    let value: serde_json::Value = serde_json::from_str(&output)?;
    let row = &value["payload"]["rows"][0];

    assert_eq!(
        row["evidence_refs"].as_array().map(Vec::len),
        Some(MAX_PROJECT_CONTEXT_EVIDENCE_REFS)
    );
    assert!(value["caveats"].as_array().is_some_and(|caveats| {
        caveats
            .iter()
            .any(|caveat| caveat["id"] == "project_context.evidence_truncated")
            && caveats
                .iter()
                .any(|caveat| caveat["id"] == "policy.disclosure_applied")
    }));
    Ok(())
}

#[sinex_test]
async fn desktop_context_output_rejects_streaming_formats() -> xtask::sandbox::TestResult<()> {
    let event_cards = EventCardListView {
        schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
        count: 0,
        cards: Vec::new(),
        next_cursor: None,
        total_estimate: None,
    };
    let sources = grouped_context_sources(&event_cards.cards);
    let result = render_desktop_context_output(
        &event_cards,
        &sources,
        "2h",
        OutputFormat::Ndjson,
        false,
        false,
        false,
        false,
    );

    assert!(result.is_err(), "desktop context must remain a finite view");
    Ok(())
}
