use super::*;

#[test]
fn assessment_marks_empty_recent_window_as_quiet() {
    let dlq = DlqSummary {
        unresolved: 0,
        resolved: 0,
    };
    let assessment = assess_store(&[], &[], &dlq, 15);

    assert!(assessment.current_ingest_quiet);
    assert_eq!(assessment.top_recent_event_type, None);
    assert_eq!(assessment.unresolved_dlq, 0);
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("no events observed"))
    );
}

#[test]
fn assessment_flags_browser_history_flood_and_large_material_inventory() {
    let recent = vec![
        EventMixRow {
            event_type: "page.visited".to_string(),
            events: 656_216,
        },
        EventMixRow {
            event_type: "metric.gauge".to_string(),
            events: 3_574,
        },
    ];
    let materials = vec![
        SourceMaterialRollup {
            source_base: "browser.history".to_string(),
            status: "completed".to_string(),
            materials: 259,
            total_bytes: Some(25_000_000_000),
            parsed_events: 55_827_312,
        },
        SourceMaterialRollup {
            source_base: "browser.history".to_string(),
            status: "failed".to_string(),
            materials: 25,
            total_bytes: None,
            parsed_events: 1_811_867,
        },
    ];
    let dlq = DlqSummary {
        unresolved: 0,
        resolved: 0,
    };

    let assessment = assess_store(&recent, &materials, &dlq, 60);

    assert!(!assessment.current_ingest_quiet);
    assert_eq!(assessment.top_recent_event_type.as_deref(), Some("page.visited"));
    assert_eq!(assessment.browser_history_materials_total, 284);
    assert_eq!(assessment.browser_history_parsed_events_total, 57_639_179);
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("browser history dominates"))
    );
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("material inventory is large"))
    );
}

#[test]
fn assessment_surfaces_unresolved_dlq() {
    let dlq = DlqSummary {
        unresolved: 7,
        resolved: 3,
    };

    let assessment = assess_store(&[], &[], &dlq, 5);

    assert_eq!(assessment.unresolved_dlq, 7);
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("7 unresolved DLQ"))
    );
}
