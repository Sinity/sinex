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

#[test]
fn jetstream_assessment_surfaces_sql_vs_stream_dlq_divergence() {
    let snapshot = JetStreamStoreSnapshot {
        nats_url: "nats://localhost:4308".to_string(),
        available: true,
        error: None,
        streams: vec![JetStreamStreamSnapshot {
            role: "dlq".to_string(),
            stream: "DEV_SINEX_RAW_EVENTS_DLQ".to_string(),
            present: true,
            messages: Some(4_381),
            bytes: Some(413_000_000),
            first_sequence: Some(1),
            last_sequence: Some(4_381),
            consumer_count: Some(0),
            consumers: Vec::new(),
            error: None,
        }],
        sql_dlq_unresolved: 0,
        jetstream_dlq_messages: Some(4_381),
        warnings: Vec::new(),
    };

    let warnings = assess_jetstream(&snapshot);

    assert!(
        warnings.iter().any(|warning| {
            warning.contains("JetStream DLQ has 4381 message")
                && warning.contains("SQL unresolved DLQ rows: 0")
        })
    );
}

#[test]
fn jetstream_assessment_surfaces_raw_consumer_backlog() {
    let snapshot = JetStreamStoreSnapshot {
        nats_url: "nats://localhost:4308".to_string(),
        available: true,
        error: None,
        streams: vec![JetStreamStreamSnapshot {
            role: "raw".to_string(),
            stream: "DEV_SINEX_RAW_EVENTS".to_string(),
            present: true,
            messages: Some(100),
            bytes: Some(1024),
            first_sequence: Some(1),
            last_sequence: Some(100),
            consumer_count: Some(1),
            consumers: vec![JetStreamConsumerSnapshot {
                name: "event-engine".to_string(),
                durable_name: Some("event-engine".to_string()),
                filter_subject: "dev.sinex.events.raw.>".to_string(),
                num_pending: 42,
                num_ack_pending: 0,
                num_redelivered: 0,
                num_waiting: 0,
                delivered_stream_sequence: 58,
                ack_floor_stream_sequence: 58,
            }],
            error: None,
        }],
        sql_dlq_unresolved: 0,
        jetstream_dlq_messages: None,
        warnings: Vec::new(),
    };

    let warnings = assess_jetstream(&snapshot);

    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("raw JetStream consumer backlog: 42 pending"))
    );
}

#[test]
fn jetstream_assessment_degrades_when_nats_is_unavailable() {
    let snapshot = JetStreamStoreSnapshot {
        nats_url: "nats://localhost:4308".to_string(),
        available: false,
        error: Some("connection refused".to_string()),
        streams: Vec::new(),
        sql_dlq_unresolved: 3,
        jetstream_dlq_messages: None,
        warnings: Vec::new(),
    };

    let warnings = assess_jetstream(&snapshot);

    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("JetStream unavailable"))
    );
}
