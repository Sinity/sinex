use super::*;

#[test]
fn assessment_marks_empty_recent_window_as_quiet() {
    let dlq = DlqSummary {
        unresolved: 0,
        resolved: 0,
    };
    let assessment = assess_store(&[], &[], &[], &dlq, 15);

    assert!(assessment.current_ingest_quiet);
    assert_eq!(assessment.top_recent_event_type, None);
    assert_eq!(assessment.active_source_materials, 0);
    assert_eq!(assessment.active_source_materials_over_60m, 0);
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

    let assessment = assess_store(&recent, &[], &materials, &dlq, 60);

    assert!(!assessment.current_ingest_quiet);
    assert_eq!(
        assessment.top_recent_event_type.as_deref(),
        Some("page.visited")
    );
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

    let assessment = assess_store(&[], &[], &[], &dlq, 5);

    assert_eq!(assessment.unresolved_dlq, 7);
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("7 unresolved DLQ"))
    );
}

#[test]
fn assessment_surfaces_old_active_source_materials() {
    let active = vec![
        ActiveSourceMaterialRow {
            material_id: "019f2a2a-03fd-7201-bd5d-b206ea39ad02".to_string(),
            source_identifier: "fs#material=019f2a2a-03fd-7201-bd5d-b206ea39ad02".to_string(),
            age_seconds: 4_400,
            parsed_events: 30,
            total_bytes: None,
        },
        ActiveSourceMaterialRow {
            material_id: "019f2a3d-a9fc-7240-b834-0f27ec456751".to_string(),
            source_identifier:
                "sinex.self-observation.sinexd#material=019f2a3d-a9fc-7240-b834-0f27ec456751"
                    .to_string(),
            age_seconds: 900,
            parsed_events: 1_000,
            total_bytes: None,
        },
    ];
    let dlq = DlqSummary {
        unresolved: 0,
        resolved: 0,
    };

    let assessment = assess_store(&[], &active, &[], &dlq, 5);

    assert_eq!(assessment.active_source_materials, 2);
    assert_eq!(assessment.active_source_materials_over_60m, 1);
    assert!(
        assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("1 active source material"))
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

    assert!(warnings.iter().any(|warning| {
        warning.contains("JetStream DLQ has 4381 message")
            && warning.contains("SQL unresolved DLQ rows: 0")
    }));
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
fn jetstream_assessment_surfaces_source_material_redelivery_pressure() {
    let snapshot = JetStreamStoreSnapshot {
        nats_url: "nats://localhost:4308".to_string(),
        available: true,
        error: None,
        streams: vec![JetStreamStreamSnapshot {
            role: "source-material".to_string(),
            stream: "DEV_SOURCE_MATERIAL".to_string(),
            present: true,
            messages: Some(1_671),
            bytes: Some(229_000_000),
            first_sequence: Some(2_194_885),
            last_sequence: Some(2_676_890),
            consumer_count: Some(1),
            consumers: vec![JetStreamConsumerSnapshot {
                name: "event_engine_material_frames".to_string(),
                durable_name: Some("event_engine_material_frames".to_string()),
                filter_subject: "dev.source_material.frames.>".to_string(),
                num_pending: 0,
                num_ack_pending: 3,
                num_redelivered: 1_670,
                num_waiting: 1,
                delivered_stream_sequence: 2_676_890,
                ack_floor_stream_sequence: 2_676_887,
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
            .any(|warning| warning.contains("3 ack-pending frame"))
    );
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("1670 redelivered frame"))
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
