//! Unit tests for `SubscriptionFilter` in-memory event matching.

use serde_json::json;
use sinex_primitives::domain::{EventSource, EventType, HostName};
use sinex_primitives::events::builder::{OffsetKind, Provenance};
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::query::{PathOp, PayloadFilter, SubscriptionFilter};
use sinex_primitives::{Id, Timestamp};
use xtask::sandbox::sinex_test;

/// Build a test event with the given source, type, host, and payload.
fn test_event(source: &str, event_type: &str, host: &str, payload: serde_json::Value) -> Event {
    let host = match HostName::new(host) {
        Ok(host) => host,
        Err(error) => panic!("test host should be valid: {error}"),
    };

    Event {
        id: None,
        source: source.into(),
        event_type: event_type.into(),
        host,
        payload,
        ts_orig: Some(Timestamp::now()),
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::new(),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::default(),
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    }
}

#[sinex_test]
async fn empty_filter_matches_everything() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter::default();
    let event = test_event("fs-watcher", "file.created", "myhost", json!({}));
    assert!(filter.matches(&event));
    Ok(())
}

#[sinex_test]
async fn source_filter_matches() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        sources: vec![EventSource::from_static("fs-watcher")],
        ..Default::default()
    };
    let matching = test_event("fs-watcher", "file.created", "myhost", json!({}));
    let non_matching = test_event("terminal", "command.exec", "myhost", json!({}));
    assert!(filter.matches(&matching));
    assert!(!filter.matches(&non_matching));
    Ok(())
}

#[sinex_test]
async fn multiple_sources_filter() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        sources: vec![
            EventSource::from_static("fs-watcher"),
            EventSource::from_static("terminal"),
        ],
        ..Default::default()
    };
    assert!(filter.matches(&test_event("fs-watcher", "x", "h", json!({}))));
    assert!(filter.matches(&test_event("terminal", "x", "h", json!({}))));
    assert!(!filter.matches(&test_event("desktop", "x", "h", json!({}))));
    Ok(())
}

#[sinex_test]
async fn event_type_filter_matches() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        event_types: vec![EventType::from_static("file.created")],
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "file.created", "h", json!({}))));
    assert!(!filter.matches(&test_event("x", "file.deleted", "h", json!({}))));
    Ok(())
}

#[sinex_test]
async fn host_filter_matches() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        hosts: vec![HostName::from_static("server01")],
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "x", "server01", json!({}))));
    assert!(!filter.matches(&test_event("x", "x", "server02", json!({}))));
    Ok(())
}

#[sinex_test]
async fn combined_filters_and_semantics() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        sources: vec![EventSource::from_static("fs-watcher")],
        event_types: vec![EventType::from_static("file.created")],
        ..Default::default()
    };
    // Both match
    assert!(filter.matches(&test_event("fs-watcher", "file.created", "h", json!({}))));
    // Source matches, type doesn't
    assert!(!filter.matches(&test_event("fs-watcher", "file.deleted", "h", json!({}))));
    // Type matches, source doesn't
    assert!(!filter.matches(&test_event("terminal", "file.created", "h", json!({}))));
    Ok(())
}

#[sinex_test]
async fn payload_contains_filter() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Contains {
            value: json!({"path": "/home"}),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"path": "/home", "size": 100})
    )));
    assert!(!filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"path": "/tmp", "size": 100})
    )));
    Ok(())
}

#[sinex_test]
async fn payload_text_search_filter() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::TextSearch {
            text: "important".to_string(),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"title": "an important document"})
    )));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"title": "trivial item"}))));
    Ok(())
}

#[sinex_test]
async fn payload_text_search_filter_is_case_insensitive() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::TextSearch {
            text: "important".to_string(),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"title": "IMPORTANT document"})
    )));
    Ok(())
}

#[sinex_test]
async fn payload_has_key_filter() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::HasKey {
            key: "error".to_string(),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"error": "something broke"})
    )));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"status": "ok"}))));
    Ok(())
}

#[sinex_test]
async fn payload_path_eq_filter() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Path {
            path: "status".to_string(),
            op: PathOp::Eq(json!("ok")),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "x", "h", json!({"status": "ok"}))));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"status": "err"}))));
    Ok(())
}

#[sinex_test]
async fn payload_path_gt_filter() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Path {
            path: "size".to_string(),
            op: PathOp::Gt(json!(100)),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "x", "h", json!({"size": 200}))));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"size": 50}))));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"size": 100}))));
    Ok(())
}

#[sinex_test]
async fn payload_path_like_filter() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Path {
            path: "name".to_string(),
            op: PathOp::Like("%.rs".to_string()),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "x", "h", json!({"name": "main.rs"}))));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"name": "main.py"}))));
    Ok(())
}

#[sinex_test]
async fn payload_path_like_filter_matches_sql_case_sensitively() -> ::xtask::sandbox::TestResult<()>
{
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Path {
            path: "name".to_string(),
            op: PathOp::Like("%.rs".to_string()),
        }),
        ..Default::default()
    };
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"name": "MAIN.RS"}))));
    Ok(())
}

#[sinex_test]
async fn payload_path_like_filter_handles_many_wildcards() -> ::xtask::sandbox::TestResult<()> {
    let repeated = "%a".repeat(48);
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Path {
            path: "name".to_string(),
            op: PathOp::Like(format!("{repeated}b")),
        }),
        ..Default::default()
    };
    let event = test_event("x", "x", "h", json!({ "name": "a".repeat(48) }));
    assert!(
        !filter.matches(&event),
        "path LIKE matching should stay bounded even for many '%' wildcards"
    );
    Ok(())
}

#[sinex_test]
async fn payload_and_composition() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::And {
            filters: vec![
                PayloadFilter::HasKey {
                    key: "path".to_string(),
                },
                PayloadFilter::Path {
                    path: "size".to_string(),
                    op: PathOp::Gt(json!(0)),
                },
            ],
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"path": "/file", "size": 100})
    )));
    // Missing "path" key
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"size": 100}))));
    // Size is 0 (not > 0)
    assert!(!filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"path": "/file", "size": 0})
    )));
    Ok(())
}

#[sinex_test]
async fn payload_or_composition() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Or {
            filters: vec![
                PayloadFilter::HasKey {
                    key: "error".to_string(),
                },
                PayloadFilter::HasKey {
                    key: "warning".to_string(),
                },
            ],
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "x", "h", json!({"error": "e"}))));
    assert!(filter.matches(&test_event("x", "x", "h", json!({"warning": "w"}))));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"info": "i"}))));
    Ok(())
}

#[sinex_test]
async fn payload_not_composition() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Not {
            filter: Box::new(PayloadFilter::HasKey {
                key: "debug".to_string(),
            }),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "x", "h", json!({"info": "i"}))));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"debug": "d"}))));
    Ok(())
}

#[sinex_test]
async fn payload_contains_nested_objects() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Contains {
            value: json!({"metadata": {"priority": "high"}}),
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"metadata": {"priority": "high", "category": "test"}, "data": 1})
    )));
    assert!(!filter.matches(&test_event(
        "x",
        "x",
        "h",
        json!({"metadata": {"priority": "low"}})
    )));
    Ok(())
}

#[sinex_test]
async fn payload_path_is_null() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Path {
            path: "optional".to_string(),
            op: PathOp::IsNull,
        }),
        ..Default::default()
    };
    assert!(filter.matches(&test_event("x", "x", "h", json!({"other": 1}))));
    assert!(filter.matches(&test_event("x", "x", "h", json!({"optional": null}))));
    assert!(!filter.matches(&test_event("x", "x", "h", json!({"optional": "value"}))));
    Ok(())
}

#[sinex_test]
async fn validate_rejects_deep_nesting() -> ::xtask::sandbox::TestResult<()> {
    // Build deeply nested filter (depth > 4)
    let mut filter_inner = PayloadFilter::HasKey {
        key: "x".to_string(),
    };
    for _ in 0..6 {
        filter_inner = PayloadFilter::Not {
            filter: Box::new(filter_inner),
        };
    }
    let filter = SubscriptionFilter {
        payload: Some(filter_inner),
        ..Default::default()
    };
    assert!(filter.validate().is_err());
    Ok(())
}

#[sinex_test]
async fn validate_accepts_shallow_nesting() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::And {
            filters: vec![
                PayloadFilter::HasKey {
                    key: "a".to_string(),
                },
                PayloadFilter::Not {
                    filter: Box::new(PayloadFilter::HasKey {
                        key: "b".to_string(),
                    }),
                },
            ],
        }),
        ..Default::default()
    };
    assert!(filter.validate().is_ok());
    Ok(())
}

#[sinex_test]
async fn validate_rejects_payload_text_search() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::TextSearch {
            text: "important".to_string(),
        }),
        ..Default::default()
    };

    let error = filter
        .validate()
        .expect_err("text search should be rejected for SSE filters");
    assert!(
        error
            .to_string()
            .contains("does not support payload text search"),
        "unexpected validation error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn validate_rejects_nested_payload_text_search() -> ::xtask::sandbox::TestResult<()> {
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::And {
            filters: vec![
                PayloadFilter::HasKey {
                    key: "title".to_string(),
                },
                PayloadFilter::Not {
                    filter: Box::new(PayloadFilter::TextSearch {
                        text: "secret".to_string(),
                    }),
                },
            ],
        }),
        ..Default::default()
    };

    assert!(filter.validate().is_err());
    Ok(())
}
