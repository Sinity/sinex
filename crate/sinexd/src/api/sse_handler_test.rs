use super::{
    LAST_EVENT_ID_HEADER, disclose_and_match_sse_event, parse_last_event_id,
    serialize_sse_payload,
};
use crate::event_engine::policy::PolicyEngine;
use axum::http::{HeaderMap, HeaderValue};
use serde::Serialize;
use sinex_db::DbPoolExt;
use sinex_primitives::events::{DynamicPayload, Event, SourceMaterial};
use sinex_primitives::query::{PayloadFilter, SubscriptionFilter};
use sinex_primitives::{EventSource, EventType, Id, JsonValue, SinexError, Uuid};
use xtask::sandbox::sinex_test;

struct FailingSerialize;

impl Serialize for FailingSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("boom"))
    }
}

#[sinex_test]
async fn serialize_sse_payload_surfaces_serialization_failures() -> TestResult<()> {
    let payload = serialize_sse_payload("event", &FailingSerialize)
        .expect_err("serialization failure should produce structured error payload");

    assert_eq!(payload.code, "serialization_error");
    assert!(
        payload.message.contains("event") && payload.message.contains("boom"),
        "unexpected error message: {}",
        payload.message
    );
    Ok(())
}

#[sinex_test]
async fn parse_last_event_id_accepts_persisted_event_uuid() -> TestResult<()> {
    let event_id = Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7());
    let mut headers = HeaderMap::new();
    headers.insert(
        LAST_EVENT_ID_HEADER,
        HeaderValue::from_str(&event_id.to_string())?,
    );

    let parsed = parse_last_event_id(&headers).map_err(SinexError::validation)?;
    assert_eq!(parsed, Some(event_id));
    Ok(())
}

#[sinex_test]
async fn parse_last_event_id_rejects_non_uuid_values() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert(LAST_EVENT_ID_HEADER, HeaderValue::from_static("42"));

    let error = parse_last_event_id(&headers).expect_err("sequence ids must be rejected");
    assert!(error.contains("persisted event UUID"));
    Ok(())
}

#[sinex_test]
async fn payload_subscription_matches_disclosed_event_not_raw_secret(
    ctx: TestContext,
) -> TestResult<()> {
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "sse-view-secret",
            "test view disclosure policy for SSE payload filters",
            "regex",
            r"sse_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<SSE_SECRET>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("sse-view-secret", None, None, Some("/secret"), 0)
        .await?;

    let policy = PolicyEngine::load(ctx.pool().clone()).await?;
    let event = DynamicPayload::new(
        EventSource::from_static("sse-test"),
        EventType::from_static("sse.event"),
        serde_json::json!({
            "secret": "sse_secret_alpha",
            "public": "visible"
        }),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()))
    .build()?;

    let raw_value_filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Contains {
            value: serde_json::json!({"secret": "sse_secret_alpha"}),
        }),
        ..Default::default()
    };
    assert!(
        disclose_and_match_sse_event(&event, &policy, &raw_value_filter)
            .await
            .is_none(),
        "SSE payload filters must not match redacted raw values"
    );

    let disclosed_value_filter = SubscriptionFilter {
        payload: Some(PayloadFilter::Contains {
            value: serde_json::json!({"secret": "<SSE_SECRET>"}),
        }),
        ..Default::default()
    };
    let Some((disclosed_event, caveats)) =
        disclose_and_match_sse_event(&event, &policy, &disclosed_value_filter).await
    else {
        panic!("disclosed replacement value should be matchable");
    };
    assert_eq!(disclosed_event.payload["secret"], "<SSE_SECRET>");
    assert!(
        caveats
            .iter()
            .any(|caveat| caveat.id == "policy.disclosure_applied"),
        "SSE clients must see that policy changed the emitted payload"
    );
    Ok(())
}
