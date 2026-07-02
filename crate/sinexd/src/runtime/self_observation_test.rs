use super::*;
use xtask::sandbox::prelude::*;

fn test_observer() -> SelfObserver {
    SelfObserver {
        publisher: None,
        materializer: None,
        component: "test-component".to_string(),
        enabled: true,
        metric_emissions: Arc::new(RwLock::new(HashMap::new())),
        min_interval: Duration::from_secs(1),
        module_run_id: Arc::new(OnceLock::new()),
    }
}

#[sinex_test]
async fn test_metric_identity_key_distinguishes_name_and_labels() -> TestResult<()> {
    let first = JsonValue::Object(
        serde_json::json!({
            "component": "event_engine",
            "name": "event_engine.consumer.lag.pending",
            "labels": { "consumer": "alpha" },
            "value": 1.0
        })
        .as_object()
        .cloned()
        .expect("json object"),
    );
    let second = JsonValue::Object(
        serde_json::json!({
            "component": "event_engine",
            "name": "event_engine.consumer.lag.ack_pending",
            "labels": { "consumer": "alpha" },
            "value": 1.0
        })
        .as_object()
        .cloned()
        .expect("json object"),
    );

    let first_key = SelfObserver::metric_identity_key("metric.gauge", &first);
    let second_key = SelfObserver::metric_identity_key("metric.gauge", &second);

    assert_ne!(first_key, second_key);
    Ok(())
}

#[sinex_test]
async fn health_status_metric_identity_includes_transition() -> TestResult<()> {
    let initial = JsonValue::Object(
        serde_json::json!({
            "component": "source.email",
            "previous_status": "healthy",
            "current_status": "healthy",
            "reason": "initial observation"
        })
        .as_object()
        .cloned()
        .expect("json object"),
    );
    let degraded = JsonValue::Object(
        serde_json::json!({
            "component": "source.email",
            "previous_status": "healthy",
            "current_status": "degraded",
            "reason": "status changed"
        })
        .as_object()
        .cloned()
        .expect("json object"),
    );

    let initial_key = SelfObserver::metric_identity_key("health.status", &initial);
    let degraded_key = SelfObserver::metric_identity_key("health.status", &degraded);

    assert_ne!(
        initial_key, degraded_key,
        "health.status rate limiting must not collapse a real status transition into the initial observation slot"
    );
    Ok(())
}

#[sinex_test]
async fn test_metric_reservations_are_per_metric_identity() -> TestResult<()> {
    let observer = test_observer();

    assert!(
        observer
            .reserve_metric_slot("metric.counter|name=assembly_started")
            .await
    );
    assert!(
        observer
            .reserve_metric_slot("metric.counter|name=assembly_completed")
            .await
    );
    assert!(
        !observer
            .reserve_metric_slot("metric.counter|name=assembly_started")
            .await
    );
    Ok(())
}

#[sinex_test]
async fn test_release_metric_slot_clears_failed_publish_reservation() -> TestResult<()> {
    let observer = test_observer();
    let key = "metric.counter|name=assembly_completed";

    assert!(observer.reserve_metric_slot(key).await);
    observer.release_metric_slot(key).await;
    assert!(observer.reserve_metric_slot(key).await);
    Ok(())
}

#[sinex_test]
async fn test_publish_fails_honestly_without_nats_client() -> TestResult<()> {
    let observer = test_observer();

    let first_error = observer
        .emit_counter("requests.total", 1, None)
        .await
        .expect_err("expected missing NATS client to fail");
    assert!(matches!(first_error, SelfObservationError::Unavailable));

    let second_error = observer
        .emit_counter("requests.total", 1, None)
        .await
        .expect_err("expected reservation to be released after missing client");
    assert!(matches!(second_error, SelfObservationError::Unavailable));
    Ok(())
}
