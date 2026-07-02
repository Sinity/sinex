use super::*;
use xtask::sandbox::sinex_test;

// Inline because these exercise private host-identity resolution helpers directly.
#[sinex_test]
async fn resolve_host_identity_prefers_valid_machine_id() -> TestResult<()> {
    let host = resolve_host_identity(Some("0123456789abcdef"), Some("sinnix-prime"));
    assert_eq!(host.as_str(), "0123456789abcdef");
    Ok(())
}

// Inline because these exercise private host-identity resolution helpers directly.
#[sinex_test]
async fn resolve_host_identity_falls_back_to_valid_hostname() -> TestResult<()> {
    let host = resolve_host_identity(Some("bad machine id"), Some("sinnix-prime"));
    assert_eq!(host.as_str(), "sinnix-prime");
    Ok(())
}

// Inline because these exercise private host-identity resolution helpers directly.
#[sinex_test]
async fn resolve_host_identity_derives_deterministic_fallback_from_invalid_inputs()
-> TestResult<()> {
    let host = resolve_host_identity(Some("bad machine id"), Some("bad host"));
    assert_eq!(host.as_str(), "host-887759893f18d0bb");
    Ok(())
}

// Inline because these exercise private host-identity resolution helpers directly.
#[sinex_test]
async fn resolve_host_identity_uses_unknown_host_only_when_no_identity_material_exists()
-> TestResult<()> {
    let host = resolve_host_identity(None, Some("   "));
    assert_eq!(host.as_str(), "unknown-host");
    Ok(())
}

// -------------------------------------------------------------------------
// #1570 Prong B — builder ts_orig inversion
// -------------------------------------------------------------------------

fn material_builder() -> EventBuilder<serde_json::Value, HasProvenance> {
    EventBuilder::new_internal(
        EventSource::from_static("test.source"),
        EventType::new("test.event").expect("valid event type"),
        serde_json::json!({}),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()), 0)
}

/// A material event with no explicit timestamp leaves `ts_orig = None` (the
/// "derive me at persistence" signal) rather than being stamped `now()`.
#[sinex_test]
async fn material_event_without_timestamp_defers_ts_orig() -> TestResult<()> {
    let event = material_builder().build()?;
    assert_eq!(
        event.ts_orig, None,
        "material defers ts_orig to persistence"
    );
    assert_eq!(event.ts_quality, None);
    Ok(())
}

/// A parser that resolved intrinsic timing keeps it, with the rung recorded.
#[sinex_test]
async fn material_event_with_explicit_quality_is_owned_by_parser() -> TestResult<()> {
    let ts = Timestamp::from_const(time::macros::datetime!(2021-01-02 03:04:05 UTC));
    let event = material_builder()
        .at_time_with_quality(ts, TemporalSourceType::IntrinsicContent)
        .build()?;
    assert_eq!(event.ts_orig, Some(ts));
    assert_eq!(event.ts_quality, Some(TemporalSourceType::IntrinsicContent));
    Ok(())
}

/// The deferred signal is deterministic: re-building the same material event
/// (as replay does) yields the same `None` — no ephemeral `now()` sneaks in.
#[sinex_test]
async fn material_deferral_is_replay_stable() -> TestResult<()> {
    assert_eq!(material_builder().build()?.ts_orig, None);
    assert_eq!(material_builder().build()?.ts_orig, None);
    Ok(())
}

/// Derived events have no source material to resolve against, so they keep
/// the wall-clock synthesis-time fallback and a `None` quality rung.
#[sinex_test]
async fn derived_event_without_timestamp_uses_synthesis_now() -> TestResult<()> {
    let parent = Id::<Event>::from_uuid(Uuid::now_v7());
    let event = EventBuilder::new_internal(
        EventSource::from_static("test.source"),
        EventType::new("test.derived").expect("valid event type"),
        serde_json::json!({}),
    )
    .from_parents([parent])?
    .build()?;
    assert!(
        event.ts_orig.is_some(),
        "derived events keep synthesis-time ts_orig"
    );
    assert_eq!(event.ts_quality, None);
    Ok(())
}
