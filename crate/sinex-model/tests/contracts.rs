use serde_json::json;
use sinex_model::events::*;

#[test]
fn derived_wire_rejects_empty_parents_and_two_origins() {
    assert!(
        serde_json::from_value::<Provenance>(json!({"origin":"derived","parents":[]})).is_err()
    );
    assert!(
        serde_json::from_value::<Provenance>(
            json!({"origin":"derived","parents":[EventId::new_v7()], "coordinates":{}})
        )
        .is_err()
    );
}
#[test]
fn canonical_keys_preserve_boundaries_and_utf8() {
    assert_ne!(
        EquivalenceKey::from_parts(["a:b", "c"]),
        EquivalenceKey::from_parts(["a", "b:c"])
    );
    let key = EquivalenceKey::from_parts(["é", "", "3:x"]);
    assert_eq!(
        EquivalenceKey::try_from(key.as_str().to_owned()).unwrap(),
        key
    );
    for invalid in ["01:a", "2:a", "1:é", ":", "-1:a"] {
        assert!(
            EquivalenceKey::try_from(invalid.to_owned()).is_err(),
            "{invalid}"
        );
    }
}
#[test]
fn explicit_unknown_does_not_become_staging_time() {
    let unknown: TsOrig = serde_json::from_value(json!({"state":"unknown"})).unwrap();
    assert_eq!(unknown.at(), None);
    assert!(serde_json::from_value::<TsOrig>(json!({"state":"unknown","at":0})).is_err());
}
#[test]
fn event_id_rejects_non_v7_and_read_orders_require_same_namespace() {
    assert!(EventId::try_from(uuid::Uuid::nil()).is_err());
    assert_eq!(EventId::new_v7().as_uuid().get_version_num(), 7);
    assert_eq!(
        ReadOrder {
            namespace: "a".into(),
            order: 1
        }
        .compare(&ReadOrder {
            namespace: "b".into(),
            order: 2
        }),
        None
    );
}

#[test]
fn tagged_empty_variants_reject_fields_from_other_states() {
    for precision in ["instant", "second", "minute", "day", "month", "year"] {
        let valid = json!({"precision":precision});
        let parsed: TimePrecision = serde_json::from_value(valid.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), valid);
        assert!(
            serde_json::from_value::<TimePrecision>(json!({"precision":precision,"until":0}))
                .is_err()
        );
    }
    let valid = json!({"phase":"register_bootstrap"});
    let parsed: sinex_model::batch::AutomatonProgress =
        serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), valid);
    assert!(
        serde_json::from_value::<sinex_model::batch::AutomatonProgress>(
            json!({"phase":"register_bootstrap","snapshot_seq":1})
        )
        .is_err()
    );
}

#[test]
fn bounded_time_preserves_equal_endpoints_and_rejects_reversed_bounds() {
    let at = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    assert!(
        TsOrig::from_record_field(at, "observed_at", TimePrecision::Bounded { until: at }).is_ok()
    );
    assert!(
        TsOrig::from_record_field(
            at,
            "observed_at",
            TimePrecision::Bounded {
                until: at - time::Duration::NANOSECOND
            },
        )
        .is_err()
    );
}

#[test]
fn capture_clock_is_required_and_projection_preserves_evidence() {
    let id = EventId::new_v7();
    let future = time::OffsetDateTime::from_unix_timestamp(4_102_444_800).unwrap();
    let producer = TsOrig::captured(
        future,
        CaptureClock::Producer,
        TimePrecision::Bounded { until: future },
    )
    .unwrap();
    let wire = serde_json::to_value(&producer).unwrap();
    assert_eq!(
        serde_json::from_value::<TsOrig>(wire.clone()).unwrap(),
        producer
    );
    let mut missing = wire.clone();
    missing["quality"]["captured"]
        .as_object_mut()
        .unwrap()
        .remove("clock");
    assert!(serde_json::from_value::<TsOrig>(missing).is_err());
    let mut extra = wire;
    extra["quality"]["captured"]["extra"] = json!(true);
    assert!(serde_json::from_value::<TsOrig>(extra).is_err());
    let row = producer.project(id, CaptureClock::Producer).unwrap();
    assert_eq!(row.at, Some(future));
    assert_eq!(row.until, Some(future));
    assert_eq!(row.precision, "bounded");
    assert_eq!(row.suspect, Some("future"));
    assert!(producer.project(id, CaptureClock::Sinex).is_err());
    assert!(
        TsOrig::captured(future, CaptureClock::Sinex, TimePrecision::Instant)
            .unwrap()
            .project(id, CaptureClock::Sinex)
            .is_err()
    );
    let unknown = TsOrig::unknown()
        .project(id, CaptureClock::Producer)
        .unwrap();
    assert_eq!(
        (unknown.at, unknown.quality, unknown.suspect),
        (None, "unknown", None)
    );
    let intrinsic = TsOrig::from_record_field(future, "observed_at", TimePrecision::Year).unwrap();
    assert_eq!(
        intrinsic.project(id, CaptureClock::Sinex).unwrap().at,
        Some(future)
    );
    let (seconds, nanos) = id.as_uuid().get_timestamp().unwrap().to_unix();
    let boundary = time::OffsetDateTime::from_unix_timestamp(seconds as i64).unwrap()
        + time::Duration::nanoseconds(nanos.into())
        + time::Duration::seconds(CAPTURED_FUTURE_SKEW_SECONDS);
    assert!(
        TsOrig::captured(boundary, CaptureClock::Sinex, TimePrecision::Instant)
            .unwrap()
            .project(id, CaptureClock::Sinex)
            .is_ok()
    );
    assert!(
        TsOrig::captured(
            boundary + time::Duration::NANOSECOND,
            CaptureClock::Sinex,
            TimePrecision::Instant
        )
        .unwrap()
        .project(id, CaptureClock::Sinex)
        .is_err()
    );
}
