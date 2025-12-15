use chrono::Utc;
use serde_json::json;
use sinex_core::repositories::DbPoolExt;
use sinex_core::types::domain::{EventSource, EventType, HostName};
use sinex_core::types::Id;
use sinex_core::{Event, Provenance};
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn events_repository_inserts_dynamic_events(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_core::db::repositories::source_materials::legacy_material_types::STREAM,
            Some("test-event-source-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_core::models::SourceMaterial>::from_ulid(material_record.id);

    let event = Event::dynamic(
        EventSource::new("test.source"),
        EventType::new("test.event"),
        json!({"test": "data"}),
    )
    .with_provenance(Provenance::from_material(material_id, 0, None, None))
    .build()
    .with_host(HostName::new("test-host"));

    let inserted = ctx.pool.events().insert(event).await?;
    assert_eq!(inserted.source.as_str(), "test.source");
    assert_eq!(inserted.event_type.as_str(), "test.event");
    assert_eq!(inserted.host.as_str(), "test-host");
    assert_eq!(inserted.payload["test"], "data");
    assert!(inserted.id.is_some());
    Ok(())
}

#[sinex_test]
async fn events_repository_preserves_provenance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_core::db::repositories::source_materials::legacy_material_types::STREAM,
            Some("test-source-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_core::models::SourceMaterial>::from_ulid(material_record.id);

    let source_event = Event::dynamic(
        EventSource::new("test.source"),
        EventType::new("source.event"),
        json!({"original": true}),
    )
    .with_provenance(Provenance::from_material(material_id, 0, None, None))
    .build()
    .with_host(HostName::new("test-host"));

    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    let derived_event = Event::dynamic(
        EventSource::new("test.processor"),
        EventType::new("derived.event"),
        json!({"derived": true}),
    )
    .with_provenance(Provenance::from_synthesis(vec![source_id.clone()]).unwrap())
    .build()
    .with_host(HostName::new("test-host"));

    let inserted = ctx.pool.events().insert(derived_event).await?;
    match inserted.provenance {
        Provenance::Synthesis {
            source_event_ids, ..
        } => {
            assert_eq!(source_event_ids.len(), 1);
            assert_eq!(source_event_ids[0], source_id);
        }
        _ => panic!("Expected synthesis provenance"),
    }
    Ok(())
}

#[sinex_test]
async fn cleanup_test_events_does_not_match_production_names(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_core::db::repositories::source_materials::legacy_material_types::STREAM,
            Some("prod-source-material"),
            json!({ "note": "production" }),
        )
        .await?;
    let material_id = Id::<sinex_core::models::SourceMaterial>::from_ulid(material_record.id);

    let event = Event::dynamic(
        EventSource::new("deployment"),
        EventType::new("release.completed"),
        json!({"severity": "info"}),
    )
    .with_provenance(Provenance::from_material(material_id, 0, None, None))
    .build()
    .with_host(HostName::new("latest-prod-node"));

    let inserted = ctx.pool.events().insert(event).await?;
    let deleted = ctx
        .pool
        .events()
        .cleanup_test_events_with_context(None, None, "tester", "cleanup sweep")
        .await?;

    assert_eq!(
        deleted, 0,
        "cleanup should not delete events with production hostnames"
    );

    let fetched = ctx
        .pool
        .events()
        .get_by_id(inserted.id.expect("event id"))
        .await?;
    assert!(
        fetched.is_some(),
        "event should still exist after cleanup guard"
    );
    Ok(())
}

#[sinex_test]
async fn register_external_in_flight_uses_provided_id(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let forced_id = sinex_core::types::ulid::Ulid::new();
    let identifier = format!("test-material-{}", forced_id);
    let record = ctx
        .pool
        .source_materials()
        .register_external_in_flight(
            forced_id,
            sinex_core::db::repositories::source_materials::legacy_material_types::FILE,
            Some(&identifier),
            json!({"note": "external registration"}),
            Utc::now(),
        )
        .await?;

    assert_eq!(record.id, forced_id);
    assert_eq!(record.source_identifier, identifier);
    Ok(())
}
