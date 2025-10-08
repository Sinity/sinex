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
