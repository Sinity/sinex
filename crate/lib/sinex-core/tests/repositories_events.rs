use chrono::Utc;
use serde_json::json;
use sinex_core::repositories::DbPoolExt;
use sinex_core::types::domain::SanitizedPath;
use sinex_core::types::events::payloads::{FileCreatedPayload, KittyCommandExecutedPayload};
use sinex_core::types::Id;
use sinex_core::{Event, Provenance};
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn events_repository_inserts_typed_events(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_core::db::repositories::source_materials::material_types::STREAM,
            Some("test-event-source-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_core::models::SourceMaterial>::from_ulid(material_record.id);

    let mut payload = FileCreatedPayload::test_default(
        SanitizedPath::from_str_validated("/tmp/repo-insert.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    payload.size = 512;
    let event = Event::new(
        payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let expected_host = event.host.clone();
    let inserted = ctx.pool.events().insert(event).await?;
    assert_eq!(inserted.source.as_str(), "fs-watcher");
    assert_eq!(inserted.event_type.as_str(), "file.created");
    assert_eq!(inserted.host, expected_host);
    assert_eq!(inserted.payload["path"], json!("/tmp/repo-insert.txt"));
    assert_eq!(inserted.payload["size"], json!(512));
    assert!(inserted.id.is_some());
    Ok(())
}

#[sinex_test]
async fn events_repository_preserves_provenance(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_core::db::repositories::source_materials::material_types::STREAM,
            Some("test-source-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_core::models::SourceMaterial>::from_ulid(material_record.id);

    let source_payload = KittyCommandExecutedPayload::test_default("echo provenance");
    let source_event = Event::new(
        source_payload,
        Provenance::from_material(material_id, 0, None, None),
    );

    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    let derived_payload = FileCreatedPayload::test_default(
        SanitizedPath::from_str_validated("/tmp/derived.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let derived_event = Event::builder(derived_payload)
        .from_parents(vec![source_id.clone()])?
        .build()?;

    let inserted = ctx.pool.events().insert(derived_event).await?;
    match inserted.provenance() {
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
async fn register_external_in_flight_uses_provided_id(ctx: TestContext) -> TestResult<()> {
    let forced_id = sinex_core::types::ulid::Ulid::new();
    let identifier = format!("test-material-{}", forced_id);
    let record = ctx
        .pool
        .source_materials()
        .register_external_in_flight(
            forced_id,
            sinex_core::db::repositories::source_materials::material_types::FILE,
            Some(&identifier),
            json!({"note": "external registration"}),
            Utc::now(),
        )
        .await?;

    assert_eq!(record.id, forced_id);
    assert_eq!(record.source_identifier, identifier);
    Ok(())
}
