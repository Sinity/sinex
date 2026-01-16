use sinex_core::repositories::DbPoolExt;
use sinex_core::{CreateEntity, Event, Id, JsonValue};
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn merge_entities_unions_fields_and_logs_audit(ctx: TestContext) -> color_eyre::Result<()> {
    let repo = ctx.pool.knowledge_graph();

    let source_event_id = Id::<Event<JsonValue>>::new();
    let target_event_id = Id::<Event<JsonValue>>::new();

    let source = repo
        .create_entity(
            CreateEntity::person("Alice")
                .with_aliases(["Al"])
                .with_properties(serde_json::json!({
                    "tags": ["alpha"],
                    "meta": {
                        "origin": "source",
                        "shared": "source"
                    },
                    "count": 1
                }))
                .with_source_event_ids(vec![source_event_id.clone()])
                .with_confidence_score(0.4),
        )
        .await?;

    let target = repo
        .create_entity(
            CreateEntity::person("Alicia")
                .with_aliases(["Ali"])
                .with_properties(serde_json::json!({
                    "tags": ["beta"],
                    "meta": {
                        "owner": "target",
                        "shared": "target"
                    },
                    "count": 1
                }))
                .with_source_event_ids(vec![target_event_id.clone()])
                .with_confidence_score(0.8),
        )
        .await?;

    let expected_created_at = if source.created_at < target.created_at {
        source.created_at
    } else {
        target.created_at
    };

    repo.merge_entities(source.id, target.id).await?;

    let merged_target = repo.get_entity(target.id).await?.expect("target entity");
    let merged_source = repo.get_entity(source.id).await?.expect("source entity");

    assert!(merged_source.is_merged);
    assert_eq!(merged_source.merged_into_id, Some(target.id));
    assert!(!merged_target.is_merged);
    assert_eq!(merged_target.merged_into_id, None);

    for alias in ["Ali", "Al", "Alice", "alice"] {
        assert!(
            merged_target.aliases.iter().any(|item| item == alias),
            "missing alias: {alias}"
        );
    }

    assert!(merged_target.source_event_ids.contains(&source_event_id));
    assert!(merged_target.source_event_ids.contains(&target_event_id));
    assert_eq!(merged_target.confidence_score, 0.8);
    assert_eq!(merged_target.created_at, expected_created_at);

    let tags = merged_target
        .properties
        .get("tags")
        .and_then(|value| value.as_array())
        .expect("tags array");
    assert!(tags.contains(&serde_json::Value::String("alpha".to_string())));
    assert!(tags.contains(&serde_json::Value::String("beta".to_string())));

    let shared = merged_target
        .properties
        .get("meta")
        .and_then(|value| value.get("shared"))
        .and_then(|value| value.as_str())
        .expect("meta.shared");
    assert_eq!(shared, "target");

    let operations = ctx.pool.state().get_recent_operations(25).await?;
    let merge_op = operations
        .into_iter()
        .find(|op| op.operation_type == "entity_merge")
        .expect("entity merge audit entry");

    let scope = merge_op.scope.expect("merge scope");
    let source_id = source.id.to_string();
    let target_id = target.id.to_string();
    assert_eq!(
        scope.get("source_id").and_then(|value| value.as_str()),
        Some(source_id.as_str())
    );
    assert_eq!(
        scope.get("target_id").and_then(|value| value.as_str()),
        Some(target_id.as_str())
    );

    let summary = merge_op.preview_summary.expect("merge summary");
    let conflicts = summary
        .get("conflicts")
        .and_then(|value| value.as_array())
        .expect("conflicts array");
    assert!(conflicts.iter().any(|entry| {
        entry
            .get("path")
            .and_then(|value| value.as_str())
            .is_some_and(|path| path == "meta.shared")
    }));

    Ok(())
}
