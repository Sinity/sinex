use sinex_db::JsonValue;
use sinex_db::models::Event;
use sinex_db::repositories::{CreateEntity, CreateEntityRelation, DbPoolExt};
use sinex_primitives::{Id, Timestamp};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn merge_entities_preserves_earliest_created_at_precision(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.knowledge_graph();
    let source = repo
        .create_entity(CreateEntity::person("source-person"))
        .await?;
    let target = repo
        .create_entity(CreateEntity::person("target-person"))
        .await?;

    let source_created_at = Timestamp::from_unix_timestamp_nanos(1_700_000_000_123_456_000)
        .expect("test timestamp should be valid");
    let target_created_at = Timestamp::from_unix_timestamp_nanos(1_700_000_100_654_321_000)
        .expect("test timestamp should be valid");

    sqlx::query!(
        r#"
        UPDATE core.entities
        SET created_at = $2, updated_at = $2
        WHERE id = $1
        "#,
        *source.id.as_uuid() as _,
        *source_created_at
    )
    .execute(&ctx.pool)
    .await?;

    sqlx::query!(
        r#"
        UPDATE core.entities
        SET created_at = $2, updated_at = $2
        WHERE id = $1
        "#,
        *target.id.as_uuid() as _,
        *target_created_at
    )
    .execute(&ctx.pool)
    .await?;

    repo.merge_entities(source.id, target.id).await?;

    let merged_target = repo
        .get_entity(target.id)
        .await?
        .expect("target entity should still exist after merge");
    let merged_source = repo
        .get_entity(source.id)
        .await?
        .expect("source entity should remain queryable after merge");

    assert_eq!(merged_target.created_at, source_created_at);
    assert!(merged_source.is_merged);
    assert_eq!(merged_source.merged_into_id, Some(target.id));
    Ok(())
}

/// When the source and target both have an outgoing relation to the same
/// (to_entity, relation_type), `merge_entities` must union their
/// `source_event_ids` onto the survivor instead of dropping the duplicate
/// row's provenance. This is the regression covered by #1176.
#[sinex_test]
async fn merge_duplicate_outgoing_relations_unions_source_event_ids(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.knowledge_graph();

    let target = repo
        .create_entity(CreateEntity::person("merge-outgoing-target"))
        .await?;
    let source = repo
        .create_entity(CreateEntity::person("merge-outgoing-source"))
        .await?;
    let other = repo
        .create_entity(CreateEntity::person("merge-outgoing-other"))
        .await?;

    let e1 = Id::<Event<JsonValue>>::new();
    let e2 = Id::<Event<JsonValue>>::new();
    let e3 = Id::<Event<JsonValue>>::new();
    let e4 = Id::<Event<JsonValue>>::new();

    repo.create_relation(
        CreateEntityRelation::new(target.id, other.id, "knows")
            .with_source_event_ids(vec![e1, e2]),
    )
    .await?;
    repo.create_relation(
        CreateEntityRelation::new(source.id, other.id, "knows")
            .with_source_event_ids(vec![e3, e4]),
    )
    .await?;

    repo.merge_entities(source.id, target.id).await?;

    let relations = repo
        .get_entity_relations(target.id, Some("knows"), true)
        .await?;
    assert_eq!(
        relations.len(),
        1,
        "expected exactly one surviving target->other knows relation, got {:?}",
        relations
    );
    let survivor = &relations[0];
    assert_eq!(survivor.from_entity_id, target.id);
    assert_eq!(survivor.to_entity_id, other.id);

    let mut got: Vec<_> = survivor
        .source_event_ids
        .iter()
        .map(|id| *id.as_uuid())
        .collect();
    got.sort();
    let mut want: Vec<_> = [e1, e2, e3, e4].iter().map(|id| *id.as_uuid()).collect();
    want.sort();
    assert_eq!(
        got, want,
        "merge dropped source_event_ids from the duplicate relation"
    );
    Ok(())
}

/// Symmetric coverage of incoming relations: X->target with [e1, e2] and
/// X->source with [e3, e4] must union onto X->target after merge.
#[sinex_test]
async fn merge_duplicate_incoming_relations_unions_source_event_ids(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.knowledge_graph();

    let target = repo
        .create_entity(CreateEntity::person("merge-incoming-target"))
        .await?;
    let source = repo
        .create_entity(CreateEntity::person("merge-incoming-source"))
        .await?;
    let other = repo
        .create_entity(CreateEntity::person("merge-incoming-other"))
        .await?;

    let e1 = Id::<Event<JsonValue>>::new();
    let e2 = Id::<Event<JsonValue>>::new();
    let e3 = Id::<Event<JsonValue>>::new();
    let e4 = Id::<Event<JsonValue>>::new();

    repo.create_relation(
        CreateEntityRelation::new(other.id, target.id, "follows")
            .with_source_event_ids(vec![e1, e2]),
    )
    .await?;
    repo.create_relation(
        CreateEntityRelation::new(other.id, source.id, "follows")
            .with_source_event_ids(vec![e3, e4]),
    )
    .await?;

    repo.merge_entities(source.id, target.id).await?;

    let relations = repo
        .get_entity_relations(target.id, Some("follows"), true)
        .await?;
    assert_eq!(
        relations.len(),
        1,
        "expected exactly one surviving other->target follows relation, got {:?}",
        relations
    );
    let survivor = &relations[0];
    assert_eq!(survivor.from_entity_id, other.id);
    assert_eq!(survivor.to_entity_id, target.id);

    let mut got: Vec<_> = survivor
        .source_event_ids
        .iter()
        .map(|id| *id.as_uuid())
        .collect();
    got.sort();
    let mut want: Vec<_> = [e1, e2, e3, e4].iter().map(|id| *id.as_uuid()).collect();
    want.sort();
    assert_eq!(
        got, want,
        "merge dropped source_event_ids from the duplicate incoming relation"
    );
    Ok(())
}

/// Disjoint relations (no triple collision) must be preserved as-is when their
/// owning entity is merged. The (target -> C) and the rewired (target -> D)
/// relations remain distinct rows after merging source into target.
#[sinex_test]
async fn merge_disjoint_relations_preserved(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.knowledge_graph();

    let target = repo
        .create_entity(CreateEntity::person("merge-disjoint-target"))
        .await?;
    let source = repo
        .create_entity(CreateEntity::person("merge-disjoint-source"))
        .await?;
    let c = repo
        .create_entity(CreateEntity::person("merge-disjoint-c"))
        .await?;
    let d = repo
        .create_entity(CreateEntity::person("merge-disjoint-d"))
        .await?;

    let e1 = Id::<Event<JsonValue>>::new();
    let e2 = Id::<Event<JsonValue>>::new();

    repo.create_relation(
        CreateEntityRelation::new(target.id, c.id, "knows").with_source_event_ids(vec![e1]),
    )
    .await?;
    repo.create_relation(
        CreateEntityRelation::new(source.id, d.id, "knows").with_source_event_ids(vec![e2]),
    )
    .await?;

    repo.merge_entities(source.id, target.id).await?;

    let relations = repo
        .get_entity_relations(target.id, Some("knows"), true)
        .await?;
    assert_eq!(
        relations.len(),
        2,
        "expected both disjoint relations to survive on the target, got {:?}",
        relations
    );

    let to_c = relations
        .iter()
        .find(|r| r.to_entity_id == c.id)
        .expect("target -> c knows relation should still exist");
    let to_d = relations
        .iter()
        .find(|r| r.to_entity_id == d.id)
        .expect("target -> d knows relation should have been rewired from source");

    let to_c_ids: Vec<_> = to_c.source_event_ids.iter().map(|i| *i.as_uuid()).collect();
    let to_d_ids: Vec<_> = to_d.source_event_ids.iter().map(|i| *i.as_uuid()).collect();
    assert_eq!(to_c_ids, vec![*e1.as_uuid()]);
    assert_eq!(to_d_ids, vec![*e2.as_uuid()]);

    Ok(())
}

/// `merge_entities` should be safe to call a second time on an already-merged
/// pair: the first call rewired everything, so the second call has no
/// duplicate relations to union and no remaining source-side rows to rewire.
/// This guards against accidental array-mutation on idempotent retries.
#[sinex_test]
async fn merge_entities_idempotent(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.knowledge_graph();

    let target = repo
        .create_entity(CreateEntity::person("merge-idempotent-target"))
        .await?;
    let source = repo
        .create_entity(CreateEntity::person("merge-idempotent-source"))
        .await?;
    let other = repo
        .create_entity(CreateEntity::person("merge-idempotent-other"))
        .await?;

    let e1 = Id::<Event<JsonValue>>::new();
    let e2 = Id::<Event<JsonValue>>::new();
    let e3 = Id::<Event<JsonValue>>::new();

    repo.create_relation(
        CreateEntityRelation::new(target.id, other.id, "knows")
            .with_source_event_ids(vec![e1, e2]),
    )
    .await?;
    repo.create_relation(
        CreateEntityRelation::new(source.id, other.id, "knows")
            .with_source_event_ids(vec![e3]),
    )
    .await?;

    repo.merge_entities(source.id, target.id).await?;

    let after_first = repo
        .get_entity_relations(target.id, Some("knows"), true)
        .await?;
    assert_eq!(after_first.len(), 1);
    let mut ids_after_first: Vec<_> = after_first[0]
        .source_event_ids
        .iter()
        .map(|i| *i.as_uuid())
        .collect();
    ids_after_first.sort();

    // Second merge of the (now-merged) source into target. The source row is
    // already flagged as merged_into_id = target, so the call is allowed to
    // be a structural no-op for the relation arrays.
    repo.merge_entities(source.id, target.id).await?;

    let after_second = repo
        .get_entity_relations(target.id, Some("knows"), true)
        .await?;
    assert_eq!(after_second.len(), 1);
    let mut ids_after_second: Vec<_> = after_second[0]
        .source_event_ids
        .iter()
        .map(|i| *i.as_uuid())
        .collect();
    ids_after_second.sort();

    assert_eq!(
        ids_after_first, ids_after_second,
        "second merge mutated source_event_ids on already-merged pair"
    );
    Ok(())
}
