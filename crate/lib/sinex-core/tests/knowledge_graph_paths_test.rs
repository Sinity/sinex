use sinex_core::db::repositories::{CreateEntity, CreateEntityRelation};
use sinex_core::repositories::DbPoolExt;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn find_paths_returns_direct_and_indirect(ctx: TestContext) -> color_eyre::Result<()> {
    let repo = ctx.pool.knowledge_graph();

    let entity_a = repo.create_entity(CreateEntity::person("Alice")).await?;
    let entity_b = repo
        .create_entity(CreateEntity::project("Project Beta"))
        .await?;
    let entity_c = repo
        .create_entity(CreateEntity::topic("Graph Theory"))
        .await?;

    let direct = repo
        .create_relation(CreateEntityRelation::new(
            entity_a.id,
            entity_c.id,
            "related_to",
        ))
        .await?;
    let step_one = repo
        .create_relation(CreateEntityRelation::new(
            entity_a.id,
            entity_b.id,
            "works_on",
        ))
        .await?;
    let step_two = repo
        .create_relation(CreateEntityRelation::new(
            entity_b.id,
            entity_c.id,
            "related_to",
        ))
        .await?;

    let shallow_paths = repo.find_paths(entity_a.id, entity_c.id, 1).await?;
    assert_eq!(shallow_paths.len(), 1);
    assert_eq!(shallow_paths[0].len(), 1);
    assert_eq!(shallow_paths[0][0].id, direct.id);

    let paths = repo.find_paths(entity_a.id, entity_c.id, 3).await?;
    assert_eq!(paths.len(), 2);

    let direct_path = paths.iter().find(|p| p.len() == 1).expect("direct path");
    assert_eq!(direct_path[0].id, direct.id);
    assert_eq!(direct_path[0].from_entity_id, entity_a.id);
    assert_eq!(direct_path[0].to_entity_id, entity_c.id);

    let indirect_path = paths.iter().find(|p| p.len() == 2).expect("indirect path");
    assert_eq!(indirect_path[0].id, step_one.id);
    assert_eq!(indirect_path[1].id, step_two.id);
    assert_eq!(
        indirect_path[0].to_entity_id,
        indirect_path[1].from_entity_id
    );

    Ok(())
}
