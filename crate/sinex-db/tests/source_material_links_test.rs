use serde_json::json;
use sinex_db::repositories::{DbPoolExt, SourceMaterialLink, source_material_relation_types};
use uuid::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn source_material_links_are_idempotent_and_queryable(ctx: TestContext) -> TestResult<()> {
    let row_stream = ctx
        .create_source_material(Some("sqlite-link-row-stream"))
        .await?
        .to_uuid();
    let snapshot = ctx
        .create_source_material(Some("sqlite-link-snapshot"))
        .await?
        .to_uuid();

    let repo = ctx.pool.source_materials();
    let first = repo
        .link_backing_material(
            row_stream,
            snapshot,
            json!({"format": "jsonl", "origin": "terminal-history"}),
        )
        .await?;
    let second = repo
        .link_backing_material(row_stream, snapshot, json!({"snapshot_kind": "sqlite"}))
        .await?;

    assert_eq!(first.id, second.id, "duplicate link should be idempotent");
    assert_eq!(second.from_material_id, row_stream);
    assert_eq!(second.to_material_id, snapshot);
    assert_eq!(
        second.relation_type,
        source_material_relation_types::BACKED_BY
    );
    assert_eq!(second.metadata["format"], "jsonl");
    assert_eq!(second.metadata["origin"], "terminal-history");
    assert_eq!(second.metadata["snapshot_kind"], "sqlite");

    let from_links = repo.links_from(row_stream).await?;
    assert_eq!(from_links.len(), 1);
    assert_eq!(from_links[0].id, first.id);

    let to_links = repo.links_to(snapshot).await?;
    assert_eq!(to_links.len(), 1);
    assert_eq!(to_links[0].id, first.id);

    let touching_links = repo.links_for_materials(&[row_stream]).await?;
    assert_eq!(touching_links.len(), 1);
    assert_eq!(touching_links[0].id, first.id);

    Ok(())
}

#[sinex_test]
async fn source_material_links_reject_invalid_edges(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("sqlite-link-invalid"))
        .await?
        .to_uuid();
    let repo = ctx.pool.source_materials();

    let self_link = repo
        .link_materials(SourceMaterialLink::backed_by(material_id, material_id))
        .await;
    assert!(self_link.is_err(), "self links should fail before SQL");

    let invalid_relation = repo
        .link_materials(SourceMaterialLink::new(
            material_id,
            Uuid::now_v7(),
            "BackedBy",
        ))
        .await;
    assert!(
        invalid_relation.is_err(),
        "relation types must use the canonical lower-case wire format"
    );

    let missing_material = repo
        .link_materials(SourceMaterialLink::backed_by(material_id, Uuid::now_v7()))
        .await;
    assert!(
        missing_material.is_err(),
        "links should keep foreign-key integrity to source materials"
    );

    Ok(())
}
