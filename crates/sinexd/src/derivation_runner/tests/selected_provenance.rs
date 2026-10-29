use super::*;

#[devkit::test]
async fn stored_product_versions_follow_exact_output_parents(db: devkit::pg::TestDatabase) {
    let world = World::with_keys(db, vec![]).await;
    let selected = zsh("selected fixture", 0, 0);
    let mut unused = zsh("unused fixture", 1, 1);
    unused.source = "unselected.reader".into();
    world.admit(vec![selected.clone(), unused.clone()]).await;
    let payload: CommandImported = serde_json::from_value(selected.payload.clone()).unwrap();
    let output = sinex_derivations::derived_event::derived(
        &payload,
        "analysis.products",
        [&selected],
    );
    let mut computed = Computed {
        outputs: vec![output],
        ..Computed::default()
    };
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    selected::stamp(
        &mut conn,
        &crate::analysis::stored::StoredProducts,
        &json!({"request":"synthetic"}),
        &[selected, unused],
        None,
        7,
        &mut computed,
    )
    .await
    .unwrap();
    let versions = computed.outputs[0].payload["producer_versions"].as_array().unwrap();
    assert!(versions.iter().any(|row| row["producer"] == "terminal.zsh"));
    assert!(versions.iter().all(|row| row["producer"] != "unselected.reader"));
}
