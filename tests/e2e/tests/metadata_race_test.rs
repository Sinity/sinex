use serde_json::json;
use sinex_db::models::SourceMaterial;
use sinex_db::repositories::SourceMaterialRepository;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Barrier;
use xtask::sandbox::prelude::*;
use xtask::sandbox::{sinex_test, TestContext, TestResult};

#[sinex_test]
async fn test_metadata_update_race_condition(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let repo = SourceMaterialRepository::new(&pool);

    // 1. Create a source material with initial metadata
    let mut material = SourceMaterial::file("/tmp/race_test.log");
    material = material.with_metadata(json!({
        "config": {
            "initial": true
        }
    }));

    let record = repo.register_material(material).await?;
    let material_id = record.id;
    let material_id_str = material_id.to_string();

    // 2. Spawn two concurrent tasks to update different fields in the same nested object
    let barrier = Arc::new(Barrier::new(2));
    let success_count = Arc::new(AtomicUsize::new(0));

    let pool1 = pool.clone();
    let barrier1 = barrier.clone();
    let success1 = success_count.clone();
    let id1 = material_id.clone();

    let task1 = tokio::spawn(async move {
        let repo = SourceMaterialRepository::new(&pool1);
        barrier1.wait().await;

        // Update config.worker_1
        let update = json!({
            "config": {
                "worker_1": "done"
            }
        });

        if let Ok(_) = repo.update_metadata(id1, update).await {
            success1.fetch_add(1, Ordering::SeqCst);
        }
    });

    let pool2 = pool.clone();
    let barrier2 = barrier.clone();
    let success2 = success_count.clone();
    let id2 = material_id.clone();

    let task2 = tokio::spawn(async move {
        let repo = SourceMaterialRepository::new(&pool2);
        barrier2.wait().await;

        // Update config.worker_2
        let update = json!({
            "config": {
                "worker_2": "done"
            }
        });

        if let Ok(_) = repo.update_metadata(id2, update).await {
            success2.fetch_add(1, Ordering::SeqCst);
        }
    });

    // Wait for both to finish
    let _ = tokio::join!(task1, task2);

    assert_eq!(
        success_count.load(Ordering::SeqCst),
        2,
        "Both updates should succeed at DB level"
    );

    // 3. Verify the final state
    let final_record = repo
        .get_by_id(material_id)
        .await?
        .expect("Material should exist");
    let metadata = final_record.metadata;

    println!("Final metadata: {}", metadata);

    let config = metadata.get("config").expect("config should exist");

    // Check if we lost data
    let has_worker_1 = config.get("worker_1").map(|v| v == "done").unwrap_or(false);
    let has_worker_2 = config.get("worker_2").map(|v| v == "done").unwrap_or(false);

    // With a shallow merge (||), one of these will likely be missing if they ran concurrently
    // If we had deep merge, both would be present.
    //
    // Note: This test is probabilistic, but with the barrier it's highly likely to trigger the race.
    // If the test passes (both present), it means either:
    // a) The database already does deep merging (unlikely for ||)
    // b) We got lucky and they ran sequentially

    if has_worker_1 && has_worker_2 {
        println!("Both updates persisted (Deep merge or lucky sequence)");
    } else if has_worker_1 {
        println!("Lost update 2 (Shallow merge behavior confirmed)");
        // This is what we expect to fail if we assertion failure implies we want deep merge
        // BUT for a reproduction test, we might want to assert that it FAILS to confirm the bug?
        // Or we assert that it SUCCEEDS and expect the test to fail initially?
        // Let's assert success, so the test fails, demonstrating the bug.
    } else if has_worker_2 {
        println!("Lost update 1 (Shallow merge behavior confirmed)");
    } else {
        println!("Lost both updates? (Should not happen)");
    }

    assert!(has_worker_1, "worker_1 update should be present");
    assert!(has_worker_2, "worker_2 update should be present");

    Ok(())
}
