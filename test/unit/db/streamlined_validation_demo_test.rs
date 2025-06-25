//! Demonstration of streamlined test patterns using the new abstractions

use crate::common::prelude::*;
use crate::common::{parameterized, scenario_builders, test_dsl};

#[test]
fn test_validation_with_parameterized_helper() {
    // Before: 25+ lines of repetitive code
    // After: 10 lines with clear test cases
    
    let test_cases = vec![
        // (name, payload, should_succeed)
        ("valid filesystem event", json!({"path": "/home/test.txt", "size": 1024}), true),
        ("missing path", json!({"size": 1024}), false),
        ("empty path", json!({"path": "", "size": 1024}), false),
        ("relative path", json!({"path": "relative/path", "size": 1024}), false),
        ("unicode path", json!({"path": "/home/用户/文档.txt", "size": 1024}), true),
    ];
    
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        parameterized::test_validation_pairs(test_cases, |payload| {
            RawEventBuilder::new("filesystem", "file.created", payload).build()
        }).await;
    });
}

#[sinex_test]
async fn test_event_scenarios_with_builder(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = crate::common::create_test_db_pool().await.unwrap();
    
    // Before: 50+ lines of setup, insertion, and verification
    // After: Clear, declarative scenario
    
    scenario_builders::EventScenarioBuilder::new()
        .with_filesystem_event("/valid/path.txt", true)
        .with_filesystem_event("", false)
        .with_terminal_event("ls -la", true)
        .with_terminal_event("", false)
        .with_validation(|event| event.payload["size"].as_u64().is_some())
        .execute(&pool)
        .await
        .unwrap();
    Ok(())
}

#[sinex_test]
async fn test_worker_scenario(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = crate::common::create_test_db_pool().await.unwrap();
    
    // Before: 100+ lines of worker setup, execution, and verification
    // After: Declarative worker scenario
    
    let result = scenario_builders::WorkerScenarioBuilder::new("test_worker")
        .with_events(20)
        .with_workers(3)
        .with_failures(vec![5, 10]) // Events 5 and 10 will fail
        .execute(&pool)
        .await
        .unwrap();
    
    // Verify distribution across workers
    pretty_assertions::assert_eq!(result.total_processed, 20);
    assert!(result.worker_stats.len() == 3);
    
    // All workers should have participated
    for (worker, count) in &result.worker_stats {
        assert!(*count > 0, "Worker {} should have processed events", worker);
    }
    Ok(())
}

#[sinex_test]
async fn test_complex_pipeline_with_dsl(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let pool = crate::common::create_test_db_pool().await.unwrap();
    
    // Before: 150+ lines of complex test orchestration
    // After: Clear, readable test scenario
    
    use crate::common::events;
    use test_dsl::TestScenario;
    
    TestScenario::new("Complex event pipeline test")
        .insert_event(events::filesystem_event("file.created", "/test1.txt"))
        .insert_event(events::filesystem_event("file.created", "/test2.txt"))
        .verify_event_count(2)
        .insert_event(events::kitty_event("echo 'test'"))
        .verify_event_count(3)
        .run_worker("test_worker")
        .verify_worker_processed("test_worker", 3)
        .custom_step(|pool| {
            // Custom verification logic
            println!("Running custom verification");
            Ok(())
        })
        .execute(&pool)
        .await
        .unwrap();
    Ok(())
}

#[test]
fn test_multiple_validation_rules_streamlined() {
    // Before: 50+ lines with repetitive validator creation and assertions
    // After: Concise parameterized test
    
    use crate::common::validation_test_utils;
    
    let event_creators = vec![
        ("filesystem", |p| RawEventBuilder::new("filesystem", "file.created", p).build();
        ("terminal", |p| RawEventBuilder::new("terminal_kitty", "command.executed", p).build();
        ("window", |p| RawEventBuilder::new("hyprland", "window.focus", p).build();
    ];
    
    for (name, creator) in event_creators {
        println!("Testing {} events", name);
        
        // Valid event
        let valid_event = match name {
            "filesystem" => creator(json!({"path": "/test.txt", "size": 1024});
            "terminal" => creator(json!({"command": "ls", "exit_code": 0});
            "window" => creator(json!({"window_id": 123, "title": "Test"});
            _ => unreachable!(),
        };
        validation_test_utils::assert_valid_event(&valid_event);
        
        // Invalid event (empty payload)
        let invalid_event = creator(json!({});
        validation_test_utils::assert_invalid_event(&invalid_event, "");
    }
}

// Example of how a complex concurrent test can be simplified
#[sinex_test]
async fn test_concurrent_operations_streamlined(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    use crate::common::parallelization;
    
    let pool = Arc::new(crate::common::create_test_db_pool().await.unwrap());
    
    // Before: 80+ lines of manual concurrent task management
    // After: Clear parallel test execution
    
    let operations: Vec<_> = (0..10).map(|i| {
        let pool = ctx.pool().clone();
        move |p: Arc<sqlx::PgPool>| async move {
            let event = RawEventBuilder::new(
                "filesystem",
                "file.created",
                json!({"path": format!("/test_{}.txt", i), "size": i * 1024})
            ).build();
            
            crate::common::insert_test_event(&*p, &event).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
    }).collect();
    
    let results = parallelization::ParallelTestExecutor::new(5)
        .execute_db_parallel(ctx.pool().clone(), operations)
        .await;
    
    // Verify all succeeded
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "Operation {} failed: {:?}", i, result);
    }
    
    // Verify count
    let count = crate::common::get_event_count(&pool).await.unwrap();
    pretty_assertions::assert_eq!(count, 10);
    Ok(())
}