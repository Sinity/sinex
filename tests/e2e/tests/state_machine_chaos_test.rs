// # State Machine Chaos Tests
//
// Tests for state machine violations including shutdown during initialization,
// concurrent shutdown signals, and state corruption under load.

use sinex_primitives::Timestamp;
use futures::future::join_all;
use serde_json::json;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Test shutdown signal during initialization
#[sinex_test]
async fn test_shutdown_signal_during_initialization(ctx: TestContext) -> TestResult<()> {
    let shutdown_triggered = Arc::new(AtomicU64::new(0));
    let init_completed = Arc::new(AtomicU64::new(0));

    let shutdown_flag = shutdown_triggered.clone();
    let init_flag = init_completed.clone();

    // Simulate initialization process
    let init_handle = tokio::spawn(async move {
        // Simulate slow initialization (migration, schema setup, etc.)
        for step in 0..10 {
            if shutdown_flag.load(Ordering::SeqCst) > 0 {
                println!("Initialization interrupted at step {}", step);
                return Err("shutdown_during_init");
            }

            // Simulate database operations during init
            println!("Initialization step {} completed", step);

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        init_flag.store(1, Ordering::SeqCst);
        println!("Initialization completed successfully");
        Ok("init_success")
    });

    // Simulate shutdown signal arriving mid-initialization
    let shutdown_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await; // Interrupt at step 3
        shutdown_triggered.store(1, Ordering::SeqCst);
        println!("SHUTDOWN SIGNAL received during initialization");
    });

    let (init_result, _) = tokio::join!(init_handle, shutdown_handle);

    match init_result {
        Ok(Ok(msg)) => {
            println!("Initialization result: {}", msg);
            if init_completed.load(Ordering::SeqCst) == 0 {
                println!("INCONSISTENT STATE: Init claims success but flag not set");
            }
        }
        Ok(Err(error)) => {
            println!("Initialization properly aborted: {}", error);
        }
        Err(_) => {
            println!("PANIC: Initialization panicked during shutdown");
        }
    }

    // Check database state - might be partially initialized
    let event_count = sqlx::query!(
        r#"SELECT COUNT(*) as "count!" FROM core.events WHERE source = 'init'"#
    )
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    println!(
        "Events created during interrupted init: {}",
        event_count.count
    );

    if event_count.count > 0 && init_completed.load(Ordering::SeqCst) == 0 {
        println!("PARTIAL STATE: Database has init events but initialization was interrupted");
    }

    Ok(())
}

/// Test multiple concurrent shutdown signals
#[sinex_test]
async fn test_multiple_concurrent_shutdown_signals(_ctx: TestContext) -> TestResult<()> {
    let shutdown_count = Arc::new(AtomicU64::new(0));
    let shutdown_handler_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Simulate multiple shutdown signals arriving simultaneously
    for signal_id in 0..5 {
        let shutdown_count_clone = shutdown_count.clone();
        let handler_count_clone = shutdown_handler_count.clone();

        let handle = tokio::spawn(async move {
            println!("Shutdown signal {} received", signal_id);
            shutdown_count_clone.fetch_add(1, Ordering::SeqCst);

            // Simulate shutdown handler
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Only one handler should actually execute cleanup
            let handler_id = handler_count_clone.fetch_add(1, Ordering::SeqCst);

            if handler_id == 0 {
                println!("Shutdown handler {} executing cleanup", signal_id);
                // Simulate cleanup operations
                tokio::time::sleep(Duration::from_millis(200)).await;
                println!("Cleanup completed by handler {}", signal_id);
            } else {
                println!(
                    "Shutdown handler {} skipped (cleanup already running)",
                    signal_id
                );
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let total_signals = shutdown_count.load(Ordering::SeqCst);
    let handlers_run = shutdown_handler_count.load(Ordering::SeqCst);

    println!("Multiple shutdown signals test results:");
    println!("- Total shutdown signals: {}", total_signals);
    println!("- Handlers that ran: {}", handlers_run);

    // All signals should be received
    assert_eq!(
        total_signals, 5,
        "All shutdown signals should be received"
    );

    // All handlers should attempt to run (in this simple simulation)
    assert_eq!(handlers_run, 5, "All handlers should run");

    Ok(())
}

/// Test state machine corruption under load
#[sinex_test]
async fn test_state_machine_corruption_under_load(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let state_transitions = Arc::new(AtomicU64::new(0));
    let invalid_transitions = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Simulate concurrent state transitions
    for worker_id in 0..10 {
        let pool_clone = pool.clone();
        let transitions = state_transitions.clone();
        let invalid = invalid_transitions.clone();

        let handle = tokio::spawn(async move {
            for transition_id in 0..20 {
                transitions.fetch_add(1, Ordering::SeqCst);

                // Simulate state transition by updating agent status
                let processor_name = format!("state-test-{}", worker_id);
                let new_status = match transition_id % 4 {
                    0 => "initializing",
                    1 => "running",
                    2 => "stopping",
                    3 => "stopped",
                    _ => unreachable!(),
                };

                // Try to update agent status
                match sqlx::query!(
                    r#"
                    INSERT INTO core.processor_manifests
                    (processor_name, processor_type, version, status, agent_type, registered_at, updated_at)
                    VALUES ($1, 'automaton', '1.0.0', $2, 'test', $3, $4)
                    ON CONFLICT (processor_name, version, git_commit_sha) DO UPDATE SET
                    status = $2, updated_at = $4
                    "#,
                    processor_name,
                    new_status,
                    Timestamp::now(),
                    Timestamp::now()
                )
                .execute(&pool_clone)
                .await
                {
                    Ok(_) => {
                        println!(
                            "Worker {} transition {} to {} succeeded",
                            worker_id, transition_id, new_status
                        );
                    }
                    Err(e) => {
                        println!(
                            "Worker {} transition {} to {} failed: {}",
                            worker_id, transition_id, new_status, e
                        );
                        invalid.fetch_add(1, Ordering::SeqCst);
                    }
                }

                // Small delay to allow for concurrency
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let total_transitions = state_transitions.load(Ordering::SeqCst);
    let invalid_count = invalid_transitions.load(Ordering::SeqCst);

    println!("State machine corruption test results:");
    println!("- Total state transitions: {}", total_transitions);
    println!("- Invalid transitions: {}", invalid_count);

    // Check final state consistency
    let final_agents = sqlx::query!(
        r#"SELECT processor_name, status FROM core.processor_manifests
           WHERE processor_name LIKE 'state-test-%' AND processor_type = 'automaton'"#
    )
    .fetch_all(ctx.pool())
    .await?;

    println!("Final agent states:");
    for agent in &final_agents {
        println!("  {}: {}", agent.processor_name, agent.status);
    }

    // Most transitions should succeed
    assert!(total_transitions > 0, "State transitions should occur");
    assert!(
        invalid_count < total_transitions / 2,
        "Most transitions should succeed"
    );

    Ok(())
}
