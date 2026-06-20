//! Chaos test: simulate network partition between event_engine and NATS.
//!
//! Injects a network partition on the loopback interface targeting NATS port 4222,
//! verifies event_engine remains active and recovers when partition is healed.

use std::time::Duration;

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::TestRunner;

use super::chaos_support::{
    SINEXD_SERVICE, command_status, event_count, service_is_active, wait_for_event_count_increase,
    write_watched_files,
};

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Network Partition tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_baseline_pipeline(runner, &pool).await;
    test_partition_event_engine_survives(runner, &pool).await;
    test_during_partition_period(runner, &pool).await;
    test_partition_healed_event_engine_active(runner, &pool).await;
    test_events_reach_db_after_heal(runner, &pool).await;

    Ok(())
}

async fn test_baseline_pipeline(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: baseline pipeline is working";

    let before = event_count(pool).await;
    write_watched_files("chaos-baseline", 10, "baseline");

    // Wait for events to be captured
    tokio::time::sleep(Duration::from_secs(5)).await;

    let after = event_count(pool).await;
    if after > before {
        runner.pass(name);
    } else {
        runner.fail(
            name,
            &format!("no baseline events captured (before={before}, after={after})"),
        );
    }
}

async fn test_partition_event_engine_survives(runner: &mut TestRunner, _pool: &PgPool) {
    let name = "chaos-network-partition: event_engine survives NATS partition";

    // Inject iptables rules to drop traffic to NATS port 4222
    let inject_rules = vec![
        "iptables -A INPUT -p tcp --dport 4222 -j DROP",
        "iptables -A OUTPUT -p tcp --dport 4222 -j DROP",
    ];

    for rule in inject_rules {
        let _ = command_status("sh", &["-c", rule]);
    }

    // Also inject packet loss on loopback via tc (traffic control)
    let _ = command_status("sh", &["-c", "tc qdisc add dev lo root handle 1: prio"]);
    let _ = command_status(
        "sh",
        &[
            "-c",
            "tc qdisc add dev lo parent 1:3 handle 30: netem loss 100%",
        ],
    );
    let _ = command_status(
        "sh",
        &[
            "-c",
            "tc filter add dev lo protocol ip parent 1:0 prio 3 u32 match ip dport 4222 0xffff flowid 1:3",
        ],
    );

    // Wait for partition to stabilize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check event_engine is still active
    if service_is_active(SINEXD_SERVICE) {
        runner.pass(name);
    } else {
        runner.fail(name, "event_engine crashed during NATS partition injection");
    }
}

async fn test_during_partition_period(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: event_engine survives during-partition period";

    let _before = event_count(pool).await;
    write_watched_files("chaos-during", 20, "during");

    // Wait to allow event_engine to process (even if buffered)
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check event_engine still active during partition
    if service_is_active(SINEXD_SERVICE) {
        runner.pass(name);
    } else {
        runner.fail(name, "event_engine became inactive during partition period");
    }
}

async fn test_partition_healed_event_engine_active(runner: &mut TestRunner, _pool: &PgPool) {
    let name = "chaos-network-partition: partition healed, event_engine still active";

    // Heal the partition
    let _ = command_status("sh", &["-c", "iptables -F INPUT"]);
    let _ = command_status("sh", &["-c", "iptables -F OUTPUT"]);
    let _ = command_status("sh", &["-c", "tc qdisc del dev lo root"]);

    // Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify event_engine is active
    if service_is_active(SINEXD_SERVICE) {
        runner.pass(name);
    } else {
        runner.fail(name, "event_engine crashed after partition heal");
    }
}

async fn test_events_reach_db_after_heal(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: events reach DB after partition heal";

    let before = event_count(pool).await;
    write_watched_files("chaos-post-heal", 10, "post-heal");

    match wait_for_event_count_increase(
        pool,
        before,
        Duration::from_secs(30),
        Duration::from_secs(2),
    )
    .await
    {
        Some(_) => runner.pass(name),
        None => runner.fail(
            name,
            &format!("no events reached DB after 30s of partition heal (before={before})"),
        ),
    }
}
