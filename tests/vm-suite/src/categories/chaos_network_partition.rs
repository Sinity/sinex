//! Chaos test: simulate network partition between event_engine and NATS.
//!
//! Injects a network partition on the loopback interface targeting NATS port 4222,
//! verifies event_engine remains active and recovers when partition is healed.

use std::time::Duration;

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::{TestOutcome, TestRunner};

use super::chaos_support::{
    command_status, observed_event_count, report_event_count_increase, report_service_active,
    report_watched_files_written,
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

    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if !report_watched_files_written(runner, name, "chaos-baseline", 10, "baseline") {
        return;
    }

    // Wait for events to be captured
    tokio::time::sleep(Duration::from_secs(5)).await;

    let Some(after) = observed_event_count(runner, name, pool).await else {
        return;
    };
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

    let mut failed_injections = Vec::new();
    for rule in inject_rules {
        if !command_status("sh", &["-c", rule]) {
            failed_injections.push(rule.to_string());
        }
    }

    // Also inject packet loss on loopback via tc (traffic control)
    for rule in [
        "tc qdisc add dev lo root handle 1: prio",
        "tc qdisc add dev lo parent 1:3 handle 30: netem loss 100%",
        "tc filter add dev lo protocol ip parent 1:0 prio 3 u32 match ip dport 4222 0xffff flowid 1:3",
    ] {
        if !command_status("sh", &["-c", rule]) {
            failed_injections.push(rule.to_string());
        }
    }

    if !failed_injections.is_empty() {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            &format!(
                "network partition was not fully injected; failed commands: {}",
                failed_injections.join("; ")
            ),
        );
        return;
    }

    // Wait for partition to stabilize
    tokio::time::sleep(Duration::from_secs(3)).await;

    report_service_active(
        runner,
        name,
        "event_engine crashed during NATS partition injection",
    );
}

async fn test_during_partition_period(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: event_engine survives during-partition period";

    let Some(_before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if !report_watched_files_written(runner, name, "chaos-during", 20, "during") {
        return;
    }

    // Wait to allow event_engine to process (even if buffered)
    tokio::time::sleep(Duration::from_secs(5)).await;

    report_service_active(
        runner,
        name,
        "event_engine became inactive during partition period",
    );
}

async fn test_partition_healed_event_engine_active(runner: &mut TestRunner, _pool: &PgPool) {
    let name = "chaos-network-partition: partition healed, event_engine still active";

    // Heal the partition
    let _ = command_status("sh", &["-c", "iptables -F INPUT"]);
    let _ = command_status("sh", &["-c", "iptables -F OUTPUT"]);
    let _ = command_status("sh", &["-c", "tc qdisc del dev lo root"]);

    // Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(10)).await;

    report_service_active(runner, name, "event_engine crashed after partition heal");
}

async fn test_events_reach_db_after_heal(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: events reach DB after partition heal";

    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if !report_watched_files_written(runner, name, "chaos-post-heal", 10, "post-heal") {
        return;
    }

    report_event_count_increase(
        runner,
        name,
        pool,
        before,
        Duration::from_secs(30),
        Duration::from_secs(2),
        |before| format!("no events reached DB after 30s of partition heal (before={before})"),
    )
    .await;
}
