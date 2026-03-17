//! Chaos test: simulate network partition between ingestd and NATS.
//!
//! Injects a network partition on the loopback interface targeting NATS port 4222,
//! verifies ingestd remains active and recovers when partition is healed.

use std::process::Command;
use std::time::{Duration, Instant};

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::TestRunner;

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Network Partition tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_baseline_pipeline(runner, &pool).await;
    test_partition_ingestd_survives(runner, &pool).await;
    test_during_partition_period(runner, &pool).await;
    test_partition_healed_ingestd_active(runner, &pool).await;
    test_events_reach_db_after_heal(runner, &pool).await;

    Ok(())
}

async fn test_baseline_pipeline(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: baseline pipeline is working";

    let before = event_count(pool).await;
    let watched = "/var/lib/sinex/watched";
    let _ = std::fs::create_dir_all(watched);

    // Generate 10 baseline events
    for i in 0..10_u32 {
        let _ = std::fs::write(
            format!("{watched}/chaos-baseline-{i}.txt"),
            format!("baseline {i}"),
        );
    }

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

async fn test_partition_ingestd_survives(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: ingestd survives NATS partition";

    // Inject iptables rules to drop traffic to NATS port 4222
    let inject_rules = vec![
        "iptables -A INPUT -p tcp --dport 4222 -j DROP",
        "iptables -A OUTPUT -p tcp --dport 4222 -j DROP",
    ];

    for rule in inject_rules {
        let _ = Command::new("sh")
            .args(["-c", rule])
            .status();
    }

    // Also inject packet loss on loopback via tc (traffic control)
    let _ = Command::new("sh")
        .args(["-c", "tc qdisc add dev lo root handle 1: prio"])
        .status();
    let _ = Command::new("sh")
        .args(["-c", "tc qdisc add dev lo parent 1:3 handle 30: netem loss 100%"])
        .status();
    let _ = Command::new("sh")
        .args(["-c", "tc filter add dev lo protocol ip parent 1:0 prio 3 u32 match ip dport 4222 0xffff flowid 1:3"])
        .status();

    // Wait for partition to stabilize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check ingestd is still active
    let active = Command::new("systemctl")
        .args(["is-active", "--quiet", "sinex-ingestd"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if active {
        runner.pass(name);
    } else {
        runner.fail(name, "ingestd crashed during NATS partition injection");
    }
}

async fn test_during_partition_period(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: ingestd survives during-partition period";

    let before = event_count(pool).await;
    let watched = "/var/lib/sinex/watched";

    // Generate events during partition
    for i in 0..20_u32 {
        let _ = std::fs::write(
            format!("{watched}/chaos-during-{i}.txt"),
            format!("during {i}"),
        );
    }

    // Wait to allow ingestd to process (even if buffered)
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check ingestd still active during partition
    let active = Command::new("systemctl")
        .args(["is-active", "--quiet", "sinex-ingestd"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if active {
        runner.pass(name);
    } else {
        runner.fail(name, "ingestd became inactive during partition period");
    }
}

async fn test_partition_healed_ingestd_active(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: partition healed, ingestd still active";

    // Heal the partition
    let _ = Command::new("sh")
        .args(["-c", "iptables -F INPUT"])
        .status();
    let _ = Command::new("sh")
        .args(["-c", "iptables -F OUTPUT"])
        .status();
    let _ = Command::new("sh")
        .args(["-c", "tc qdisc del dev lo root"])
        .status();

    // Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify ingestd is active
    let active = Command::new("systemctl")
        .args(["is-active", "--quiet", "sinex-ingestd"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if active {
        runner.pass(name);
    } else {
        runner.fail(name, "ingestd crashed after partition heal");
    }
}

async fn test_events_reach_db_after_heal(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-network-partition: events reach DB after partition heal";

    let before = event_count(pool).await;
    let watched = "/var/lib/sinex/watched";

    // Generate post-heal events
    for i in 0..10_u32 {
        let _ = std::fs::write(
            format!("{watched}/chaos-post-heal-{i}.txt"),
            format!("post-heal {i}"),
        );
    }

    // Wait with deadline for events to be captured
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after = event_count(pool).await;
        if after > before {
            runner.pass(name);
            return;
        }
        if Instant::now() >= deadline {
            runner.fail(
                name,
                &format!("no events reached DB after 30s of partition heal (before={before}, after={after})"),
            );
            return;
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn event_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
}
