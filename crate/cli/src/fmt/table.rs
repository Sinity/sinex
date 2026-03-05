use console::style;
use sinex_primitives::temporal::Timestamp;
use tabled::{builder::Builder, settings::Style};

use sinex_primitives::rpc::coordination::InstanceInfo;
use sinex_primitives::rpc::replay::{ReplayOperation, ReplayState};

/// Format nodes as a table
pub fn format_table_nodes(nodes: &[InstanceInfo]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["TYPE", "ID", "HOSTNAME", "LEADER", "LAST HEARTBEAT"]);

    for node in nodes {
        let leader_icon = if node.is_leader { "★" } else { "" };
        let heartbeat = node
            .last_heartbeat
            .as_ref()
            .map_or_else(|| style("none").dim().to_string(), format_heartbeat_age);

        builder.push_record([
            node.node_type.to_string(),
            short_id(&node.instance_id),
            node.hostname.as_deref().unwrap_or("-").to_string(),
            leader_icon.to_string(),
            heartbeat,
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

/// Format replay operations as a table
pub fn format_table_replay(operations: &[ReplayOperation]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["ID", "STATUS", "NODE", "CREATED"]);

    for op in operations {
        builder.push_record([
            short_id(&op.operation_id),
            format_replay_status(&op.state),
            op.scope.node_id.clone(),
            op.created_at.clone(),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

// ==================== Helper Functions ====================

/// Shorten a UUIDv7 to first 8 characters for display
fn short_id(id: &str) -> String {
    if id.len() > 8 {
        format!("{}...", &id[..8])
    } else {
        id.to_string()
    }
}

/// Format replay state with color
fn format_replay_status(state: &ReplayState) -> String {
    match state {
        ReplayState::Planning => style("planning").cyan().to_string(),
        ReplayState::Previewed => style("previewed").blue().to_string(),
        ReplayState::Approved => style("approved").blue().to_string(),
        ReplayState::Executing => style("executing").yellow().to_string(),
        ReplayState::Committing => style("committing").yellow().to_string(),
        ReplayState::Completed => style("completed").green().to_string(),
        ReplayState::Cancelled => style("cancelled").dim().to_string(),
        ReplayState::Failed => style("failed").red().to_string(),
    }
}

/// Format heartbeat timestamp as "X ago"
pub fn format_heartbeat_age(timestamp: &Timestamp) -> String {
    format_age(timestamp)
}

/// Format timestamp as "X ago" or "X from now"
fn format_age(timestamp: &Timestamp) -> String {
    let now = Timestamp::now();
    let duration = *now - **timestamp;

    if duration.whole_seconds() < 0 {
        // Future timestamp
        let abs_duration = -duration;
        if abs_duration.whole_seconds() < 60 {
            format!("in {}s", abs_duration.whole_seconds())
        } else if abs_duration.whole_minutes() < 60 {
            format!("in {}m", abs_duration.whole_minutes())
        } else if abs_duration.whole_hours() < 24 {
            format!("in {}h", abs_duration.whole_hours())
        } else {
            format!("in {}d", abs_duration.whole_days())
        }
    } else {
        // Past timestamp
        if duration.whole_seconds() < 60 {
            format!("{}s ago", duration.whole_seconds())
        } else if duration.whole_minutes() < 60 {
            format!("{}m ago", duration.whole_minutes())
        } else if duration.whole_hours() < 24 {
            format!("{}h ago", duration.whole_hours())
        } else {
            format!("{}d ago", duration.whole_days())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::domain::{HostName, InstanceId, NodeType};
    use sinex_primitives::rpc::coordination::InstanceInfo;
    use sinex_primitives::rpc::replay::{
        ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState,
    };
    use sinex_primitives::temporal::Duration;
    use sinex_primitives::temporal::Timestamp;
    use std::collections::HashMap;
    use xtask::sandbox::sinex_test;

    fn make_timestamp_seconds_ago(secs: i64) -> Timestamp {
        Timestamp::now() - Duration::seconds(secs)
    }

    // --- short_id tests ---

    #[sinex_test]
    async fn short_id_truncates_long_ids() -> TestResult<()> {
        assert_eq!(short_id("01HXYZ123456789ABCDEFGHIJK"), "01HXYZ12...");
        Ok(())
    }

    #[sinex_test]
    async fn short_id_preserves_short_ids() -> TestResult<()> {
        assert_eq!(short_id("abc"), "abc");
        Ok(())
    }

    #[sinex_test]
    async fn short_id_preserves_exactly_8_chars() -> TestResult<()> {
        assert_eq!(short_id("12345678"), "12345678");
        Ok(())
    }

    #[sinex_test]
    async fn short_id_truncates_9_chars() -> TestResult<()> {
        assert_eq!(short_id("123456789"), "12345678...");
        Ok(())
    }

    #[sinex_test]
    async fn short_id_empty_string() -> TestResult<()> {
        assert_eq!(short_id(""), "");
        Ok(())
    }

    // --- format_age tests ---

    #[sinex_test]
    async fn format_age_seconds() -> TestResult<()> {
        let ts = make_timestamp_seconds_ago(30);
        let result = format_age(&ts);
        assert!(
            result.ends_with("s ago"),
            "expected seconds ago, got: {result}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn format_age_minutes() -> TestResult<()> {
        let ts = make_timestamp_seconds_ago(120);
        let result = format_age(&ts);
        assert!(
            result.ends_with("m ago"),
            "expected minutes ago, got: {result}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn format_age_hours() -> TestResult<()> {
        let ts = make_timestamp_seconds_ago(7200);
        let result = format_age(&ts);
        assert!(
            result.ends_with("h ago"),
            "expected hours ago, got: {result}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn format_age_days() -> TestResult<()> {
        let ts = make_timestamp_seconds_ago(86400 * 3);
        let result = format_age(&ts);
        assert!(
            result.ends_with("d ago"),
            "expected days ago, got: {result}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn format_age_future() -> TestResult<()> {
        let ts = Timestamp::now() + Duration::seconds(120);
        let result = format_age(&ts);
        assert!(
            result.starts_with("in "),
            "expected future time, got: {result}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn format_age_zero() -> TestResult<()> {
        let ts = Timestamp::now();
        let result = format_age(&ts);
        // Should be "0s ago" or close to it
        assert!(
            result.ends_with("s ago"),
            "expected seconds ago, got: {result}"
        );
        Ok(())
    }

    // --- format_heartbeat_age delegates to format_age ---

    #[sinex_test]
    async fn format_heartbeat_age_delegates() -> TestResult<()> {
        let ts = make_timestamp_seconds_ago(45);
        let result = format_heartbeat_age(&ts);
        assert!(
            result.ends_with("s ago"),
            "expected seconds ago, got: {result}"
        );
        Ok(())
    }

    // --- format_replay_status tests ---

    #[sinex_test]
    async fn format_replay_status_all_states() -> TestResult<()> {
        // Verify all variants produce non-empty strings (color codes included)
        let states = vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Committing,
            ReplayState::Completed,
            ReplayState::Cancelled,
            ReplayState::Failed,
        ];
        for state in states {
            let result = format_replay_status(&state);
            assert!(!result.is_empty(), "empty status for {state:?}");
        }
        Ok(())
    }

    // --- format_table_nodes tests ---

    #[sinex_test]
    async fn format_table_nodes_empty() -> TestResult<()> {
        let result = format_table_nodes(&[]);
        // Should still produce a header row
        assert!(result.contains("TYPE"));
        assert!(result.contains("ID"));
        Ok(())
    }

    #[sinex_test]
    async fn format_table_nodes_single() -> TestResult<()> {
        let node = InstanceInfo {
            instance_id: InstanceId::new("01HXYZ123456789ABCDEFGHIJK"),
            node_type: NodeType::Ingestor,
            hostname: Some(HostName::new("testhost")),
            last_heartbeat: Some(Timestamp::now()),
            is_leader: true,
        };
        let result = format_table_nodes(&[node]);
        assert!(result.contains("ingestor"));
        assert!(result.contains("01HXYZ12..."));
        assert!(result.contains("testhost"));
        assert!(result.contains("★"));
        Ok(())
    }

    #[sinex_test]
    async fn format_table_nodes_no_heartbeat() -> TestResult<()> {
        let node = InstanceInfo {
            instance_id: InstanceId::new("SHORTID"),
            node_type: NodeType::Automaton,
            hostname: None,
            last_heartbeat: None,
            is_leader: false,
        };
        let result = format_table_nodes(&[node]);
        assert!(result.contains("automaton"));
        assert!(result.contains("SHORTID"));
        assert!(result.contains('-')); // hostname fallback
        Ok(())
    }

    // --- format_table_replay tests ---

    #[sinex_test]
    async fn format_table_replay_empty() -> TestResult<()> {
        let result = format_table_replay(&[]);
        assert!(result.contains("ID"));
        assert!(result.contains("STATUS"));
        Ok(())
    }

    #[sinex_test]
    async fn format_table_replay_single() -> TestResult<()> {
        let op = ReplayOperation {
            operation_id: "01HXYZ123456789ABCDEFGHIJK".to_string(),
            state: ReplayState::Executing,
            scope: ReplayScope {
                node_id: "my-node".to_string(),
                time_window: None,
                material_filter: None,
                filters: HashMap::new(),
            },
            preview_summary: None,
            checkpoint: ReplayCheckpoint {
                processed_events: 50,
                total_events: 100,
                last_event_id: None,
                batch_number: 1,
                savepoint_id: None,
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            },
            actor: "test-user".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            approved_by: None,
            approved_at: None,
            executor_node: None,
            started_at: None,
            finished_at: None,
            outcome: None,
            error_details: None,
        };
        let result = format_table_replay(&[op]);
        assert!(result.contains("01HXYZ12..."));
        assert!(result.contains("my-node"));
        Ok(())
    }
}
