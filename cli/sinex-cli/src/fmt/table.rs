use chrono::{DateTime, Utc};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, CellAlignment, ContentArrangement, Table};
use console::style;

use crate::model::nodes::NodeInfo;
use crate::model::replay::{DlqInfo, ReplayOperation, ReplayStatus};

/// Format nodes as a table
pub fn format_table_nodes(nodes: &[NodeInfo]) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("ROLE").set_alignment(CellAlignment::Left),
            Cell::new("ID").set_alignment(CellAlignment::Left),
            Cell::new("NAME").set_alignment(CellAlignment::Left),
            Cell::new("STATUS").set_alignment(CellAlignment::Left),
            Cell::new("LAST HEARTBEAT").set_alignment(CellAlignment::Right),
        ]);

    for node in nodes {
        table.add_row(vec![
            Cell::new(node.role.to_string()),
            Cell::new(short_id(&node.id)),
            Cell::new(&node.name),
            Cell::new(format_status(&node.status)),
            Cell::new(format_age(&node.last_heartbeat)),
        ]);
    }

    table.to_string()
}

/// Format replay operations as a table
pub fn format_table_replay(operations: &[ReplayOperation]) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("ID").set_alignment(CellAlignment::Left),
            Cell::new("STATUS").set_alignment(CellAlignment::Left),
            Cell::new("PROGRESS").set_alignment(CellAlignment::Right),
            Cell::new("EVENTS").set_alignment(CellAlignment::Right),
            Cell::new("CREATED").set_alignment(CellAlignment::Right),
        ]);

    for op in operations {
        table.add_row(vec![
            Cell::new(short_id(&op.id)),
            Cell::new(format_replay_status(&op.status)),
            Cell::new(format!("{:.1}%", op.progress * 100.0)),
            Cell::new(format!("{}/{}", op.events_processed, op.total_events)),
            Cell::new(format_age(&op.created_at)),
        ]);
    }

    table.to_string()
}

/// Format DLQ information as a table
pub fn format_table_dlq(queues: &[DlqInfo]) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("SUBJECT").set_alignment(CellAlignment::Left),
            Cell::new("MESSAGES").set_alignment(CellAlignment::Right),
            Cell::new("FIRST").set_alignment(CellAlignment::Right),
            Cell::new("LAST").set_alignment(CellAlignment::Right),
        ]);

    for queue in queues {
        table.add_row(vec![
            Cell::new(&queue.subject),
            Cell::new(queue.message_count.to_string()),
            Cell::new(
                queue
                    .first_message_at
                    .as_ref()
                    .map(format_age)
                    .unwrap_or_else(|| "-".to_string()),
            ),
            Cell::new(
                queue
                    .last_message_at
                    .as_ref()
                    .map(format_age)
                    .unwrap_or_else(|| "-".to_string()),
            ),
        ]);
    }

    table.to_string()
}

// ==================== Helper Functions ====================

/// Shorten a ULID to first 8 characters for display
fn short_id(id: &str) -> String {
    if id.len() > 8 {
        format!("{}...", &id[..8])
    } else {
        id.to_string()
    }
}

/// Format node status with color
fn format_status(status: &crate::model::nodes::NodeStatus) -> String {
    use crate::model::nodes::NodeStatus;
    match status {
        NodeStatus::Active => style("active").green().to_string(),
        NodeStatus::Draining => style("draining").yellow().to_string(),
        NodeStatus::Inactive => style("inactive").dim().to_string(),
        NodeStatus::Error => style("error").red().to_string(),
    }
}

/// Format replay status with color
fn format_replay_status(status: &ReplayStatus) -> String {
    match status {
        ReplayStatus::Planned => style("planned").cyan().to_string(),
        ReplayStatus::Approved => style("approved").blue().to_string(),
        ReplayStatus::Running => style("running").yellow().to_string(),
        ReplayStatus::Completed => style("completed").green().to_string(),
        ReplayStatus::Cancelled => style("cancelled").dim().to_string(),
        ReplayStatus::Failed => style("failed").red().to_string(),
    }
}

/// Format timestamp as "X ago" or "X from now"
fn format_age(timestamp: &DateTime<Utc>) -> String {
    let now = Utc::now();
    let duration = now.signed_duration_since(*timestamp);

    if duration.num_seconds() < 0 {
        // Future timestamp
        let abs_duration = -duration;
        if abs_duration.num_seconds() < 60 {
            format!("in {}s", abs_duration.num_seconds())
        } else if abs_duration.num_minutes() < 60 {
            format!("in {}m", abs_duration.num_minutes())
        } else if abs_duration.num_hours() < 24 {
            format!("in {}h", abs_duration.num_hours())
        } else {
            format!("in {}d", abs_duration.num_days())
        }
    } else {
        // Past timestamp
        if duration.num_seconds() < 60 {
            format!("{}s ago", duration.num_seconds())
        } else if duration.num_minutes() < 60 {
            format!("{}m ago", duration.num_minutes())
        } else if duration.num_hours() < 24 {
            format!("{}h ago", duration.num_hours())
        } else {
            format!("{}d ago", duration.num_days())
        }
    }
}
