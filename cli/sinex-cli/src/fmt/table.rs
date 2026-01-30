use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, CellAlignment, ContentArrangement, Table};
use console::style;
use sinex_primitives::temporal::Timestamp;

use sinex_primitives::rpc::coordination::InstanceInfo;
use sinex_primitives::rpc::replay::{ReplayOperation, ReplayState};

/// Format nodes as a table
pub fn format_table_nodes(nodes: &[InstanceInfo]) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("TYPE").set_alignment(CellAlignment::Left),
            Cell::new("ID").set_alignment(CellAlignment::Left),
            Cell::new("HOSTNAME").set_alignment(CellAlignment::Left),
            Cell::new("LEADER").set_alignment(CellAlignment::Center),
            Cell::new("LAST HEARTBEAT").set_alignment(CellAlignment::Right),
        ]);

    for node in nodes {
        let leader_icon = if node.is_leader { "★" } else { "" };
        let heartbeat = node
            .last_heartbeat
            .as_ref()
            .map(|hb| format_heartbeat_age(&(*hb).into()))
            .unwrap_or_else(|| style("none").dim().to_string());

        table.add_row(vec![
            Cell::new(&node.node_type),
            Cell::new(short_id(&node.instance_id)),
            Cell::new(node.hostname.as_deref().unwrap_or("-")),
            Cell::new(leader_icon),
            Cell::new(heartbeat),
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
            Cell::new("PROCESSOR").set_alignment(CellAlignment::Left),
            Cell::new("CREATED").set_alignment(CellAlignment::Right),
        ]);

    for op in operations {
        table.add_row(vec![
            Cell::new(short_id(&op.operation_id)),
            Cell::new(format_replay_status(&op.state)),
            Cell::new(&op.scope.processor_id),
            Cell::new(&op.created_at),
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
