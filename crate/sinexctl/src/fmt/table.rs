use console::style;
use tabled::{builder::Builder, settings::Style};

use crate::fmt::format_timestamp_age;
use sinex_primitives::rpc::coordination::InstanceInfo;
use sinex_primitives::rpc::replay::{ReplayOperation, ReplayState};
use sinex_primitives::temporal::Timestamp;

/// Format runtime modules as a table.
pub fn format_table_runtime(modules: &[InstanceInfo]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["TYPE", "ID", "HOSTNAME", "LEADER", "LAST HEARTBEAT"]);

    for module in modules {
        let leader_icon = if module.is_leader { "★" } else { "" };
        let heartbeat = module
            .last_heartbeat
            .as_ref()
            .map_or_else(|| style("none").dim().to_string(), format_heartbeat_age);

        builder.push_record([
            module.module_kind.to_string(),
            short_id(&module.instance_id),
            module.hostname.as_deref().unwrap_or("-").to_string(),
            leader_icon.to_string(),
            heartbeat,
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

/// Format replay operations as a table
#[must_use]
pub fn format_table_replay(operations: &[ReplayOperation]) -> String {
    let mut builder = Builder::new();
    builder.push_record(["ID", "STATUS", "SOURCE", "CREATED"]);

    for op in operations {
        builder.push_record([
            short_id(&op.operation_id),
            format_replay_status(&op.state),
            op.scope.source_name.clone(),
            op.created_at.clone(),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

// ==================== Helper Functions ====================

/// Shorten a `UUIDv7` to first 8 characters for display
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
        ReplayState::Cancelling => style("cancelling").yellow().to_string(),
        ReplayState::Committing => style("committing").yellow().to_string(),
        ReplayState::Completed => style("completed").green().to_string(),
        ReplayState::Cancelled => style("cancelled").dim().to_string(),
        ReplayState::Failed => style("failed").red().to_string(),
    }
}

/// Format heartbeat timestamp as "X ago"
#[must_use]
pub fn format_heartbeat_age(timestamp: &Timestamp) -> String {
    format_timestamp_age(timestamp)
}

#[cfg(test)]
#[path = "table_test.rs"]
mod tests;
