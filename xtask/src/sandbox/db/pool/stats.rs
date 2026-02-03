use serde::Serialize;

/// Pool statistics for monitoring
#[derive(Debug, Clone, Serialize)]
pub struct PoolStats {
    pub total_acquisitions: usize,
    pub average_wait_time_ms: u64,
    pub cleanup_failures: usize,
    pub template_recreations: usize,
    pub total_connections: usize,
    pub idle_connections: usize,
}

/// Slot-level connection stats
#[derive(Debug, Clone, Serialize)]
pub struct SlotStats {
    pub name: String,
    pub total_connections: usize,
    pub idle_connections: usize,
    pub last_clean_time: Option<String>,
    pub last_clean_result: Option<String>,
    pub residuals: Option<Vec<(String, i64)>>,
    pub quarantined: bool,
}

#[derive(Debug, Clone)]
pub struct CleanupDiagnostics {
    pub slot_name: String,
    pub template_name: Option<String>,
    pub last_clean_time: Option<String>,
    pub last_clean_result: Option<String>,
    pub residuals: Option<Vec<(String, i64)>>,
    pub quarantined: bool,
}

impl CleanupDiagnostics {
    pub(crate) fn format_for_error(&self) -> String {
        let template_name = self.template_name.as_deref().unwrap_or("unknown");
        let last_clean_time = self.last_clean_time.as_deref().unwrap_or("unknown");
        let last_clean_result = self.last_clean_result.as_deref().unwrap_or("unknown");
        let residuals = match &self.residuals {
            Some(rows) if !rows.is_empty() => rows
                .iter()
                .map(|(table, count)| format!("{table}:{count}"))
                .collect::<Vec<_>>()
                .join(", "),
            _ => "none".to_string(),
        };

        format!(
            "slot={}\ntemplate={}\nlast_clean_time={}\nlast_clean_result={}\nresiduals={}\nquarantined={}",
            self.slot_name,
            template_name,
            last_clean_time,
            last_clean_result,
            residuals,
            self.quarantined
        )
    }
}

/// Database statistics for debugging
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub event_count: i64,
    pub agent_count: i64,
    pub checkpoint_count: i64,
}
