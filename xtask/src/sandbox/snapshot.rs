use std::collections::HashMap;

use color_eyre::eyre::{eyre, Result};

/// Lightweight state snapshot used by chaos/perf suites.
#[derive(Debug, Default, Clone)]
pub struct TestSnapshot {
    pub db_events: u64,
    pub jetstream_msgs: u64,
    pub dlq_entries: u64,
    pub metrics: HashMap<String, u64>,
}

impl TestSnapshot {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assert_events_persisted(&self, expected: u64) -> Result<()> {
        if self.db_events == expected {
            Ok(())
        } else {
            Err(eyre!(
                "expected {expected} events, found {} in snapshot",
                self.db_events
            ))
        }
    }

    pub fn assert_confirmations_received(&self, expected: u64) -> Result<()> {
        if self.jetstream_msgs >= expected {
            Ok(())
        } else {
            Err(eyre!(
                "expected at least {expected} confirmations, found {}",
                self.jetstream_msgs
            ))
        }
    }

    pub fn assert_no_dlq_entries(&self) -> Result<()> {
        if self.dlq_entries == 0 {
            Ok(())
        } else {
            Err(eyre!(
                "expected zero DLQ entries, found {}",
                self.dlq_entries
            ))
        }
    }
}
