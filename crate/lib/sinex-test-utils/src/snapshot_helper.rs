//! Snapshot testing helpers using insta

use serde::Serialize;

/// Helper for advanced snapshot testing with custom redactions
pub struct SnapshotTestHelper {
    settings: insta::Settings,
}

impl SnapshotTestHelper {
    /// Create a new snapshot helper with default settings
    pub fn new() -> Self {
        let mut settings = insta::Settings::clone_current();
        settings.set_snapshot_path("../snapshots");
        Self { settings }
    }

    /// Add common redactions for event fields
    pub fn with_redactions(mut self) -> Self {
        self.settings.add_redaction(".id", "[id]");
        self.settings.add_redaction(".ts_ingest", "[timestamp]");
        self.settings.add_redaction(".ts_orig", "[timestamp]");
        self.settings.add_redaction(".host", "[hostname]");
        self
    }

    /// Add a custom redaction
    pub fn add_redaction(mut self, selector: &str, replacement: &str) -> Self {
        self.settings.add_redaction(selector, replacement);
        self
    }

    /// Create a snapshot with the configured settings
    pub fn snapshot<T: Serialize>(&self, value: &T, name: &str) {
        self.settings.bind(|| {
            insta::assert_json_snapshot!(name, value);
        });
    }
}

impl Default for SnapshotTestHelper {
    fn default() -> Self {
        Self::new()
    }
}
