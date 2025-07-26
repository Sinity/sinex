//! Centralized constants for event types, sources, and service names
//!
//! This module provides a single source of truth for all string constants used
//! throughout the Sinex system, replacing scattered string literals with typed
//! constants for better maintainability and type safety.

/// Event source identifiers for the various satellites and services
pub mod sources {
    // Core system sources
    pub const SINEX: &str = "sinex";
    pub const FS: &str = "fs";

    // Shell integration sources
    pub const SHELL_KITTY: &str = "shell.kitty";
    pub const SHELL_ATUIN: &str = "shell.atuin";
    pub const SHELL_HISTORY: &str = "shell.history";
    pub const SHELL_BASH_HISTFILE: &str = "shell.bash_histfile";
    pub const SHELL_ZSH_HISTFILE: &str = "shell.zsh_histfile";
    pub const SHELL_FISH_HISTORY: &str = "shell.fish_history";
    pub const SHELL_RECORDING: &str = "shell.recording";
    pub const SHELL_ASCIINEMA: &str = "shell.asciinema";
    pub const SHELL_SCROLLBACK: &str = "shell.scrollback";

    // Desktop environment sources
    pub const WM_HYPRLAND: &str = "wm.hyprland";
    pub const CLIPBOARD: &str = "clipboard";

    // System sources
    pub const DBUS: &str = "dbus";
    pub const JOURNALD: &str = "journald";
    pub const UDEV: &str = "udev";
    pub const SYSTEMD: &str = "systemd";

    // Alternative naming patterns found in codebase
    pub const TERMINAL_KITTY: &str = "terminal.kitty";

    // Specialized sources found in codebase
    pub const HEALTH_AGGREGATOR: &str = "health-aggregator";
    pub const BLOB_STORAGE: &str = "blob_storage";

    // Test sources
    pub const TEST: &str = "test";
}

/// Service names for the various Sinex components
pub mod services {
    // Core services
    pub const INGESTD: &str = "sinex-ingestd";
    pub const GATEWAY: &str = "sinex-gateway";
    pub const PREFLIGHT: &str = "sinex-preflight";

    // Satellite services
    pub const FS_WATCHER: &str = "sinex-fs-watcher";
    pub const TERMINAL_SATELLITE: &str = "sinex-terminal-satellite";
    pub const DESKTOP_SATELLITE: &str = "sinex-desktop-satellite";
    pub const SYSTEM_SATELLITE: &str = "sinex-system-satellite";

    // Automaton services
    pub const HEALTH_AGGREGATOR: &str = "sinex-health-aggregator";
    pub const TERMINAL_COMMAND_CANONICALIZER: &str = "sinex-terminal-command-canonicalizer";
    pub const CONTENT_AUTOMATON: &str = "sinex-content-automaton";
    pub const SEARCH_AUTOMATON: &str = "sinex-search-automaton";
    pub const PKM_AUTOMATON: &str = "sinex-pkm-automaton";
    pub const ANALYTICS_AUTOMATON: &str = "sinex-analytics-automaton";

    // Generic processor names
    pub const FS_PROCESSOR: &str = "sinex-fs-processor";
    pub const PROCESSOR: &str = "sinex-processor";

    // Additional services
    pub const RPC_DISPATCHER: &str = "sinex-rpc-dispatcher";
    pub const METRICS: &str = "sinex-metrics";
    pub const PIPELINE: &str = "sinex-pipeline";

    // Legacy/alternative names
    pub const COLLECTOR: &str = "sinex-collector";
    pub const ROUTER: &str = "sinex-router";
    pub const UNIFIED_COLLECTOR: &str = "sinex-unified-collector";
    pub const PROMO_WORKER: &str = "sinex-promo-worker";
}

/// Event type constants organized by domain
pub mod event_types {
    /// Sinex system internal events
    pub mod sinex {
        // Automaton lifecycle events
        pub const AUTOMATON_STARTUP: &str = "automaton.startup";
        pub const AUTOMATON_SHUTDOWN: &str = "automaton.shutdown";
        pub const AUTOMATON_HEARTBEAT: &str = "automaton.heartbeat";
        pub const AUTOMATON_ERROR: &str = "automaton.error";
        pub const AUTOMATON_DLQ_EVENT_WRITTEN: &str = "automaton.dlq_event_written";

        // Scanner events
        pub const SCAN_STARTED: &str = "scan.started";
        pub const SCAN_COMPLETED: &str = "scan.completed";

        // Process events
        pub const PROCESS_STARTED: &str = "process.started";
        pub const PROCESS_HEARTBEAT: &str = "process.heartbeat";
        pub const PROCESS_SHUTDOWN: &str = "process.shutdown";

        // Sensor events
        pub const SENSOR_ACTIVATED: &str = "sensor.activated";
        pub const SENSOR_DEACTIVATED: &str = "sensor.deactivated";

        // Health and system monitoring
        pub const SYSTEM_HEALTH_SUMMARY: &str = "system.health.summary";
    }

    /// Filesystem events
    pub mod filesystem {
        // File operations
        pub const FILE_CREATED: &str = "file.created";
        pub const FILE_MODIFIED: &str = "file.modified";
        pub const FILE_DELETED: &str = "file.deleted";
        pub const FILE_MOVED: &str = "file.moved";
        pub const FILE_RENAMED: &str = "file.renamed"; // Alternative name found in validation

        // Directory operations
        pub const DIR_CREATED: &str = "dir.created";
        pub const DIR_DELETED: &str = "dir.deleted";

        // Alternative namespace patterns found in tests
        pub const FILESYSTEM_FILE_CREATED: &str = "filesystem.file.created";
    }

    /// Shell and terminal events
    pub mod shell {
        // Command events
        pub const COMMAND_EXECUTED: &str = "command.executed";
        pub const COMMAND_COMPLETED: &str = "command.completed";
        pub const COMMAND_FAILED: &str = "command.failed";
        pub const COMMAND_IMPORTED: &str = "command.imported";
        pub const COMMAND_OUTPUT: &str = "command.output";

        // Session events
        pub const SESSION_STARTED: &str = "session.started";
        pub const SESSION_ENDED: &str = "session.ended";

        // Recording events
        pub const RECORDING_STARTED: &str = "recording.started";
        pub const RECORDING_ENDED: &str = "recording.ended";

        // Tab events
        pub const TAB_CREATED: &str = "tab.created";
        pub const TAB_FOCUSED: &str = "tab.focused";
        pub const TAB_CLOSED: &str = "tab.closed";

        // Process and config events
        pub const PROCESS_CHANGED: &str = "process.changed";
        pub const CONFIG_CHANGED: &str = "config.changed";

        // Terminal content events
        pub const SCROLLBACK_FULL: &str = "scrollback.full";
        pub const ENTRY_IMPORTED: &str = "entry.imported";

        // Alternative command event patterns found in codebase
        pub const SHELL_COMMAND_EXECUTED: &str = "shell.command.executed";
        pub const SHELL_COMMAND_COMPLETED: &str = "shell.command.completed";

        // Historical events
        pub const SHELL_COMMAND_HISTORICAL: &str = "shell.command.historical";
        pub const SHELL_HISTORY_HISTORICAL: &str = "shell.history.historical";

        // Alternative namespace patterns found in tests
        pub const TERMINAL_COMMAND_EXECUTED: &str = "terminal.command.executed";
    }

    /// Window manager events
    pub mod window_manager {
        // Window events
        pub const WINDOW_OPENED: &str = "window.opened";
        pub const WINDOW_CLOSED: &str = "window.closed";
        pub const WINDOW_FOCUSED: &str = "window.focused";
        pub const WINDOW_MOVED: &str = "window.moved";
        pub const WINDOW_RESIZED: &str = "window.resized";

        // Workspace events
        pub const WORKSPACE_SWITCHED: &str = "workspace.switched";
        pub const WORKSPACE_CREATED: &str = "workspace.created";
        pub const WORKSPACE_DESTROYED: &str = "workspace.destroyed";
        pub const WORKSPACE_CHANGED: &str = "workspace.changed"; // Alternative name

        // Display events
        pub const DISPLAY_CONNECTED: &str = "display.connected";
        pub const DISPLAY_DISCONNECTED: &str = "display.disconnected";
        pub const MONITOR_FOCUSED: &str = "monitor.focused";

        // State events
        pub const STATE_CAPTURED: &str = "state.captured";

        // Alternative window event names found in tests
        pub const WINDOW_CREATED: &str = "window.created";

        // Alternative namespace patterns found in tests
        pub const WINDOW_MANAGER_WINDOW_FOCUSED: &str = "window_manager.window.focused";
    }

    /// Clipboard events
    pub mod clipboard {
        pub const COPIED: &str = "clipboard.copied";
        pub const SELECTED: &str = "clipboard.selected";
    }

    /// D-Bus events
    pub mod dbus {
        // Core D-Bus events
        pub const SIGNAL_RECEIVED: &str = "signal.received";
        pub const METHOD_CALLED: &str = "method.called";
        pub const NOTIFICATION_SENT: &str = "notification.sent";

        // Device events
        pub const DEVICE_CONNECTED: &str = "device.connected";
        pub const DEVICE_DISCONNECTED: &str = "device.disconnected";
        pub const DEVICE_CHANGED: &str = "device.changed";

        // State change events
        pub const MEDIA_STATE_CHANGED: &str = "media.state_changed";
        pub const POWER_STATE_CHANGED: &str = "power.state_changed";
        pub const NETWORK_STATE_CHANGED: &str = "network.state_changed";
        pub const BLUETOOTH_DEVICE_CHANGED: &str = "bluetooth.device_changed";
        pub const SESSION_STATE_CHANGED: &str = "session.state_changed";
        pub const SCREENSAVER_STATE_CHANGED: &str = "screensaver.state_changed";

        // Mount events
        pub const MOUNT_CHANGED: &str = "mount.changed";

        // Security events
        pub const SECURITY_AUTHORIZATION: &str = "security.authorization";
    }

    /// Systemd events
    pub mod systemd {
        // Unit state events
        pub const UNIT_STARTED: &str = "unit.started";
        pub const UNIT_STOPPED: &str = "unit.stopped";
        pub const UNIT_CHANGED: &str = "unit.changed";
        pub const UNIT_STATE_CHANGED: &str = "unit.state_changed";
    }

    /// Journal events
    pub mod journald {
        pub const ENTRY_WRITTEN: &str = "entry.written";
        pub const SYNC_COMPLETED: &str = "sync.completed";
    }

    /// Generic state events found in tests
    pub mod generic {
        pub const STATE_CHANGED: &str = "state.changed";
    }

    /// Metrics and monitoring events
    pub mod metrics {
        // Blob storage metrics
        pub const BLOB_STORAGE_OPERATION: &str = "metrics.blob_storage.operation";
        pub const BLOB_STORAGE_STATISTICS: &str = "metrics.blob_storage.statistics";
    }

    /// Test event types
    pub mod test {
        // Generic test event
        pub const GENERIC: &str = "test.generic";

        // Performance testing event types
        pub const BASELINE_INSERTION_TEST: &str = "baseline.insertion.test";
        pub const BASELINE_STREAM_WRITE: &str = "baseline.stream.write";
        pub const CONCURRENT_BASELINE_TEST: &str = "concurrent.baseline.test";
        pub const RECOVERY_BASELINE_TEST: &str = "recovery.baseline.test";

        // Bottleneck testing
        pub const BOTTLENECK_DATABASE_NORMAL: &str = "bottleneck.database.normal";
        pub const BOTTLENECK_DATABASE_LIMITED: &str = "bottleneck.database.limited";
        pub const BOTTLENECK_DATABASE_RECOVERY: &str = "bottleneck.database.recovery";
        pub const DATABASE_BOTTLENECK_TEST: &str = "database.bottleneck.test";
        pub const MEMORY_BOTTLENECK_TEST: &str = "memory.bottleneck.test";
        pub const CONCURRENT_BOTTLENECK_TEST: &str = "concurrent.bottleneck.test";

        // Performance testing
        pub const CONCURRENT_PERFORMANCE_TEST: &str = "concurrent.performance.test";
        pub const END_TO_END_PERFORMANCE_TEST: &str = "end.to.end.performance.test";
        pub const CONCURRENT_DATABASE_TEST: &str = "concurrent.database.test";
        pub const TRANSACTION_PERFORMANCE_TEST_1: &str = "transaction.performance.test.1";
        pub const TRANSACTION_PERFORMANCE_TEST_2: &str = "transaction.performance.test.2";
        pub const CONCURRENT_TRANSACTION_TEST: &str = "concurrent.transaction.test";
        pub const CONCURRENT_STRESS_TEST: &str = "concurrent.stress.test";

        // Memory testing
        pub const CONCURRENT_MEMORY_TEST: &str = "concurrent.memory.test";
        pub const MEMORY_STRESS_TEST: &str = "memory.stress.test";
        pub const SUSTAINED_MEMORY_TEST: &str = "sustained.memory.test";

        // Load testing
        pub const CONCURRENT_INGESTION_TEST: &str = "concurrent.ingestion.test";
        pub const MIXED_WORKLOAD_TEST: &str = "mixed.workload.test";
        pub const RATE_LIMITED_TEST: &str = "rate.limited.test";
        pub const BURST_LOAD_TEST: &str = "burst.load.test";
        pub const BURST_COOLDOWN_TEST: &str = "burst.cooldown.test";

        // Regression testing
        pub const DATABASE_REGRESSION_TEST: &str = "database.regression.test";
        pub const REGRESSION_BASELINE_TEST: &str = "regression.baseline.test";
        pub const REGRESSION_NORMAL_TEST: &str = "regression.normal.test";
        pub const REGRESSION_DEGRADED_TEST: &str = "regression.degraded.test";
        pub const REGRESSION_SEVERE_TEST: &str = "regression.severe.test";
        pub const STRICT_THRESHOLD_BASELINE: &str = "strict.threshold.baseline";
        pub const STRICT_THRESHOLD_TEST: &str = "strict.threshold.test";
        pub const LENIENT_THRESHOLD_TEST: &str = "lenient.threshold.test";

        // Comprehensive testing
        pub const COMPREHENSIVE_PERFORMANCE_TEST: &str = "comprehensive.performance.test";
        pub const COMPREHENSIVE_CONCURRENT_TEST: &str = "comprehensive.concurrent.test";
        pub const COMPREHENSIVE_RESOURCE_TEST: &str = "comprehensive.resource.test";

        // Stream testing
        pub const CONCURRENT_STREAM_TEST: &str = "concurrent.stream.test";
        pub const VARIABLE_SIZE_TEST: &str = "variable.size.test";

        // Integration testing
        pub const SEQUENCE_TEST: &str = "sequence_test";
        pub const CONCURRENT_TEST: &str = "concurrent_test";
        pub const RAPID_BATCH: &str = "rapid_batch";
        pub const DELAYED_BATCH: &str = "delayed_batch";
        pub const PERFORMANCE_TEST: &str = "performance_test";

        // Consistency testing
        pub const CONSISTENCY_TEST: &str = "consistency_test";
        pub const BATCH1: &str = "batch1";
        pub const BATCH2: &str = "batch2";
        pub const STALE_TEST: &str = "stale_test";
        pub const SHARED_EVENT: &str = "shared_event";
        pub const FUTURE_REFERENCE: &str = "future_reference";
        pub const SEQUENCE_EVENT: &str = "sequence_event";
        pub const POST_CHECKPOINT_EVENT: &str = "post_checkpoint_event";

        // Data integrity testing
        pub const VALID_EVENT: &str = "valid_event";
        pub const ORDERING_TEST: &str = "ordering_test";
        pub const BATCH1_EVENT: &str = "batch1_event";
        pub const BATCH2_EVENT: &str = "batch2_event";
        pub const FOREIGN_KEY_TEST: &str = "foreign_key_test";

        // End-to-end workflow testing
        pub const NORMAL_OPERATION: &str = "normal.operation";
        pub const PERFORMANCE_LOAD_EVENT: &str = "performance.load.event";
    }

    /// RPC event types
    pub mod rpc {
        // RPC request/response types
        pub const REQUEST: &str = "request";
        pub const RESPONSE: &str = "response";
        pub const ERROR: &str = "error";

        // RPC source prefixes
        pub const GATEWAY_PREFIX: &str = "rpc.gateway";
        pub const PKM_PREFIX: &str = "rpc.pkm";
        pub const SEARCH_PREFIX: &str = "rpc.search";
        pub const CONTENT_PREFIX: &str = "rpc.content";
        pub const ANALYTICS_PREFIX: &str = "rpc.analytics";
    }
}

/// Common file paths and directories used by Sinex components
pub mod paths {
    // Socket paths
    pub const INGEST_SOCKET: &str = "/tmp/sinex-ingestd.sock";
    pub const HOST_SOCKET: &str = "/tmp/sinex-host.sock";
    pub const RPC_SOCKET_DIR: &str = "/tmp/sinex-rpc";

    // Data directories
    pub const CONTENT_DIR: &str = "/tmp/sinex-content";
    pub const SEARCH_DIR: &str = "/tmp/sinex-search";
    pub const PKM_DIR: &str = "/tmp/sinex-pkm";
    pub const ANALYTICS_DIR: &str = "/tmp/sinex-analytics";
    pub const CLIPBOARD_ANNEX: &str = "/tmp/sinex-clipboard-annex";
    pub const ANNEX_DIR: &str = "/tmp/sinex-annex";

    // Test directories
    pub const TEST_DIR: &str = "/tmp/sinex-test";
    pub const PREFLIGHT_FS_TEST: &str = "/tmp/sinex-preflight-fs-test";

    // Production paths
    pub const RUN_DIR: &str = "/run/sinex";
    pub const REALM_ANNEX: &str = "/realm/sinex-annex";
}

/// Configuration and environment variable names
pub mod config {
    // Environment variables
    pub const ANNEX_PATH_ENV: &str = "SINEX_ANNEX_PATH";
    pub const DATABASE_URL_ENV: &str = "DATABASE_URL";

    // Default configuration values
    pub const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
    pub const DEFAULT_BATCH_SIZE: usize = 100;
    pub const DEFAULT_TIMEOUT_SECS: u64 = 30;
}

/// Test-specific constants
pub mod test_constants {
    // Test service names with unique suffixes
    pub const PREFLIGHT_INTEGRATION_TEST: &str = "sinex-preflight-integration-test";
    pub const PREFLIGHT_TX_TEST: &str = "sinex-preflight-tx-test";
    pub const PREFLIGHT_CONCURRENT_TEST: &str = "sinex-preflight-concurrent-test";
    pub const PREFLIGHT_PIPELINE_TEST: &str = "sinex-preflight-pipeline-test";

    // Test repository names
    pub const TEST_REPO: &str = "sinex-test";
    pub const TEST_REPO_INIT: &str = "sinex-test-repo";
}

/// String patterns used in git-annex operations and metadata
pub mod git_annex {
    pub const GENERATED_BY_PREFIX: &str = "# Generated by";
    pub const RECORDING_WATCHER_SUFFIX: &str = "RecordingWatcher";
}

// Re-export commonly used constants for convenience (specific imports to avoid conflicts)
// NOTE: Commented out to avoid self-referential imports
// pub use event_types::filesystem::*;
// pub use event_types::shell::*;
// pub use event_types::window_manager::*;

// Sources (without HEALTH_AGGREGATOR to avoid conflict)
// pub use sources::{FS, SHELL_KITTY, SHELL_RECORDING, SHELL_ASCIINEMA, SHELL_SCROLLBACK, WM_HYPRLAND, CLIPBOARD, DBUS, JOURNALD, UDEV, SYSTEMD, TERMINAL_KITTY, BLOB_STORAGE, SINEX};

// Services (all service names)
// pub use services::*;
