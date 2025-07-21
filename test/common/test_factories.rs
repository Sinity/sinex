//! High-level test data factories for creating realistic test scenarios
//!
//! This module provides factories that generate complete, realistic test scenarios
//! with properly structured events and relationships. These factories build on top
//! of the lower-level TestEventBuilder to provide semantic test data creation.

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder, BatchEventBuilder, TestScenarioBuilder};
use sinex_db::RawEvent;
use sinex_events::constants::{event_types, sources};
use sinex_ulid::Ulid;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;

/// Factory for generating realistic user activity patterns
pub struct UserActivityFactory;

impl UserActivityFactory {
    /// Create a complete user session with login, activities, and logout
    pub fn create_user_session(duration_minutes: i64, activity_count: usize) -> Vec<RawEvent> {
        let start_time = Utc::now() - Duration::minutes(duration_minutes);
        let mut events = Vec::new();
        
        // Session start
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::SESSION_STARTED)
                .with_timestamp(start_time)
                .with_field("session_id", json!(Ulid::new().to_string()))
                .with_field("shell", json!("zsh"))
                .with_field("terminal", json!("kitty"))
                .with_field("user", json!(whoami::username()))
                .build()
        );
        
        // Generate activities throughout the session
        let activity_interval = Duration::minutes(duration_minutes) / activity_count as i32;
        for i in 0..activity_count {
            let activity_time = start_time + activity_interval * i as i32;
            
            // Mix of different activities
            match i % 5 {
                0 => {
                    // File operation
                    events.push(
                        TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_MODIFIED)
                            .with_timestamp(activity_time)
                            .with_field("path", json!(format!("/home/user/project/file_{}.rs", i)))
                            .with_field("size", json!(1024 + i * 100))
                            .with_field("editor", json!("nvim"))
                            .build()
                    );
                }
                1 => {
                    // Command execution
                    let commands = ["git status", "cargo build", "rg pattern", "fd file", "just test"];
                    let cmd = commands[i % commands.len()];
                    events.push(
                        TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                            .with_timestamp(activity_time)
                            .with_field("command", json!(cmd))
                            .with_field("cwd", json!("/home/user/project"))
                            .with_field("exit_code", json!(0))
                            .with_field("duration_ms", json!(100 + i * 10))
                            .build()
                    );
                }
                2 => {
                    // Window focus change
                    let apps = ["firefox", "kitty", "code", "slack", "spotify"];
                    let app = apps[i % apps.len()];
                    events.push(
                        TestEventBuilder::new(sources::WM_HYPRLAND, event_types::window_manager::WINDOW_FOCUSED)
                            .with_timestamp(activity_time)
                            .with_field("window_class", json!(app))
                            .with_field("window_title", json!(format!("{} - Active", app)))
                            .with_field("workspace", json!(1 + (i % 4)))
                            .build()
                    );
                }
                3 => {
                    // Clipboard operation
                    events.push(
                        TestEventBuilder::new(sources::CLIPBOARD, event_types::clipboard::COPIED)
                            .with_timestamp(activity_time)
                            .with_field("content_type", json!("text/plain"))
                            .with_field("content_length", json!(50 + i * 5))
                            .with_field("source_app", json!("kitty"))
                            .build()
                    );
                }
                _ => {
                    // Directory navigation
                    events.push(
                        TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                            .with_timestamp(activity_time)
                            .with_field("command", json!(format!("cd /home/user/project/module_{}", i % 3)))
                            .with_field("cwd", json!("/home/user/project"))
                            .with_field("exit_code", json!(0))
                            .build()
                    );
                }
            }
        }
        
        // Session end
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::SESSION_ENDED)
                .with_timestamp(start_time + Duration::minutes(duration_minutes))
                .with_field("duration_seconds", json!(duration_minutes * 60))
                .with_field("command_count", json!(activity_count / 2))
                .build()
        );
        
        events
    }
    
    /// Create a development workflow (edit, build, test cycle)
    pub fn create_development_workflow() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(30);
        
        // Open editor
        events.push(
            TestEventBuilder::new(sources::WM_HYPRLAND, event_types::window_manager::WINDOW_OPENED)
                .with_timestamp(start_time)
                .with_field("window_class", json!("neovim"))
                .with_field("window_title", json!("nvim - src/main.rs"))
                .build()
        );
        
        // Edit files
        for i in 0..5 {
            events.push(
                TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_MODIFIED)
                    .with_timestamp(start_time + Duration::minutes(i * 2))
                    .with_field("path", json!(format!("/home/user/project/src/{}.rs", 
                        ["main", "lib", "config", "utils", "tests"][i as usize])))
                    .with_field("size", json!(2048 + i * 512))
                    .with_field("editor", json!("nvim"))
                    .build()
            );
        }
        
        // Save and format
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(10))
                .with_field("command", json!("cargo fmt"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        // Build
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(12))
                .with_field("command", json!("cargo build"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(5000))
                .build()
        );
        
        // Run tests
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(15))
                .with_field("command", json!("cargo test"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(3000))
                .with_field("test_count", json!(42))
                .build()
        );
        
        // Git operations
        for (i, cmd) in ["git add -A", "git commit -m 'feat: implement new feature'", "git push"].iter().enumerate() {
            events.push(
                TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                    .with_timestamp(start_time + Duration::minutes(20 + i as i64))
                    .with_field("command", json!(cmd))
                    .with_field("cwd", json!("/home/user/project"))
                    .with_field("exit_code", json!(0))
                    .build()
            );
        }
        
        events
    }
}

/// Factory for generating system monitoring events
pub struct SystemEventFactory;

impl SystemEventFactory {
    /// Create a series of system monitoring events
    pub fn create_system_monitoring(duration_minutes: i64, interval_seconds: i64) -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(duration_minutes);
        let intervals = (duration_minutes * 60) / interval_seconds;
        
        for i in 0..intervals {
            let event_time = start_time + Duration::seconds(i * interval_seconds);
            
            // CPU and memory stats
            events.push(
                TestEventBuilder::new(sources::SINEX, event_types::sinex::SYSTEM_HEALTH_SUMMARY)
                    .with_timestamp(event_time)
                    .with_field("cpu_usage_percent", json!(20.0 + (i % 30) as f64))
                    .with_field("memory_used_mb", json!(8192 + (i % 100) * 50))
                    .with_field("memory_total_mb", json!(16384))
                    .with_field("disk_usage_percent", json!(45.0 + (i % 10) as f64 * 0.1))
                    .with_field("load_average", json!([1.2, 1.5, 1.1]))
                    .with_field("uptime_seconds", json!(i * interval_seconds))
                    .build()
            );
            
            // Process heartbeats for various services
            if i % 5 == 0 {
                for service in &["fs-watcher", "terminal-satellite", "desktop-satellite"] {
                    events.push(
                        TestEventBuilder::new(sources::SINEX, event_types::sinex::PROCESS_HEARTBEAT)
                            .with_timestamp(event_time + Duration::seconds(1))
                            .with_field("service", json!(format!("sinex-{}", service)))
                            .with_field("version", json!("1.0.0"))
                            .with_field("status", json!("healthy"))
                            .with_field("events_processed", json!(1000 + i * 10))
                            .build()
                    );
                }
            }
        }
        
        events
    }
    
    /// Create a system startup sequence
    pub fn create_system_startup() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(5);
        
        // System boot
        events.push(
            TestEventBuilder::new(sources::SYSTEMD, event_types::systemd::UNIT_STARTED)
                .with_timestamp(start_time)
                .with_field("unit", json!("multi-user.target"))
                .with_field("result", json!("success"))
                .build()
        );
        
        // Start core services
        let services = [
            ("postgresql.service", 5),
            ("redis.service", 8),
            ("sinex-ingestd.service", 10),
            ("sinex-fs-watcher.service", 12),
            ("sinex-terminal-satellite.service", 15),
            ("sinex-desktop-satellite.service", 18),
        ];
        
        for (service, delay) in &services {
            events.push(
                TestEventBuilder::new(sources::SYSTEMD, event_types::systemd::UNIT_STARTED)
                    .with_timestamp(start_time + Duration::seconds(*delay))
                    .with_field("unit", json!(service))
                    .with_field("result", json!("success"))
                    .with_field("startup_time_ms", json!(100 + delay * 10))
                    .build()
            );
            
            // Service announces startup
            if service.starts_with("sinex-") {
                events.push(
                    TestEventBuilder::new(sources::SINEX, event_types::sinex::PROCESS_STARTED)
                        .with_timestamp(start_time + Duration::seconds(delay + 2))
                        .with_field("service", json!(service))
                        .with_field("version", json!("1.0.0"))
                        .with_field("pid", json!(1000 + delay))
                        .build()
                );
            }
        }
        
        events
    }
}

/// Factory for creating file system operation scenarios
pub struct FileSystemScenarioFactory;

impl FileSystemScenarioFactory {
    /// Create a realistic file operation workflow
    pub fn create_file_workflow(base_path: &str) -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(10);
        
        // Create directory structure
        events.push(
            TestEventBuilder::new(sources::FS, event_types::filesystem::DIR_CREATED)
                .with_timestamp(start_time)
                .with_field("path", json!(base_path))
                .with_field("mode", json!("0755"))
                .build()
        );
        
        // Create initial files
        for (i, filename) in ["README.md", "Cargo.toml", "src/lib.rs", "src/main.rs"].iter().enumerate() {
            let file_path = format!("{}/{}", base_path, filename);
            events.push(
                TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_CREATED)
                    .with_timestamp(start_time + Duration::seconds(i as i64 * 2))
                    .with_field("path", json!(file_path))
                    .with_field("size", json!(100 + i * 50))
                    .with_field("mode", json!("0644"))
                    .build()
            );
        }
        
        // Edit files multiple times
        for i in 0..5 {
            events.push(
                TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_MODIFIED)
                    .with_timestamp(start_time + Duration::minutes(2 + i))
                    .with_field("path", json!(format!("{}/src/lib.rs", base_path)))
                    .with_field("size", json!(500 + i * 100))
                    .with_field("editor", json!("nvim"))
                    .build()
            );
        }
        
        // Move/rename file
        events.push(
            TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_MOVED)
                .with_timestamp(start_time + Duration::minutes(8))
                .with_field("old_path", json!(format!("{}/src/main.rs", base_path)))
                .with_field("new_path", json!(format!("{}/src/bin/main.rs", base_path)))
                .build()
        );
        
        // Delete temporary files
        events.push(
            TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_DELETED)
                .with_timestamp(start_time + Duration::minutes(9))
                .with_field("path", json!(format!("{}/.tmp", base_path)))
                .build()
        );
        
        events
    }
    
    /// Create a build process with file generation
    pub fn create_build_process() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(5);
        let base_path = "/home/user/project";
        
        // Clean build directory
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time)
                .with_field("command", json!("rm -rf target/"))
                .with_field("cwd", json!(base_path))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        // Start build
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::seconds(2))
                .with_field("command", json!("cargo build --release"))
                .with_field("cwd", json!(base_path))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        // Generate build artifacts
        let artifacts = [
            "target/release/deps",
            "target/release/build",
            "target/release/project",
            "target/release/project.d",
        ];
        
        for (i, artifact) in artifacts.iter().enumerate() {
            let event_type = if artifact.contains("deps") || artifact.contains("build") {
                event_types::filesystem::DIR_CREATED
            } else {
                event_types::filesystem::FILE_CREATED
            };
            
            events.push(
                TestEventBuilder::new(sources::FS, event_type)
                    .with_timestamp(start_time + Duration::seconds(5 + i as i64 * 2))
                    .with_field("path", json!(format!("{}/{}", base_path, artifact)))
                    .with_field("size", json!(if artifact.ends_with("project") { 5242880 } else { 1024 }))
                    .build()
            );
        }
        
        events
    }
}

/// Factory for creating multi-step workflow scenarios
pub struct WorkflowFactory;

impl WorkflowFactory {
    /// Create a git workflow (branch, edit, commit, push)
    pub fn create_git_workflow() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(20);
        let branch_name = "feature/new-capability";
        
        // Create and checkout branch
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time)
                .with_field("command", json!(format!("git checkout -b {}", branch_name)))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        // Make changes
        let files_changed = ["src/lib.rs", "src/config.rs", "tests/integration_test.rs"];
        for (i, file) in files_changed.iter().enumerate() {
            events.push(
                TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_MODIFIED)
                    .with_timestamp(start_time + Duration::minutes(2 + i as i64 * 3))
                    .with_field("path", json!(format!("/home/user/project/{}", file)))
                    .with_field("size", json!(2048 + i * 512))
                    .build()
            );
            
            // Stage file
            events.push(
                TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                    .with_timestamp(start_time + Duration::minutes(3 + i as i64 * 3))
                    .with_field("command", json!(format!("git add {}", file)))
                    .with_field("cwd", json!("/home/user/project"))
                    .with_field("exit_code", json!(0))
                    .build()
            );
        }
        
        // Run tests
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(12))
                .with_field("command", json!("cargo test"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(5000))
                .build()
        );
        
        // Commit
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(15))
                .with_field("command", json!("git commit -m 'feat: add new capability'"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        // Push
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(16))
                .with_field("command", json!(format!("git push -u origin {}", branch_name)))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        events
    }
    
    /// Create a data pipeline workflow
    pub fn create_data_pipeline() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(15);
        
        // Download data
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time)
                .with_field("command", json!("wget https://example.com/dataset.csv"))
                .with_field("cwd", json!("/home/user/data"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(3000))
                .build()
        );
        
        // File created
        events.push(
            TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_CREATED)
                .with_timestamp(start_time + Duration::seconds(3))
                .with_field("path", json!("/home/user/data/dataset.csv"))
                .with_field("size", json!(1048576))
                .build()
        );
        
        // Process data
        let processing_steps = [
            ("python clean_data.py dataset.csv", "dataset_clean.csv", 2000),
            ("python transform_data.py dataset_clean.csv", "dataset_transformed.csv", 3000),
            ("python analyze_data.py dataset_transformed.csv", "results.json", 1500),
        ];
        
        for (i, (cmd, output, duration)) in processing_steps.iter().enumerate() {
            let step_time = start_time + Duration::minutes(2 + i as i64 * 3);
            
            events.push(
                TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                    .with_timestamp(step_time)
                    .with_field("command", json!(cmd))
                    .with_field("cwd", json!("/home/user/data"))
                    .with_field("exit_code", json!(0))
                    .with_field("duration_ms", json!(duration))
                    .build()
            );
            
            events.push(
                TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_CREATED)
                    .with_timestamp(step_time + Duration::milliseconds(*duration as i64))
                    .with_field("path", json!(format!("/home/user/data/{}", output)))
                    .with_field("size", json!(524288 + i * 102400))
                    .build()
            );
        }
        
        // Upload results
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(12))
                .with_field("command", json!("aws s3 cp results.json s3://bucket/results/"))
                .with_field("cwd", json!("/home/user/data"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(2000))
                .build()
        );
        
        events
    }
    
    /// Create a deployment workflow
    pub fn create_deployment_workflow() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(10);
        
        // Build release
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time)
                .with_field("command", json!("cargo build --release"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(30000))
                .build()
        );
        
        // Run tests
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(1))
                .with_field("command", json!("cargo test --release"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(10000))
                .build()
        );
        
        // Package application
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(3))
                .with_field("command", json!("tar czf app.tar.gz target/release/app"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        // Copy to server
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(4))
                .with_field("command", json!("scp app.tar.gz server:/tmp/"))
                .with_field("cwd", json!("/home/user/project"))
                .with_field("exit_code", json!(0))
                .with_field("duration_ms", json!(5000))
                .build()
        );
        
        // Deploy on server
        let deploy_commands = [
            "ssh server 'cd /opt/app && tar xzf /tmp/app.tar.gz'",
            "ssh server 'systemctl stop app.service'",
            "ssh server 'cp /tmp/app /opt/app/app'",
            "ssh server 'systemctl start app.service'",
            "ssh server 'systemctl status app.service'",
        ];
        
        for (i, cmd) in deploy_commands.iter().enumerate() {
            events.push(
                TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                    .with_timestamp(start_time + Duration::minutes(5) + Duration::seconds(i as i64 * 10))
                    .with_field("command", json!(cmd))
                    .with_field("cwd", json!("/home/user/project"))
                    .with_field("exit_code", json!(0))
                    .build()
            );
        }
        
        events
    }
}

/// Factory for generating error scenarios
pub struct ErrorScenarioFactory;

impl ErrorScenarioFactory {
    /// Create an error cascade scenario
    pub fn create_error_cascade() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(5);
        
        // Initial error
        events.push(
            TestEventBuilder::new(sources::SINEX, event_types::sinex::AUTOMATON_ERROR)
                .with_timestamp(start_time)
                .with_field("automaton", json!("health-aggregator"))
                .with_field("error", json!("Connection refused"))
                .with_field("severity", json!("error"))
                .with_field("retry_count", json!(0))
                .build()
        );
        
        // Retry attempts
        for i in 1..=3 {
            events.push(
                TestEventBuilder::new(sources::SINEX, event_types::sinex::AUTOMATON_ERROR)
                    .with_timestamp(start_time + Duration::seconds(i * 5))
                    .with_field("automaton", json!("health-aggregator"))
                    .with_field("error", json!("Connection refused"))
                    .with_field("severity", json!("warning"))
                    .with_field("retry_count", json!(i))
                    .build()
            );
        }
        
        // Service restart
        events.push(
            TestEventBuilder::new(sources::SYSTEMD, event_types::systemd::UNIT_STOPPED)
                .with_timestamp(start_time + Duration::seconds(20))
                .with_field("unit", json!("sinex-health-aggregator.service"))
                .with_field("result", json!("failed"))
                .build()
        );
        
        events.push(
            TestEventBuilder::new(sources::SYSTEMD, event_types::systemd::UNIT_STARTED)
                .with_timestamp(start_time + Duration::seconds(25))
                .with_field("unit", json!("sinex-health-aggregator.service"))
                .with_field("result", json!("success"))
                .build()
        );
        
        // Recovery
        events.push(
            TestEventBuilder::new(sources::SINEX, event_types::sinex::PROCESS_STARTED)
                .with_timestamp(start_time + Duration::seconds(30))
                .with_field("service", json!("sinex-health-aggregator"))
                .with_field("version", json!("1.0.0"))
                .with_field("recovery", json!(true))
                .build()
        );
        
        events
    }
    
    /// Create various error conditions
    pub fn create_error_conditions() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(10);
        
        // Permission denied
        events.push(
            TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_CREATED)
                .with_timestamp(start_time)
                .with_field("path", json!("/root/unauthorized.txt"))
                .with_field("error", json!("Permission denied"))
                .with_field("errno", json!(13))
                .build()
        );
        
        // Command failure
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_FAILED)
                .with_timestamp(start_time + Duration::minutes(1))
                .with_field("command", json!("false"))
                .with_field("exit_code", json!(1))
                .with_field("stderr", json!("Command failed"))
                .build()
        );
        
        // Disk space error
        events.push(
            TestEventBuilder::new(sources::FS, event_types::filesystem::FILE_CREATED)
                .with_timestamp(start_time + Duration::minutes(2))
                .with_field("path", json!("/tmp/large_file.dat"))
                .with_field("error", json!("No space left on device"))
                .with_field("errno", json!(28))
                .build()
        );
        
        // Network timeout
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_FAILED)
                .with_timestamp(start_time + Duration::minutes(3))
                .with_field("command", json!("curl https://unreachable.example.com"))
                .with_field("exit_code", json!(7))
                .with_field("error", json!("Failed to connect: Connection timed out"))
                .with_field("duration_ms", json!(30000))
                .build()
        );
        
        // Memory allocation failure
        events.push(
            TestEventBuilder::new(sources::SINEX, event_types::sinex::AUTOMATON_ERROR)
                .with_timestamp(start_time + Duration::minutes(4))
                .with_field("automaton", json!("content-automaton"))
                .with_field("error", json!("Cannot allocate memory"))
                .with_field("severity", json!("critical"))
                .with_field("memory_requested_mb", json!(4096))
                .build()
        );
        
        events
    }
    
    /// Create a recovery scenario
    pub fn create_recovery_scenario() -> Vec<RawEvent> {
        let mut events = Vec::new();
        let start_time = Utc::now() - Duration::minutes(8);
        
        // System under load
        events.push(
            TestEventBuilder::new(sources::SINEX, event_types::sinex::SYSTEM_HEALTH_SUMMARY)
                .with_timestamp(start_time)
                .with_field("cpu_usage_percent", json!(95.0))
                .with_field("memory_used_mb", json!(15000))
                .with_field("memory_total_mb", json!(16384))
                .with_field("status", json!("degraded"))
                .build()
        );
        
        // Errors start occurring
        for i in 0..5 {
            events.push(
                TestEventBuilder::new(sources::SINEX, event_types::sinex::AUTOMATON_ERROR)
                    .with_timestamp(start_time + Duration::seconds(10 + i * 5))
                    .with_field("automaton", json!("search-automaton"))
                    .with_field("error", json!("Processing timeout"))
                    .with_field("severity", json!("warning"))
                    .build()
            );
        }
        
        // Intervention - restart service
        events.push(
            TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_timestamp(start_time + Duration::minutes(2))
                .with_field("command", json!("systemctl restart sinex-search-automaton"))
                .with_field("exit_code", json!(0))
                .build()
        );
        
        // Service restart events
        events.push(
            TestEventBuilder::new(sources::SYSTEMD, event_types::systemd::UNIT_STOPPED)
                .with_timestamp(start_time + Duration::minutes(2) + Duration::seconds(5))
                .with_field("unit", json!("sinex-search-automaton.service"))
                .build()
        );
        
        events.push(
            TestEventBuilder::new(sources::SYSTEMD, event_types::systemd::UNIT_STARTED)
                .with_timestamp(start_time + Duration::minutes(2) + Duration::seconds(10))
                .with_field("unit", json!("sinex-search-automaton.service"))
                .build()
        );
        
        // System recovers
        events.push(
            TestEventBuilder::new(sources::SINEX, event_types::sinex::SYSTEM_HEALTH_SUMMARY)
                .with_timestamp(start_time + Duration::minutes(3))
                .with_field("cpu_usage_percent", json!(45.0))
                .with_field("memory_used_mb", json!(8192))
                .with_field("memory_total_mb", json!(16384))
                .with_field("status", json!("healthy"))
                .build()
        );
        
        // Normal operation resumes
        events.push(
            TestEventBuilder::new(sources::SINEX, event_types::sinex::PROCESS_HEARTBEAT)
                .with_timestamp(start_time + Duration::minutes(4))
                .with_field("service", json!("sinex-search-automaton"))
                .with_field("status", json!("healthy"))
                .with_field("events_processed", json!(150))
                .build()
        );
        
        events
    }
}

/// Convenience module for common test scenarios
pub mod scenarios {
    use super::*;
    
    /// Create a complete user workday scenario
    pub fn user_workday() -> Vec<RawEvent> {
        let mut events = Vec::new();
        
        // Morning startup
        events.extend(SystemEventFactory::create_system_startup());
        
        // Development sessions
        events.extend(UserActivityFactory::create_user_session(120, 50));
        events.extend(WorkflowFactory::create_git_workflow());
        events.extend(UserActivityFactory::create_development_workflow());
        
        // System monitoring throughout
        events.extend(SystemEventFactory::create_system_monitoring(240, 60));
        
        // Sort by timestamp
        events.sort_by_key(|e| e.ts_orig.unwrap_or_else(Utc::now));
        events
    }
    
    /// Create a stress test scenario
    pub fn stress_test_scenario() -> Vec<RawEvent> {
        let mut events = Vec::new();
        
        // High activity
        for _ in 0..10 {
            events.extend(UserActivityFactory::create_user_session(10, 20));
        }
        
        // Error conditions
        events.extend(ErrorScenarioFactory::create_error_conditions());
        events.extend(ErrorScenarioFactory::create_error_cascade());
        
        // Recovery
        events.extend(ErrorScenarioFactory::create_recovery_scenario());
        
        // Sort by timestamp
        events.sort_by_key(|e| e.ts_orig.unwrap_or_else(Utc::now));
        events
    }
    
    /// Create a data processing scenario
    pub fn data_processing_scenario() -> Vec<RawEvent> {
        let mut events = Vec::new();
        
        events.extend(WorkflowFactory::create_data_pipeline());
        events.extend(FileSystemScenarioFactory::create_file_workflow("/home/user/data/output"));
        events.extend(SystemEventFactory::create_system_monitoring(30, 30));
        
        // Sort by timestamp
        events.sort_by_key(|e| e.ts_orig.unwrap_or_else(Utc::now));
        events
    }
}