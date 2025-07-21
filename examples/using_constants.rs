//! Using Constants Examples
//! 
//! This file demonstrates proper usage of constants from sinex-events
//! instead of hardcoding string literals throughout the code.

use sinex_events::{event_types, sources, services, RawEvent, EventFactory};
use sinex_ulid::Ulid;
use serde_json::json;

/// Example 1: Using event type constants
fn create_heartbeat_event(sequence: u64) -> RawEvent {
    // ❌ WRONG: Hardcoded event type
    // let factory = EventFactory::new("sinex.process");
    // factory.create_event("process.heartbeat", json!({ "sequence": sequence }))

    // ✅ CORRECT: Using constants
    let factory = EventFactory::new(sources::SINEX);
    factory.create_event(event_types::sinex::PROCESS_HEARTBEAT, json!({ "sequence": sequence }))
}

/// Example 2: Checking event types
fn is_system_event(event: &RawEvent) -> bool {
    // ❌ WRONG: Hardcoded comparisons
    // event.event_type == "process.start" 
    //     || event.event_type == "process.heartbeat"
    //     || event.event_type == "process.stop"

    // ✅ CORRECT: Using constants
    matches!(
        event.event_type.as_str(),
        event_types::sinex::PROCESS_STARTED
            | event_types::sinex::PROCESS_HEARTBEAT
            | event_types::sinex::PROCESS_SHUTDOWN
    )
}

/// Example 3: Source-specific event creation
fn create_file_event(path: &str, action: FileAction) -> RawEvent {
    // ❌ WRONG: Hardcoded source and event types
    // let factory = EventFactory::new("fs");
    // let event_type = match action {
    //     FileAction::Created => "file.created",
    //     FileAction::Modified => "file.modified",
    //     FileAction::Deleted => "file.deleted",
    // };
    // factory.create_event(event_type, json!({ "path": path }))

    // ✅ CORRECT: Using constants and EventFactory
    let factory = EventFactory::new(sources::FS);
    match action {
        FileAction::Created => factory.filesystem().path(path).created().build(),
        FileAction::Modified => factory.filesystem().path(path).modified().build(),
        FileAction::Deleted => factory.filesystem().path(path).deleted().build(),
    }
}

/// Example 4: Terminal-specific events with proper source
fn create_terminal_event(terminal_type: TerminalType, content: &str) -> RawEvent {
    // ❌ WRONG: Building source strings
    // let source = format!("terminal.{}", terminal_type.as_str());

    // ✅ CORRECT: Using predefined constants
    let source = match terminal_type {
        TerminalType::Kitty => sources::TERMINAL_KITTY,
        TerminalType::Alacritty => sources::TERMINAL_KITTY /* TODO: Add ALACRITTY constant */,
        TerminalType::WezTerm => sources::TERMINAL_KITTY /* TODO: Add WEZTERM constant */,
    };

    let factory = EventFactory::new(source);
    factory.terminal()
        .command_output(content)
        .timestamp(chrono::Utc::now())
        .build_executed()
}

/// Example 5: Service identification
fn identify_service(service_name: &str) -> Option<ServiceInfo> {
    // ❌ WRONG: Hardcoded service names
    // match service_name {
    //     "ingestd" => Some(ServiceInfo::Ingestd),
    //     "gateway" => Some(ServiceInfo::Gateway),
    //     _ => None,
    // }

    // ✅ CORRECT: Using service constants
    match service_name {
        services::INGESTD => Some(ServiceInfo::Ingestd),
        services::GATEWAY => Some(ServiceInfo::Gateway),
        services::ANALYTICS_AUTOMATON => Some(ServiceInfo::Analytics),
        _ => None,
    }
}

/// Example 6: Event filtering by type
fn filter_knowledge_events(events: Vec<RawEvent>) -> Vec<RawEvent> {
    // ❌ WRONG: Hardcoded prefixes
    // events.into_iter()
    //     .filter(|e| e.event_type.starts_with("knowledge."))
    //     .collect()

    // ✅ CORRECT: Using constant prefixes
    events.into_iter()
        .filter(|e| {
            matches!(
                e.event_type.as_str(),
                "knowledge.note.created" /* TODO: Add to constants */
                    | "knowledge.note.updated" /* TODO: Add to constants */
                    | "knowledge.note.deleted" /* TODO: Add to constants */
                    | "knowledge.tag.added" /* TODO: Add to constants */
                    | "knowledge.tag.removed" /* TODO: Add to constants */
            )
        })
        .collect()
}

/// Example 7: Building event type hierarchies
mod event_categorizer {
    use super::*;

    pub fn categorize_event(event_type: &str) -> EventCategory {
        // ✅ CORRECT: Using constant modules for categorization
        if event_type.starts_with("file.") {
            EventCategory::File
        } else if event_type.starts_with("process.") {
            EventCategory::Process
        } else if event_type.starts_with("knowledge.") {
            EventCategory::Knowledge
        } else if event_type.starts_with("terminal.") {
            EventCategory::Terminal
        } else {
            EventCategory::Other
        }
    }

    pub fn get_all_file_events() -> Vec<&'static str> {
        // ✅ CORRECT: Centralized event type listing
        vec![
            event_types::filesystem::FILE_CREATED,
            event_types::filesystem::FILE_MODIFIED,
            event_types::filesystem::FILE_DELETED,
            event_types::filesystem::FILE_RENAMED,
            event_types::filesystem::FILE_MODIFIED,
        ]
    }
}

/// Example 8: Configuration with constants
fn create_default_config() -> Config {
    // ❌ WRONG: Hardcoded configuration values
    // Config {
    //     event_types: vec![
    //         "file.created".to_string(),
    //         "file.modified".to_string(),
    //         "process.start".to_string(),
    //     ],
    //     sources: vec!["fs".to_string(), "terminal.kitty".to_string()],
    // }

    // ✅ CORRECT: Using constants for configuration
    Config {
        event_types: vec![
            event_types::filesystem::FILE_CREATED.to_string(),
            event_types::filesystem::FILE_MODIFIED.to_string(),
            event_types::sinex::PROCESS_STARTED.to_string(),
        ],
        sources: vec![
            sources::FS.to_string(),
            sources::TERMINAL_KITTY.to_string(),
        ],
        services: vec![
            services::INGESTD.to_string(),
            services::GATEWAY.to_string(),
        ],
    }
}

/// Example 9: Pattern matching with constants
fn handle_event(event: &RawEvent) -> Result<(), sinex_error::CoreError> {
    use sinex_error::CoreError;

    match (event.source.as_str(), event.event_type.as_str()) {
        // ✅ CORRECT: Pattern matching with constants
        (sources::FS, event_types::filesystem::FILE_CREATED) => {
            handle_file_created(event)
        }
        (sources::FS, event_types::filesystem::FILE_DELETED) => {
            handle_file_deleted(event)
        }
        (sources::TERMINAL_KITTY | sources::TERMINAL_KITTY /* TODO: Add ALACRITTY constant */, event_types::shell::COMMAND_OUTPUT) => {
            handle_terminal_output(event)
        }
        (sources::SINEX, event_types::sinex::PROCESS_HEARTBEAT) => {
            handle_heartbeat(event)
        }
        _ => {
            // Unknown event type/source combination
            Err(CoreError::Unknown(format!(
                "Unknown event type: {} from source: {}",
                event.event_type, event.source
            )))
        }
    }
}

// Helper types and functions for examples
enum FileAction {
    Created,
    Modified,
    Deleted,
}

enum TerminalType {
    Kitty,
    Alacritty,
    WezTerm,
}

enum ServiceInfo {
    Ingestd,
    Gateway,
    Analytics,
}

enum EventCategory {
    File,
    Process,
    Knowledge,
    Terminal,
    Other,
}

struct Config {
    event_types: Vec<String>,
    sources: Vec<String>,
    services: Vec<String>,
}

fn handle_file_created(_event: &RawEvent) -> Result<(), sinex_error::CoreError> {
    Ok(())
}

fn handle_file_deleted(_event: &RawEvent) -> Result<(), sinex_error::CoreError> {
    Ok(())
}

fn handle_terminal_output(_event: &RawEvent) -> Result<(), sinex_error::CoreError> {
    Ok(())
}

fn handle_heartbeat(_event: &RawEvent) -> Result<(), sinex_error::CoreError> {
    Ok(())
}

fn main() {
    println!("This is an example file demonstrating usage of constants from sinex-events.");
    println!("See the individual functions for usage examples.");

    // Example usage
    let heartbeat = create_heartbeat_event(42);
    println!("Created heartbeat event: {:?}", heartbeat.event_type);

    let file_event = create_file_event("/tmp/test.txt", FileAction::Created);
    println!("Created file event: {:?}", file_event.event_type);
}