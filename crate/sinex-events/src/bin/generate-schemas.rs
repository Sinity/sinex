//! Schema Generation Tool
//!
//! Generates JSON Schema files from Rust structs with #[derive(JsonSchema)]

use anyhow::{Context, Result};
use schemars::schema_for;
use sinex_events::strongly_typed_events::*;
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    let schema_dir = Path::new("schemas/v1");

    println!("Generating JSON schemas...");

    // Filesystem schemas
    generate_schema::<FileCreatedPayload>(schema_dir, "filesystem", "file_created.json")?;
    generate_schema::<FileModifiedPayload>(schema_dir, "filesystem", "file_modified.json")?;
    generate_schema::<FileDeletedPayload>(schema_dir, "filesystem", "file_deleted.json")?;
    generate_schema::<FileMovedPayload>(schema_dir, "filesystem", "file_moved.json")?;
    generate_schema::<DirCreatedPayload>(schema_dir, "filesystem", "dir_created.json")?;
    generate_schema::<DirDeletedPayload>(schema_dir, "filesystem", "dir_deleted.json")?;

    // Shell schemas
    generate_schema::<CommandExecutedPayload>(schema_dir, "shell", "command_executed.json")?;
    generate_schema::<CommandCompletedPayload>(schema_dir, "shell", "command_completed.json")?;
    generate_schema::<SessionStartedPayload>(schema_dir, "shell", "session_started.json")?;
    generate_schema::<SessionEndedPayload>(schema_dir, "shell", "session_ended.json")?;

    // Clipboard schemas
    generate_schema::<ClipboardCopiedPayload>(schema_dir, "clipboard", "content_copied.json")?;
    generate_schema::<ClipboardSelectedPayload>(schema_dir, "clipboard", "content_selected.json")?;

    // Window manager schemas
    generate_schema::<WindowOpenedPayload>(schema_dir, "window_manager", "window_opened.json")?;
    generate_schema::<WindowClosedPayload>(schema_dir, "window_manager", "window_closed.json")?;
    generate_schema::<WindowFocusedPayload>(schema_dir, "window_manager", "window_focused.json")?;
    generate_schema::<WorkspaceSwitchedPayload>(
        schema_dir,
        "window_manager",
        "workspace_switched.json",
    )?;

    // System schemas
    generate_schema::<JournalEntryPayload>(schema_dir, "system", "journal_entry.json")?;
    generate_schema::<SystemStatePayload>(schema_dir, "system", "state_changed.json")?;

    // Scan schemas
    generate_schema::<ScanStartedPayload>(schema_dir, "scan", "scan_started.json")?;
    generate_schema::<ScanCompletedPayload>(schema_dir, "scan", "scan_completed.json")?;

    // Process schemas
    generate_schema::<ProcessStartedPayload>(schema_dir, "process", "process_started.json")?;
    generate_schema::<ProcessHeartbeatPayload>(schema_dir, "process", "process_heartbeat.json")?;
    generate_schema::<ProcessShutdownPayload>(schema_dir, "process", "process_shutdown.json")?;

    // Shell import schemas
    generate_schema::<AtuinEntryPayload>(schema_dir, "shell", "atuin_entry.json")?;
    generate_schema::<CommandImportedPayload>(schema_dir, "shell", "command_imported.json")?;

    // Sensor schemas
    generate_schema::<SensorActivatedPayload>(schema_dir, "process", "sensor_activated.json")?;
    generate_schema::<SensorDeactivatedPayload>(schema_dir, "process", "sensor_deactivated.json")?;

    println!("✅ Schema generation complete!");

    Ok(())
}

fn generate_schema<T: schemars::JsonSchema>(
    base_dir: &Path,
    category: &str,
    filename: &str,
) -> Result<()> {
    let schema = schema_for!(T);

    // Convert to JSON
    let mut json_schema =
        serde_json::to_value(&schema).context("Failed to convert schema to JSON")?;

    // Add metadata
    if let Some(obj) = json_schema.as_object_mut() {
        obj.insert(
            "$schema".to_string(),
            serde_json::json!("http://json-schema.org/draft-07/schema#"),
        );
        obj.insert(
            "$id".to_string(),
            serde_json::json!(format!(
                "https://sinex.io/schemas/v1/{}/{}",
                category, filename
            )),
        );
    }

    // Create directory if it doesn't exist
    let dir_path = base_dir.join(category);
    fs::create_dir_all(&dir_path)
        .with_context(|| format!("Failed to create directory: {:?}", dir_path))?;

    // Write schema to file
    let file_path = dir_path.join(filename);
    let json_string =
        serde_json::to_string_pretty(&json_schema).context("Failed to serialize JSON schema")?;

    fs::write(&file_path, json_string)
        .with_context(|| format!("Failed to write schema to: {:?}", file_path))?;

    println!("  📝 Generated: {}/{}", category, filename);

    Ok(())
}
