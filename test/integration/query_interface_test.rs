use crate::common::prelude::*;
use std::process::Command;
use crate::common::{events, assertions, generators};
use sinex_test_macros::sinex_test;

/// Test the Python CLI query interface
#[sinex_test]
async fn test_exo_cli_basic_queries(ctx: TestContext) -> sqlx::Result<()> {
    
    // Insert test events using helpers
    let test_events = vec![
        events::file_created_event("/test/file1.txt"),
        events::file_modified_event("/test/file2.txt"),
        events::kitty_event("ls -la"),
        crate::common::create_test_event_with_payload(
            "clipboard",
            "content.changed",
            json!({"content": "test data", "format": "text"})
        ),
    ];
    
    for event in test_events {
        assertions::assert_event_inserted(&ctx.pool(), &event).await.unwrap();
    }
    
    // Test various CLI commands
    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");
    
    // Test 1: Basic query (default limit)
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    assert!(output.status.success(), "CLI should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("filesystem"), "Should show filesystem events");
    
    // Test 2: Query with source filter
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .arg("--source")
        .arg("terminal")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("command.executed"), "Should show terminal events");
    assert!(!stdout.contains("file.created"), "Should not show filesystem events");
    
    // Test 3: Query with time filter
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .arg("--after")
        .arg("1 hour ago")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    assert!(output.status.success(), "Time filter query should work");
    
    // Test 4: Query with custom limit
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .arg("--limit")
        .arg("2")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    let event_count = stdout.matches("Event ID:").count();
    pretty_assertions::assert_eq!(event_count, 2, "Should return exactly 2 events");
    
    Ok(())
}

/// Test schema management commands
#[sinex_test]
async fn test_exo_cli_schema_commands(ctx: TestContext) -> sqlx::Result<()> {
    
    // Insert test schema
    let test_schema = json!({
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "size": {"type": "number"}
        },
        "required": ["path"]
    });
    
    // Use schema test utilities to insert schema
    crate::common::schema_test_utils::database::insert_test_schema(&ctx.pool(),
        "test.filesystem",
        "file_event",
        "1.0.0",
        test_schema
    ).await.unwrap();
    
    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");
    
    // Test schema list
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("schema")
        .arg("list")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("test.filesystem.v1"), "Should list the test schema");
    
    // Test schema get
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("schema")
        .arg("get")
        .arg("test.filesystem.v1")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"type\": \"object\""), "Should show schema JSON");
    assert!(stdout.contains("\"path\""), "Should show path property");
    
    Ok(())
}

/// Test agent monitoring commands
#[sinex_test]
async fn test_exo_cli_agent_commands(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>>{
    let _database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = ctx.pool();
    
    // Insert test agent manifest using helpers
    let mut manifest = generators::test_agent_manifest("test-collector");
    manifest.status = "active".to_string();
    manifest.produces_event_types = Some(json!(["filesystem", "terminal"]));
    assertions::assert_manifest_registered(&pool, &manifest).await?;
    
    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");
    
    // Test agent list
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("agent")
        .arg("list")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("test-collector"), "Should list the test agent");
    assert!(stdout.contains("active"), "Should show agent status");
    
    // Test agent status
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("agent")
        .arg("status")
        .arg("test-collector")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("test-collector"), "Should show agent details");
    assert!(stdout.contains("filesystem"), "Should show capabilities");
    Ok(())
}

/// Test error handling in CLI
#[sinex_test]
async fn test_exo_cli_error_handling(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");
    
    // Test 1: Invalid database URL
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .env("DATABASE_URL", "postgresql://invalid/db")
        .output()
        .expect("Failed to execute CLI");
    
    assert!(!output.status.success(), "Should fail with invalid DB");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Error") || stderr.contains("failed"), 
        "Should show error message");
    
    // Test 2: Invalid command
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("invalid-command")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    assert!(!output.status.success(), "Should fail with invalid command");
    
    // Test 3: Missing required argument
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("schema")
        .arg("get")
        // Missing schema ID
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    assert!(!output.status.success(), "Should fail with missing argument");
    Ok(())
}

/// Test advanced query features
#[sinex_test]
async fn test_exo_cli_advanced_queries(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let _database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = ctx.pool();
    
    // Insert events with different timestamps
    let base_time = chrono::Utc::now();
    
    for i in 0..20 {
        let event = crate::common::events::generic_adversarial_event(
            &format!("source_{}", if i % 2 == 0 { "a" } else { "b" }),
            &format!("event.type_{}", i % 3),
            json!({
                "index": i,
                "data": format!("test data {}", i),
                "important": i % 5 == 0
            }),
            Some(&(base_time - chrono::Duration::minutes(i + 1)).to_rfc3339())
        );
        
        sinex_db::queries::insert_event(&pool, &event).await.unwrap();
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");
    
    // Test 1: Multiple source filter
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .arg("--source")
        .arg("source_a")
        .arg("--limit")
        .arg("50")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("source_a"), "Should show source_a events");
    assert!(!stdout.contains("source_b"), "Should not show source_b events");
    
    // Test 2: Event type filter
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .arg("--type")
        .arg("event.type_0")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("event.type_0"), "Should show filtered event type");
    
    // Test 3: Time range query
    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .arg("--after")
        .arg("5 minutes ago")
        .arg("--before")
        .arg("2 minutes ago")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");
    
    assert!(output.status.success(), "Time range query should work");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let event_count = stdout.matches("Event ID:").count();
    assert!(event_count > 0 && event_count < 20, "Should return subset of events");
    Ok(())
}

/// Test output formatting options
#[sinex_test]
async fn test_exo_cli_output_formats(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let _database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = ctx.pool();
    
    // Insert a test event
    let event = crate::common::events::generic_adversarial_event("test", "test.event", json!({"test": true}), None);
    
    sinex_db::queries::insert_event(&pool, &event).await.unwrap();
    
    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");
    
    // Test different verbosity levels
    for verbose_flag in &["", "-v", "-vv"] {
        let mut cmd = Command::new("python3");
        cmd.arg(&cli_path).arg("query");
        
        if !verbose_flag.is_empty() {
            cmd.arg(verbose_flag);
        }
        
        let output = cmd
            .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
            .output()
            .expect("Failed to execute CLI");
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        
        if verbose_flag.is_empty() {
            // Normal output
            assert!(stdout.contains("test.event"), "Should show event type");
        } else if verbose_flag == &"-v" {
            // Verbose output
            assert!(stdout.contains("payload"), "Should show payload in verbose mode");
        } else {
            // Very verbose output
            assert!(stdout.contains("message"), "Should show full details in -vv mode");
        }
    }
    Ok(())
}