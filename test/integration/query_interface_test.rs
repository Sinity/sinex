use sinex_db::{DbPool, establish_pool};
use sinex_ulid::Ulid;
use serde_json::json;
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

/// Test the Python CLI query interface
#[tokio::test]
async fn test_exo_cli_basic_queries() {
    let pool = establish_pool().await.expect("Failed to create pool");
    
    // Insert test events
    let test_events = vec![
        ("filesystem", "file.created", json!({"path": "/test/file1.txt", "size": 1024})),
        ("filesystem", "file.modified", json!({"path": "/test/file2.txt", "size": 2048})),
        ("terminal", "command.executed", json!({"command": "ls -la", "exit_code": 0})),
        ("clipboard", "content.changed", json!({"content": "test data", "format": "text"})),
    ];
    
    for (source, event_type, payload) in test_events {
        let event = sinex_core::RawEvent {
            id: Ulid::new(),
            source: source.to_string(),
            event_type: event_type.to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test-host".to_string(),
            ingestor_version: Some("test-1.0".to_string()),
            payload_schema_id: None,
            payload,
        };
        
        sinex_db::queries::insert_event(&pool, &event).await.unwrap();
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
    assert_eq!(event_count, 2, "Should return exactly 2 events");
}

/// Test schema management commands
#[tokio::test]
async fn test_exo_cli_schema_commands() {
    let pool = establish_pool().await.expect("Failed to create pool");
    
    // Insert test schema
    let test_schema = json!({
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "size": {"type": "number"}
        },
        "required": ["path"]
    });
    
    sqlx::query("INSERT INTO sinex_schemas.event_payload_schemas (schema_id, schema_json) VALUES ($1, $2)")
        .bind("test.filesystem.v1")
        .bind(&test_schema)
        .execute(&pool)
        .await
        .unwrap();
    
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
}

/// Test agent monitoring commands
#[tokio::test]
async fn test_exo_cli_agent_commands() {
    let pool = establish_pool().await.expect("Failed to create pool");
    
    // Insert test agent manifest
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (name, version, capabilities, status, last_heartbeat) 
         VALUES ($1, $2, $3, $4, $5)"
    )
        .bind("test-collector")
        .bind("1.0.0")
        .bind(json!(["filesystem", "terminal"]))
        .bind("active")
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .unwrap();
    
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
}

/// Test error handling in CLI
#[tokio::test]
async fn test_exo_cli_error_handling() {
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
}

/// Test advanced query features
#[tokio::test]
async fn test_exo_cli_advanced_queries() {
    let pool = establish_pool().await.expect("Failed to create pool");
    
    // Insert events with different timestamps
    let base_time = chrono::Utc::now();
    
    for i in 0..20 {
        let event = sinex_core::RawEvent {
            id: Ulid::new(),
            source: if i % 2 == 0 { "source_a" } else { "source_b" }.to_string(),
            event_type: format!("event.type_{}", i % 3),
            ts_ingest: base_time - chrono::Duration::minutes(i),
            ts_orig: Some(base_time - chrono::Duration::minutes(i + 1)),
            host: "test-host".to_string(),
            ingestor_version: Some("test-1.0".to_string()),
            payload_schema_id: None,
            payload: json!({
                "index": i,
                "data": format!("test data {}", i),
                "important": i % 5 == 0
            }),
        };
        
        sinex_db::queries::insert_event(&pool, &event).await.unwrap();
        
        // Small delay to ensure different timestamps
        sleep(Duration::from_millis(10)).await;
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
}

/// Test output formatting options
#[tokio::test]
async fn test_exo_cli_output_formats() {
    let pool = establish_pool().await.expect("Failed to create pool");
    
    // Insert a test event
    let event = sinex_core::RawEvent {
        id: Ulid::new(),
        source: "test".to_string(),
        event_type: "test.event".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test-host".to_string(),
        ingestor_version: Some("test-1.0".to_string()),
        payload_schema_id: None,
        payload: json!({
            "message": "Test message",
            "level": "info",
            "tags": ["test", "cli"]
        }),
    };
    
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
}