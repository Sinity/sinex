use crate::common::prelude::*;
use sinex_events::atuin::{AtuinDbReader, AtuinConfig, CommandExecutedAtuin, CommandExecutedAtuinPayload};
use sinex_core::{EventSource, EventType, EventSourceContext};
use sinex_db::models::RawEvent;
use tokio::sync::mpsc;
use std::path::PathBuf;
use std::time::Duration;
use chrono::{Utc, TimeZone};
use crate::common::{resources, create_test_db_pool, database, event_sources};
use anyhow::Result;




/// Create a test Atuin database with sample data using real SQLite schema
fn create_test_atuin_db(path: &PathBuf, entries: Vec<TestAtuinEntry>) -> anyhow::Result<()> {
    use rusqlite::{Connection, params};
    
    let conn = Connection::open(path)?;
    
    // Create the Atuin history table with the real schema
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS history (
            id TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            exit INTEGER NOT NULL,
            command TEXT NOT NULL,
            cwd TEXT NOT NULL,
            session TEXT NOT NULL,
            hostname TEXT NOT NULL,
            deleted_at INTEGER
        )
        "#,
        [],
    )?;
    
    // Insert test entries if provided
    for entry in entries {
        conn.execute(
            r#"
            INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
            "#,
            params![
                entry.id,
                entry.timestamp_ns,
                entry.duration_ns,
                entry.exit_code,
                entry.command,
                entry.cwd,
                entry.session,
                entry.hostname,
            ],
        )?;
    }
    
    Ok(())
}

struct TestAtuinEntry {
    id: String,
    timestamp_ns: i64,
    duration_ns: i64,
    exit_code: i32,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
}

impl TestAtuinEntry {
    fn builder() -> TestAtuinEntryBuilder {
        TestAtuinEntryBuilder::default()
    }
    
    fn simple_command(command: &str) -> Self {
        Self {
            id: format!("test-{}", uuid::Uuid::new_v4()),
            timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            duration_ns: 1_000_000_000, // 1 second default
            exit_code: 0,
            command: command.to_string(),
            cwd: "/tmp".to_string(),
            session: "test-session".to_string(),
            hostname: "test-host".to_string(),
        }
    }
}

#[derive(Default)]
struct TestAtuinEntryBuilder {
    id: Option<String>,
    timestamp_ns: Option<i64>,
    duration_ns: Option<i64>,
    exit_code: Option<i32>,
    command: Option<String>,
    cwd: Option<String>,
    session: Option<String>,
    hostname: Option<String>,
}

impl TestAtuinEntryBuilder {
    fn with_id(mut self, id: &str) -> Self {
        self.id = Some(id.to_string());
        self
    }
    
    fn with_command(mut self, command: &str) -> Self {
        self.command = Some(command.to_string());
        self
    }
    
    fn with_exit_code(mut self, code: i32) -> Self {
        self.exit_code = Some(code);
        self
    }
    
    fn with_timestamp_seconds(mut self, timestamp: i64) -> Self {
        self.timestamp_ns = Some(timestamp * 1_000_000_000);
        self
    }
    
    fn with_duration_ms(mut self, duration_ms: i64) -> Self {
        self.duration_ns = Some(duration_ms * 1_000_000);
        self
    }
    
    fn with_cwd(mut self, cwd: &str) -> Self {
        self.cwd = Some(cwd.to_string());
        self
    }
    
    fn build(self) -> TestAtuinEntry {
        TestAtuinEntry {
            id: self.id.unwrap_or_else(|| format!("test-{}", uuid::Uuid::new_v4())),
            timestamp_ns: self.timestamp_ns.unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()),
            duration_ns: self.duration_ns.unwrap_or(1_000_000_000),
            exit_code: self.exit_code.unwrap_or(0),
            command: self.command.unwrap_or_else(|| "echo test".to_string()),
            cwd: self.cwd.unwrap_or_else(|| "/tmp".to_string()),
            session: self.session.unwrap_or_else(|| "test-session".to_string()),
            hostname: self.hostname.unwrap_or_else(|| "test-host".to_string()),
        }
    }
}

#[tokio::test]
async fn test_atuin_reader_initialization() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Create empty database
    create_test_atuin_db(&db_path, vec![]).unwrap();
    
    let config = AtuinConfig {
        db_path: db_path.clone(),
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let reader = AtuinDbReader::initialize(ctx).await;
    assert!(reader.is_ok(), "Should initialize with valid database");
    
    // Test with non-existent database
    let bad_config = AtuinConfig {
        db_path: temp_dir.path().join("nonexistent.db"),
        ..config
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&bad_config).unwrap());
    let reader = AtuinDbReader::initialize(ctx).await;
    assert!(reader.is_err(), "Should fail with non-existent database");
    Ok(())
}

#[tokio::test]
async fn test_atuin_event_capture() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Create test entries using builder pattern
    let now = Utc::now();
    let entries = vec![
        TestAtuinEntry::builder()
            .with_id("test-id-1")
            .with_command("ls -la")
            .with_cwd("/home/test")
            .with_duration_ms(1000)
            .with_timestamp_seconds(now.timestamp())
            .build(),
        TestAtuinEntry::builder()
            .with_id("test-id-2")
            .with_command("git status")
            .with_exit_code(1)
            .with_cwd("/home/test/project")
            .with_duration_ms(500)
            .with_timestamp_seconds(now.timestamp() + 10)
            .build(),
    ];
    
    create_test_atuin_db(&db_path, entries).unwrap();
    
    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    
    // Start reading in background
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Collect events
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= 2 {
                break;
            }
        }
    }).await.unwrap();
    
    handle.abort();
    
    // Verify events
    assert_eq!(events.len(), 2, "Should capture both test entries");
    
    // Check first event
    let event1 = &events[0];
    assert_eq!(event1.event_type, CommandExecutedAtuin::EVENT_NAME);
    assert_eq!(event1.source, "ingestor.atuin_db_reader");
    
    let payload1: CommandExecutedAtuinPayload = serde_json::from_value(event1.payload.clone()).unwrap();
    assert_eq!(payload1.command_string, "ls -la");
    assert_eq!(payload1.cwd, "/home/test");
    assert_eq!(payload1.exit_code, 0);
    assert_eq!(payload1.atuin_history_id, "test-id-1");
    
    // Check second event
    let event2 = &events[1];
    let payload2: CommandExecutedAtuinPayload = serde_json::from_value(event2.payload.clone()).unwrap();
    assert_eq!(payload2.command_string, "git status");
    assert_eq!(payload2.exit_code, 1);
    assert_eq!(payload2.atuin_history_id, "test-id-2");
    Ok(())
}

#[tokio::test]
async fn test_atuin_watermarking() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Create initial entries
    let entries = vec![
        TestAtuinEntry {
            id: "aaa".to_string(),
            timestamp_ns: 1_000_000_000,
            duration_ns: 100_000_000,
            exit_code: 0,
            command: "echo first".to_string(),
            cwd: "/tmp".to_string(),
            session: "s1".to_string(),
            hostname: "host1".to_string(),
        },
        TestAtuinEntry {
            id: "bbb".to_string(),
            timestamp_ns: 2_000_000_000,
            duration_ns: 100_000_000,
            exit_code: 0,
            command: "echo second".to_string(),
            cwd: "/tmp".to_string(),
            session: "s1".to_string(),
            hostname: "host1".to_string(),
        },
    ];
    
    create_test_atuin_db(&db_path, entries).unwrap();
    
    let config = AtuinConfig {
        db_path: db_path.clone(),
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    // First read
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Collect initial events
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= 2 {
                break;
            }
        }
    }).await.unwrap();
    
    handle.abort();
    assert_eq!(events.len(), 2, "Should get both initial entries");
    
    // Now add a new entry to the SQLite database to test watermarking
    let new_entry = TestAtuinEntry::builder()
        .with_command("echo third")
        .with_timestamp_seconds(3000)
        .build();
    
    // Add the new entry to the existing SQLite database
    {
        use rusqlite::{Connection, params};
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            r#"
            INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
            "#,
            params![
                new_entry.id,
                new_entry.timestamp_ns,
                new_entry.duration_ns,
                new_entry.exit_code,
                new_entry.command,
                new_entry.cwd,
                new_entry.session,
                new_entry.hostname,
            ],
        ).unwrap();
    }
    
    // Setup PostgreSQL database for watermarking
    let pg_pool = create_test_db_pool().await?;
    
    // Second read with PostgreSQL connection for watermarking
    let ctx_with_db = event_sources::test_context(serde_json::to_value(&config).unwrap())
        .with_db_pool(pg_pool);
    let mut reader2 = AtuinDbReader::initialize(ctx_with_db).await.unwrap();
    let (tx2, mut rx2) = mpsc::channel(100);
    
    let handle2 = tokio::spawn(async move {
        let _ = reader2.stream_events(tx2).await;
    });
    
    // Collect events from second read - should include watermarking behavior
    let mut second_events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx2.recv().await {
            second_events.push(event);
            if second_events.len() >= 3 {
                break;
            }
        }
    }).await.unwrap();
    
    handle2.abort();
    
    // With proper watermarking, this should get all 3 entries on first run
    // (since no previous watermark exists)
    assert_eq!(second_events.len(), 3, "Should get all 3 entries including the new one");
    
    // Verify the new entry is included
    let commands: Vec<String> = second_events.iter()
        .map(|e| {
            let payload: CommandExecutedAtuinPayload = serde_json::from_value(e.payload.clone()).unwrap();
            payload.command_string
        })
        .collect();
    
    assert!(commands.contains(&"echo third".to_string()), "Should include the newly added command");
    Ok(())
}

#[tokio::test]
async fn test_atuin_watermarking_resume_behavior() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Create initial batch of entries
    let initial_entries = vec![
        TestAtuinEntry::builder()
            .with_command("echo first")
            .with_timestamp_seconds(1000)
            .build(),
        TestAtuinEntry::builder()
            .with_command("echo second")
            .with_timestamp_seconds(2000)
            .build(),
    ];
    
    create_test_atuin_db(&db_path, initial_entries).unwrap();
    
    let config = AtuinConfig {
        db_path: db_path.clone(),
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    // Setup PostgreSQL database for watermarking persistence
    let pg_pool = create_test_db_pool().await?;
    
    // Create test agent for work queue operations
    crate::common::create_test_agent(&pg_pool, "test-agent").await.unwrap();
    
    // First run: Process initial entries with watermarking
    let ctx1 = event_sources::test_context(serde_json::to_value(&config).unwrap())
        .with_db_pool(pg_pool.clone());
    let mut reader1 = AtuinDbReader::initialize(ctx1).await.unwrap();
    let (tx1, mut rx1) = mpsc::channel(100);
    
    let handle1 = tokio::spawn(async move {
        let _ = reader1.stream_events(tx1).await;
    });
    
    // Collect and PERSIST initial events for watermarking to work
    let mut first_run_events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx1.recv().await {
            // CRITICAL: Save event to database for watermarking to work
            // Use the simplified pattern from other tests
            let _inserted_id = crate::common::insert_event(&pg_pool, &event).await.unwrap();
            
            first_run_events.push(event);
            if first_run_events.len() >= 2 {
                break;
            }
        }
    }).await.unwrap();
    
    handle1.abort();
    assert_eq!(first_run_events.len(), 2, "First run should process both initial entries");
    
    // Add a small delay to ensure watermarking is written
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Add new entries while the reader is "offline"
    // Use timestamps clearly newer than the initial entries (1000, 2000)
    let new_entries = vec![
        TestAtuinEntry::builder()
            .with_command("echo third")
            .with_timestamp_seconds(10000)  // Much newer timestamp
            .build(),
        TestAtuinEntry::builder()
            .with_command("echo fourth")
            .with_timestamp_seconds(20000)  // Much newer timestamp  
            .build(),
    ];
    
    // Add new entries to SQLite
    {
        use rusqlite::{Connection, params};
        let conn = Connection::open(&db_path).unwrap();
        for entry in new_entries {
            conn.execute(
                r#"
                INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
                "#,
                params![
                    entry.id,
                    entry.timestamp_ns,
                    entry.duration_ns,
                    entry.exit_code,
                    entry.command,
                    entry.cwd,
                    entry.session,
                    entry.hostname,
                ],
            ).unwrap();
        }
    }
    
    // Second run: Should only process NEW entries due to watermarking
    let ctx2 = event_sources::test_context(serde_json::to_value(&config).unwrap())
        .with_db_pool(pg_pool);
    let mut reader2 = AtuinDbReader::initialize(ctx2).await.unwrap();
    let (tx2, mut rx2) = mpsc::channel(100);
    
    let handle2 = tokio::spawn(async move {
        let _ = reader2.stream_events(tx2).await;
    });
    
    // Collect second run events
    let mut second_run_events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx2.recv().await {
            second_run_events.push(event);
            if second_run_events.len() >= 2 {
                break;
            }
        }
    }).await.unwrap();
    
    handle2.abort();
    
    // Watermarking should ensure only NEW entries are processed
    assert_eq!(second_run_events.len(), 2, "Second run should only process the 2 new entries");
    
    let second_run_commands: Vec<String> = second_run_events.iter()
        .map(|e| {
            let payload: CommandExecutedAtuinPayload = serde_json::from_value(e.payload.clone()).unwrap();
            payload.command_string
        })
        .collect();
    
    // Debug: Print what commands were actually captured in the second run
    println!("DEBUG: Second run captured commands: {:?}", second_run_commands);
    
    // Should only contain the NEW commands, not the old ones
    assert!(second_run_commands.contains(&"echo third".to_string()));
    assert!(second_run_commands.contains(&"echo fourth".to_string()));
    assert!(!second_run_commands.contains(&"echo first".to_string()));
    assert!(!second_run_commands.contains(&"echo second".to_string()));
    
    println!("✅ Watermarking test passed! First run: {} events, Second run: {} events", 
             first_run_events.len(), second_run_events.len());
    Ok(())
}

#[tokio::test]
async fn test_atuin_timestamp_conversion() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Create entry with specific timestamp
    let timestamp = Utc.timestamp_opt(1749700000, 0).unwrap(); // Known timestamp
    let entries = vec![
        TestAtuinEntry {
            id: "time-test".to_string(),
            timestamp_ns: timestamp.timestamp_nanos_opt().unwrap(),
            duration_ns: 2_500_000_000, // 2.5 seconds
            exit_code: 0,
            command: "sleep 2.5".to_string(),
            cwd: "/tmp".to_string(),
            session: "s1".to_string(),
            hostname: "host1".to_string(),
        },
    ];
    
    create_test_atuin_db(&db_path, entries).unwrap();
    
    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Get the event
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    
    handle.abort();
    
    let payload: CommandExecutedAtuinPayload = serde_json::from_value(event.payload).unwrap();
    
    // Verify timestamps
    assert_eq!(payload.ts_end_orig, timestamp);
    assert_eq!(payload.duration_ns, 2_500_000_000);
    
    // Start time should be 2.5 seconds before end time
    let expected_start = timestamp - chrono::Duration::milliseconds(2500);
    assert_eq!(payload.ts_start_orig, expected_start);
    Ok(())
}

#[tokio::test]
async fn test_atuin_error_conditions() -> Result<(), Box<dyn std::error::Error>> {
    // Test with non-existent database file
    let temp_dir = resources::temp_dir()?;
    let bad_config = AtuinConfig {
        db_path: temp_dir.path().join("nonexistent.db"),
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&bad_config).unwrap());
    let result = AtuinDbReader::initialize(ctx).await;
    assert!(result.is_err(), "Should fail with non-existent database");
    
    // Test with corrupted database file
    let corrupted_db_path = temp_dir.path().join("corrupted.db");
    std::fs::write(&corrupted_db_path, "this is not a valid sqlite database").unwrap();
    
    let corrupted_config = AtuinConfig {
        db_path: corrupted_db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&corrupted_config).unwrap());
    // Note: AtuinDbReader initialization only checks if file exists, 
    // actual corruption would be detected during event streaming
    let reader = AtuinDbReader::initialize(ctx).await;
    assert!(reader.is_ok(), "Initialization should succeed even with corrupted file");
    Ok(())
}

#[tokio::test]
async fn test_atuin_builder_patterns() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Test using the builder pattern for more readable test data
    let entries = vec![
        TestAtuinEntry::builder()
            .with_command("git status")
            .with_exit_code(0)
            .with_cwd("/home/user/project")
            .with_duration_ms(250)
            .build(),
        TestAtuinEntry::builder()
            .with_command("cargo test")
            .with_exit_code(1)
            .with_duration_ms(5000)
            .with_timestamp_seconds(1000)
            .build(),
        TestAtuinEntry::simple_command("ls -la"),
    ];
    
    create_test_atuin_db(&db_path, entries).unwrap();
    
    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Collect events
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= 3 {
                break;
            }
        }
    }).await.unwrap();
    
    handle.abort();
    
    // Verify different command types were captured
    assert_eq!(events.len(), 3);
    let commands: Vec<String> = events.iter()
        .map(|e| {
            let payload: CommandExecutedAtuinPayload = serde_json::from_value(e.payload.clone()).unwrap();
            payload.command_string
        })
        .collect();
    
    assert!(commands.contains(&"git status".to_string()));
    assert!(commands.contains(&"cargo test".to_string()));
    assert!(commands.contains(&"ls -la".to_string()));
    
    // Verify exit codes were preserved
    let exit_codes: Vec<i32> = events.iter()
        .map(|e| {
            let payload: CommandExecutedAtuinPayload = serde_json::from_value(e.payload.clone()).unwrap();
            payload.exit_code
        })
        .collect();
    
    assert!(exit_codes.contains(&0)); // git status success
    assert!(exit_codes.contains(&1)); // cargo test failure
    Ok(())
}

#[tokio::test]
async fn test_atuin_edge_cases() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Test edge cases: empty commands, very long commands, special characters
    let entries = vec![
        TestAtuinEntry::builder()
            .with_command("") // Empty command
            .build(),
        TestAtuinEntry::builder()
            .with_command("echo 'Hello \"World\" with $VARS and (parens) and [brackets]'") // Special chars
            .build(),
        TestAtuinEntry::builder()
            .with_command(&"x".repeat(1000)) // Very long command
            .build(),
        TestAtuinEntry::builder()
            .with_command("echo\n\t\r") // Whitespace characters
            .build(),
    ];
    
    create_test_atuin_db(&db_path, entries).unwrap();
    
    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Collect events - should handle all edge cases without panicking
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= 4 {
                break;
            }
        }
    }).await.unwrap();
    
    handle.abort();
    
    assert_eq!(events.len(), 4, "Should process all edge case commands");
    
    // Verify all events have valid structure
    for event in &events {
        assert!(!event.source.is_empty());
        assert!(!event.event_type.is_empty());
        assert!(event.payload.is_object());
        
        let payload: CommandExecutedAtuinPayload = serde_json::from_value(event.payload.clone()).unwrap();
        // Even empty commands should be captured
        assert!(payload.atuin_history_id.len() > 0);
        assert!(payload.cwd.len() > 0);
    }
    Ok(())
}

#[tokio::test]
async fn test_atuin_global_history() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Create entries from multiple hosts
    let entries = vec![
        TestAtuinEntry {
            id: "host1-cmd".to_string(),
            timestamp_ns: 1_000_000_000,
            duration_ns: 100_000_000,
            exit_code: 0,
            command: "from host1".to_string(),
            cwd: "/tmp".to_string(),
            session: "s1".to_string(),
            hostname: "host1".to_string(),
        },
        TestAtuinEntry {
            id: "host2-cmd".to_string(),
            timestamp_ns: 2_000_000_000,
            duration_ns: 100_000_000,
            exit_code: 0,
            command: "from host2".to_string(),
            cwd: "/tmp".to_string(),
            session: "s2".to_string(),
            hostname: "host2".to_string(),
        },
        TestAtuinEntry {
            id: "host3-cmd".to_string(),
            timestamp_ns: 3_000_000_000,
            duration_ns: 100_000_000,
            exit_code: 0,
            command: "from host3".to_string(),
            cwd: "/tmp".to_string(),
            session: "s3".to_string(),
            hostname: "host3".to_string(),
        },
    ];
    
    create_test_atuin_db(&db_path, entries).unwrap();
    
    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Collect all events
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= 3 {
                break;
            }
        }
    }).await.unwrap();
    
    handle.abort();
    
    // Verify we got events from all hosts (global history)
    assert_eq!(events.len(), 3, "Should capture commands from all hosts");
    
    let hosts: Vec<String> = events.iter()
        .map(|e| e.host.clone())
        .collect();
    
    assert!(hosts.contains(&"host1".to_string()));
    assert!(hosts.contains(&"host2".to_string()));
    assert!(hosts.contains(&"host3".to_string()));
    Ok(())
}

#[tokio::test]
async fn test_atuin_performance_with_many_entries() -> Result<(), anyhow::Error> {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");
    
    // Create a larger number of entries to test performance
    let num_entries = 100;
    let entries: Vec<TestAtuinEntry> = (0..num_entries)
        .map(|i| {
            TestAtuinEntry::builder()
                .with_command(&format!("echo 'Command number {}'", i))
                .with_timestamp_seconds(1000 + i)
                .with_exit_code(if i % 10 == 0 { 1 } else { 0 }) // Some failures
                .build()
        })
        .collect();
    
    let start_time = std::time::Instant::now();
    create_test_atuin_db(&db_path, entries).unwrap();
    let db_creation_time = start_time.elapsed();
    
    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 50, // Test batching
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(1000);
    
    let processing_start = std::time::Instant::now();
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Collect all events
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= num_entries as usize {
                break;
            }
        }
    }).await.unwrap();
    
    let processing_time = processing_start.elapsed();
    handle.abort();
    
    assert_eq!(events.len(), num_entries as usize, "Should process all {} entries", num_entries);
    
    // Performance assertions (adjust thresholds as needed for test environment)
    assert!(db_creation_time.as_millis() < 2000, "Database creation should be fast ({}ms)", db_creation_time.as_millis());
    assert!(processing_time.as_millis() < 3000, "Processing {} entries should be fast ({}ms)", num_entries, processing_time.as_millis());
    
    // Verify data integrity across all entries
    let command_count = events.iter()
        .map(|e| {
            let payload: CommandExecutedAtuinPayload = serde_json::from_value(e.payload.clone()).unwrap();
            payload.command_string
        })
        .filter(|cmd| cmd.starts_with("echo 'Command number"))
        .count();
    
    assert_eq!(command_count, num_entries as usize, "All commands should be captured correctly");
    
    println!("Performance test: {} entries processed in {}ms (DB creation: {}ms)", 
             num_entries, processing_time.as_millis(), db_creation_time.as_millis());
    Ok(())
}

/// Test against real Atuin database if available
/// This test is ignored by default since it requires a real Atuin installation
#[tokio::test]
#[ignore = "requires real Atuin database"]
async fn test_real_atuin_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Check for real Atuin database in standard locations
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let atuin_db_path = PathBuf::from(&home).join(".local/share/atuin/history.db");
    
    if !atuin_db_path.exists() {
        eprintln!("Skipping real Atuin test - database not found at: {:?}", atuin_db_path);
        eprintln!("To run this test:");
        eprintln!("1. Install Atuin: curl --proto '=https' --tlsv1.2 -LsSf https://setup.atuin.sh | sh");
        eprintln!("2. Run some commands to populate history");
        eprintln!("3. Run: cargo test test_real_atuin_integration -- --ignored");
        return Ok(());
    }
    
    let config = AtuinConfig {
        db_path: atuin_db_path.clone(),
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    // Collect a few real events
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= 5 { // Just test a few entries
                break;
            }
        }
    }).await.unwrap_or_default();
    
    handle.abort();
    
    if events.is_empty() {
        eprintln!("Warning: No Atuin history entries found. Run some commands first!");
        return Ok(());
    }
    
    println!("Successfully processed {} real Atuin entries", events.len());
    
    // Verify real events have expected structure
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.source, "ingestor.atuin_db_reader");
        assert_eq!(event.event_type, "shell.command.executed_atuin");
        
        let payload: CommandExecutedAtuinPayload = serde_json::from_value(event.payload.clone()).unwrap();
        
        // Real commands should have meaningful data
        assert!(!payload.command_string.is_empty(), "Real command should not be empty");
        assert!(!payload.cwd.is_empty(), "Real CWD should not be empty");
        assert!(!payload.atuin_history_id.is_empty(), "Real history ID should not be empty");
        assert!(payload.duration_ns >= 0, "Duration should be non-negative");
        
        println!("Real entry {}: '{}' (exit: {}, duration: {}ms)", 
                 i + 1, 
                 payload.command_string,
                 payload.exit_code,
                 payload.duration_ns / 1_000_000);
    }
    
    println!("✅ Real Atuin integration test passed!");
    Ok(())
}

/// Test that demonstrates how to run against a live Atuin database
/// while Atuin is actively being used (without interfering)
#[tokio::test]
#[ignore = "requires live Atuin usage"]
async fn test_live_atuin_monitoring() -> Result<(), Box<dyn std::error::Error>> {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
    let atuin_db_path = PathBuf::from(&home).join(".local/share/atuin/history.db");
    
    if !atuin_db_path.exists() {
        eprintln!("Skipping live Atuin test - database not found");
        return Ok(());
    }
    
    let config = AtuinConfig {
        db_path: atuin_db_path,
        polling_interval_secs: 2, // Poll every 2 seconds
        batch_size: 50,
        use_file_watch: true, // Use file watching for live updates
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(1000);
    
    println!("🔍 Monitoring live Atuin database for 10 seconds...");
    println!("💡 Run some shell commands in another terminal to see live capture!");
    
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });
    
    let mut total_events = 0;
    let start_time = std::time::Instant::now();
    
    // Monitor for 10 seconds
    while start_time.elapsed().as_secs() < 10 {
        match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await {
            Ok(Some(event)) => {
                total_events += 1;
                let payload: CommandExecutedAtuinPayload = serde_json::from_value(event.payload).unwrap();
                println!("📝 Captured: '{}'", payload.command_string);
            },
            Ok(None) => break, // Channel closed
            Err(_) => {
                // Timeout - no new commands
                print!(".");
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
    }
    
    handle.abort();
    
    println!("\n🎉 Live monitoring complete! Captured {} new commands", total_events);
    
    if total_events == 0 {
        println!("💡 No new commands detected. Try running:");
        println!("   echo 'test command'");
        println!("   ls");
        println!("   pwd");
        println!("   Then re-run this test!");
    }
    Ok(())
}

#[cfg(test)]
mod test_helpers {
    use super::*;
    
    /// Helper to verify event payload structure
    #[allow(dead_code)]
    pub fn verify_atuin_payload(event: &RawEvent) -> anyhow::Result<()> {
        assert_eq!(event.event_type, "shell.command.executed_atuin");
        assert_eq!(event.source, "ingestor.atuin_db_reader");
        
        let payload: CommandExecutedAtuinPayload = serde_json::from_value(event.payload.clone())?;
        
        // Verify required fields exist
        assert!(!payload.command_string.is_empty());
        assert!(!payload.cwd.is_empty());
        assert!(!payload.atuin_history_id.is_empty());
        assert!(!payload.atuin_session_id.is_empty());
        assert!(payload.duration_ns >= 0);
        
        Ok(())
    }
}