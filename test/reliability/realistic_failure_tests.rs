use chrono::Utc;
use serde_json::json;
use sinex_shared::{DatabaseService, RawEventBuilder, AssumptionDetector, sources, event_type_constants};

/// Test that simulates realistic development/deployment failures
#[sqlx::test]
async fn test_realistic_development_failures(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool_no_validation(pool.clone()); // No validation to allow bad data
    let detector = AssumptionDetector::new();
    
    // SCENARIO 1: Developer copy-pastes code between ingestors
    println!("Testing copy-paste error detection...");
    
    // Good events first (to establish baseline)
    let good_events = vec![
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "path": "/home/user/doc1.txt",
                "size": 1024,
                "permissions": "644"
            })
        ).build(),
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "path": "/home/user/doc2.txt", 
                "size": 2048,
                "permissions": "755"
            })
        ).build(),
    ];
    
    for event in good_events {
        db_service.insert_event(&event).await?;
    }
    
    // Now the copy-paste error: filesystem ingestor accidentally uses terminal event code
    let copy_paste_error = RawEventBuilder::new(
        sources::FILESYSTEM, // Says it's filesystem
        event_type_constants::filesystem::FILE_CREATED, // Says it's file creation
        json!({
            "command": "ls -la",      // But has terminal fields!
            "exit_code": 0,
            "duration_ms": 150,
            "working_directory": "/home/user"
        })
    ).build();
    
    db_service.insert_event(&copy_paste_error).await?;
    
    // Test detection
    let result = detector.check_assumptions(
        &copy_paste_error.source,
        &copy_paste_error.event_type,
        &copy_paste_error.payload
    );
    
    assert!(result.is_err(), "Should detect copy-paste error");
    println!("✅ Copy-paste error detected: {:?}", result.unwrap_err());
    
    Ok(())
}

/// Test version mismatch detection
#[sqlx::test]
async fn test_version_mismatch_detection(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool_no_validation(pool.clone());
    
    println!("Testing version mismatch detection...");
    
    // Version 1.0 events (old format)
    let v1_events = vec![
        RawEventBuilder::new(
            sources::HYPRLAND,
            event_type_constants::hyprland::WINDOW_FOCUSED,
            json!({
                "window": "firefox",
                "workspace": 1
            })
        ).build(),
        RawEventBuilder::new(
            sources::HYPRLAND,
            event_type_constants::hyprland::WINDOW_FOCUSED,
            json!({
                "window": "terminal",
                "workspace": 2
            })
        ).build(),
    ];
    
    // Version 2.0 events (new format - breaking change!)
    let v2_events = vec![
        RawEventBuilder::new(
            sources::HYPRLAND,
            event_type_constants::hyprland::WINDOW_FOCUSED,
            json!({
                "window_info": {
                    "class": "firefox",
                    "title": "Mozilla Firefox",
                    "pid": 12345
                },
                "workspace_id": 1,
                "monitor": "DP-1"
            })
        ).build(),
    ];
    
    // Insert all events
    for event in v1_events.iter().chain(v2_events.iter()) {
        db_service.insert_event(event).await?;
    }
    
    // Analyze field consistency
    let field_analysis = sqlx::query!(
        r#"
        WITH field_usage AS (
            SELECT 
                jsonb_object_keys(payload) as field_name,
                COUNT(*) as usage_count
            FROM raw.events
            WHERE source = 'hyprland' AND event_type = 'window_focused'
            GROUP BY jsonb_object_keys(payload)
        ),
        total_events AS (
            SELECT COUNT(*) as total
            FROM raw.events  
            WHERE source = 'hyprland' AND event_type = 'window_focused'
        )
        SELECT 
            fu.field_name,
            fu.usage_count,
            te.total,
            (fu.usage_count::float / te.total::float) as usage_ratio
        FROM field_usage fu
        CROSS JOIN total_events te
        ORDER BY usage_ratio DESC
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    // Check for inconsistent field usage (indicates version mismatch)
    let mut inconsistent_fields = 0;
    for analysis in field_analysis {
        let ratio = analysis.usage_ratio.unwrap_or(0.0);
        println!("Field '{}': {}/{} events ({:.1}%)", 
                 analysis.field_name.unwrap_or_default(), 
                 analysis.usage_count.unwrap_or(0), 
                 analysis.total.unwrap_or(0),
                 ratio * 100.0);
        
        // Fields used in less than 80% of events might indicate version mismatch
        if ratio < 0.8 && ratio > 0.2 {
            inconsistent_fields += 1;
            println!("  ⚠️  Inconsistent usage - possible version mismatch");
        }
    }
    
    assert!(inconsistent_fields > 0, "Should detect version inconsistencies");
    println!("✅ Version mismatch detected: {} inconsistent fields", inconsistent_fields);
    
    Ok(())
}

/// Test configuration drift detection
#[sqlx::test]
async fn test_configuration_drift_detection(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool_no_validation(pool.clone());
    
    println!("Testing configuration drift detection...");
    
    // Simulate same ingestor running on different machines with different configs
    
    // Machine 1: Development (verbose logging enabled)
    let dev_events = vec![
        RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            event_type_constants::terminal::COMMAND_EXECUTED,
            json!({
                "command": "ls -la",
                "exit_code": 0,
                "duration_ms": 150,
                "working_directory": "/home/dev",
                "shell": "bash",
                "session_id": "dev-session-001",
                "debug_info": {
                    "memory_usage": "45MB",
                    "cpu_time": "0.1s"
                },
                "environment_vars": {
                    "PATH": "/usr/local/bin:/usr/bin",
                    "HOME": "/home/dev"
                }
            })
        ).build(),
    ];
    
    // Machine 2: Production (minimal logging)
    let prod_events = vec![
        RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            event_type_constants::terminal::COMMAND_EXECUTED,
            json!({
                "command": "ls -la",
                "exit_code": 0
            })
        ).build(),
    ];
    
    // Machine 3: Staging (different field names due to config mistake)
    let staging_events = vec![
        RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            event_type_constants::terminal::COMMAND_EXECUTED,
            json!({
                "cmd": "ls -la",  // Wrong field name!
                "status": 0,      // Wrong field name!
                "host": "staging-server"
            })
        ).build(),
    ];
    
    // Insert events with different host names
    for (events, host_suffix) in [
        (&dev_events, "dev"),
        (&prod_events, "prod"), 
        (&staging_events, "staging")
    ] {
        for mut event in events.iter().cloned() {
            event.host = format!("machine-{}", host_suffix);
            db_service.insert_event(&event).await?;
        }
    }
    
    // Analyze field usage by host (configuration drift detection)
    let host_analysis = sqlx::query!(
        r#"
        SELECT 
            host,
            jsonb_object_keys(payload) as field_name,
            COUNT(*) as usage_count
        FROM raw.events
        WHERE source = 'terminal.kitty' AND event_type = 'command_executed'
        GROUP BY host, jsonb_object_keys(payload)
        ORDER BY host, field_name
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    // Group by field and check if it appears in all hosts
    use std::collections::{HashMap, HashSet};
    let mut field_hosts: HashMap<String, HashSet<String>> = HashMap::new();
    
    for analysis in host_analysis {
        if let Some(field_name) = analysis.field_name {
            field_hosts
                .entry(field_name)
                .or_default()
                .insert(analysis.host);
        }
    }
    
    let all_hosts: HashSet<String> = field_hosts.values()
        .flat_map(|hosts| hosts.iter().cloned())
        .collect();
    
    let mut config_drift_detected = false;
    for (field, hosts) in field_hosts {
        if hosts.len() < all_hosts.len() {
            println!("Configuration drift detected: field '{}' only appears on hosts: {:?}", 
                     field, hosts);
            config_drift_detected = true;
        }
    }
    
    assert!(config_drift_detected, "Should detect configuration differences between hosts");
    println!("✅ Configuration drift detected across {} hosts", all_hosts.len());
    
    Ok(())
}

/// Test environment-specific data corruption
#[sqlx::test]
async fn test_environment_data_corruption(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool_no_validation(pool.clone());
    
    println!("Testing environment-specific data corruption...");
    
    // Simulate: filesystem ingestor works fine locally but gets confused in Docker
    
    // Local environment: correct filesystem events
    let local_events = vec![
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "path": "/home/user/local_file.txt",
                "size": 1024,
                "permissions": "644",
                "owner": "user",
                "group": "user"
            })
        ).build(),
    ];
    
    // Docker environment: ingestor accidentally monitors container processes instead of files
    let docker_events = vec![
        RawEventBuilder::new(
            sources::FILESYSTEM,  // Claims to be filesystem
            event_type_constants::filesystem::FILE_CREATED,  // Claims to be file creation
            json!({
                "container_id": "abc123def456",
                "image": "ubuntu:20.04", 
                "process_id": 1,
                "command": "/bin/bash",
                "memory_limit": "512MB",
                "cpu_shares": 1024,
                "status": "running",
                "environment": "docker"  // Hint about the real environment
            })
        ).build(),
    ];
    
    // Kubernetes environment: gets cluster metadata instead
    let k8s_events = vec![
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "pod_name": "sinex-ingestor-xyz",
                "namespace": "default",
                "node": "worker-node-1",
                "service_account": "sinex-sa",
                "resource_version": "12345",
                "cluster": "production",
                "environment": "kubernetes"
            })
        ).build(),
    ];
    
    // Insert all events
    for (events, env) in [
        (&local_events, "local"),
        (&docker_events, "docker"),
        (&k8s_events, "k8s")
    ] {
        for mut event in events.iter().cloned() {
            event.host = format!("{}-host", env);
            db_service.insert_event(&event).await?;
        }
    }
    
    // Detect environment-specific corruption
    let env_analysis = sqlx::query!(
        r#"
        SELECT 
            host,
            ARRAY_AGG(DISTINCT keys.key) as all_fields,
            COUNT(DISTINCT e.id) as event_count
        FROM raw.events e
        CROSS JOIN LATERAL jsonb_object_keys(e.payload) as keys(key)
        WHERE e.source = 'filesystem' AND e.event_type = 'file_created'
        GROUP BY host
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    let mut corrupted_environments = 0;
    for analysis in env_analysis {
        let fields = analysis.all_fields.unwrap_or_default();
        let has_filesystem_fields = fields.iter().any(|f| ["path", "size", "permissions"].contains(&f.as_str()));
        let has_container_fields = fields.iter().any(|f| ["container_id", "image", "pod_name"].contains(&f.as_str()));
        
        println!("Host '{}': {} events with fields: {:?}", 
                 analysis.host, analysis.event_count, fields);
        
        if !has_filesystem_fields && has_container_fields {
            println!("  🚨 Environment corruption detected: {} events with container fields instead of filesystem", 
                     analysis.event_count);
            corrupted_environments += 1;
        }
    }
    
    assert!(corrupted_environments > 0, "Should detect environment-specific corruption");
    println!("✅ Environment corruption detected in {} environments", corrupted_environments);
    
    Ok(())
}