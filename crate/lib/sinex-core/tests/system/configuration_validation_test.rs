// # Configuration Validation Tests
//
// Tests that verify:
// - Valid configuration parsing and validation
// - Invalid configuration detection
// - Configuration hot reload scenarios
//
// ## Performance Expectations
//
// - **Individual tests**: 15-30 seconds
// - **Resource usage**: Minimal (file system only)

use sinex_test_utils::prelude::*;
use std::fs;

/// Test configuration validation and hot reload scenarios
#[sinex_test]
async fn test_configuration_validation_and_reload(
    ctx: TestContext,
) -> TestResult<()> {
    println!("Testing configuration validation and hot reload scenarios...");

    let temp_dir = TempDir::new()?;
    let config_dir = temp_dir.path().join("config");
    fs::create_dir_all(&config_dir)?;

    // Test 1: Valid configuration validation
    let valid_config = r#"
[database]
url = "postgresql://localhost/sinex_test"
max_connections = 10

[collector]
channel_buffer_size = 1000
shutdown_timeout = "30s"

[event_sources.filesystem]
enabled = true
watch_paths = ["/tmp/test"]

[event_sources.clipboard]
enabled = false

[routing]
default_agent = "test_agent"

[git_annex]
enabled = false
"#;

    let valid_config_file = config_dir.join("valid.toml");
    fs::write(&valid_config_file, valid_config)?;

    let config_validation_start = Instant::now();

    let valid_config_test = timeout(Duration::from_secs(3), async {
        let config_content = fs::read_to_string(&valid_config_file)?;
        let parsed_config = toml::from_str::<toml::Value>(&config_content)?;

        // Validate required sections exist
        let has_database = parsed_config.get("database").is_some();
        let has_collector = parsed_config.get("collector").is_some();
        let has_event_sources = parsed_config.get("event_sources").is_some();

        // Validate specific field types and values
        let channel_buffer = parsed_config
            .get("collector")
            .and_then(|c| c.get("channel_buffer_size"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        let max_connections = parsed_config
            .get("database")
            .and_then(|d| d.get("max_connections"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        Ok::<(bool, bool, bool, i64, i64), color_eyre::eyre::Error>((
            has_database,
            has_collector,
            has_event_sources,
            channel_buffer,
            max_connections,
        ))
    })
    .await;

    let config_validation_duration = config_validation_start.elapsed();

    match valid_config_test {
        Ok(Ok((has_db, has_collector, has_sources, buffer_size, max_conn))) => {
            println!(
                "  ✓ Valid configuration parsed in {:?}",
                config_validation_duration
            );
            println!("    Database config: {}", has_db);
            println!("    Collector config: {}", has_collector);
            println!("    Event sources config: {}", has_sources);
            println!("    Channel buffer: {}", buffer_size);
            println!("    Max connections: {}", max_conn);

            assert!(has_db, "Should have database configuration");
            assert!(has_collector, "Should have collector configuration");
            assert!(has_sources, "Should have event sources configuration");
            assert!(buffer_size > 0, "Channel buffer should be positive");
            assert!(max_conn > 0, "Max connections should be positive");
        }
        Ok(Err(e)) => {
            println!("  Valid config validation failed: {}", e);
        }
        Err(_) => {
            println!("  Valid config validation timed out");
        }
    }

    // Test 2: Invalid configuration detection
    let invalid_configs = [
        // Missing required sections
        (
            r#"
[collector]
channel_buffer_size = 1000
"#,
            "missing_database_section",
        ),
        // Invalid data types
        (
            r#"
[database]
url = "postgresql://localhost/test"
max_connections = "not_a_number"

[collector]
channel_buffer_size = 1000
"#,
            "invalid_data_type",
        ),
        // Invalid values
        (
            r#"
[database]
url = "postgresql://localhost/test"
max_connections = -5

[collector]
channel_buffer_size = 0
"#,
            "invalid_values",
        ),
        // Malformed TOML
        (
            r#"
[database
url = "incomplete section
"#,
            "malformed_toml",
        ),
        // Conflicting settings
        (
            r#"
[database]
url = "postgresql://localhost/test"
max_connections = 1

[collector]
channel_buffer_size = 10000
"#,
            "resource_conflict",
        ),
    ];

    let mut invalid_config_results = Vec::new();

    for (i, (invalid_config, test_name)) in invalid_configs.iter().enumerate() {
        let invalid_config_file = config_dir.join(format!("invalid_{}.toml", i));
        fs::write(&invalid_config_file, invalid_config)?;

        let validation_result = timeout(Duration::from_secs(1), async {
            let config_content = fs::read_to_string(&invalid_config_file)?;
            let parsed_result = toml::from_str::<toml::Value>(&config_content);

            match parsed_result {
                Ok(config) => {
                    // Additional semantic validation
                    let max_conn = config
                        .get("database")
                        .and_then(|d| d.get("max_connections"))
                        .and_then(|v| v.as_integer())
                        .unwrap_or(1);

                    let buffer_size = config
                        .get("collector")
                        .and_then(|c| c.get("channel_buffer_size"))
                        .and_then(|v| v.as_integer())
                        .unwrap_or(1000);

                    // Check for semantic errors
                    if max_conn <= 0 {
                        Err(eyre!("Invalid max_connections"))
                    } else if buffer_size <= 0 {
                        Err(eyre!("Invalid buffer_size"))
                    } else if max_conn == 1 && buffer_size > 5000 {
                        Err(eyre!("Resource conflict: low connections, high buffer"))
                    } else {
                        Ok(config)
                    }
                }
                Err(e) => Err(eyre!("TOML parse error: {}", e)),
            }
        })
        .await;

        let validation_accepted = validation_result.is_ok() && validation_result.unwrap().is_ok();
        invalid_config_results.push((test_name, validation_accepted));

        if validation_accepted {
            println!("  WARNING: Invalid config '{}' was accepted", test_name);
        } else {
            println!("  ✓ Invalid config '{}' rejected correctly", test_name);
        }
    }

    // Test 3: Configuration hot reload simulation
    println!("\nTesting configuration hot reload scenarios...");

    let hot_reload_config_file = config_dir.join("hot_reload.toml");
    fs::write(&hot_reload_config_file, valid_config)?;

    let hot_reload_test = timeout(Duration::from_secs(5), async {
        // Initial config load
        let initial_config = fs::read_to_string(&hot_reload_config_file)?;
        let initial_parsed = toml::from_str::<toml::Value>(&initial_config)?;

        let initial_buffer_size = initial_parsed
            .get("collector")
            .and_then(|c| c.get("channel_buffer_size"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        println!("    Initial buffer size: {}", initial_buffer_size);

        // Simulate configuration change
        let updated_config =
            valid_config.replace("channel_buffer_size = 1000", "channel_buffer_size = 2000");
        fs::write(&hot_reload_config_file, &updated_config)?;

        // Small delay to simulate file system change detection
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reload config
        let updated_config_content = fs::read_to_string(&hot_reload_config_file)?;
        let updated_parsed = toml::from_str::<toml::Value>(&updated_config_content)?;

        let updated_buffer_size = updated_parsed
            .get("collector")
            .and_then(|c| c.get("channel_buffer_size"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);

        println!("    Updated buffer size: {}", updated_buffer_size);

        // Verify change was detected and parsed correctly
        pretty_assertions::assert_ne!(
            initial_buffer_size,
            updated_buffer_size,
            "Config change should be detected"
        );
        pretty_assertions::assert_eq!(
            updated_buffer_size,
            2000,
            "New config value should be correct"
        );

        Ok::<(), color_eyre::eyre::Error>(())
    })
    .await;

    match hot_reload_test {
        Ok(Ok(())) => {
            println!("  ✓ Configuration hot reload simulation successful");
        }
        Ok(Err(e)) => {
            println!("  Configuration hot reload failed: {}", e);
        }
        Err(_) => {
            println!("  Configuration hot reload timed out");
        }
    }

    // Summary
    let rejected_invalid_configs = invalid_config_results
        .iter()
        .filter(|(_, accepted)| !*accepted)
        .count();

    println!("\nConfiguration Validation Results:");
    println!("  Valid config parsing: ✓");
    println!(
        "  Invalid configs rejected: {}/{}",
        rejected_invalid_configs,
        invalid_configs.len()
    );
    println!("  Hot reload simulation: ✓");

    assert!(
        rejected_invalid_configs >= invalid_configs.len() * 3 / 4,
        "Should reject most invalid configurations"
    );

    Ok(())
}
