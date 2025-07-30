//! Example demonstrating enhanced error context with color-eyre and serde_path_to_error

use color_eyre::eyre;
use serde::{Deserialize, Serialize};
use sinex_error::{
    enhanced_context::{
        deserialize_with_path, install_error_hooks, ErrorContext, ErrorReportBuilder, SinexErrorExt,
    },
    Result, SinexError,
};

#[derive(Debug, Serialize, Deserialize)]
struct AppConfig {
    server: ServerConfig,
    database: DatabaseConfig,
    features: FeatureFlags,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerConfig {
    host: String,
    port: u16,
    tls: Option<TlsConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TlsConfig {
    cert_path: String,
    key_path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DatabaseConfig {
    url: String,
    max_connections: u32,
    timeout_seconds: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct FeatureFlags {
    enable_metrics: bool,
    enable_tracing: bool,
    experimental_features: Vec<String>,
}

fn main() -> eyre::Result<()> {
    // Install color-eyre hooks for enhanced error reporting
    install_error_hooks()?;

    // Example 1: Enhanced error context
    example_enhanced_context();

    // Example 2: JSON deserialization with path information
    example_json_path_errors();

    // Example 3: Error report builder
    example_error_report_builder();

    // Example 4: Converting to eyre Report
    example_eyre_conversion()?;

    Ok(())
}

fn example_enhanced_context() {
    println!("\n=== Enhanced Error Context Example ===");

    let error = process_request("invalid-id")
        .context_enhanced("Failed to process user request")
        .unwrap_err();

    println!("Error: {}", error);

    // Add more context
    let enriched = error
        .with_help("Ensure the ID format is valid (e.g., 'user-123')")
        .with_suggestion("Try using the list endpoint to get valid IDs")
        .with_note("This error typically occurs with legacy ID formats")
        .with_warning("Multiple failures may trigger rate limiting");

    println!("\nEnriched error: {}", enriched);
}

fn process_request(id: &str) -> Result<String> {
    if !id.starts_with("user-") {
        return Err(SinexError::validation("Invalid user ID format")
            .with_context("id", id)
            .with_context("expected_prefix", "user-"));
    }
    Ok(format!("Processed {}", id))
}

fn example_json_path_errors() {
    println!("\n\n=== JSON Path Error Example ===");

    let invalid_config = r#"{
        "server": {
            "host": "localhost",
            "port": "not-a-number",
            "tls": {
                "cert_path": "/etc/ssl/cert.pem"
            }
        },
        "database": {
            "url": "postgresql://localhost/app",
            "max_connections": 100,
            "timeout_seconds": 30
        },
        "features": {
            "enable_metrics": true,
            "enable_tracing": "yes",
            "experimental_features": ["feature1", 123]
        }
    }"#;

    match deserialize_with_path::<AppConfig>(invalid_config) {
        Ok(_) => println!("Config loaded successfully"),
        Err(e) => {
            println!("Failed to load config:");
            println!("{}", e);

            // Access detailed context
            if let Some(context) = e.context() {
                println!("\nDetailed context:");
                for (key, value) in context {
                    println!("  {}: {}", key, value);
                }
            }
        }
    }
}

fn example_error_report_builder() {
    println!("\n\n=== Error Report Builder Example ===");

    let base_error = SinexError::database("Connection failed");

    let detailed_error = ErrorReportBuilder::new(base_error)
        .section("Database", "PostgreSQL 14.5")
        .section(
            "Connection String",
            "postgresql://app_user@localhost:5432/myapp",
        )
        .section("Pool Status", "10/100 connections active")
        .with_env_context()
        .with_system_context()
        .build();

    println!("Detailed error report:");
    println!("{}", detailed_error);

    if let Some(context) = detailed_error.context() {
        println!("\nAll context fields:");
        for (key, value) in context {
            println!("  {}: {}", key, value);
        }
    }
}

fn example_eyre_conversion() -> eyre::Result<()> {
    println!("\n\n=== Eyre Conversion Example ===");

    let error = SinexError::service("API request failed")
        .with_source("Network timeout after 30s")
        .with_source("DNS resolution failed")
        .with_context("endpoint", "https://api.example.com/v1/users")
        .with_context("retry_count", "3")
        .with_context("request_id", "abc-123-def")
        .with_help("Check network connectivity and DNS settings")
        .with_suggestion("Consider increasing timeout or using retry logic");

    // Convert to eyre Report for rich error formatting
    let report = error.into_eyre_report();

    // This would display with full color formatting in a terminal
    println!("Eyre report (would be colorized in terminal):");
    println!("{:?}", report);

    Ok(())
}

// Example of using with_context_enhanced for lazy evaluation
fn complex_operation() -> Result<()> {
    std::fs::read_to_string("/nonexistent/file")
        .map(|_| ())
        .map_err(|e| SinexError::from(e))
        .with_context_enhanced(|| {
            // This closure is only called if there's an error
            let timestamp = chrono::Utc::now();
            format!("Failed to read config at {}", timestamp)
        })
}
