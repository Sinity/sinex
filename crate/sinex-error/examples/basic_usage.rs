//! Basic usage examples for sinex-error

use sinex_error::{Result, ResultExt, SinexError};
use std::fs;
use std::path::Path;

// Example: Basic error creation and context
fn connect_to_database(host: &str, port: u16) -> Result<()> {
    // Simulating a connection failure
    Err(SinexError::database("Failed to connect to database")
        .with_context("host", host)
        .with_context("port", port)
        .with_context("connection_timeout_ms", 5000))
}

// Example: Validation with detailed context
fn validate_user_input(email: &str, age: u8) -> Result<()> {
    if !email.contains('@') {
        return Err(SinexError::validation("Invalid email format")
            .with_context("field", "email")
            .with_context("value", email)
            .with_context("reason", "missing @ symbol"));
    }

    if age < 18 {
        return Err(SinexError::validation("User must be 18 or older")
            .with_context("field", "age")
            .with_context("value", age)
            .with_context("minimum_age", 18));
    }

    Ok(())
}

// Example: Error chaining with sources
fn process_file(path: &Path) -> Result<String> {
    // Read file with context
    let content = fs::read_to_string(path).context("Failed to read file")?;

    // Parse content (simulated failure)
    if content.is_empty() {
        return Err(SinexError::parse("Empty file content")
            .with_path(path)
            .with_source("File exists but contains no data"));
    }

    Ok(content)
}

// Example: Service error with retry information
fn call_external_service(endpoint: &str, retry_count: u32) -> Result<String> {
    // Simulate all retries failing
    Err(SinexError::service("External service unavailable")
        .with_context("endpoint", endpoint)
        .with_count("retry_count", retry_count as usize)
        .with_duration(std::time::Duration::from_secs(30))
        .with_source("HTTP 503 Service Unavailable")
        .with_source("All retry attempts exhausted"))
}

// Example: Using error categorization
fn handle_error(error: &SinexError) {
    println!("Error: {}", error);
    println!("HTTP Status: {}", error.status_code());
    println!("Variant: {}", error.variant_name());

    if error.is_retryable() {
        println!("This error is retryable");
    }

    if error.is_client_error() {
        println!("This is a client error - check your input");
    }

    if error.is_permanent() {
        println!("This is a permanent error - manual intervention required");
    }
}

// Example: Complex error handling flow
fn complex_operation() -> Result<()> {
    // Chain multiple operations with context
    let config = load_config().context("Failed to load configuration")?;

    let connection = establish_connection(&config).with_context(|| {
        SinexError::service("Failed to establish connection")
            .with_context("config_file", "/etc/app/config.toml")
            .with_context("environment", "production")
    })?;

    process_data(&connection).context("Data processing failed")?;

    Ok(())
}

// Helper functions for the complex example
fn load_config() -> Result<Config> {
    Err(SinexError::configuration("Config file not found")
        .with_path(Path::new("/etc/app/config.toml")))
}

fn establish_connection(_config: &Config) -> Result<Connection> {
    Ok(Connection)
}

fn process_data(_conn: &Connection) -> Result<()> {
    Ok(())
}

struct Config;
struct Connection;

fn main() {
    println!("=== Basic Error Creation ===");
    match connect_to_database("localhost", 5432) {
        Ok(_) => println!("Connected successfully"),
        Err(e) => handle_error(&e),
    }

    println!("\n=== Validation Error ===");
    match validate_user_input("invalid-email", 16) {
        Ok(_) => println!("Validation passed"),
        Err(e) => handle_error(&e),
    }

    println!("\n=== File Processing Error ===");
    match process_file(Path::new("/tmp/nonexistent.txt")) {
        Ok(_) => println!("File processed"),
        Err(e) => handle_error(&e),
    }

    println!("\n=== Service Error ===");
    match call_external_service("https://api.example.com/data", 3) {
        Ok(_) => println!("Service call succeeded"),
        Err(e) => handle_error(&e),
    }

    println!("\n=== Complex Operation ===");
    match complex_operation() {
        Ok(_) => println!("Operation completed"),
        Err(e) => {
            println!("Complex operation failed:");
            println!("{}", e);

            // Access specific context
            if let Some(env) = e.context_map().get("environment") {
                println!("Environment: {}", env);
            }
        }
    }
}
