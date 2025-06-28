use chrono::Utc;
use sinex_core::{CoreError, ResultExt};
use sinex_ulid::Ulid;

fn main() {
    // Example 1: Simple error with context
    let error = CoreError::database("Connection failed")
        .with_context("host", "localhost")
        .with_context("port", 5432)
        .with_context("retry_count", 3)
        .build();

    println!("Example 1 - Database error with context:");
    println!("{}\n", error);

    // Example 2: Validation error with event context
    let event_id = Ulid::new();
    let timestamp = Utc::now();

    let error = CoreError::validation("Invalid event payload")
        .with_event_id(event_id)
        .with_timestamp(timestamp)
        .with_field("source", "filesystem")
        .with_field("event_type", "file.created")
        .build();

    println!("Example 2 - Validation error with event context:");
    println!("{}\n", error);

    // Example 3: IO error with path and operation
    let error = CoreError::io_error("/var/log/sinex/events.log")
        .with_operation("write")
        .with_context("bytes_written", 1024)
        .with_source("Permission denied")
        .build();

    println!("Example 3 - IO error with path and source:");
    println!("{}\n", error);

    // Example 4: Configuration error with source chain
    let error = CoreError::configuration("Missing database configuration")
        .with_context("config_file", "/etc/sinex/config.toml")
        .with_source("DATABASE_URL environment variable not set")
        .with_source("No default value provided")
        .build();

    println!("Example 4 - Configuration error with source chain:");
    println!("{}\n", error);

    // Example 5: Processing error with multiple contexts
    let error = CoreError::processing_failed()
        .with_event_id(Ulid::new())
        .with_context("worker_id", "worker-001")
        .with_context("queue_size", 1000)
        .with_context("processing_time_ms", 250)
        .with_source("JSON deserialization failed")
        .build();

    println!("Example 5 - Processing error with rich context:");
    println!("{}\n", error);

    // Example 6: Using ResultExt trait
    let result: Result<(), std::io::Error> = Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "File not found",
    ));

    let enhanced_result: sinex_core::Result<()> =
        result.context("Failed to read event source configuration");

    if let Err(e) = enhanced_result {
        println!("Example 6 - Result with context:");
        println!("{}\n", e);
    }

    // Example 7: Structured error info for logging
    let error_context = CoreError::database("Query timeout")
        .with_context("query", "SELECT * FROM raw.events")
        .with_context("timeout_seconds", 30)
        .with_context("connection_pool_size", 10);

    let error_info = error_context.to_error_info();

    println!("Example 7 - Structured error info:");
    println!("{:#?}", error_info);
}
