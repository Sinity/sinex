//! Advanced patterns and best practices for sinex-error

use serde::{Deserialize, Serialize};
use sinex_error::{bail, ensure, Result, ResultExt, SinexError};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// Pattern 1: Domain-specific error helpers
trait DomainErrors {
    fn user_not_found(user_id: &str) -> SinexError {
        SinexError::not_found("User not found")
            .with_id("user_id", user_id)
            .with_context("collection", "users")
    }

    fn insufficient_permissions(user_id: &str, resource: &str, action: &str) -> SinexError {
        SinexError::permission_denied("Insufficient permissions")
            .with_id("user_id", user_id)
            .with_context("resource", resource)
            .with_context("action", action)
            .with_context("required_role", "admin")
    }

    fn rate_limit_exceeded(user_id: &str, limit: u32, window_seconds: u64) -> SinexError {
        SinexError::resource_exhausted("Rate limit exceeded")
            .with_id("user_id", user_id)
            .with_context("limit", limit)
            .with_context("window_seconds", window_seconds)
            .with_context("retry_after_seconds", window_seconds)
    }
}

impl DomainErrors for SinexError {}

// Pattern 2: Error wrapping with telemetry
struct TelemetryContext {
    request_id: String,
    span_id: String,
    start_time: Instant,
}

impl TelemetryContext {
    fn new(request_id: String) -> Self {
        Self {
            request_id,
            span_id: format!("span_{}", uuid::Uuid::new_v4()),
            start_time: Instant::now(),
        }
    }

    fn wrap_error(&self, error: SinexError) -> SinexError {
        error
            .with_context("request_id", &self.request_id)
            .with_context("span_id", &self.span_id)
            .with_duration(self.start_time.elapsed())
    }
}

// Pattern 3: Structured error responses for APIs
#[derive(Debug, Serialize, Deserialize)]
struct ApiErrorResponse {
    error: ErrorInfo,
    request_id: String,
    timestamp: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorInfo {
    code: String,
    message: String,
    details: HashMap<String, String>,
    retry_after: Option<u64>,
}

impl From<&SinexError> for ApiErrorResponse {
    fn from(error: &SinexError) -> Self {
        let mut details = HashMap::new();
        for (k, v) in error.context_map() {
            details.insert(k.clone(), v.clone());
        }

        let retry_after = if error.is_retryable() {
            details
                .get("retry_after_seconds")
                .and_then(|s| s.parse().ok())
        } else {
            None
        };

        ApiErrorResponse {
            error: ErrorInfo {
                code: error.variant_name().to_lowercase(),
                message: error.message().to_string(),
                details,
                retry_after,
            },
            request_id: details
                .get("request_id")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string()),
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}

// Pattern 4: Error aggregation for batch operations
struct BatchOperationResult<T> {
    successful: Vec<T>,
    failed: Vec<(usize, SinexError)>,
}

fn process_batch<T, F>(items: Vec<T>, operation: F) -> BatchOperationResult<T>
where
    F: Fn(&T) -> Result<()>,
{
    let mut successful = Vec::new();
    let mut failed = Vec::new();

    for (index, item) in items.into_iter().enumerate() {
        match operation(&item) {
            Ok(()) => successful.push(item),
            Err(e) => failed.push((index, e.with_context("batch_index", index))),
        }
    }

    BatchOperationResult { successful, failed }
}

// Pattern 5: Circuit breaker pattern with errors
struct CircuitBreaker {
    failure_threshold: u32,
    reset_timeout: Duration,
    failure_count: u32,
    last_failure_time: Option<Instant>,
}

impl CircuitBreaker {
    fn call<T, F>(&mut self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        // Check if circuit is open
        if let Some(last_failure) = self.last_failure_time {
            if self.failure_count >= self.failure_threshold {
                if last_failure.elapsed() < self.reset_timeout {
                    return Err(SinexError::service("Circuit breaker is open")
                        .with_context("failure_count", self.failure_count)
                        .with_context("threshold", self.failure_threshold)
                        .with_duration(last_failure.elapsed()));
                } else {
                    // Reset circuit breaker
                    self.failure_count = 0;
                    self.last_failure_time = None;
                }
            }
        }

        // Try the operation
        match operation() {
            Ok(result) => {
                self.failure_count = 0;
                Ok(result)
            }
            Err(e) => {
                self.failure_count += 1;
                self.last_failure_time = Some(Instant::now());
                Err(e.with_context("circuit_breaker_failures", self.failure_count))
            }
        }
    }
}

// Pattern 6: Using bail! and ensure! macros
fn validate_request(user_id: Option<&str>, data: &str) -> Result<()> {
    // Use ensure! for validation
    ensure!(
        user_id.is_some(),
        SinexError::validation("Missing user ID").with_context("field", "user_id")
    );

    ensure!(
        !data.is_empty(),
        SinexError::validation("Empty data").with_context("field", "data")
    );

    ensure!(
        data.len() <= 1024,
        SinexError::validation("Data too large")
            .with_context("field", "data")
            .with_context("size", data.len())
            .with_context("max_size", 1024)
    );

    // Use bail! for early returns with errors
    if data.contains("forbidden") {
        bail!(SinexError::permission_denied("Forbidden content detected")
            .with_context("user_id", user_id.unwrap()));
    }

    Ok(())
}

// Example usage
fn main() -> Result<()> {
    // Example 1: Domain-specific errors
    println!("=== Domain-Specific Errors ===");
    let error = SinexError::user_not_found("user123");
    println!("User not found: {}", error);

    // Example 2: Telemetry context
    println!("\n=== Telemetry Context ===");
    let telemetry = TelemetryContext::new("req-456".to_string());
    std::thread::sleep(Duration::from_millis(100)); // Simulate work
    let error = telemetry.wrap_error(SinexError::database("Query timeout"));
    println!("Error with telemetry: {}", error);

    // Example 3: API error response
    println!("\n=== API Error Response ===");
    let api_error = SinexError::rate_limit_exceeded("user789", 100, 3600);
    let response = ApiErrorResponse::from(&api_error);
    println!("API Response: {}", serde_json::to_string_pretty(&response)?);

    // Example 4: Batch processing
    println!("\n=== Batch Processing ===");
    let items = vec![1, 2, 3, 4, 5];
    let result = process_batch(items, |&x| {
        if x % 2 == 0 {
            Err(SinexError::validation("Even numbers not allowed").with_context("value", x))
        } else {
            Ok(())
        }
    });
    println!("Successful: {:?}", result.successful);
    println!("Failed: {} items", result.failed.len());
    for (index, error) in &result.failed {
        println!("  Item {}: {}", index, error);
    }

    // Example 5: Circuit breaker
    println!("\n=== Circuit Breaker ===");
    let mut breaker = CircuitBreaker {
        failure_threshold: 3,
        reset_timeout: Duration::from_secs(5),
        failure_count: 0,
        last_failure_time: None,
    };

    // Simulate failures
    for i in 1..=5 {
        let result = breaker.call(|| Err(SinexError::network("Service unavailable")));
        match result {
            Ok(_) => println!("Call {} succeeded", i),
            Err(e) => println!("Call {} failed: {}", i, e.message()),
        }
    }

    // Example 6: Validation with ensure!
    println!("\n=== Validation Example ===");
    match validate_request(None, "test data") {
        Ok(_) => println!("Validation passed"),
        Err(e) => println!("Validation failed: {}", e),
    }

    Ok(())
}

// Required for the example
mod uuid {
    pub struct Uuid;
    impl Uuid {
        pub fn new_v4() -> String {
            "mock-uuid".to_string()
        }
    }
}
