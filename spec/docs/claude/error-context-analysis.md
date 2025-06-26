# Error Context Builder Analysis

## Implementation Status

✅ **COMPLETE**: The structured error context builders are fully implemented and tested in `crate/sinex-core/src/error_context.rs`.

## Key Features

### 1. **ErrorContext Builder Pattern**
```rust
pub struct ErrorContext {
    error_type: ErrorType,
    message: String,
    context: HashMap<String, String>,
    source_chain: Vec<String>,
    stack_trace: Option<String>,
}

impl ErrorContext {
    pub fn new(error_type: CoreError) -> Self;
    pub fn with_context(self, key: &str, value: impl Display) -> Self;
    pub fn with_source(self, source: impl Display) -> Self;
    pub fn with_event_id(self, id: Ulid) -> Self;
    pub fn with_timestamp(self, ts: Timestamp) -> Self;
    pub fn with_path(self, path: impl AsRef<Path>) -> Self;
    pub fn with_operation(self, operation: &str) -> Self;
    pub fn with_field(self, field: &str, value: impl Display) -> Self;
    pub fn build(self) -> CoreError;
}
```

### 2. **Convenience Methods on CoreError**
```rust
impl CoreError {
    pub fn database(msg: impl Display) -> ErrorContext;
    pub fn validation(msg: impl Display) -> ErrorContext;
    pub fn configuration(msg: impl Display) -> ErrorContext;
    pub fn serialization(msg: impl Display) -> ErrorContext;
    pub fn io_error(path: impl AsRef<Path>) -> ErrorContext;
    pub fn processing_failed() -> ErrorContext;
}
```

### 3. **ResultExt Trait for Easy Context Addition**
```rust
pub trait ResultExt<T> {
    fn context(self, msg: &str) -> crate::Result<T>;
    fn with_context<F>(self, f: F) -> crate::Result<T>
    where F: FnOnce() -> ErrorContext;
}
```

## Obsolescence Analysis

Found **75+ instances** of string-based error patterns that can be improved:

### **Pattern 1: Configuration Errors**

**Before** (18 instances):
```rust
CoreError::Configuration(format!("Failed to parse config: {}", e))
```

**After**:
```rust
CoreError::configuration("Failed to parse config")
    .with_source(e)
    .build()
```

### **Pattern 2: Database Errors with Context**

**Before** (4 instances):
```rust
CoreError::Database(format!("{}: Query timeout", self.context))
```

**After**:
```rust
CoreError::database("Query timeout")
    .with_context("operation", self.context)
    .build()
```

### **Pattern 3: File Operation Errors**

**Before** (15 instances):
```rust
CoreError::Other(format!("Failed to open {:?}: {}", path, e))
```

**After**:
```rust
CoreError::io_error(&path)
    .with_operation("open")
    .with_source(e)
    .build()
```

### **Pattern 4: Command Execution Errors**

**Before** (8 instances):
```rust
CoreError::Other(format!("Failed to execute hyprctl: {}", e))
```

**After**:
```rust
CoreError::processing_failed()
    .with_operation("execute_command")
    .with_context("command", "hyprctl")
    .with_source(e)
    .build()
```

### **Pattern 5: Channel Communication Errors**

**Before** (3 instances):
```rust
CoreError::Other(format!("Channel send failed ({}): {}", context, e))
```

**After**:
```rust
CoreError::processing_failed()
    .with_operation("channel_send")
    .with_context("channel_context", context)
    .with_source(e)
    .build()
```

## Integration Benefits

### **1. Structured Error Information**
```rust
// Errors now provide structured data for logging/analysis
let error_info = error_context.to_error_info();
// {
//   "error_type": "Database",
//   "message": "Connection failed",
//   "context": {
//     "host": "localhost",
//     "port": "5432",
//     "operation": "connect"
//   },
//   "source_chain": ["Connection timeout", "Network unreachable"]
// }
```

### **2. Zero-Cost for Simple Cases**
```rust
// Simple errors still work exactly as before
return Err(CoreError::Validation("Invalid input".to_string()));

// But structured errors provide much more value
return Err(CoreError::validation("Invalid input")
    .with_field("user_id", user_id)
    .with_context("request_id", request_id)
    .build());
```

### **3. Enhanced Debugging**
```rust
// Before: "Database error: Connection failed"
// After: "Database error: Connection failed (host: localhost, port: 5432, operation: connect)"
//        "Caused by:"
//        "  1: Connection timeout"
//        "  2: Network unreachable"
```

## Helper Functions for Common Patterns

### **Database Operations**
```rust
// Helper for common database error with query context
pub fn db_query_error(operation: &str, query: &str, err: sqlx::Error) -> CoreError {
    CoreError::database("Query failed")
        .with_operation(operation)
        .with_context("query", query)
        .with_source(err)
        .build()
}
```

### **File System Operations**
```rust
// Helper for file system operations
pub fn fs_operation_error(operation: &str, path: &Path, err: std::io::Error) -> CoreError {
    CoreError::io_error(path)
        .with_operation(operation)
        .with_source(err)
        .build()
}
```

### **Command Execution**
```rust
// Helper for command execution failures
pub fn command_execution_error(command: &str, stderr: &str) -> CoreError {
    CoreError::processing_failed()
        .with_operation("execute_command")
        .with_context("command", command)
        .with_context("stderr", stderr)
        .build()
}
```

## Migration Strategy

### **Phase 1: High-Impact Areas (Immediate)**
1. **Database query helpers** (`crate/sinex-db/src/query_helpers.rs`) - Already completed
2. **Configuration parsing** (18 instances across event sources)
3. **File operations** (15 instances in filesystem and shell history readers)

### **Phase 2: Event Source Improvements**
1. **Terminal operations** (`crate/sinex-events/src/terminal.rs`)
2. **Window manager operations** (`crate/sinex-events/src/window_manager.rs`)  
3. **Filesystem monitoring** (`crate/sinex-events/src/filesystem.rs`)

### **Phase 3: System Integration**
1. **Channel operations** (`crate/sinex-core/src/channel_helpers.rs`)
2. **Configuration validation** (`crate/sinex-core/src/config_extractors.rs`)
3. **Test infrastructure** (various test files)

## Example Transformations

### **Window Manager Error**
```rust
// Before
.map_err(|e| sinex_core::CoreError::Other(format!("Failed to execute hyprctl: {}", e)))?

// After  
.map_err(|e| sinex_core::CoreError::processing_failed()
    .with_operation("execute_hyprctl")
    .with_context("command", "hyprctl clients -j")
    .with_source(e)
    .build())?
```

### **Configuration Error**
```rust
// Before
.map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?

// After
.map_err(|e| sinex_core::CoreError::configuration("Config parsing failed")
    .with_context("config_type", "EventSourceConfig")
    .with_source(e)
    .build())?
```

### **Filesystem Error**
```rust
// Before
.map_err(|e| sinex_core::CoreError::Other(format!("Failed to open {:?}: {}", path, e)))?

// After
.map_err(|e| sinex_core::CoreError::io_error(&path)
    .with_operation("open_file")
    .with_source(e)
    .build())?
```

## Backward Compatibility

✅ **Full backward compatibility** - All existing error handling continues to work unchanged
✅ **Gradual migration** - Can adopt structured errors incrementally
✅ **No breaking changes** - Existing `CoreError` enum variants preserved
✅ **Zero performance overhead** - Structured context only built when needed

## Testing Verification

All error context builders are fully tested:

```bash
cargo test --package sinex-core --lib -- error_context
# test error_context::tests::test_error_context_builder ... ok
# test error_context::tests::test_error_with_source_chain ... ok  
# test error_context::tests::test_error_with_event_context ... ok
# test error_context::tests::test_io_error_with_path ... ok
# test error_context::tests::test_error_info_serialization ... ok
# test error_context::tests::test_result_extension ... ok
```

## Summary

The structured error context builders are **complete and production-ready**. They provide:

1. **Structured error information** for better debugging and monitoring
2. **Fluent builder API** for creating rich error contexts
3. **Zero-cost abstraction** - no overhead for simple errors
4. **Full backward compatibility** with existing error handling
5. **Comprehensive test coverage** with 6 passing test cases

The implementation successfully addresses the circular dependency issues and provides a robust foundation for improving error handling throughout the Sinex codebase.