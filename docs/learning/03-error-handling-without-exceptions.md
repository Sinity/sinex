# Session 1.3: Error Handling Without Exceptions

**File**: `crate/lib/sinex-core/src/types/error.rs`
**C++ Anchor**: Exceptions, `std::expected`, error codes, HRESULT
**Time**: ~30 minutes

---

## The C++ Error Handling Spectrum

You've probably used all of these:

```cpp
// 1. Exceptions (invisible control flow)
try {
    auto data = fetch_data();
    process(data);
} catch (const std::exception& e) {
    // Handle... maybe
}

// 2. Error codes (easy to ignore)
int result = do_thing();
// Did you check result? Compiler doesn't care.

// 3. std::optional (no error info)
std::optional<Data> data = fetch();
if (!data) { /* Why did it fail? No idea. */ }

// 4. std::expected (C++23, closest to Rust)
std::expected<Data, Error> result = fetch();
```

Each has problems:
- **Exceptions**: Hidden control flow, performance cost, can be ignored with empty catch
- **Error codes**: Trivially ignorable, no type safety
- **Optional**: Loses error information
- **Expected**: Best option, but arrived in C++23

---

## Rust's Answer: `Result<T, E>`

Every fallible function returns `Result<T, E>`:

```rust
enum Result<T, E> {
    Ok(T),   // Success with value
    Err(E),  // Failure with error
}
```

**The compiler forces you to handle both cases.** You cannot accidentally ignore an error.

---

## Your Error Type: `SinexError`

Look at lines 45-117:

```rust
#[derive(Error, Display, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "details")]
pub enum SinexError {
    /// Database error: {0}
    Database(ErrorDetails),
    /// Validation error: {0}
    Validation(ErrorDetails),
    /// Service error: {0}
    Service(ErrorDetails),
    // ... 20+ more variants
}
```

This is a **sum type** - a value of type `SinexError` is exactly ONE of these variants. C++ `std::variant` is similar, but Rust enums are more ergonomic.

**Key insight**: Each variant carries data (`ErrorDetails`). This isn't just an error code - it's structured error information.

---

## ErrorDetails: Rich Context Without Stack Traces

Lines 132-142:

```rust
pub struct ErrorDetails {
    /// The primary error message
    message: String,
    /// Additional context as key-value pairs
    context: IndexMap<String, String>,
    /// Chain of source errors
    sources: Vec<String>,
}
```

This gives you:
1. **Message**: Human-readable description
2. **Context**: Structured key-value pairs (table name, query, timeout, etc.)
3. **Sources**: Causal chain ("Database failed" ← "Connection refused" ← "Network timeout")

**No stack traces needed.** The context tells you exactly what went wrong.

---

## The Builder Pattern for Errors

Lines 243-267 show ergonomic constructors:

```rust
impl SinexError {
    pub fn database(msg: impl Into<String>) -> Self {
        SinexError::Database(ErrorDetails::new(msg))
    }

    pub fn validation(msg: impl Into<String>) -> Self {
        SinexError::Validation(ErrorDetails::new(msg))
    }
    // ... more constructors
}
```

And lines 358-420 add chaining methods:

```rust
pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
    // ... adds context to the error
}

pub fn with_source(mut self, source: impl ToString) -> Self {
    // ... adds source error to chain
}
```

**Usage (from line 34-41):**

```rust
let err = SinexError::database("Query failed")
    .with_context("table", "users")
    .with_context("query_time_ms", 1500)
    .with_source("Connection pool exhausted");
```

This is like exception chaining, but explicit and immutable.

---

## C++ Comparison: Building Similar Errors

In C++, you'd write something like:

```cpp
class MyError : public std::exception {
    std::string message;
    std::map<std::string, std::string> context;
    std::vector<std::string> sources;

public:
    MyError& with_context(std::string key, std::string value) {
        context[key] = value;
        return *this;  // For chaining
    }
    // ... lots more boilerplate
};
```

Rust's `#[derive]` macros generate most of this. The `thiserror` and `displaydoc` crates handle the `Display` and `Error` trait implementations.

---

## The `?` Operator: Early Return Without Boilerplate

This is the game-changer. Consider:

```rust
fn process_file(path: &str) -> Result<Data> {
    let contents = std::fs::read_to_string(path)?;  // Returns early on error
    let parsed = serde_json::from_str(&contents)?;  // Returns early on error
    Ok(transform(parsed))
}
```

The `?` operator does:
1. If `Ok(value)` → unwrap and continue
2. If `Err(e)` → convert `e` to function's error type and return early

**C++ equivalent** (verbose):

```cpp
Result<Data> process_file(const std::string& path) {
    auto contents_result = read_to_string(path);
    if (!contents_result) return contents_result.error();
    auto contents = *contents_result;

    auto parsed_result = from_str(contents);
    if (!parsed_result) return parsed_result.error();
    auto parsed = *parsed_result;

    return transform(parsed);
}
```

The `?` operator eliminates this boilerplate while keeping error handling explicit.

---

## `From` Trait: Automatic Error Conversion

Lines 681-710 show how errors convert automatically:

```rust
impl From<std::io::Error> for SinexError {
    fn from(e: std::io::Error) -> Self {
        SinexError::Io(ErrorDetails::new(e.to_string()))
    }
}

impl From<serde_json::Error> for SinexError {
    fn from(e: serde_json::Error) -> Self {
        SinexError::Serialization(ErrorDetails::new(e.to_string()))
    }
}
```

Now the `?` operator works across error types:

```rust
fn load_config() -> Result<Config, SinexError> {
    let text = std::fs::read_to_string("config.json")?;  // io::Error → SinexError
    let config = serde_json::from_str(&text)?;           // serde::Error → SinexError
    Ok(config)
}
```

The compiler inserts the `From` conversion automatically at each `?`.

---

## Error Categorization Methods

Lines 460-519 add semantic meaning:

```rust
impl SinexError {
    pub fn is_retryable(&self) -> bool {
        matches!(self,
            SinexError::Timeout(_)
            | SinexError::Network(_)
            | SinexError::Database(_)
            | SinexError::Service(_)
        )
    }

    pub fn is_client_error(&self) -> bool {
        matches!(self,
            SinexError::Validation(_)
            | SinexError::NotFound(_)
            | SinexError::AlreadyExists(_)
            | SinexError::Parse(_)
            | SinexError::PermissionDenied(_)
        )
    }

    pub fn status_code(&self) -> u16 {
        match self {
            SinexError::Validation(_) | SinexError::Parse(_) => 400,
            SinexError::NotFound(_) => 404,
            SinexError::Timeout(_) => 408,
            // ...
        }
    }
}
```

**This is why enums beat exceptions.** You can pattern match on error types and make decisions:

```rust
match result {
    Ok(data) => process(data),
    Err(e) if e.is_retryable() => retry_later(e),
    Err(e) if e.is_client_error() => send_400_response(e),
    Err(e) => log_and_alert(e),
}
```

In C++, you'd need `dynamic_cast` or RTTI to achieve similar dispatch.

---

## Pattern Matching on Errors

The `match` expression is exhaustive:

```rust
fn handle_error(e: SinexError) {
    match e {
        SinexError::Database(details) => reconnect_and_retry(details),
        SinexError::Validation(details) => show_user_feedback(details),
        SinexError::NotFound(details) => show_404(details),
        // ... must handle ALL variants or use wildcard
        _ => log_unexpected(e),
    }
}
```

If you add a new `SinexError` variant, the compiler shows **every place** that needs updating. C++ exceptions give you no such guarantees.

---

## The `ResultExt` Trait: Adding Context to Any Error

Lines 764-796:

```rust
pub trait ResultExt<T> {
    fn context(self, msg: &str) -> Result<T>;
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> SinexError;
}
```

This lets you add context at call sites:

```rust
let user = db.get_user(id)
    .context("Failed to fetch user")?;

let config = load_config()
    .with_context(|| SinexError::configuration("Startup failed")
        .with_context("phase", "init"))?;
```

**C++ equivalent**: Custom exception wrapper or manually re-throwing with more context.

---

## Serialization: Errors as Data

Lines 43-44:

```rust
#[derive(... Serialize, Deserialize)]
#[serde(tag = "type", content = "details")]
```

Your errors serialize to JSON:

```json
{
  "type": "Database",
  "details": {
    "message": "Query failed",
    "context": {
      "table": "users",
      "query_time_ms": "1500"
    },
    "sources": ["Connection pool exhausted"]
  }
}
```

Errors can be:
- Sent over network (API responses)
- Stored in logs (structured logging)
- Deserialized back into errors

C++ exceptions don't serialize. Rust errors are just data.

---

## The Type Alias

Line 717:

```rust
pub type Result<T> = std::result::Result<T, SinexError>;
```

This lets you write `Result<User>` instead of `Result<User, SinexError>` everywhere.

---

## Key Insight: Errors in the Type Signature

```rust
// The return type TELLS you this can fail
fn fetch_user(id: UserId) -> Result<User, SinexError>

// This cannot fail (or panics, which is a bug)
fn format_name(user: &User) -> String
```

In C++, you'd need to read documentation or source code to know if a function throws. In Rust, the signature tells you.

---

## What You Built Without Knowing

Your error system implements several patterns:

1. **Sum types for errors** - Each error variant is distinct and matchable
2. **Builder pattern** - `.with_context()` chaining
3. **Error conversion** - `From` trait for automatic conversion
4. **Context enrichment** - `ResultExt` trait extension
5. **Error categorization** - `is_retryable()`, `is_client_error()`, etc.
6. **Serializable errors** - Errors are data, not special

This is production-quality error handling. Many Rust projects don't go this far.

---

## Exercise: Trace Error Flow

Find a function in the codebase that uses `?` multiple times. For example, search for functions returning `Result<`.

Then:
1. List each place `?` appears
2. Identify the original error type at each `?`
3. Find the `From` impl that converts it to `SinexError`
4. Try removing one `?` and handling the error manually

You'll appreciate what `?` provides.

---

## Next Session: Traits

We'll see how Rust's trait system differs from C++ virtual functions - more flexible, zero-cost by default, and composable through blanket implementations.
