# Error Handling Patterns in Sinex

This document outlines the error handling patterns used throughout the Sinex codebase and identifies areas for improvement.

## Current State Analysis

### ✅ Proper ErrorContext Usage

These components correctly use the ErrorContext pattern:

1. **sinex-core-types** - Base ErrorContext implementation
2. **sinex-error** - Extended ErrorContext with utilities
3. **Test infrastructure** - Uses ErrorContext for structured test failures

### ⚠️ Inconsistent Patterns (Needs Improvement)

The following components use `format!` macros for error messages, violating the plan.md rule:

#### High Priority (Core System)
- **sinex-satellite-sdk/src/config.rs** (lines 289, 299)
- **sinex-*-satellite** components (multiple instances)
- **sinex-desktop-satellite/src/window_manager.rs** (lines 151, 155, 215, etc.)
- **sinex-terminal-satellite/src/atuin.rs** (line 69)

#### Medium Priority (Utility Libraries)
- **sinex-metrics-lib** (multiple format! usages for MetricsError)

## Recommended Patterns

### Instead of format! for errors:

```rust
// ❌ FORBIDDEN per plan.md
return Err(SatelliteError::Processing(format!(
    "Failed to process event: {}", error
)));

// ✅ RECOMMENDED
return Err(
    ErrorContext::new(CoreError::Processing("Failed to process event"))
        .with_context("error_details", error)
        .with_context("component", "satellite")
        .into()
);
```

### For configuration errors:

```rust
// ❌ Current pattern
return Err(ConfigError::Validation(format!(
    "Invalid log level: {}", level
)));

// ✅ Recommended pattern
return Err(ConfigError::Validation(
    ErrorContext::new(CoreError::Validation("Invalid log level"))
        .with_context("provided_level", level)
        .with_context("valid_levels", "trace, debug, info, warn, error")
        .to_string()
));
```

## Migration Strategy

### Phase 1: Core Satellite SDK (Priority: High)
Update sinex-satellite-sdk to use ErrorContext pattern:
1. Add sinex-error dependency
2. Replace format! usage in config.rs
3. Update SatelliteError to include ErrorContext variants

### Phase 2: Satellite Implementations (Priority: High)
Update all satellite services:
1. sinex-terminal-satellite
2. sinex-desktop-satellite  
3. sinex-system-satellite

### Phase 3: Utility Libraries (Priority: Medium)
Update utility libraries like sinex-metrics-lib to follow patterns or justify exceptions.

## Benefits of Consistent ErrorContext Usage

1. **Structured Error Data**: Key-value context instead of formatted strings
2. **Source Chain Tracking**: Complete error propagation history
3. **Debugging Enhancement**: Rich context for troubleshooting
4. **Observability**: Structured errors work better with monitoring systems
5. **Testing**: Easier to assert on specific error conditions

## Implementation Guidelines

### For new code:
- NEVER use format! for error messages
- Always use ErrorContext for fallible operations
- Include relevant context (event_id, file_path, timestamp, etc.)

### For existing code migration:
- Identify format! usage in error paths
- Replace with ErrorContext builder pattern
- Add relevant structured context
- Maintain backwards compatibility where possible

## Exceptions

The only acceptable use of format! is for:
- Log messages (not error creation)
- User-facing display formatting
- Test assertion messages
- Debug output

## Enforcement

The plan.md explicitly states this is a **non-negotiable implementation requirement**:
> "The `format!` macro is **forbidden** for creating error messages."
> "**All** fallible functions must use the `ErrorContext` builder"

Consider adding a clippy lint to enforce this pattern across the codebase.