# Sinex Codebase - Remaining Refactoring Tasks

This file contains the TODO items that still need to be addressed. Completed items have been moved to LOCAL_complete.md.

## Critical Security Issue

### Path Validation (HIGH PRIORITY)
**From Agent 1 Path Newtype Domain Audit:**
- **95 files** using raw file operations without proper path validation (SECURITY RISK)
- **61 files** using std::path types instead of domain types
- **59 files** using camino types instead of domain types
- Domain newtypes (SanitizedPath, RelativePath, AbsoluteUri) exist but are severely underutilized
- **Immediate action needed** for directory traversal attack prevention

## Remaining Refactoring Tasks by File

### sinex-services

#### analytics.rs
- **Lines 26-48**: Extract duplicated time-range query logic with client-side filtering
  - Two similar branches doing client-side filtering for end_time
  - Extract common query logic and apply filters systematically

#### content.rs
- **Lines 42-51**: Remove duplicate method implementation
  - `store_content` just delegates to `store_large_content` with identical signature
  - Remove redundant method or add doc explaining the distinction
  
- **Lines 34-35, 58-59, 72-74**: Manual error wrapping without using SinexError context
  - Use SinexError's .with_operation() or .with_context() for richer error context

#### search.rs
- **Lines 110-133**: Manual SQL string construction with potential SQL injection risk
  - Use SeaQuery helpers for type-safe query construction
  
- **Line 62**: Manual PostgreSQL array formatting
  - Use proper sqlx array parameter binding
  
- **Lines 99-111**: Complex manual tuple destructuring
  - Use repository pattern methods or create dedicated query struct with FromRow
  
- **Line 88**: Inefficient ILIKE text search
  - Use PostgreSQL full-text search or JSON operators for better performance
  
- **Lines 141-145**: Manual string slicing without UTF-8 safety
  - Use safe string truncation methods

#### pkm.rs
- **Lines 78-87**: Long match statement for entity type mapping
  - Use `impl From<&str> for EntityType` or static HashMap
  
- **Lines 35, 89-93**: Repeated inline JSON metadata construction
  - Extract into structured metadata builder or use serde Serialize struct
  
- **Lines 327-339, 354-364**: Manual JSON response construction
  - Create MaterialSummary struct with serde derives
  
- **Lines 182-186**: Manual content preview generation without UTF-8 safety
  - Extract into safe content preview helper

### sinex-satellite-sdk

#### config.rs
- **Lines 354-358**: Complex error handling with multiple fallbacks
  - Extract to named function with clear error handling

#### grpc_client.rs  
- **Lines 164**: Missing inline hint for small constructor functions
  - Add #[inline] for performance

#### stream_processor.rs
- **Lines 232-237**: Manual builder implementation for Checkpoint
  - Consider using bon::Builder derive
  
- **Lines 439**: TODO comment about macro usage
  - Fix macro to use sinex_core instead of sinex_db

#### cli.rs
- **Lines 138-150**: Complex match logic for checkpoint parsing
  - Extract parsing logic to separate functions
  
- **Line 138**: Unnecessary string allocation in to_lowercase()
  - Use str methods directly or matches! macro

### sinex-macros

#### error_context.rs
- **Line 46**: Large function (350+ lines)
  - Extract validation logic, context building, and code generation
  
- **Lines 248-253**: Manual eprintln! for warnings
  - Use proper diagnostic reporting for proc macros

### sinex-test-utils

#### lib.rs
- **Line 721**: Manual error conversion in test
  - Implement proper error conversion trait
  
- **Line 790**: Generic error assertion
  - Test for specific error types or use structured error assertions

#### builders.rs
- **Lines 66-77**: Manual builder implementation while bon::Builder is available
  - Use bon::Builder derive for consistency

### sinex-migrations

#### lib.rs
- **Lines 28-49**: Large Vec construction with many Box::new calls
  - Consider using a macro to reduce boilerplate

### sinex-ingestd

#### main.rs
- **Lines 82-94**: Manual validation logic in main
  - Extract validation to config method, use ? operator

#### config.rs
- **Lines 81-103**: Manual builder pattern when bon is available
  - Use bon builder pattern consistently
  
- **Lines 232-237**: Manual URL validation instead of using existing utilities
  - Use url crate parsing

#### service.rs
- **Lines 296-347**: Complex async task with nested select
  - Extract shutdown signal handling to utility function
  
- **Lines 520-523**: Manual error counting
  - Use project's telemetry system consistently
  
- **Line 43**: Type alias without descriptive naming
  - Make proper newtype with methods for cache operations
  
- **Lines 671-677**: Hardcoded column defaults in query
  - Use actual schema values from event validation

#### validator.rs
- **Lines 37-42**: Complex nested generic types
  - Create newtype wrapper for the cache
  
- **Lines 25-32**: Manual cache entry struct when simpler approach available
  - Use interned strings or string IDs

#### figment_config.rs
- **Lines 83-113**: Separate default functions instead of using defaults
  - Use serde default attributes with const values
  
- **Lines 136-163**: Duplicate figment setup logic
  - Extract common figment setup to helper method
  
- **Lines 198-224**: Custom validation when validator crate has built-ins
  - Use validator crate's built-in validators

#### schema_sync.rs
- **Lines 41-55**: Manual counting in loop
  - Use iterator methods to count results
  
- **Lines 46-48**: Computing hash on every iteration
  - Pre-compute hashes or cache them

### sinex-gateway

#### main.rs
- **Lines 56-64**: Manual tracing setup
  - Use project's established tracing patterns or extract to helper
  
- **Lines 78-79**: Using ? operator without additional context
  - Add operation context using .with_operation()

#### service_container.rs
- **Lines 28-30**: Using wrap_err from color_eyre
  - Use SinexError::configuration() with project's context patterns
  
- **Lines 33-35**: Manual error conversion with wrap_err
  - Use established db_error() helper
  
- **Lines 43-44**: Using with_context with formatted string
  - Use SinexError::io().with_path()

#### handlers.rs
- **Lines 46-52**: Verbose error handling
  - Extract ULID parsing to helper function
  
- **Line 198**: Unsafe string conversion
  - Use proper error handling or encoding detection
  
- **Lines 15-29**: Duplicate parameter extraction
  - Create generic parameter extraction helper or macro

#### rpc_server.rs
- **Lines 154-156**: Unix socket handling but binds to TCP
  - Either implement actual Unix socket or remove socket handling code
  
- **Lines 174-175**: Hardcoded TCP address
  - Make configurable through CLI parameter or environment variable
  
- **Lines 84-126**: Large match statement
  - Extract to dispatch table or use handler registry pattern

#### native_messaging.rs
- **Lines 132-161**: Duplicate dispatch logic
  - Extract common dispatch logic to shared module
  
- **Line 3**: Using byteorder crate for simple endian operations
  - Use native Rust methods

#### cascade_analyzer.rs
- **Lines 166-169**: Manual ULID/UUID conversion
  - Use established UlidArrayExt trait
  
- **Lines 316-318**: Unused type annotation
  - Fix type annotation to match actual database types
  
- **Line 99**: ULID usage for temporary names
  - Use simpler random identifier
  
- **Lines 240-245**: Missing error context
  - Add context using db_error() helper

#### replay_state_machine.rs
- **Lines 45-92**: Complex state validation
  - Consider using state transition table or state machine library
  
- **Lines 497-501**: Manual byte manipulation
  - Extract to helper function
  
- **Lines 208-223**: Direct SQLX without error helpers
  - Use db_error() helper

### Terminal Satellites

#### shell_detection.rs
- **Lines 192-200**: Verbose command output parsing
  - Use ? operator in helper function
  
- **Lines 207-217**: Platform-specific code could be cleaner
  - Use sysinfo crate

#### kitty.rs
- **Lines 128-135**: Manual socket path construction
  - Use iterator combinators and extract to helper
  
- **Lines 229-284**: Deep nested JSON parsing
  - Use serde_json::from_value with proper struct types
  
- **Lines 565-581**: Duplicate duration elapsed check
  - Extract to helper method or use boolean flag

#### unified_processor.rs (terminal)
- **Lines 54-82**: Default implementation could use builder pattern
  - Use bon builder pattern
  
- **Lines 204-233**: File existence check with metadata repeated
  - Extract to helper method
  
- **Lines 773-787**: File size estimation logic duplicated
  - Extract to helper method

#### unified_processor.rs (canonicalizer)
- **Lines 40-56**: Manual JSON value extraction trait
  - Use serde_json built-in methods
  
- **Lines 263-286**: String literal array for event sources
  - Use constants or enum
  
- **Lines 146-150**: Unsafe ULID access
  - Use proper error handling

### FS-Watcher/System Satellites

#### fs-watcher/unified_processor.rs
- **Line 429**: Extract should_process variable
  - `let should_process = self.matches_patterns(utf8_path);`
  
- **Lines 452-481**: Split into separate match methods
  - Extract matches_watch_patterns(), matches_ignore_patterns()
  
- **Lines 585-590**: Define error constants
  - Hardcoded error strings should be constants
  
- **Lines 613-621**: Pass parameters instead of cloning
  - Clone-heavy temporary instance creation
  
- **Lines 870-881**: Extract configure_watch_patterns()
  - Complex initialization logic in trait method
  
- **Line 1056**: Use .as_str() instead of .to_string()
  - Inefficient iterator usage

#### fs-watcher/cli.rs
- **Line 61**: Return proper error
  - Return Err(eyre!("Direct mode not supported"))
  
- **Lines 50-89**: Extract run methods
  - Extract run_direct_mode(), run_sensd_mode()
  
- **Lines 72-80**: Implement From trait
  - Implement From<Args> for SensdIntegrationConfig

#### system-satellite/lib.rs
- **Lines 74-87**: Add context methods
  - Add context methods like SinexError pattern
  
- **Lines 89-93**: Use #[from] attribute
  - Use #[from] attribute in error enum

#### system-satellite/unified_processor.rs
- **Lines 112-125**: Use ..Default::default()
  - Manual field initialization in new()
  
- **Lines 142-147**: Use builder pattern
  - Mutable variable accumulation

#### system-satellite/dbus_watcher.rs
- **Lines 106-112**: Create error helper
  - Repeated error pattern
  
- **Lines 44-48**: Create MonitorConfig struct
  - Complex closure parameter
  
- **Lines 78-92**: Use tokio-retry
  - Manual retry loop implementation

### Desktop/Health Satellites

#### desktop-satellite/clipboard.rs
- **Lines 671-748**: Split check methods
  - Split into check_main_clipboard(), check_primary_selection()

#### desktop-satellite/window_manager.rs
- **Lines 154, 161-165, 200-205**: Remove unused fields
  - Multiple _ prefixed fields that are stored but never used

#### health-aggregator/lib.rs
- **Lines 74-83**: Extract create_empty_scan_report()
  - Repetitive ScanReport construction

#### health-aggregator/unified_processor.rs
- **Line 146**: Add Event import
  - Missing import: use sinex_core::types::events::Event;
  
- **Lines 126, 128, 131**: Define threshold constants
  - Magic numbers for health thresholds
  
- **Lines 210-218, 268-276, 289-297**: Extract build_scan_report()
  - Repetitive ScanReport construction

### Automaton Services

#### analytics-automaton/lib.rs
- **Lines 46-57**: Update trait signature
  - Using deprecated initialize signature
  
- **Lines 68-84**: Add TODO or implement
  - Stubbed implementation pattern
  
- **Lines 105-148**: Extract to shared SDK
  - Identical ExplorationProvider implementation

#### analytics-automaton/unified_processor.rs
- **Lines 38-43**: Reconcile interfaces
  - Initialize method signature doesn't match lib.rs
  
- **Lines 92-98**: Add missing fields
  - SourceState creation missing required fields
  
- **Lines 113-122**: Fix field names
  - Inconsistent field names in CoverageAnalysis

#### content-automaton/lib.rs
- **Lines 46-57**: Extract common base implementation
  - Copy-paste implementation with only processor name changed
  
- **Lines 105-148**: Use configurable description
  - Hardcoded processor description

#### content-automaton/unified_processor.rs
- **Lines 25-27**: Use SinexError::validation()
  - Ad-hoc error construction
  
- **Lines 42-50**: Use ? operator with proper error context
  - Nested Option/Result handling

#### pkm-automaton/lib.rs
- **Line 107**: Move import to module level
  - Redundant import inside function scope
  
- **Lines 108-116**: Create shared factory function
  - Identical SourceState construction

#### search-automaton/lib.rs
- **Lines 105-148**: Extract to shared implementation
  - Complete code duplication of ExplorationProvider

### Document Ingestor

#### lib.rs
- **Lines 70-73**: Use SinexError::processing()
  - Manual error creation without using project's patterns
  
- **Lines 87-90**: Add debug log for mime type
  - mime_guess operation without logging
  
- **Lines 213-218**: Extract to helper function
  - Manual path conversion with verbose error handling
  
- **Lines 278-300**: Consider using builder pattern
  - Manual capabilities definition
  
- **Lines 155-167**: Define as named constants
  - String magic values for material types

### RPC Dispatcher

#### lib.rs
- **Lines 17-30**: Define typed configuration struct
  - Generic HashMap for server_config
  
- **Lines 57-82**: Add TODO comments or warning logs
  - No-op scan implementation
  
- **Lines 64-70**: Simplify match arms
  - Repetitive match arms
  
- **Lines 103-146**: Return NotImplemented errors
  - Placeholder implementation returning empty values
  
- **Lines 125-127**: Make configurable
  - Hardcoded time range