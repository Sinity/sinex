# Sinex Codebase - Remaining Refactoring Tasks

This file contains the TODO items that still need to be addressed. Completed items have been moved to LOCAL_complete.md.

## Critical Security Issue

### Path Validation (MOSTLY COMPLETE - Multiple Agents)

**Session 4 (Agent 1):**
- ✅ Document Ingestor - Fixed arbitrary file read vulnerability
- ✅ Configuration Loading - Fixed SINEX_CONFIG environment variable exploit
- ✅ Filesystem Watcher - Added validation for scan and export operations
- ✅ Terminal Satellite - Added validation for export paths

**Session 5 (6 Security Agents):**
- ✅ **Core Libraries** (Agent 3) - Secured sinex-core, sinex-services, sinex-satellite-sdk with validation utilities
- ✅ **Test Infrastructure** (Agent 4) - Added path validation to test utilities, temp file creation secured
- ✅ **Database/Migrations** (Agent 5) - Added PostgreSQL validation functions, secured blob manager
- ✅ **CLI Interfaces** (Agent 6) - All CLI path arguments now use SanitizedPath validation
- ✅ **Configuration Files** (Agent 7) - Comprehensive SecurePath framework, custom deserializers
- ✅ **File Watchers** (Agent 8) - FileWatchingSecurityPolicy system, symlink protection

**Security Infrastructure Created:**
- FileWatchingSecurityPolicy with configurable levels
- SecurePath wrapper with validation levels
- PostgreSQL path validation functions
- Comprehensive test coverage for all attack vectors

**Remaining Work:**
- Some legacy files may still need migration
- Monitor for any missed edge cases in production

## Remaining Refactoring Tasks by File

### sinex-services [✅ COMPLETED - Agent 2]

All tasks completed:
- ✅ Fixed SQL injection vulnerability using SeaQuery
- ✅ Fixed UTF-8 unsafe string operations  
- ✅ Extracted time-range query logic helper
- ✅ Removed duplicate store_content method
- ✅ Created MaterialSummary struct with serde
- ✅ Added MetadataBuilder for consistent JSON construction
- ✅ Implemented safe content preview helpers
- ✅ Simplified entity type mapping with EntityTypeMapper

### sinex-satellite-sdk, sinex-macros, sinex-test-utils, sinex-migrations [✅ COMPLETED - Agent 4]

All 12 tasks completed:
- ✅ config.rs - Extracted get_cache_dir_or_fallback() helper
- ✅ grpc_client.rs - Verified #[inline] already present
- ✅ stream_processor.rs - Fixed TODO, enabled auto_event_metrics macro
- ✅ cli.rs - Used matches! macro to avoid allocations
- ✅ error_context.rs - Broke up 350+ line function into helpers
- ✅ error_context.rs - Used proc macro Diagnostic API
- ✅ test-utils lib.rs - Implemented proper error conversion traits
- ✅ test-utils builders.rs - Already using bon::Builder
- ✅ migrations lib.rs - Created migrations! macro for boilerplate reduction

### sinex-ingestd [✅ COMPLETED - Agent 5]

All tasks completed:
- ✅ main.rs - Extracted validation to IngestdConfig::validate_and_exit()
- ✅ config.rs - Already using bon builder, upgraded URL validation with url crate
- ✅ service.rs - Extracted shutdown_signal() helper, enhanced telemetry, created SubjectCache newtype, using actual schema values
- ✅ validator.rs - Created SchemaCache/SchemaLookup newtypes with proper methods
- ✅ figment_config.rs - Replaced separate defaults with const values, extracted figment helpers
- ✅ schema_sync.rs - Using iterator methods with fold(), pre-computing hashes

### sinex-gateway [✅ COMPLETED - Agent 8 from Session 5]

All 7 files completed:
- ✅ main.rs - Extracted setup_tracing(), added .with_operation() context
- ✅ service_container.rs - Used SinexError patterns, db_error() helper
- ✅ rpc_server.rs - Fixed critical Unix socket vs TCP issue, made configurable, created dispatch table
- ✅ handlers.rs - Extracted ULID/parameter helpers, fixed unsafe conversions
- ✅ native_messaging.rs - Removed byteorder dependency, extracted shared dispatch logic
- ✅ cascade_analyzer.rs - Applied UlidArrayExt trait, added db_error() context, simplified temp table naming
- ✅ replay_state_machine.rs - Created state transition table, extracted ulid_to_lock_id() helper, consistent db_error() usage

### Terminal Satellites [✅ COMPLETED - Agent 6]

All tasks completed:
- ✅ shell_detection.rs - Added sysinfo crate, refactored with ? operator
- ✅ kitty.rs - Used iterator combinators, created JSON structs, extracted duration helper
- ✅ unified_processor.rs (terminal) - Implemented bon::Builder, extracted file metadata helpers
- ✅ unified_processor.rs (canonicalizer) - Replaced JsonExtractor with json_helpers module, created TerminalEventSource enum, added ULID validation

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

### System/Desktop/Health Satellites [✅ COMPLETED - Agent 7]

All tasks completed:
- ✅ system-satellite/lib.rs - Added context methods and #[from] attributes
- ✅ system-satellite/unified_processor.rs - Used ..Default::default() and builder pattern
- ✅ system-satellite/dbus_watcher.rs - Created error helper, MonitorConfig struct, implemented tokio-retry
- ✅ desktop-satellite/clipboard.rs - Verified methods already properly split
- ✅ desktop-satellite/window_manager.rs - Removed unused fields
- ✅ health-aggregator/lib.rs - Extracted create_empty_scan_report() helper
- ✅ health-aggregator/unified_processor.rs - Added imports, defined constants, extracted build_scan_report()

### Automaton Services [NEEDS WORK - Agent 9 had API error]

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
---

## Summary of Latest Refactoring Session (2025-08-10 - Session 4)

7 parallel refactoring agents completed major portions of the remaining work:

### Highlights:
- **Critical Security Fixes**: Agent 1 fixed path traversal vulnerabilities in 4 critical files
- **SQL Injection Fixed**: Agent 2 eliminated SQL injection risks in search service
- **Unix Socket Issue Resolved**: Agent 3 fixed major architectural inconsistency in RPC server
- **100% Completion**: Agents 2, 4, 5, 6 completed ALL assigned tasks
- **Partial Completion**: Agents 3, 7 completed majority of tasks with some remaining

See LOCAL_complete.md for the full history of completed work.

---

## Summary of Latest Refactoring Session (2025-08-10 - Session 5)

8 parallel refactoring agents tackled remaining work and critical security issues:

### Major Accomplishments:
- **Path Validation Security**: 6 agents secured different layers (core, test, DB, CLI, config, watchers)
- **Gateway Completion**: Agent 8 completed all remaining gateway refactoring (3 files)
- **Automaton Work**: Agent 9 attempted but encountered API error - needs retry

### Security Infrastructure Created:
- Comprehensive path validation across all layers
- FileWatchingSecurityPolicy system with configurable levels
- SecurePath wrapper with multiple validation levels
- PostgreSQL path validation functions
- Attack vector test coverage

Most critical security vulnerabilities have been addressed. See LOCAL_complete.md for full history.
