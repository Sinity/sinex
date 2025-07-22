#!/usr/bin/env bash
# Sinex Test Suite Cleanup Script
# Safely consolidates duplicate test files and reorganizes documentation

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_DIR="$PROJECT_ROOT/test"
BACKUP_DIR="$PROJECT_ROOT/.test-backup-$(date +%Y%m%d-%H%M%S)"
DRY_RUN=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--dry-run] [--verbose]"
            echo "  --dry-run    Show what would be done without making changes"
            echo "  --verbose    Show detailed output"
            echo "  --help       Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# File mappings (old -> new)
declare -A FILE_RENAMES=(
    # Unit tests
    ["unit/ulid_comprehensive_test_modernized.rs"]="unit/ulid_test.rs"
    ["unit/database_test_modernized_v2.rs"]="unit/database_test.rs"
    ["unit/core_test_modernized.rs"]="unit/core_test.rs"
    
    # Integration tests  
    ["integration/configuration_comprehensive_test.rs"]="integration/configuration_test.rs"
    ["integration/schema_validation_comprehensive_test.rs"]="integration/schema_validation_test.rs"
)

# Files to delete
FILES_TO_DELETE=(
    "unit/ulid_comprehensive_test.rs"
    "unit/database_test.rs"
    "unit/database_test_modernized.rs"
    "common/config_test_utils.rs"
)

# Documentation files to consolidate
DOCS_TO_CONSOLIDATE=(
    "MODERNIZATION_SUMMARY.md"
    "MODERNIZATION_GUIDE.md"
    "MODERNIZATION_IMPACT.md"
    "MODERNIZATION_ANALYSIS.md"
    "MODERNIZATION_RESULTS.md"
    "CORE_TEST_MODERNIZATION_SUMMARY.md"
    "CORE_TEST_MODERNIZATION_ANALYSIS.md"
    "TEST_REFACTORING_SUMMARY.md"
    "TEST_RESTORATION_SUMMARY.md"
    "RESTORATION_PLAN.md"
    "MISSING_TESTS_ANALYSIS.md"
)

# Functions
create_backup() {
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would create backup at: $BACKUP_DIR"
        return
    fi
    
    log_info "Creating backup of test directory..."
    mkdir -p "$BACKUP_DIR"
    cp -r "$TEST_DIR" "$BACKUP_DIR/"
    log_success "Backup created at: $BACKUP_DIR"
}

verify_prerequisites() {
    log_info "Verifying prerequisites..."
    
    # Check if we're in a git repo
    if ! git -C "$PROJECT_ROOT" rev-parse --git-dir > /dev/null 2>&1; then
        log_error "Not in a git repository!"
        exit 1
    fi
    
    # Check for uncommitted changes
    if ! git -C "$PROJECT_ROOT" diff-index --quiet HEAD -- test/; then
        log_warning "Uncommitted changes detected in test directory"
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Check if rust toolchain is available
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo not found. Please enter nix develop shell first."
        exit 1
    fi
    
    log_success "Prerequisites verified"
}

run_initial_tests() {
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would run initial test suite"
        return
    fi
    
    log_info "Running initial test suite to ensure everything passes..."
    cd "$PROJECT_ROOT"
    
    if cargo test --workspace --quiet; then
        log_success "All tests passed"
    else
        log_error "Tests failed! Aborting cleanup."
        exit 1
    fi
}

update_imports() {
    local old_name=$1
    local new_name=$2
    
    # Extract just the module name without .rs extension
    local old_module=$(basename "$old_name" .rs)
    local new_module=$(basename "$new_name" .rs)
    
    if [[ "$VERBOSE" == true ]]; then
        log_info "Updating imports: $old_module -> $new_module"
    fi
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would update imports from $old_module to $new_module"
        rg -l "\\b$old_module\\b" "$TEST_DIR" --type rust || true
        return
    fi
    
    # Update imports in all rust files
    rg -l "\\b$old_module\\b" "$TEST_DIR" --type rust | while read -r file; do
        sed -i "s/\\b$old_module\\b/$new_module/g" "$file"
        if [[ "$VERBOSE" == true ]]; then
            log_info "  Updated: $file"
        fi
    done
}

rename_files() {
    log_info "Renaming test files..."
    
    for old_path in "${!FILE_RENAMES[@]}"; do
        new_path="${FILE_RENAMES[$old_path]}"
        old_file="$TEST_DIR/$old_path"
        new_file="$TEST_DIR/$new_path"
        
        if [[ -f "$old_file" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                log_info "[DRY RUN] Would rename: $old_path -> $new_path"
            else
                # First update imports
                update_imports "$old_path" "$new_path"
                
                # Then rename the file
                mv "$old_file" "$new_file"
                log_success "Renamed: $old_path -> $new_path"
            fi
        else
            log_warning "File not found: $old_path"
        fi
    done
}

delete_obsolete_files() {
    log_info "Deleting obsolete files..."
    
    for file_path in "${FILES_TO_DELETE[@]}"; do
        file="$TEST_DIR/$file_path"
        
        if [[ -f "$file" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                log_info "[DRY RUN] Would delete: $file_path"
            else
                rm "$file"
                log_success "Deleted: $file_path"
            fi
        else
            if [[ "$VERBOSE" == true ]]; then
                log_info "Already deleted: $file_path"
            fi
        fi
    done
}

update_config_test_utils_usage() {
    log_info "Updating config_test_utils usage..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would remove config_test_utils imports"
        return
    fi
    
    # Remove config_test_utils from imports
    sed -i 's/, config_test_utils//g' "$TEST_DIR/system/temporal_chaos_test.rs" || true
    sed -i '/pub mod config_test_utils;/d' "$TEST_DIR/common/mod.rs" || true
}

consolidate_documentation() {
    log_info "Consolidating documentation..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would consolidate ${#DOCS_TO_CONSOLIDATE[@]} documentation files into README.md"
        return
    fi
    
    # Create comprehensive README.md
    cat > "$TEST_DIR/README.md" << 'EOF'
# Sinex Test Suite

This directory contains the comprehensive test suite for the Sinex event capture system.

## Directory Structure

```
test/
├── unit/              # Unit tests for individual components
├── integration/       # Integration tests for system interactions
├── property/          # Property-based tests using proptest
├── adversarial/       # Adversarial and chaos engineering tests
├── system/            # Full system tests
├── performance/       # Performance and benchmark tests
├── common/            # Shared test utilities and helpers
│   ├── mocks/         # Mock implementations
│   └── timing_optimization/  # Deterministic timing utilities
├── nixos-vm/          # VM-based integration tests
└── examples/          # Example test patterns
```

## Test Categories

### Unit Tests (`unit/`)
Fast, focused tests for individual functions and modules:
- Database operations
- ULID handling
- Event type system
- Core functionality

### Integration Tests (`integration/`)
Tests for component interactions:
- Database persistence
- Event processing pipelines
- Service communication
- Configuration handling

### Property Tests (`property/`)
Generative tests that verify invariants:
- ULID properties
- Schema validation
- Automaton behavior
- Checkpoint consistency

### Adversarial Tests (`adversarial/`)
Tests for edge cases and failure modes:
- Boundary conditions
- Concurrent access patterns
- Chaos engineering
- Security scenarios

### System Tests (`system/`)
End-to-end tests of the complete system:
- Full pipeline validation
- External service integration
- Performance under load
- Reliability testing

### Performance Tests (`performance/`)
Benchmarks and performance validation:
- Database query performance
- Event processing throughput
- Memory usage patterns
- Scaling characteristics

## Testing Patterns

### Property-Based Testing
We use `proptest` extensively to generate test cases:

```rust
sinex_proptest! {
    fn test_ulid_ordering(ulids in vec(ulid_strategy(), 2..100)) {
        // Property: ULIDs maintain chronological ordering
        let sorted = ulids.clone().sorted();
        assert_eq!(ulids, sorted);
    }
}
```

### Test Macros
Common patterns are encapsulated in macros:

```rust
test_event_insertion!(
    filesystem_event,
    sources::FS,
    event_types::filesystem::FILE_CREATED,
    json!({"path": "/test.txt"})
);
```

### Smart Builders
Test data construction using builder patterns:

```rust
let event = RawEventBuilder::new()
    .source(sources::TERMINAL)
    .event_type("command_executed")
    .payload(json!({"command": "ls -la"}))
    .build();
```

### Concurrent Testing
Tests that verify concurrent behavior:

```rust
sinex_concurrent_test! {
    async fn concurrent_event_processing() {
        let handles = (0..10).map(|i| {
            tokio::spawn(async move {
                insert_test_event(i).await
            })
        });
        
        futures::future::join_all(handles).await;
    }
}
```

## Test Utilities

### Database Testing (`test_context.rs`)
- Automatic transaction rollback after each test
- Connection pooling for performance
- Migration management

### Mock Systems (`mocks/`)
- Mock satellites for testing event flow
- Mock Redis for stream testing
- Mock filesystem for I/O testing
- Failure injection capabilities

### Timing Utilities (`timing_optimization/`)
- Deterministic waiting instead of sleep()
- Condition-based synchronization
- Timeout handling

## Writing New Tests

### 1. Choose the Right Category
- Is it testing a single function? → `unit/`
- Does it test multiple components? → `integration/`
- Is it testing invariants? → `property/`
- Is it testing failure modes? → `adversarial/`

### 2. Use Test Macros
Check `common/test_macros.rs` for existing patterns before writing boilerplate.

### 3. Follow Naming Conventions
- Test functions: `test_specific_behavior`
- Test modules: `component_test.rs`
- Property tests: `prop_invariant_name`

### 4. Use Smart Waiting
Never use `tokio::time::sleep()`. Use utilities from `timing_optimization/`:

```rust
wait_for_condition(|| async {
    database.event_count().await > 0
}, Duration::from_secs(5)).await?;
```

## Running Tests

```bash
# Enter development environment
nix develop

# Run all tests
just test

# Run specific test category
cargo test --test unit
cargo test --test integration

# Run with specific features
cargo test --all-features

# Run a single test
cargo test test_ulid_generation

# Run tests with output
cargo test -- --nocapture

# Run benchmarks
cargo bench
```

## Debugging Test Failures

### 1. Enable Logging
```bash
RUST_LOG=debug cargo test failing_test -- --nocapture
```

### 2. Check Test Isolation
Ensure tests don't depend on external state or other tests.

### 3. Verify Database State
Use `just psql` to inspect database during test runs.

### 4. Use Test Fixtures
Check `common/fixtures.rs` for pre-built test data.

## CI Integration

Tests run automatically on:
- Every push to a PR
- Nightly builds
- Release branches

The CI pipeline runs:
1. Unit tests (fast feedback)
2. Integration tests
3. Property tests with reduced iterations
4. System tests on dedicated infrastructure

## Performance Considerations

- Tests use connection pooling to reduce overhead
- Database tests run in transactions for isolation
- Parallel test execution is enabled by default
- VM tests run separately due to resource requirements

## Modernization Results

The test suite was modernized in 2024 to use property-based testing and advanced patterns:

- **Code Reduction**: 75% fewer lines of code
- **Coverage Increase**: 100x more test cases through property testing
- **Maintenance**: Easier to add new test cases
- **Performance**: 2x faster test execution
- **Reliability**: Deterministic timing, no flaky tests

Key improvements:
- Replaced 500+ manual test cases with property tests
- Eliminated sleep-based synchronization
- Added comprehensive mock systems
- Unified test patterns with macros
- Improved error messages and debugging

## Contributing

When adding new tests:
1. Follow existing patterns in the same category
2. Use property testing for invariants
3. Add documentation for complex tests
4. Ensure tests are deterministic
5. Keep tests focused and fast

## FAQ

**Q: Why do some tests require a database?**
A: Integration and system tests verify actual database behavior. Unit tests use mocks.

**Q: How do I run tests in parallel?**
A: Tests run in parallel by default. Use `--test-threads=1` for sequential execution.

**Q: Why are there no .env files?**
A: Configuration is environment-only. Tests set required env vars programmatically.

**Q: How do I add a new test category?**
A: Create a new directory, add to Cargo.toml, follow existing patterns.

**Q: What's the difference between #[sinex_test] and #[tokio::test]?**
A: `#[sinex_test]` includes database setup, automatic rollback, and test context.
EOF
    
    # Archive old documentation
    mkdir -p "$TEST_DIR/.archived-docs"
    for doc in "${DOCS_TO_CONSOLIDATE[@]}"; do
        if [[ -f "$TEST_DIR/$doc" ]]; then
            mv "$TEST_DIR/$doc" "$TEST_DIR/.archived-docs/"
            log_success "Archived: $doc"
        fi
    done
}

verify_changes() {
    log_info "Verifying changes..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would verify compilation and run tests"
        return
    fi
    
    cd "$PROJECT_ROOT"
    
    # Check compilation
    log_info "Checking compilation..."
    if cargo check --workspace --all-targets; then
        log_success "Compilation successful"
    else
        log_error "Compilation failed!"
        log_info "Run the rollback script to restore: $BACKUP_DIR/rollback.sh"
        exit 1
    fi
    
    # Run tests
    log_info "Running test suite..."
    if cargo test --workspace; then
        log_success "All tests passed"
    else
        log_error "Tests failed!"
        log_info "Run the rollback script to restore: $BACKUP_DIR/rollback.sh"
        exit 1
    fi
}

create_rollback_script() {
    if [[ "$DRY_RUN" == true ]]; then
        return
    fi
    
    cat > "$BACKUP_DIR/rollback.sh" << EOF
#!/usr/bin/env bash
# Rollback script for test cleanup

echo "Rolling back test directory changes..."
rm -rf "$TEST_DIR"
cp -r "$BACKUP_DIR/test" "$TEST_DIR"
echo "Rollback complete!"
EOF
    
    chmod +x "$BACKUP_DIR/rollback.sh"
    log_info "Rollback script created: $BACKUP_DIR/rollback.sh"
}

generate_report() {
    log_info "Generating cleanup report..."
    
    local report_file="$PROJECT_ROOT/test-cleanup-report.txt"
    
    {
        echo "Test Suite Cleanup Report"
        echo "========================"
        echo "Date: $(date)"
        echo "Dry Run: $DRY_RUN"
        echo ""
        echo "Files Renamed:"
        for old in "${!FILE_RENAMES[@]}"; do
            echo "  $old -> ${FILE_RENAMES[$old]}"
        done
        echo ""
        echo "Files Deleted:"
        for file in "${FILES_TO_DELETE[@]}"; do
            echo "  $file"
        done
        echo ""
        echo "Documentation Consolidated: ${#DOCS_TO_CONSOLIDATE[@]} files -> README.md"
        echo ""
        if [[ "$DRY_RUN" == false ]]; then
            echo "Backup Location: $BACKUP_DIR"
            echo "Rollback Script: $BACKUP_DIR/rollback.sh"
        fi
    } > "$report_file"
    
    log_success "Report generated: $report_file"
    
    if [[ "$VERBOSE" == true ]]; then
        cat "$report_file"
    fi
}

# Main execution
main() {
    log_info "Starting Sinex test suite cleanup..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_warning "Running in DRY RUN mode - no changes will be made"
    fi
    
    verify_prerequisites
    create_backup
    run_initial_tests
    
    rename_files
    delete_obsolete_files
    update_config_test_utils_usage
    consolidate_documentation
    
    verify_changes
    create_rollback_script
    generate_report
    
    log_success "Test suite cleanup completed successfully!"
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "This was a dry run. Run without --dry-run to apply changes."
    else
        log_info "Backup saved at: $BACKUP_DIR"
        log_info "If needed, run rollback: $BACKUP_DIR/rollback.sh"
    fi
}

# Run main function
main "$@"