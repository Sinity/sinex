# CI/Pre-commit Enforcement for Test Quality

## Pre-commit Hook Options

### 1. Basic SQL Detection
```bash
#!/bin/bash
# .git/hooks/pre-commit
# Prevents committing tests with raw SQL

if git diff --cached --name-only | grep -q "test/.*\.rs"; then
    # Check for raw SQL in staged test files
    if git diff --cached --name-only | grep "test/.*\.rs" | xargs grep -l "sqlx::query"; then
        echo "❌ Raw SQL queries found in tests!"
        echo "Use TestQueries and TestEventBuilder instead."
        echo ""
        echo "Files with raw SQL:"
        git diff --cached --name-only | grep "test/.*\.rs" | xargs grep -l "sqlx::query" | sed 's/^/  - /'
        exit 1
    fi
fi
```

### 2. Advanced Pattern Detection
```bash
#!/bin/bash
# More sophisticated checks

VIOLATIONS=0

# Check for raw SQL
if git diff --cached --name-only | grep "test/.*\.rs" | xargs grep -E "sqlx::(query|query_as|query_scalar)" 2>/dev/null; then
    echo "❌ Raw SQL detected. Use TestQueries instead."
    VIOLATIONS=$((VIOLATIONS + 1))
fi

# Check for manual ULID conversions
if git diff --cached --name-only | grep "test/.*\.rs" | xargs grep -E "to_uuid\(\)|::uuid|::text.*ulid" 2>/dev/null; then
    echo "❌ Manual ULID conversions detected. Query builders handle this automatically."
    VIOLATIONS=$((VIOLATIONS + 1))
fi

# Check for missing test macro
if git diff --cached --name-only | grep "test/.*\.rs" | xargs grep -E "async fn test_" | grep -v "#\[sinex_test\]" 2>/dev/null; then
    echo "❌ Test functions without #[sinex_test] macro detected."
    VIOLATIONS=$((VIOLATIONS + 1))
fi

exit $VIOLATIONS
```

## GitHub Actions CI

### 1. Test Quality Check Action
```yaml
# .github/workflows/test-quality.yml
name: Test Quality Checks

on:
  pull_request:
    paths:
      - 'test/**/*.rs'

jobs:
  check-test-patterns:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Check for raw SQL in tests
        run: |
          if find test -name "*.rs" -exec grep -l "sqlx::query" {} \; | grep -q .; then
            echo "::error::Raw SQL queries found in tests. Use TestQueries instead."
            find test -name "*.rs" -exec grep -n "sqlx::query" {} + | head -20
            exit 1
          fi
      
      - name: Check for ULID conversions
        run: |
          if find test -name "*.rs" -exec grep -E "to_uuid\(\)|::uuid.*ulid|ulid.*::uuid" {} \; | grep -q .; then
            echo "::error::Manual ULID conversions found. Query builders handle this automatically."
            exit 1
          fi
      
      - name: Check test structure
        run: |
          # Ensure tests use proper abstractions
          python test/check_test_quality.py --strict
```

### 2. Automated Suggestion Bot
```yaml
# .github/workflows/suggest-improvements.yml
name: Test Improvement Suggestions

on:
  pull_request:
    types: [opened, synchronize]

jobs:
  suggest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Analyze test patterns
        id: analyze
        run: |
          python test/refactor_tests.py --output suggestions.md
          
      - name: Comment on PR
        if: steps.analyze.outputs.suggestions
        uses: actions/github-script@v6
        with:
          script: |
            const fs = require('fs');
            const suggestions = fs.readFileSync('suggestions.md', 'utf8');
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: suggestions
            });
```

## Rust-based Enforcement

### 1. Custom Clippy Lints
```rust
// clippy.toml
disallowed-methods = [
    # Disallow raw SQL in tests
    { path = "sqlx::query", reason = "Use TestQueries in tests" },
    { path = "sqlx::query_as", reason = "Use TestQueries in tests" },
    { path = "sqlx::query_scalar", reason = "Use TestQueries in tests" },
]
```

### 2. Compile-time Checks
```rust
// test/common/compile_checks.rs
#[cfg(test)]
compile_error!("This module enforces test patterns at compile time");

// Macro to enforce patterns
#[macro_export]
macro_rules! enforce_test_patterns {
    ($path:expr) => {
        #[cfg(all(test, not(doc)))]
        const _: () = {
            include_str!($path);
            // This will fail if the file contains banned patterns
            assert!(!include_str!($path).contains("sqlx::query"));
        };
    };
}
```

## Integration with Development Workflow

### 1. VS Code Settings
```json
// .vscode/settings.json
{
  "files.associations": {
    "**/test/**/*.rs": "rust-test"
  },
  "rust-analyzer.diagnostics.disabled": [
    "unresolved-import"
  ],
  "rust-analyzer.checkOnSave.command": "clippy",
  "rust-analyzer.checkOnSave.extraArgs": [
    "--tests",
    "--",
    "-W", "clippy::disallowed_methods"
  ]
}
```

### 2. Just Commands
```makefile
# justfile additions
check-tests:
    @echo "Checking test quality..."
    @if find test -name "*.rs" -exec grep -l "sqlx::query" {} \; | grep -q .; then \
        echo "❌ Raw SQL found in tests"; \
        exit 1; \
    fi
    @echo "✅ Test quality checks passed"

fix-tests:
    @echo "Attempting automatic test fixes..."
    @python test/refactor_tests.py --fix --safe-only
```

## Gradual Enforcement Strategy

### Phase 1: Warning Only
- Add checks but don't block commits
- Collect metrics on violations
- Education through PR comments

### Phase 2: Block New Violations
- Prevent new raw SQL in tests
- Allow existing code to remain
- Track reduction over time

### Phase 3: Full Enforcement
- No raw SQL allowed anywhere in tests
- All tests must use approved patterns
- Automated fixes suggested

## Metrics and Monitoring

```bash
#!/bin/bash
# Track improvement over time
echo "Test Quality Metrics - $(date)"
echo "Raw SQL queries: $(find test -name "*.rs" -exec grep -c "sqlx::query" {} + | awk '{sum+=$1} END {print sum}')"
echo "Tests using builders: $(find test -name "*.rs" -exec grep -c "TestEventBuilder" {} + | awk '{sum+=$1} END {print sum}')"
echo "Tests using TestQueries: $(find test -name "*.rs" -exec grep -c "TestQueries::" {} + | awk '{sum+=$1} END {print sum}')"
```