# Test Conversion Example: atuin_tests.rs

## Before: 1123 lines of manual test code

### Key Problems Identified:
1. **Manual database setup** (lines 12-23)
2. **Manual event insertion** (lines 41-58) 
3. **Manual SQLite test database creation** (lines 61-148)
4. **Repetitive test data structures** (lines 156-193)
5. **Manual verification loops** (lines 500-650)
6. **Duplicated test scenarios** across multiple test functions

## After: ~400 lines with test utilities

### 1. Replace Manual Database Setup

**Before** (12 lines):
```rust
async fn setup_test_db() -> Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
    let pool = create_test_pool(&database_url).await?;
    
    // Use DELETE instead of TRUNCATE for TimescaleDB hypertables
    sqlx::query("DELETE FROM raw.events")
        .execute(&pool)
        .await?;
    
    Ok(pool)
}
```

**After** (1 line):
```rust
let pool = common::create_test_db_pool().await?;
```

### 2. Replace Manual Event Insertion

**Before** (18 lines):
```rust
async fn insert_test_event_simple(pool: &PgPool, event: &RawEvent) -> Result<Ulid> {
    let record = sqlx::query!(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload, ts_orig, ingestor_version)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id::uuid as "id!"
        "#,
        event.source,
        event.event_type,
        event.host,
        event.payload,
        event.ts_orig,
        event.ingestor_version
    ).fetch_one(pool).await?;
    
    Ok(Ulid::from_uuid(record.id))
}
```

**After** (1 line):
```rust
let id = common::insert_test_event(pool, &event).await?;
```

### 3. Use EventScenarioBuilder for Test Scenarios

**Before** (150+ lines per test):
```rust
#[tokio::test]
async fn test_atuin_event_generation() -> Result<()> {
    let pool = setup_test_db().await?;
    let temp_dir = TempDir::new()?;
    
    // Manual setup of test data...
    // Manual event creation...
    // Manual verification...
}
```

**After** (20 lines):
```rust
#[tokio::test]
async fn test_atuin_event_generation() -> Result<()> {
    let pool = common::create_test_db_pool().await?;
    
    AtuinTestScenario::new()
        .with_commands(vec![
            ("ls -la", 0, 150),
            ("git status", 0, 200),
            ("cargo build", 0, 5000),
        ])
        .with_watermark_checking(true)
        .execute(&pool)
        .await?
}
```

### 4. Create Domain-Specific Test Builder

```rust
pub struct AtuinTestScenario {
    commands: Vec<(String, i32, i64)>, // (command, exit_code, duration_ms)
    check_watermark: bool,
    worker_count: usize,
}

impl AtuinTestScenario {
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
            check_watermark: true,
            worker_count: 1,
        }
    }
    
    pub fn with_commands(mut self, commands: Vec<(&str, i32, i64)>) -> Self {
        self.commands = commands.into_iter()
            .map(|(cmd, exit, dur)| (cmd.to_string(), exit, dur))
            .collect();
        self
    }
    
    pub async fn execute(self, pool: &PgPool) -> Result<TestResult> {
        // 1. Create test SQLite database with commands
        let atuin_db = self.create_test_atuin_db()?;
        
        // 2. Run AtuinDbReader 
        let events = self.run_reader(&atuin_db).await?;
        
        // 3. Insert events and verify
        let results = scenario_builders::EventScenarioBuilder::new()
            .with_events(events)
            .with_validation(|e| e.source == "shell")
            .execute(pool)
            .await?;
            
        // 4. Check watermark if enabled
        if self.check_watermark {
            self.verify_watermark(pool).await?;
        }
        
        Ok(results)
    }
}
```

### 5. Parameterized Test Cases

**Before** (50+ lines per edge case):
```rust
#[tokio::test]
async fn test_empty_command_handling() -> Result<()> {
    // Full test setup for one edge case
}

#[tokio::test] 
async fn test_unicode_command_handling() -> Result<()> {
    // Full test setup for another edge case
}
```

**After** (15 lines total):
```rust
#[tokio::test]
async fn test_atuin_edge_cases() -> Result<()> {
    let test_cases = vec![
        ("empty command", "", true),
        ("unicode command", "echo '你好世界'", true),
        ("very long command", &"x".repeat(10000), true),
        ("special chars", "rm -rf / --no-preserve-root", true),
        ("null bytes", "echo \0", false),
    ];
    
    parameterized::test_atuin_commands(test_cases).await
}
```

### 6. Timing Optimization

**Before**:
```rust
tokio::time::sleep(Duration::from_millis(100)).await;
tokio::time::sleep(Duration::from_millis(500)).await; 
tokio::time::sleep(Duration::from_secs(1)).await;
```

**After**:
```rust
timing::adaptive_delay(timing::DelayPurpose::EventPropagation).await;
timing::adaptive_delay(timing::DelayPurpose::DatabaseSync).await;
timing::adaptive_delay(timing::DelayPurpose::WorkerStartup).await;
```

## Conversion Results

### Metrics
- **Lines of code**: 1123 → ~400 (64% reduction)
- **Test functions**: 15 → 6 (better organization)
- **Duplication**: High → Minimal
- **Readability**: Complex → Clear intent
- **Maintainability**: Hard → Easy

### Benefits
1. **Clearer test intent** - What is being tested is obvious
2. **Reusable patterns** - AtuinTestScenario can be used across tests
3. **Better error messages** - Utilities provide context
4. **Faster execution** - Optimized timing and setup
5. **Easier to extend** - Add new test cases without duplication

### Coverage Verification
```rust
#[test]
fn verify_atuin_test_coverage() {
    let coverage = CoverageTracker::get_coverage_report();
    
    // Ensure we still test all original scenarios
    assert!(coverage.event_types.contains(&("shell", "command.executed_atuin")));
    assert!(coverage.edge_cases["atuin"].len() >= 10);
    assert!(coverage.concurrency_scenarios.contains("concurrent_atuin_reads"));
    assert!(coverage.error_conditions.contains("malformed_atuin_db"));
}
```

## Reusable Conversion Pattern

This conversion demonstrates patterns applicable to all large test files:

1. **Extract common setup** → Use test utilities
2. **Identify test scenarios** → Create domain-specific builders  
3. **Group similar tests** → Use parameterized testing
4. **Remove manual loops** → Use bulk operations
5. **Optimize timing** → Use adaptive delays
6. **Track coverage** → Ensure nothing is lost

The same approach can be applied to:
- `operational_scenarios_test.rs` → Create `OperationalScenarioBuilder`
- `work_queue_algorithm_test.rs` → Create `WorkQueueTestScenario`
- All other large test files following similar patterns