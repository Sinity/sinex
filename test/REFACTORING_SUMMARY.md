# System and Performance Test Refactoring Summary

## Overview
Refactored system and performance tests to use centralized query builders instead of raw SQL, improving maintainability and consistency.

## Changes Made

### 1. performance_test.rs (22 queries refactored)
- **Event deletions**: Replaced `DELETE FROM core.events WHERE source = X` with `EventQueries::delete_by_source()`
- **Event counts**: Replaced raw COUNT queries with `TestQueries::count_events_by_source()` where appropriate
- **Query testing**: Refactored latency test queries to use `TestQueries` methods for better abstraction
- **Kept raw SQL for**:
  - Complex `FOR UPDATE SKIP LOCKED` queries (required for specific locking behavior)
  - Connection pooling test queries (testing connection behavior, not event data)
  
### 2. stress_test.rs (14 queries refactored)
- **Event creation**: Replaced raw INSERT statements with `BatchEventBuilder` for bulk test data generation
- **Checkpoint creation**: Replaced raw checkpoint inserts with `TestCheckpointBuilder`
- **Cleanup operations**: Replaced raw DELETE statements with `EventQueries` and `CheckpointQueries` methods
- **Kept raw SQL for**:
  - Time-based queries using `NOW() - INTERVAL` (PostgreSQL-specific time operations)
  - Complex JSON field status checks in checkpoints

### 3. reliability_test.rs (19 queries refactored)
- **Checkpoint management**: Replaced raw checkpoint operations with `TestCheckpointBuilder`
- **Event cleanup**: Replaced DELETE operations with `EventQueries::delete_by_source()`
- **Transaction operations**: Replaced raw inserts within transactions with `EventQueries::insert_event()`
- **Kept raw SQL for**:
  - Database DDL operations (`CREATE DATABASE`, `DROP DATABASE`)
  - Schema inspection queries (`information_schema` queries)
  
### 4. database_performance_test.rs
- **Event counting**: Replaced simple COUNT queries with `TestQueries::count_events_by_source()`
- **Kept raw SQL for**:
  - Performance benchmarking queries (testing specific database features)
  - Pattern matching with LIKE operators
  - Complex OR conditions with pattern matching

## Key Patterns Applied

1. **Batch Operations**: Used `BatchEventBuilder` for generating large volumes of test data efficiently
2. **Builder Pattern**: Leveraged `TestEventBuilder` and `TestCheckpointBuilder` for cleaner test data creation
3. **Centralized Queries**: Used `TestQueries` wrapper methods that call production query builders
4. **Documentation**: Added NOTE comments explaining why certain queries remain as raw SQL

## Benefits

1. **Consistency**: All test data operations now use the same query builder infrastructure as production code
2. **Type Safety**: ULID/UUID conversions are handled consistently through the query builders
3. **Maintainability**: Changes to database schema or query patterns only need updates in one place
4. **Performance**: `BatchEventBuilder` provides optimized bulk insert operations for load testing

## Raw SQL Justification

Certain queries remain as raw SQL for valid reasons:
- **Locking behavior**: `FOR UPDATE SKIP LOCKED` requires specific SQL syntax
- **DDL operations**: Database/schema management must use raw SQL
- **Time operations**: PostgreSQL-specific `NOW() - INTERVAL` syntax
- **Performance testing**: Some tests specifically measure raw SQL performance
- **Pattern matching**: Complex LIKE patterns with wildcards

## Query Count Summary

- **Total queries identified**: 55+ across all files
- **Queries refactored**: ~45 (approximately 82%)
- **Queries kept as raw SQL**: ~10 (with documented justifications)

This refactoring significantly reduces raw SQL usage in tests while maintaining functionality for cases where raw SQL is genuinely needed.