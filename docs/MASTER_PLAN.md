# Sinex Service Layer Testing & Migration Master Plan

## Overview
This document outlines the phased approach to complete the Sinex service layer refactoring, with a focus on atomic, valuable deliverables. Each phase can be committed independently and adds immediate value.

## Guiding Principles
1. **Value First**: Most important work comes first
2. **Atomic Commits**: Each phase is independently valuable
3. **Stop Anytime**: System is always better than when we started
4. **Test Before Migrate**: Build confidence before big changes

## Phase Breakdown

### Phase 1: Service Container Foundation ⭐ CRITICAL PATH
**Time**: 2 hours  
**Deliverable**: `test/unit/service_container_test.rs`
- Test ServiceContainer initialization
- Test dependency injection
- Test service retrieval
- Validate all services wire together correctly

**Commit**: `test: add ServiceContainer initialization and DI tests`

### Phase 2: Core Service Tests
**Time**: 4 hours (can parallelize)  
**Deliverables**: Individual test files per service

#### 2a. SearchService Tests (1 hour)
- `test/unit/search_service_test.rs`
- SQL query building (no injection!)
- Result formatting
- **Commit**: `test: add SearchService query building tests`

#### 2b. AnalyticsService Tests (1 hour)
- `test/unit/analytics_service_test.rs`
- Time range queries
- Aggregation logic
- **Commit**: `test: add AnalyticsService aggregation tests`

#### 2c. PkmService Tests (1 hour)
- `test/unit/pkm_service_test.rs`
- Entity CRUD operations
- Artifact management
- **Commit**: `test: add PkmService entity/artifact tests`

#### 2d. ContentService Tests (1 hour)
- `test/unit/content_service_test.rs`
- Blob operations via BlobManager
- Content retrieval
- **Commit**: `test: add ContentService blob operation tests`

### Phase 3: RPC Integration Tests ⭐ ENABLES CLI
**Time**: 2 hours  
**Deliverable**: `test/integration/rpc_handlers_test.rs`
- Test JSON-RPC request/response cycle
- Test all service method handlers
- Validate error handling

**Commit**: `test: add RPC handler integration tests`

### Phase 4: Exo.py RPC Migration ⭐ PROVES IT ALL WORKS
**Time**: 3 hours  
**Steps**:
1. Create RPC client module in exo.py
2. Migrate commands: query, stream, tail, stats
3. Update CLI tests with RPC mocks

**Commit**: `refactor: migrate exo.py CLI to use sinex-host RPC`

**MILESTONE**: System is fully functional with new architecture!

---

## Robustness Phases (High Value, Optional Order)

### Phase 5: Property-Based Event Fuzzing
**Time**: 3 hours  
**Deliverable**: `test/property/event_model_fuzzing_test.rs`
- Generate weird but valid event payloads
- Test with extreme values, unicode, empty strings
- Assert: No panics in collector/worker pipeline

**Commit**: `test: add property-based fuzzing for event robustness`

### Phase 6: Temporal Chaos Tests
**Time**: 2 hours  
**Deliverable**: `test/system/temporal_chaos_test.rs`
- Thundering herd: 1000+ events in 100ms
- Worker idempotency: duplicate work items
- Event ordering violations

**Commit**: `test: add temporal chaos and idempotency tests`

### Phase 7: Missing Refactoring Items
**Time**: 2 hours
- Implement `#[with_context]` error macro
- Complete database rename (raw → sinex_db)
- Finish typed event pipeline

**Commit**: `refactor: implement with_context macro and complete db rename`

---

## Optional Enhancement Phases

### Phase 8: Blob Storage Tests
**Time**: 2 hours
- BlobManager deduplication
- Git-annex integration
- Content verification

### Phase 9: Automaton Worker Tests
**Time**: 1 hour
- Work claim/process/complete cycle
- Placeholder processor logic

### Phase 10: Preflight Enhancement
**Time**: 1 hour
- Additional verification phases
- Phase isolation tests

---

## Task Agent Strategy

To maintain context across sessions:

1. **"Service Test Implementation"** (Phases 1-3)
   - Knows test patterns, DB constraints, service signatures

2. **"CLI Migration"** (Phase 4)
   - Knows exo.py structure, RPC protocol

3. **"Chaos Testing"** (Phases 5-6)
   - Knows proptest patterns, timing scenarios

4. **"Refactoring Cleanup"** (Phase 7)
   - Knows macro patterns, schema migration

---

## Success Metrics

After Phase 4:
- ✅ Service layer has test coverage
- ✅ RPC interface is tested
- ✅ CLI uses services, not direct DB
- ✅ Can stop here with confidence!

After Phase 7:
- ✅ System is robust against weird inputs
- ✅ Handles real-world timing chaos
- ✅ Codebase is fully consistent

## Next Step
Begin Phase 1: Service Container Foundation tests