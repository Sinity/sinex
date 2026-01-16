# Parallel Execution Setup: Phase 1 & 2

**Strategy:** 3 background Task agents + git worktrees + hybrid compilation

---

## Infrastructure Setup

### 1. Git Worktrees (Parallel Branch Development)

```bash
cd /realm/project/sinex

# Stream A: KV Coordination (1 week)
git worktree add ../sinex-stream-a -b feat/kv-coordination-completion

# Stream B: Gateway RPC (2 weeks)
git worktree add ../sinex-stream-b -b feat/gateway-rpc-completeness

# Stream C: Desktop Native APIs (1-2 weeks)
git worktree add ../sinex-stream-c -b feat/desktop-native-apis

# Verify
git worktree list
```

**Result:** 3 independent working directories sharing git history

---

## Agent Coordination Strategy

### Phase 1: Launch Background Agents (Week 1-2)

```python
# Pseudo-code for agent spawn
Task(
    description="Stream A: KV Coordination",
    prompt="""
    Work in /realm/project/sinex-stream-a

    Complete tasks A1-A4 from plan:
    - A1: Schema cache fallback logic
    - A2: Migrate legacy lifecycle test
    - A3: Gateway coordination RPC endpoints
    - A4: Drop legacy tables

    COMPILATION RULES:
    - Only build affected crates: sinex-node-sdk, sinex-core, sinex-gateway
    - Use: cargo build -p sinex-node-sdk -p sinex-core -p sinex-gateway
    - Skip full workspace build until ready to merge

    TEST RULES:
    - Coordinate test runs via lock file: /tmp/sinex-test.lock
    - Before running tests: flock /tmp/sinex-test.lock -c "cargo nextest run ..."
    - This serializes tests to avoid DB conflicts

    Report completion status and compilation.log results.
    """,
    subagent_type="general-purpose",
    run_in_background=True,
    model="sonnet"
)

# Similar for B and C...
```

### Compilation Coordination

**Hybrid Strategy:**
- Each agent compiles only affected crates: `-p crate-name`
- Avoids full workspace lock contention
- Final integration build on merge

**Lock File System:**
```bash
# Agents use flock for compilation serialization
flock /tmp/sinex-build.lock -c "cargo build -p sinex-node-sdk"

# Tests MUST be serialized (DB conflicts)
flock /tmp/sinex-test.lock -c "cargo nextest run --package sinex-node-sdk"
```

---

## Agent Task Assignment

### Stream A Agent (Background)
**Worktree:** `/realm/project/sinex-stream-a`
**Crates:** `sinex-node-sdk`, `sinex-core`, `sinex-gateway`
**Duration:** 1 week (20-24 hours work)

**Tasks:**
1. A1: Modify `schema_validator.rs` (4-6h)
2. A2: Update `node_lifecycle_test.rs` (2-3h)
3. A3: Create `handlers/coordination.rs` (8-12h)
4. A4: Write migration, drop tables (2-3h)

**Build command:** `cargo build -p sinex-node-sdk -p sinex-core -p sinex-gateway`

---

### Stream B Agent (Background)
**Worktree:** `/realm/project/sinex-stream-b`
**Crates:** `sinex-gateway`, `sinex-node-sdk` (for DLQ), `sinex-core`
**Duration:** 2 weeks (32-40 hours work)

**Tasks:**
1. B1: Create `handlers/dlq.rs` (8-16h)
2. B2: Create `handlers/nodes.rs` (16-24h)
3. B3: Create `handlers/ops.rs` (16-24h)
4. B4: Create `handlers/audit.rs` (8h)
5. B5: Blob relay implementation (8h)

**Build command:** `cargo build -p sinex-gateway`

---

### Stream C Agent (Background)
**Worktree:** `/realm/project/sinex-stream-c`
**Crates:** `sinex-desktop-node`
**Duration:** 1-2 weeks (16-24 hours work)

**Tasks:**
1. C1: Modify `clipboard.rs`, add `arboard` dep (24-32h)
2. C2: Modify `window_manager.rs` (16-24h)
3. C3: Add health checks (8h)

**Build command:** `cargo build -p sinex-desktop-node`

---

## Execution Timeline

### Week 1: Parallel Start (3 agents)

**Day 1:**
```bash
# Setup worktrees
bash setup-worktrees.sh

# Launch agents
claude-code "Launch Stream A agent per parallel-execution-setup.md"
claude-code "Launch Stream B agent per parallel-execution-setup.md"
claude-code "Launch Stream C agent per parallel-execution-setup.md"

# Monitor progress
watch -n 60 "git worktree list && git branch -vv"
```

**Day 2-5:**
- Agents work in parallel
- Check agent status via TaskOutput
- Each agent compiles only its crates (no contention)
- Tests run serially via flock (no DB conflicts)

**Day 5-7:**
- Stream A completes (~20-24h work over 5 days with pauses)
- Stream C likely completes (~16-24h work)
- Stream B continues (longer timeline)

### Week 2: Stream B Continues + Integration

**Day 8-14:**
- Stream B completes remaining tasks
- Integration testing of A + C merges
- Resolve any conflicts

---

## Resource Management

### Compilation Lock Strategy

**Problem:** 3 agents building simultaneously = 3x rustc processes
**Solution:** Flock-based serialization

```bash
# In each agent's build commands:
flock /tmp/sinex-build.lock -c "cargo build -p $CRATE_NAME 2>&1 | tee compilation.log"
```

This ensures:
- Only 1 agent compiles at a time
- Others wait their turn
- No redundant compilation
- Shared build cache in `target/` (if same filesystem)

### Test Serialization

**Problem:** Tests hit same PostgreSQL database
**Solution:** MANDATORY flock on all test runs

```bash
# EVERY test run must use this pattern:
flock /tmp/sinex-test.lock -c "cargo nextest run --package $PKG 2>&1 | tee test.log"
```

This prevents:
- Database lock contention
- Race conditions in DB state
- Test flakiness

### Worktree Considerations

**Shared:**
- `.git/` directory (history, refs, objects)
- Build cache: `target/` (if on same FS)

**Independent:**
- Working directory files
- Branch state
- Uncommitted changes

**Cleanup After Merge:**
```bash
# After merging a stream
git worktree remove ../sinex-stream-a
git branch -d feat/kv-coordination-completion
```

---

## Agent Monitoring & Control

### Check Agent Status

```bash
# List running background tasks
/tasks

# Get output from specific agent
TaskOutput(task_id="<agent-id>", block=false)
```

### Integration Checkpoints

**After Stream A completes:**
```bash
cd /realm/project/sinex-stream-a
flock /tmp/sinex-build.lock -c "cargo build --workspace 2>&1 | tee full-build.log"
flock /tmp/sinex-test.lock -c "cargo xtask test --profile reliable 2>&1 | tee full-test.log"

# If green:
cd /realm/project/sinex
git merge feat/kv-coordination-completion
git worktree remove ../sinex-stream-a
```

**Repeat for C, then B**

---

## Fallback Plan

If multi-agent approach causes issues:

### Option 1: Sequential Batching
- Work on Stream A fully (1 week)
- Then Stream C (1-2 weeks)
- Then Stream B (2 weeks)
- Total: 4-5 weeks (slower but safer)

### Option 2: Manual Coordination
- Set up worktrees
- User specifies which stream to work on each session
- Agent switches between worktrees as directed

---

## Success Criteria

- [ ] 3 worktrees created and functional
- [ ] 3 background agents launched
- [ ] Flock coordination prevents build/test conflicts
- [ ] Stream A completes in Week 1
- [ ] Stream C completes in Week 1-2
- [ ] Stream B completes in Week 2
- [ ] All streams merge cleanly
- [ ] Full workspace build succeeds after merge

**Timeline:** 2 weeks vs 4-5 weeks sequential = **50% time savings**
