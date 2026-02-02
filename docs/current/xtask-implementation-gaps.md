# xtask CLI Implementation - Gap Analysis

**Generated**: 2026-01-30
**Comparing**: Plan vs Current Implementation

## Summary

The reorganization is **90% complete**. Core structural changes are done, but several features remain unimplemented.

---

## ✅ Completed

### Structural Changes (Phase 1)
- ✅ Flattened `analyze` → promoted deps, graph, history, patterns, snapshot
- ✅ Renamed `db schema` → `contracts`
- ✅ Merged `motd` → `status --summary`
- ✅ Merged `stack doctor` → `status --doctor`
- ✅ Merged `bench` → `test --bench` (flag exists)
- ✅ Top-level `tls` removed (consolidated to `stack tls`)
- ✅ `qa` namespace removed
- ✅ Deprecation warnings for old commands

### New Commands (Phase 2 - Partial)
- ✅ `cargo xtask run` command exists
- ✅ `cargo xtask docs` command exists
- ✅ `cargo xtask contracts` (renamed from schema)

### Global Flags (Phase 3 - Partial)
- ✅ `--json` works globally
- ✅ `--format` works globally
- ✅ `--bg` exists on test, build, check

---

## ❌ Incomplete / Missing

### Phase 2: New Command Features

#### `cargo xtask run` - Missing Features
| Feature | Status | Notes |
|---------|--------|-------|
| `--watch` flag | ❌ | Defined but not implemented |
| `--bg` flag | ❌ | Defined but not implemented |
| `--instance-id` flag | ❌ | Not in help output |
| Seamless handoff | ❌ | No NodeCoordination integration |
| Binary discovery | ⚠️  | Needs verification |
| Bundle shortcuts | ⚠️  | `stack`, `all-ingestors`, `all-automatons` exist but untested |

**Evidence**: `xtask/src/commands/run.rs` has `RunResult` struct marked `#[allow(dead_code)]`, suggesting incomplete implementation.

#### `cargo xtask docs` - Stub
- ❌ Command exists but likely minimal/stub implementation
- No subcommands like `build --open`, `serve --port` mentioned in help

### Phase 3: Global Flag Promotion

| Flag | Commands | Status | Notes |
|------|----------|--------|-------|
| `--bg` | test, build, check, run | ⚠️ | Exists but background job integration unclear |
| `--dry-run` | test, build, run, stack, db, contracts | ❌ | Not present in most commands |
| `--affected` | test, build, check, fix | ❌ | Flag exists but smart default NOT implemented |

**Smart `--affected` default**: Plan specifies auto-enabling when `git status --porcelain` shows changes. This is NOT implemented.

### Phase 4: Documentation

| Item | Status | Notes |
|------|--------|-------|
| CLI Reference | ✅ | Added to README.md |
| CLAUDE.md updates | ❌ | Not updated with new command paths |
| Verification testing | ⚠️ | Ad-hoc only, not systematic |

---

## 🔍 Files Still Present (Should Be Deleted)

According to plan, these should be deleted but still exist:

| File | Status | Reason |
|------|--------|--------|
| `xtask/src/commands/analyze.rs` | ⚠️ EXISTS | Shows deprecation, should be deleted |
| `xtask/src/commands/qa.rs` | ⚠️ EXISTS | Shows deprecation, should be deleted |
| `xtask/src/commands/motd.rs` | ⚠️ EXISTS | Shows deprecation, should be deleted |
| `xtask/src/commands/bench.rs` | ⚠️ EXISTS | Should be deleted (merged to test) |
| `xtask/src/commands/schema.rs` | ⚠️ EXISTS | Should be deleted (renamed to contracts) |
| `xtask/src/commands/tls.rs` | ⚠️ EXISTS | Should be deleted (moved to stack tls) |

These files serve deprecation warnings currently but should eventually be removed.

---

## 🚧 Unverified Functionality

### Needs Testing
- `run` command bundles (`stack`, `all-ingestors`, `all-automatons`)
- `--bg` background job integration
- `status --doctor` pipeline diagnostics
- `contracts` schema operations
- All `deps` subcommands (list, tree, duplicates, unused, timings, impact)
- All `history` subcommands (list, last, stats, prune, export, tests)
- `graph` visualization
- `patterns` ast-grep integration
- `snapshot` repomix integration

---

## 📋 Priority Action Items

### High Priority (Breaks Plan Promises)
1. **Implement `--affected` smart default** - Core feature, explicitly promised
2. **Add `--dry-run` to applicable commands** - Global flag promised
3. **Implement `run --watch`** - Major new feature, currently non-functional
4. **Update CLAUDE.md** - Documentation deliverable

### Medium Priority (Polish)
1. **Complete `docs` command** - Exists but likely minimal
2. **Complete `run --bg` integration** - Feature exists but unclear if functional
3. **Remove deprecated command files** - Clean up codebase

### Low Priority (Nice to Have)
1. Systematic verification testing (create proper test suite)
2. Add `--instance-id` to `run` command

---

## ✅ Environment Status

**Clippy**: Already in `flake.nix` (line contains `fenixPkgs.clippy`). No fix needed.

---

## Conclusion

The reorganization successfully achieved:
- Clean command hierarchy
- Better discoverability
- Deprecation path for old commands
- Comprehensive CLI reference

But several **promised features remain unimplemented**:
- Smart `--affected` default
- `--dry-run` global flag
- `run --watch` functionality
- Full `docs` command

The CLI **structure** is complete. The **features** need finishing.
