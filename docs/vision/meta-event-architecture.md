# Meta-Event Architecture Vision

> **Status**: Alternative approach, archived for future reference.
> This document explores collapsing UX into DX (users configure via events).
> See `aspirational-sdk-features.md` for the active vision (holographic DX).

## The Core Insight

The current aspirational-sdk-features.md is fundamentally about **making Rust development easier**:
- streamlined node traits (reduce boilerplate)
- Aggregation Runner (abstract state management)
- sx tool (better CLI)
- Wasm plugins (hot-reload)

But this maintains a sharp divide: **Users** query events, **Developers** write Rust.

The radical alternative: **Collapse this boundary entirely.**

---

## Paradigm Shift: Using IS Extending

### Inspiration from Other Systems

| System | Insight |
|--------|---------|
| **Emacs** | Configuration IS programming. Users become developers naturally through Lisp. |
| **Smalltalk** | Always running, always modifiable. No compile/deploy cycle. |
| **Unix pipes** | Using tools = composing tools = building new tools. |
| **Excel** | Formulas are the extension mechanism. Same interface for use and programming. |
| **Nix** | Configuration IS the system. Declarative expression = deployment. |

### What This Means for Sinex

**Current Model:**
```
USER ──> CLI ──> Gateway ──> Query DB
DEVELOPER ──> Write Rust ──> Compile ──> Deploy ──> Node runs
```

**Seamless Model:**
```
ANYONE ──> emit event ──> system reacts ──> more events
           (rules, transforms, aggregations are all events)
```

The key: **Events are the universal interface for both using AND extending the system.**

---

## The Meta-Event Architecture

### Everything Becomes an Event

Instead of compiled Rust nodes for 90% of use cases, users emit **meta-events** that configure system behavior:

```yaml
# Define a new rule (this IS the programming interface)
emit: system.rule.defined
payload:
  name: git-activity-detector
  when: terminal.command.executed
  where: "payload.command STARTS_WITH 'git'"
  then:
    emit: git.activity.detected
    payload:
      command: "{{ input.payload.command }}"
      repo: "{{ input.payload.cwd }}"
```

This event:
1. Gets persisted to the event stream (with provenance)
2. Gets picked up by a Rule Engine node
3. Creates a new reactive transformation
4. All future matching events trigger the rule

**No Rust. No compilation. No deployment. Just events.**

### Meta-Event Types

| Meta-Event | Purpose |
|------------|---------|
| `system.rule.defined` | Pattern → Action transformation |
| `system.transform.defined` | Field projection/enrichment |
| `system.aggregate.defined` | Continuous materialized view |
| `system.schema.defined` | Declare new event type |
| `system.rule.disabled` | Pause a rule |
| `system.rule.deleted` | Remove a rule |

### Query-as-Node Pattern

A saved query becomes a continuous node:

```bash
sx query --save "health-monitor" \
  --sql "SELECT node_id, COUNT(*) as heartbeats
         FROM events
         WHERE event_type = 'heartbeat'
         GROUP BY node_id
         HAVING COUNT(*) < 3 IN LAST 5 MINUTES"
```

This emits:
```yaml
emit: system.aggregate.defined
payload:
  name: health-monitor
  query: "SELECT ..."
  output_type: health.nodes.unhealthy
  emit_interval: 60s
```

Now it runs continuously. Users define nodes by querying.

---

## Expression Language: The Bridge

### Requirements
- **Safe**: No arbitrary code execution, no SQL injection
- **Expressive**: Projections, predicates, basic transformations
- **Familiar**: JSON-path-like syntax, SQL-like predicates

### Proposed Syntax (CEL-inspired)

**Predicates (where clauses):**
```
payload.command STARTS_WITH "git"
payload.exit_code != 0
payload.duration > 5s
source IN ["terminal", "shell.atuin"]
```

**Projections (field selection/transformation):**
```
{
  command: input.payload.command,
  cwd: input.payload.working_directory,
  timestamp: input.ts_orig,
  user: input.host
}
```

**Built-in Functions:**
```
STARTS_WITH, ENDS_WITH, CONTAINS, MATCHES (regex)
LOWER, UPPER, TRIM, SPLIT
NOW, DURATION, DATE_TRUNC
COALESCE, IF
```

### Safety Model

1. **No arbitrary execution**: Expression language is interpreted, not compiled
2. **Capability-scoped**: Rules can only emit to whitelisted subjects
3. **Resource limits**: Max rules per user, max complexity score
4. **Audit trail**: Every rule activation is an event with provenance

---

## The Recipe Pattern

### YAML/TOML Files as First-Class Citizens

Instead of Rust code, users write **recipes**:

```yaml
# ~/.config/sinex/recipes/git-enricher.recipe.yaml
name: git-enricher
version: 1.0.0
description: Enrich terminal commands with git context

subscribe:
  - terminal.command.executed

filter:
  payload.command: { starts_with: "git" }

transform:
  command: input.payload.command
  subcommand: "{{ EXTRACT(input.payload.command, 'git (\\w+)') }}"
  repo_path: input.payload.cwd
  timestamp: input.ts_orig

emit:
  type: git.command.executed

# Provenance automatically set to Synthesis from input event
```

### Recipe Lifecycle

1. User creates `*.recipe.yaml` file
2. `sx recipe sync` emits `system.recipe.defined` event
3. Recipe Engine node processes the event
4. Rule becomes active immediately
5. Changes to file → `sx recipe sync` → new version event
6. Old version disabled, new version active (atomic cutover)

---

## Architecture Layers

### Layer 1: Core (Compiled Rust)
- Event ingestion (ingestd)
- Event persistence (PostgreSQL)
- NATS JetStream transport
- Schema validation
- **Rule Engine node** (new - interprets meta-events)
- **Recipe Loader** (new - watches recipe files, emits meta-events)

### Layer 2: Meta-Event Processors (Compiled but Generic)
- `RuleEngine`: Interprets `system.rule.defined` events
- `AggregateEngine`: Interprets `system.aggregate.defined` events
- `TransformEngine`: Interprets `system.transform.defined` events

### Layer 3: User-Defined (Zero Compilation)
- Rules (pattern → action)
- Transforms (projection/enrichment)
- Aggregates (continuous queries)
- Recipes (YAML files)

### Layer 4: Escape Hatch (When Needed)
- Wasm plugins for complex logic
- Rust nodes for performance-critical processing
- But these should be <10% of use cases

---

## Comparison: Current vs. Seamless

| Aspect | Current Aspirational | Seamless UX=DX |
|--------|---------------------|----------------|
| **New transformation** | Write Rust, compile, deploy | Emit `system.transform.defined` event |
| **New aggregation** | Implement Aggregator trait | Emit `system.aggregate.defined` event |
| **Filter events** | Write Rust predicate | Declare filter in rule YAML |
| **Hot reload** | Wasm plugins | Native (rules are events) |
| **Learning curve** | Learn Rust + SDK | Learn expression syntax |
| **Time to first rule** | Hours (compile, deploy) | Minutes (emit event) |
| **Debugging** | Logs, traces, debugger | Events (rules are events, activations are events) |

---

## Key Design Decisions

1. **Expression Language Choice**
   - Custom minimal DSL?
   - CEL (Common Expression Language)?
   - SQL subset?
   - JMESPath for projections?

2. **Rule Storage**
   - In event stream (replay to rebuild)?
   - Separate KV bucket (faster lookup)?
   - Both (events for audit, KV for runtime)?

3. **Capability Model**
   - Can any rule emit any event type?
   - Namespace isolation?
   - Rate limits?

4. **Aggregation State**
   - In-memory only (rebuild on restart)?
   - Persisted to KV?
   - Periodic snapshots to event stream?

5. **Recipe Format**
   - YAML only?
   - TOML support?
   - JSON (programmatic creation)?

---

## Success Criteria

1. A user can define a new event transformation in <5 minutes
2. No Rust compilation required for common use cases
3. Rules are events (auditable, replayable, versionable)
4. Expression language is safe (no injection, bounded resources)
5. Seamless upgrade path (rules can call Wasm for complex cases)
