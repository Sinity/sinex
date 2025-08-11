Prompt: Architectural Cohesion Audit for Sinex Codebase

Context

You are auditing the Sinex codebase for architectural "cancer" - code that violates the established doctrine, creates parallel
systems, or adds features without considering existing infrastructure. You have access to the canonical architecture document 
@docs/TARGET_canonical.md which defines the system's invariants and proper patterns. (note: canonical model might be _incomplete_, undnerspecified)

Your Mission

Systematically explore the codebase to identify and categorize architectural violations. Focus on detecting:

1. Redundant Systems: Features that duplicate existing infrastructure
2. Invariant Violations: Code that breaks the Ten Commandments (listed in TARGET_canonical.md)
3. Parallel Implementations: Multiple ways of doing the same thing
4. Orphaned Features: Functionality that doesn't integrate with the core architecture
5. Schema Drift: Database structures that don't match the canonical model (note: canonical model might be _incomplete_, undnerspecified).

Methodology

Phase 1: Map the Territory; You've entire TARGET_canonical.md in your context window now. Your task is to look around the codebase, explore, figure out how things are put together, how they relate, how they connnect. Once you have some high-level idea of what's going on, somme sort of a framework - you enter Phase 2.

Phase 2: Management of refactoring subagents. You can launch these. You can launch several at once, one after another. It is important you do use that capability when it makes sense to. You can launch 10, or frankly even 30 at once. That means that you could actually process through the codebase in parallel, and so it doesn't take ages. Ofc there are sligght issues, with subagents possibly getting into each  other's way. But they can often handle appropaitely broken down work. E.g. each scans 'their assigned' 30 files. 10 such subagents can scan through the codebase in one go. In maybe 20min not entire day. I do not have entire day to wait for such shit.

Anyway, so do note that your role from Phase 2 onwards, is in general one of a high-level overseer of refactoring subagents. You communicate with me, you delegate - if I ask you to check somethinng simple ofc you would yourself. You might also prepare strategy. But generally, bulk of the work should be subagents.

Note that it is critical you launch these subagents with proper prompts. That's why you did Phase 1 yourself. So that you undnerstand the codebase and can guide the subagents to some degree, so they don't produce further cancer. Note that our overall mission here is mostly assesment of the spread of the cancer so far. That means lots of these refactoringn subagents you'll laucnch won't necessarily be aimed at fixing the codebase. They might be aimed at analyzign the codebase in sophisticated ways. Ones that you deem potentially illuminating, for yourself! Use these as powerful tools.

Please strive to be transparent in your thoought processes and decisionmaking, always tell at least roughly why you're doing stuff and what is it, before doing it. etc.

The following are some details you might or might not find helpful, regarding the specifics of cancer. Note that these are not direct orders, these are more like "recipes" you can reuse when craftinng prompts for the subagents.


===========================================================================
For each major component, check:

A. Feature Duplication Audit
For each new feature/module:
- Ask: "What existing table/system should handle this?"
- Check operations_log scope field for similar operations
- Look for existing event types that cover the use case

Detection Patterns

Pattern 1: "Creating new table when operations_log would suffice"

// BAD: Creating replay_history table
struct ReplayHistory {
    id: Ulid,
    config: Json,
    results: Json,
    // This duplicates operations_log!
}

// GOOD: Use operations_log with proper scope
Operation {
    scope: json!({
        "operation_type": "replay",
        "processor": processor_name,
        "config": replay_config,
    }),
    // ...
}

Pattern 2: "Parallel processor implementations"

// BAD: Multiple processor traits
trait StatefulStreamProcessor { ... }
trait NatsEventBatchProcessor { ... }  // Should not exist!

// GOOD: Single unified trait
trait StatefulStreamProcessor { ... }

Pattern 3: "Direct event writes bypassing ingestd"

// BAD: Satellite writing directly
pool.events().insert(event).await?;

// GOOD: All events through ingestd
ingest_client.submit_event(event).await?;

Output Format

For each violation found, report:
VIOLATION: [Type: Redundant|Invariant|Parallel|Orphaned|Schema]
FILE: path/to/file.rs:line
DESCRIPTION: What the violation is
EXISTING_SOLUTION: What should be used instead
IMPACT: How this affects system coherence
FIX: Specific steps to resolve

Priority Scoring

- CRITICAL: Violates core invariants (e.g., single-writer, provenance XOR)
- HIGH: Creates parallel systems or duplicate state
- MEDIUM: Misuses existing infrastructure
- LOW: Naming/organization issues

Specific Areas to Investigate

1. Replay System
  - Check: crate/lib/sinex-satellite-sdk/src/replay*.rs
  - Question: Does it use operations_log or create new tracking?
2. Test Utils
  - Check: crate/lib/sinex-test-utils/
  - Question: Are there wrapper methods that duplicate repository calls?
3. Processor Implementations
  - Check: crate/satellites/*/src/
  - Question: Do they use the unified StatefulStreamProcessor?
4. Database Migrations
  - Check: schema, Recent migrations
  - Question: Do new tables duplicate existing functionality?
5. Event Creation Paths
  - Grep for: insert.*events, EventRepository::insert
  - Question: Is ingestd the only writer?
6. EVERYTHIG ELSE.

Example Execution

# Start with schema check
psql $DATABASE_URL -c "\\dt core.*" | grep -v "events\|operations_log\|..."

# Check for direct event writes
rg "pool\.events\(\)\.insert" --type rust

# Look for multiple processor traits
rg "trait.*Processor" --type rust

# Find replay-related code
rg "replay" --type rust | grep -i "history\|track\|record"

Success Criteria

The audit is complete when you can confidently state:
1. All event writes go through ingestd
2. No duplicate state tracking exists
3. All processors use the unified trait
4. operations_log is used for operation tracking
5. The provenance XOR is enforced
6. No orphaned features exist
7. In geenral, there is no duplicate code, no orphaned code, everything ties together cohesively etc.

