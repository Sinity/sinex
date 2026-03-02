# SQL-as-Automaton: The `.flow` File Concept

Addressing the "SQL-as-Automata" concept: Even if not the primary architectural north star, it remains a highly potent theoretical mechanism for simplifying the system.

Here is exactly how a system relying on `.flow` files handles schemas, automata behavior, and historical replays.

---

## 1. What is a `.flow` file?

A `.flow` file (borrowing concepts from data engineering tools like `dbt`) would be a declarative text file tracked in your git repository. It defines exactly how an output event is synthesized from an input event.

Instead of writing a 150-line Rust node handling NATS channels and graceful shutdown, an automaton becomes a literal SQL block.

**Example:** `terminal-canonicalizer.flow.sql`

```sql
---
name: "terminal-command-canonicalizer"
input_event_type: "command.executed"
output_event_type: "command.canonical"
---

INSERT INTO core.events (
    source, event_type, payload, source_event_ids, ts_orig
)
SELECT 
    'automaton.terminal_canonicalizer',
    'command.canonical',
    jsonb_build_object(
        'command', e.payload->>'command',
        'working_directory', COALESCE(e.payload->>'working_directory', ''),
        'exit_code', (COALESCE(e.payload->>'exit_code', '0'))::int,
        'duration_ms', (COALESCE(e.payload->>'duration_ms', '0'))::bigint,
        'user', COALESCE(e.payload->>'user', ''),
        'session_id', COALESCE(e.payload->>'session_id', ''),
        'environment_hash', COALESCE(e.payload->>'environment_hash', ''),
        'start_time', e.ts_orig,
        'end_time', COALESCE((e.payload->>'end_time')::timestamp, e.ts_orig),
        'source_events', jsonb_build_array(e.id),
        'enrichment_history', '[]'::jsonb
    ),
    ARRAY[e.id],
    e.ts_orig
FROM core.events e
-- Condition to fire the "Automaton"
WHERE e.event_type = 'command.executed'
  AND e.source IN ('shell.kitty', 'shell.atuin', 'shell.history.bash', 'shell.history.zsh', 'shell.history.fish')
  AND TRIM(e.payload->>'command') != '';
```

---

## 2. What about Schemas?

If an automaton is executing raw SQL to dump new events into `core.events`, how do we prevent the schema from decaying into untyped chaos?

In Sinex today, schemas are strictly enforced via the Rust types. In an SQL model, they are enforced by a database trigger attached to the `sinex-schema` crate infrastructure:

1. **Boot time:** When `sinex-schema-sync` starts, it reads `terminal-canonicalizer.flow.sql`.
2. **Schema Registration:** It registers the `command.canonical` JSON Schema into the `core.payload_schemas` table.
3. **Trigger Validation:** A `BEFORE INSERT ON core.events` trigger evaluates the SQL output. If `terminal-canonicalizer.flow.sql` attempts to insert an `exit_code` as a String when the schema demands a JSON Number, the PostgreSQL trigger aborts the write payload via a strict `json_matches_schema()` postgres extension evaluation.

The schema boundary remains absolutely impenetrable, but it lives at the DB boundary rather than inside the Rust binary.

---

## 3. How does Replaying work?

You asked: *"Replay is about events, right? How do we do that in sql?"*

When an automaton is built in Rust, "replaying" events means extracting old rows from `core.events`, feeding them into NATS JetStream, passing them into the Rust binary, and trusting the Rust binary to calculate and emit the synthesized output events back onto NATS.

When an automaton is a declarative `.flow` file, replaying skips NATS entirely.

To run a replay of the Terminal Canonicalizer from Jan 1st to Jan 5th, your `ReplayStateMachine` executes:

```sql
DO $$ 
BEGIN 
  -- The core automaton logic runs locally on the DB server:
  INSERT INTO core.events (...)
  SELECT ... FROM core.events e
  WHERE e.ts_orig >= '2026-01-01' AND e.ts_orig <= '2026-01-05'
    AND e.event_type = 'command.executed' ...
END $$;
```

It processes the events inside PostgreSQL using set-based operations.

1. **Speed:** It runs orders of magnitude faster because JSON payloads don't cross the network layer into a Rust daemon.
2. **ACID Safety:** You can effortlessly chunk the time window to meet your `ReplayCheckpoint` limits, meaning replaying 50,000 synthesized terminal commands resolves with 100% data consistency.
