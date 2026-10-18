# Shell sessions and recall

The `session` automaton groups canonical shell commands into sessions, and `recall last` returns the latest session, optionally for one project. Enable it with `automata = ["session"]`, which requires indexed search ([search.md](search.md)).

## Grouping rule

The profile is `shell-session-v1-warsaw04-gap300`:

- Logical days run from 04:00 Europe/Warsaw time.
- Each canonical command contributes one recorded span; an unknown end is a point.
- A new session starts when the next command begins more than 300 seconds after the latest recorded end in the current session. Overlapping and nested spans stay in the same session.

Sessions are `activity.session.boundary` events citing their canonical commands, so `show` and `trace` lead back to the original source records. A session whose bounds or members change replaces the old reading. Raw queries for `kind = activity.session.boundary` or `source = session` return `partial` when the automaton is disabled.

The gap rule classifies recorded spans. It says nothing about attention or real task boundaries; there are no focus or AFK sources yet.

## Recall

```sh
sinexctl --endpoint URL --token-file TOKEN recall last --project tide --json
sinexctl --endpoint URL --token-file TOKEN query 'recall last where project = tide' --json
```

MCP `sinex_recall` takes optional `project` and `timeout_ms`; `sinex_query` accepts the same query text.

With a project, recall picks the latest session that has at least one direct member in that project, then returns every eligible member as context. Each row has `project_evidence`: `known` with its project, `unconfigured` (`no_configured_root`), or `unknown` (`missing_cwd`, `invalid_cwd` or `ambiguous_roots`). `attribution` summarizes the direct members. `newer_unresolved_candidates` lists newer sessions that have members with unknown project evidence. A configured project with no match returns `session: null` and empty `rows`; an unknown project name is `bad_query`. Without a project, recall returns the latest complete session.

Limits:

- Recall inspects at most 128 sessions and 8192 direct members; beyond that it returns `partial`.
- A recalled session returns at most 100 command rows, with no cursor; larger sessions return `partial` and the caller should use `query`, `show` and `trace`.
- A session holds at most 8192 canonical parents; larger groups fail rather than being truncated.

Recall waits up to 500 ms for the indexed projection to catch up, within the query deadline, and refuses at once while session or grouping work is pending. `complete` is false while any source's latest read is still `reading` or has record errors.

## Maintenance

The writer marks logical days dirty when canonical commands are admitted or archived; each dirty row has an inclusive `through_day` bound so changes spanning several days are followed. The background scheduler loads the day's commands in 16-row turns into temporary tables, publishes at most two sessions per turn and archives replaced sessions in 16-row turns. Statements have a two-second deadline and turns three seconds; deadline failures retry after 250 ms, and other failures leave the day unavailable. `status` shows dirty count, day, phase and failure.

Schema change 3 adds the session tables and seeds dirty days from existing canonical commands ([schema.md](schema.md)).
