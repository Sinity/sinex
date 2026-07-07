---
created: "2026-06-30T03:10:00+02:00"
purpose: "Assimilate /realm/inbox/download/conductor-sinex.md into the active Sinex devloop state"
status: "active"
project: "sinex"
---

# Conductor Sinex Assimilation

## Source

Read `/realm/inbox/download/conductor-sinex.md` on 2026-06-30.

## Understanding

The conductor role is loop-process ownership, not one implementation lane. The
operator wants rapid Sinex capability growth that proves value through real,
inspectable artifacts on live/local data: a skeptical reader should be able to
see that Sinex helped an agent or operator reconstruct work, recover context,
query activity, or widen useful machine/personal context better than the
status quo.

Dogfooding is instrumental. The goal is not "use Sinex on itself" as ritual,
and not defect reduction for its own sake; hardening, refactoring, query work,
runtime repair, and dev tooling repair are justified when they unblock a
demonstrable capability slice.

The central shape remains the EvidenceWindow / ContextPack keystone: a general
evidence algebra over occurrence-first captured data, with derived/candidate
interpretations labeled, self-observation separated, and coverage gaps stated.
Recall, incident views, agent briefs, demos, API/TUI/CLI renderings, and reports
should be thin projections over that substrate rather than source-specific
match arms or one-off "context pack" silos.

The highest-value flagship remains an honest "what was I doing around T"
reconstruction across real families: terminal, git, filesystem, journald, agent
activity, and eventually broader personal/system sources. The artifact must
headline material occurrence evidence, demote derived/self-observation volume,
and expose absence/caveats rather than silently omitting missing sources.

## Composition With Current State

Existing state already aligns in part:

- `.agent/scratch/001-standing-goal.md` has the correct north star but should be
  interpreted through the conductor objective function: demos and capability
  growth first, dogfood as the feedback engine.
- `.agent/scratch/012-dogfood-operating-log.md` has useful bringup, connection,
  and operator-surface findings; it should remain the continuous resume log,
  not a passive report archive.
- `.agent/scratch/2026-06-29-devloop-demo-query-priority.md` is directly on the
  conductor path: shared view primitives and query algebra are demo enablers,
  provided they keep collapsing CLI/report silos into reusable substrate.
- `.agent/scratch/2026-06-29-ram-io-pressure-investigation.md` and the 2026-06-30
  Sinnix cleanup are part of devloop velocity maximization: stale transient
  builds, duplicate MCP stacks, autostarted services, and unobserved catch-up
  daemons are not background annoyances if they prevent fast Sinex iteration.
- `/realm/inbox/demos_sinex` should be treated as the outward artifact shelf:
  only keep demos that can be rebuilt or honestly inspected, and prefer demos
  proving reusable primitives over bespoke reports.

Immediate implication: the next loop should not start by opening a random issue.
It should pick the smallest high-leverage capability slice that advances the
agent-context-value proof, then make the loop produce: source/log update, code
or config change, live verification, and an inspectable demo artifact.

## Operating Rules To Carry Forward

- Decide under-constrained details locally; log the rationale. Escalate only
  hard-to-reverse forks.
- Use issues as a parts bin, not the plan.
- Keep one Sinex dev daemon; serialize heavy compile/verify; parallelize
  non-build research/subagents only when host pressure is understood.
- Do not claim a heuristic reconstruction as fact. Preserve occurrence vs
  derived/candidate distinctions.
- Never let source-specific reports or CLI-private structs become the product
  shape. Promote useful forms into shared query/acquisition/projection/rendering
  primitives.
- Every loop should leave an inspectable artifact, a verification command, and
  a handoff good enough for a different harness.

## Focused Goal Candidate

Conduct the Sinex dogfood/demo devloop indefinitely: continuously choose the
highest-value live-data capability slice, produce inspectable artifacts proving
that Sinex makes agents and the operator better at reconstructing real work and
machine/personal context, collapse silos into general acquisition/query/evidence
projection/rendering substrate, verify on the active store or live local
captures, update the operating log and handoff, and use each loop's evidence to
reprioritize the next slice while maximizing devloop velocity.

