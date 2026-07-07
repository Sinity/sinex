---
created: "2026-06-28T17:30:00Z"
purpose: "Assimilate /realm/inbox/download/demo-and-agent-plan.md (Sinex-relevant) and compose it with the existing fast-dev workload (see 015)."
status: active
project: sinex
---

# Demo-value plan assimilation (Sinex parts only)

Source: `/realm/inbox/download/demo-and-agent-plan.md`. Polylogue parts (P0–P6, Polylogue agents 1–3) = SISTER AGENT, not me. This note keeps only Sinex-relevant content + how it composes with my threads.

## Core reframe (the lens)
- Shift from "backlog burn / hardening reports" → **demonstrable value: each thread should produce a real artifact + evidence it worked on REAL data.**
- Issue set = parts bin, not the plan. Every task answers: which capability does it unlock, what is the artifact, what evidence proves it.
- "One hardening lane is enough; the rest should produce artifacts." Don't let operator-hardening consume everything.
- This is ONE aspect of my workload (operator: compose, don't replace). My architectural keystone + fragmentation stay; this adds a VALUE/PRIORITIZATION lens + artifact targets.

## Sinex demo ladder (mine)
- **S0 Runtime trust & backfill resilience** = THE GATE for any demo. Artifact: `sinexctl demo prod-smoke` (recent-events query, ingestion rate/backlog, DLQ summary, import pacing/progress, restart proof). Issues #2182/#2185/#2179. ⇐ This is essentially my FRAGMENTATION thread (AGENT-FRAG already on #2184/#2182/#2185). Compose: fragmentation thread should also surface an operator-facing trust/smoke artifact, not just fix internals.
- **S1 "What was I doing around T?" recall** — multi-source timeline (terminal+cwd/project+git+journald+browser/window/file), evidence refs, coverage caveats. NOTE: `demo/sinex-recall` already exists+verified (prior session). Next iter: ≥2 independent source families. ⇐ REQUIRES real dev ingestion (≥2 sources). Strong near-term artifact.
- **S2 Incident reconstruction** — real failure, ≥3 sources fused → causal chain, paste-into-issue report. Baseline = manual grep.
- **S3 Agent pre-task briefing** — `sinexctl agent brief --repo <path>`: recent repo commands, last failing tests, branches/commits, files edited outside git, service state/errors. High agent-utility artifact.
- **S4 Polylogue+Sinex bridge** — Sinex selects time/project window → Polylogue retrieves AI sessions → joined timeline. (Cross-stack; Sinex side mine, retrieval side sister agent.)
- **S5 Provenance/correction/replay demo** — ingest→derive→find parser bug→correct→replay→show superseded interpretations + provenance chain + old-vs-new truth. Showcases the rigorous part. ⇐ ties to AGENT-CORRECT's #2194 replay work.
- **S6 email(#1469)/media(#1043)** — DEFER; only narrow staged slice if a concrete recall demo needs it.

## Sinex "do now" triage (from memo §5)
- #2182 large historical imports (occurrence-time distribution, compression-not-blocking-startup, restart drains backlog) = operational gate.
- #2185 import pacing/progress via sinexctl + telemetry.
- #2179 schema bundle drift (small, embarrassing).
- #2184 START with fixing FUTURE 1-byte material generation; general defrag second.
(All already in AGENT-FRAG mandate.)

## COMPOSITION with existing workload (015)
| Lane | Threads | Artifact target (demo lens) |
|---|---|---|
| Hardening/architecture | KEYSTONE confirmed-delivery (me) + AGENT-CORRECT (#2194 replay, #2196 schema) | architectural correctness; #2194 feeds S5 replay demo |
| Runtime-trust GATE (S0) | AGENT-FRAG (#2184/#2182/#2185) | `sinexctl demo prod-smoke` operator artifact |
| Dev ingestion substrate | me (dev SINEX_SOURCE_BINDINGS manifest, fix #2198/#2197 so ingestion doesn't lie) | ≥2 real source families flowing in dev |
| Demo-value artifacts (NEW) | after substrate: S1 recall multi-source (extend demo/sinex-recall), then S2/S3 | readable recall/brief artifact on REAL dev data |

## Prioritization adjustment
- Keep keystone (operator's explicit choice + highest-leverage architecture) but recognize it's the "one hardening lane" — don't multiply pure-hardening beyond keystone+correctness.
- ELEVATE dev-ingestion bring-up: it's the substrate for S1/S2/S3 AND for validating the keystone end-to-end. Do it next after producer-side keystone batch.
- Target ONE concrete demo artifact early (S1 multi-source recall) once ≥2 sources ingest in dev — proves value, not just internals.
- Apply red-team test (memo §6 reviewer): real data? evidence refs? baseline/counterfactual? overstated coverage? reusable?

Cross-ref: [[015-confirmed-delivery-redesign]] (main working log).
