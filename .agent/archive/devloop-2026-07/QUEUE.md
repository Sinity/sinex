# Deferred Directive Queue

This ignored active-state file records operator sequencing directives and
next-after-current obligations that must survive compaction without replacing
the current slice prematurely.

## Queued 2026-07-02T19:48:01+02:00 - external-proof campaign: recall v2 baseline arm

Directive: Run a bounded proof campaign: produce sinex-recall v2 on the dev runtime — multi-source (fs+git+shell+browser) reconstruction of one real work window through the shared context recall view, WITH a committed side-by-side baseline arm (raw atuin search + git log for the same window), stranger-readable README, and one-command regeneration. Substrate repair is in scope only where this specific artifact would be false without it. Terminal state: a cold-reader check passes (an agent given only the demo directory states what it proves and reproduces it). Details: bead sinex-9j9 (campaign epic; `bd show sinex-9j9`).
Trigger: current catch-up/readiness slice lands or is checkpointed as blocked
Status: queued
Next checkpoint: promote into ACTIVE-LOOP.md when the trigger fires

## Queued 2026-07-02T20:07:15+02:00 - external-proof campaign 2: production restore

Directive: After recall-v2 reaches its terminal state, restore the production Sinex runtime: the TimescaleDB compression-lock fix is on master but the restore has never been executed. Read-only verification plan first (data integrity of the 74M events, then service bring-up order), then execute as an explicit operator-visible control-plane operation, then prove: production sinexd healthy, event counts match pre-incident expectations, one recall query answered from prod data. Every external 'live substrate' claim is gated on this. Details: bead sinex-mhk (campaign epic; `bd show sinex-mhk`).
Trigger: recall v2 baseline-arm demo reaches terminal state (cold-reader check passes)
Status: queued
Next checkpoint: promote into ACTIVE-LOOP.md when the trigger fires

## Completed 2026-07-01T12:36:49+02:00 - after current

Outcome: Promoted into ACTIVE-LOOP.md as Meta focus after DLQ demo metadata repair closed the artifact slice.

## Completed 2026-07-01T12:22:59+02:00 - meta queue command proof

Outcome: queue lifecycle command verified and documented

## Completed 2026-07-01T07:34:00+02:00 - Meta shift after query artifact

Directive: after finishing the live query exploration already in progress,
shift to meta/devloop/process improvement using the Sinex/Polylogue convention
analysis.
Outcome: implemented the queue-channel scaffold, verified status/review, and
promoted the next object-level slice into `ACTIVE-LOOP.md`.
