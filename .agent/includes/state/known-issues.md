## Current State

**Canonical tracking lives in GitHub issues.** Use `gh issue list --state open` for the live set.
Do not duplicate issue state here.

Scratch notes are not a backlog. Promote durable findings into GitHub issues.

### Active Issue Clusters (as of 2026-05-03)

| Cluster | Key Issues | Meaning |
|---------|-----------|---------|
| Declaration→consumer drift | #744, #743, #752, #755 | 25+ orphans, type-name collisions, provenance semantics, stringly-typed identifiers |
| Runtime unification | #754, #751, #759, #762 | Config/batching/checkpoint/failure-policy consolidation; error-handling drift; DB pattern drift |
| Storage modernization | #848, #777, #987 | git-annex→local BLAKE3 CAS migration; typed content-store contracts; GC + safety |
| Intelligence layer | #331, #332, #733, #934 | Entity extraction, document retrieval, pipeline stages 2-4 |
| Operational gaps | #945, #951, #986-#994 | Backup, compression, views→CAs, schema safety, hardening, audit findings |
| CLI/TUI convergence | #846, #368, #806, #807 | Compact sinexctl IA, verify suite, demo scenarios |
| Deployment hardening | #910, #914, #915, #990 | Resource scoping, pressure observability, NixOS service hardening |
| Documentation | #805, #809, #950, #991 | Glossary, README assets, source-unit workflow, CLAUDE.md freshness |

### Known Architectural Fragilities

| Fragility | Tracking |
|-----------|----------|
| Publish backpressure not intent-aware across traffic classes | #326 |
| DLQ/processing-failure/recovery-spool semantics conflated | #327 |
| git-annex storage path is dead code; local CAS needs delete-on-tombstone (not GC) | #848, #987 |
| Cascade two-transaction gap — replay can leave dangling references | #751 |
| Settlement system (FailurePolicy, ErrorClass) designed but never wired to production | #754 |
| IngestorNodeAdapter lacks SelfObserver/HealthReporter — 6 ingestors invisible to telemetry | #754, #992 |
| Cascade archiving trigger fn_archive_before_delete needs extending to event_embeddings/cluster_members/validation_cache | #988 |
| Ingestors have no health/degradation observability — silent watcher death undetected | #992 |
| Schema sync UPSERTs in-place when version unchanged (84/102 payloads at "1.0.0") | #951 |

### Deep Audit Reference

A 47-report comprehensive audit (2026-05-03) is at `.agent/scratch/deep-audit-2026-05-03/index.md`.
Key findings are tracked in GitHub issues #986-#994 and linked from existing issues #744, #751, #754, #759, #945.
