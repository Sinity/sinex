## Current State

**Canonical tracking lives in GitHub issues.** Use `gh issue list --state open` for the live set.
Do not duplicate issue state here.

Scratch notes are not a backlog. Promote durable findings into GitHub issues.

### Active Issue Clusters (as of 2026-05-14)

| Cluster | Key Issues | Meaning |
|---------|-----------|---------|
| Declaration→consumer drift (meta) | #744 | Governance umbrella; concrete child clusters #745-#762 all closed |
| Staged-source interpretation | #1054, #1126, #1125, #1115, #1062, #1060 | Architecture spine + cross-issue sequencing; replay/drain/workbench |
| Parser backlog (#1070 tracker) | #1092 #1091 #1090 #1089 #1088 #1075 #1074 #1068 #1053 #1052 | Bulk source-material parsers; mechanical, parallelizable |
| Intelligence activation | #1087, #332 | Activate entity/relation automata; document retrieval substrate |
| Intelligence horizon | #1076, #1063, #1117, #1118, #1116 | Embeddings via recorded model effects, SQL automata, LLM router |
| Domain adapters / federation | #1119, #1122, #1123 | Authority categories; Polylogue + Lynchpin bridges |
| Privacy & curation | #1071, #1072, #1086 | Runtime private-mode, CLI audit/export/delete, proposal/finalizer |
| Deployment proof | #1135, #1132, #1129 | Production-shaped VM proof; source-unit replay/drain isolation |
| Provenance evolution | #1207, #1206, #1112, #1113, #1110, #1101 | Evidence lanes, authority consolidation, fan-in lineage |
| Process governance | #1094, #1093 | Target-vision claim ledger, event QoS policy |
| Operator surfaces | #1121, #1105, #1095, #1025 | SinexFS mount, MCP server, context packs, timeline TUI |
| Active P0 (deploy-side verification only) | #1241 | Code prongs landed (#1243 + #1250); awaits live DLQ observation on sinnix-prime |
| Dev tooling | #1221, #1214, #1213, #1211, #1222, #1224 | xtask LSP, ICU rebuild storm, watchdog dead-code, READY classification |

### Recently Closed (May 2026 cleanup waves)

The May 2026 hardening waves closed every concrete sub-cluster under #744 (#745-#762),
all 4 operational P0s from the 47-report deep audit (#945, #951, #986, #987, #988, #989,
#990, #992, #993, #994), SDK consolidation (#1009, #1010, #1011, #1012), and content-store
modernization (#848, #777). Pre-Wave-B fold landed in #1223 + #1225, collapsing 6 legacy
ingestor crates into the source-worker host.

### Architectural Notes (not fragilities — current design)

| Behavior | Reason |
|----------|--------|
| Gateway auth has no revocation (token rotation only) | Single-user trust model; revocation surface is operational rotation, not API |
| Cascade archive runs in separate DB transactions | Two-phase commit not justified at single-user scale; replay race window documented |
| Synthesis cycle detection elided | UUIDv7 monotonicity makes backward cycles structurally impossible |
| Telemetry rollups are views, not continuous aggregates | #945 split #952: CAs proved unnecessary at observed write volume |

### Deep Audit Reference

The 47-report deep audit (2026-05-03) findings are tracked in closed GitHub issues
#986-#994 plus their parents #744, #751, #754, #759, #945. Most findings closed in
the May 2026 sprints; the underlying scratch index at
`.agent/scratch/deep-audit-2026-05-03/index.md` is historical context, not active backlog.
