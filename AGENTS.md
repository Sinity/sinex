# Sinex foundation contributor guide

This isolated source tree implements the fresh Sinex foundation. The normative design is pinned in docs/rewrite; the predecessor remains in Git history. No command in this tree operates on production state by default.

Use the declared AgentCTL foundation operations for builds and real PostgreSQL acceptance. The narrow xtask frontend builds/tests this workspace; read README.md for supported commands. Do not copy predecessor xtask, runtime or schema machinery to bypass a missing implementation.

Keep concrete domain types and wire errors in sinex-model and sinex-protocol. sinex-store owns canonical schema declarations and actual role-bound connections. Only its writer owner admits events; API handlers retain read capabilities and admission-channel handles. Query-free schema bootstrap must build before the development database exists. Checked product queries compile against the isolated schema; no SQLX_OFFLINE cache.

Register only implemented operations. The README and owning contracts in docs/rewrite define supported source, query, indexed retrieval, MCP and replay subsets. Do not turn unavailable into empty success. Preserve authored state and exact retained material; do not touch live stores, consumer pins, master or external task routing during foundation preparation.

Keep substantial unit-test bodies and fixtures in external private `#[cfg(test)]` modules. Tiny tests may remain inline. Exercise CLI, MCP and integration acceptance through public routes; never widen runtime visibility merely to relocate tests.

## Development flow

Optimize routine development for fast feedback and integration. Use cheap checks and focused behavioral tests for the actual delta; full local packages, full behavioral suites and full-corpus verification are not prerequisites for every development merge. Development bugs can be repaired after integration. Keep pass, fail, pending, unrun and superseded outcomes explicit; integration is not proof of testing. Run extended validation separately on meaningful integrated batches and continue independent development while it runs. Coalesce expensive queued checks to useful latest-main candidates through supported job tooling; do not enqueue a full verification sequence for every small change. Use the normal permitted PR route and existing required checks; do not change branch protection. Real-data custody, secrets, destructive operations and deployment retain their separate safeguards.

Stage explicit paths, review public content and run scoped checks before a normal local commit. Product publication and activation require their own integration decision. Preserve other agents' edits and use named ownership for shared seams. Fixtures are synthetic; private run evidence stays outside tracked files.
