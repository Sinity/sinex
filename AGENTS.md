# Sinex foundation contributor guide

This isolated source tree implements the fresh Sinex foundation. The normative design is pinned in docs/rewrite; the predecessor remains in Git history. No command in this tree operates on production state by default.

Use the declared AgentCTL foundation operations for builds and real PostgreSQL acceptance. The narrow xtask frontend builds/tests this workspace; read README.md for supported commands. Do not copy predecessor xtask, runtime or schema machinery to bypass a missing implementation.

Keep concrete domain types and wire errors in sinex-model and sinex-protocol. sinex-store owns canonical schema declarations and actual role-bound connections. Only its writer owner admits events; API handlers retain read capabilities and admission-channel handles. Query-free schema bootstrap must build before the development database exists. Checked product queries compile against the isolated schema; no SQLX_OFFLINE cache.

Register only implemented operations. Sources, MCP, replay, confirmed delivery and native capture remain unavailable until qualified through their real routes. Do not turn unavailable into empty success. Preserve authored state and exact retained material; do not touch live stores, consumer pins, master or external task routing during foundation preparation.

Stage explicit paths, review public content and run scoped checks before a normal local commit. Product publication and activation require their own integration decision. Preserve other agents' edits and use named ownership for shared seams. Fixtures are synthetic; private run evidence stays outside tracked files.
