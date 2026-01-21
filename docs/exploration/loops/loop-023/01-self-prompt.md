Self-prompt: Blocking-in-async audit

Goal
- Identify synchronous I/O or process execution happening inside async functions or Tokio tasks.
- Focus on production code paths (not tests or build scripts) in sinex-node-sdk, sinex-core, and nodes.

Process
1) Use ripgrep to find files that contain both "async fn" and blocking APIs (std::fs, std::process::Command, std::net::ToSocketAddrs, std::thread::sleep).
2) For each candidate file, open the surrounding code to confirm whether the blocking call is inside an async function or inside an async task (tokio::spawn, join_set, etc.).
3) Distinguish between sync helper functions that are only called from sync code vs sync helpers called from async contexts.
4) Record each confirmed case with file path and line numbers, plus the call chain (where it is invoked from async code).
5) Suggest remediation: prefer tokio::fs / tokio::process, or tokio::task::spawn_blocking around unavoidable blocking calls. Note when the operation is low-frequency and acceptable.
6) Explicitly list false positives and areas not inspected to avoid over-claiming.

Output
- A concise report in 02-analysis.md listing confirmed blocking-in-async cases with evidence.
- A short meta-reflection in 03-meta-reflection.md describing gaps and follow-up needs.
- A concrete issues list in 04-issues.md (actionable fixes).
- A short brainstorm in 05-next-brainstorm.md for the next analysis.
