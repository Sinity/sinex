Self-prompt: Task lifetime and shutdown audit

Goal
- Identify long-lived tokio::spawn tasks and determine whether they are tracked, joined, or aborted on shutdown.
- Highlight orphaned tasks and any shutdown signals that are created but unused.

Process
1) Locate tokio::spawn / JoinHandle / JoinSet usage in production code.
2) For each subsystem (ingestd, gateway, node SDK, ingestors), trace how tasks are started and how shutdown is triggered.
3) Check for: untracked JoinHandles, tasks spawned without cancellation signals, and handles stored but never awaited.
4) Summarize normal shutdown flows and where they might leak tasks.
5) List concrete issues with file references.

Output
- 02-analysis.md: task map with concrete references.
- 03-meta-reflection.md: limits and omissions.
- 04-issues.md: action items.
- 05-next-brainstorm.md: next analysis suggestion.
