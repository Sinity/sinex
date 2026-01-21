# Loop 001 - Self-Prompt

Goal: Analyze graceful shutdown and task cancellation across core binaries and the node runtime.

Process (do not skip):
1. Enumerate entry points with signal handling. Use `rg -n "ctrl_c|SignalKind|shutdown" crate/core` to find main() shutdown wiring.
2. Inspect each main/service pair for shutdown propagation and task join/abort behavior. Read files, do not guess.
3. For node runtime, trace the shutdown signal path from runner to event processor and any spawned background tasks.
4. Record concrete evidence (file paths and specific functions) for each claim.
5. Identify gaps: tasks without cancellation, shutdown signals that are not wired, and background tasks that are never awaited.
6. Summarize risks and scope in the report. Keep it factual; no speculation without evidence.

Deliverables:
- analysis report with a shutdown map + findings + risks.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- brief brainstorm on what to analyze next.
