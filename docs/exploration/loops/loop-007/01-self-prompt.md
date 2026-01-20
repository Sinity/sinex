# Loop 007 - Self-Prompt

Goal: Analyze replay control idempotency under request retries and timeouts.

Process (do not skip):
1. Trace replay control request handling from NATS server to state machine calls.
2. Identify how each request behaves under retries: plan, preview, approve, execute, cancel, status.
3. Inspect state transition guards and lock usage to determine safety when the same request is repeated.
4. Record evidence with file paths and specific functions; avoid speculation.
5. Summarize risks and recommend areas for hardening.

Deliverables:
- analysis report with idempotency map + findings.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm on next analysis.
