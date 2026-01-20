# Loop 003 - Self-Prompt

Goal: Map NATS request/response usage and verify timeouts/retry semantics are explicit and consistent.

Process (do not skip):
1. Enumerate NATS request patterns: search for `.request(`, `send_request`, and `request_timeout` usage.
2. For each request path, check whether a timeout is applied explicitly (tokio timeout or request timeout).
3. Identify retry/backoff behavior for NATS subscriptions and requests.
4. Note any default behaviors that could cause indefinite waits.
5. Record evidence with file paths and specific functions.

Deliverables:
- analysis report with timeout map + findings + risks.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm on next analysis.
