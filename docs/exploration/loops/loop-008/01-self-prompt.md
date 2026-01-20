# Loop 008 - Self-Prompt

Goal: Audit RPC input validation boundaries and sanitization across gateway handlers.

Process (do not skip):
1. Enumerate RPC handler entry points (search in `crate/core/sinex-gateway/src/handlers`).
2. For each handler, identify input parsing, validation, and any domain-specific sanitization.
3. Flag handlers that pass raw inputs to DB or NATS without validation.
4. Cross-check with any shared validators or domain types that already enforce constraints.
5. Record evidence with file paths and function names.

Deliverables:
- analysis report with a handler map + findings.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm on next analysis.
