# Loop 006 - Self-Prompt

Goal: Audit NATS subject naming for environment/namespace consistency across core, gateway, and node SDK.

Process (do not skip):
1. Identify use of `SinexEnvironment::nats_subject` and raw subject strings.
2. For each raw subject string, check if it should be environment-namespaced.
3. Note any NATS KV bucket names that look non-namespaced but are used in multi-env contexts.
4. Record evidence with file paths and functions.
5. Summarize consistency gaps and potential impact.

Deliverables:
- analysis report with a subject map + findings.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm on next analysis.
