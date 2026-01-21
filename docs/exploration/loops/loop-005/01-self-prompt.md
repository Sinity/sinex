# Loop 005 - Self-Prompt

Goal: Analyze the schema broadcast cache lifecycle from ingestd to node runtime.

Process (do not skip):
1. Trace where schema broadcasts are emitted and at what cadence.
2. Trace how nodes subscribe and update caches/validators.
3. Identify whether caches are used by runtime features or only tests.
4. Note failure modes: missed broadcasts, KV access issues, or cache replacement behavior.
5. Record concrete evidence with file paths and functions.

Deliverables:
- analysis report with lifecycle map + findings.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm on next analysis.
