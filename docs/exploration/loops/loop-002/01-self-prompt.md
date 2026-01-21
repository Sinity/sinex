# Loop 002 - Self-Prompt

Goal: Analyze checkpoint persistence and cleanup wiring, with focus on env var contracts and runtime behavior.

Process (do not skip):
1. Search for checkpoint-related env vars and their usage: `rg -n "CHECKPOINT" crate`.
2. Trace the checkpoint file path flow: default path, overrides, and how processors consume it.
3. Verify whether checkpoint cleanup is scheduled anywhere; if only defined, note as a gap.
4. Identify mismatches between tooling (e.g., dev command) and runtime config.
5. Record concrete evidence with file references.

Deliverables:
- analysis report with findings and implications.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm of next analysis ideas.
