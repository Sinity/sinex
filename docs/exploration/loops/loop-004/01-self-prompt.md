# Loop 004 - Self-Prompt

Goal: Analyze database pool usage in gateway/services with a focus on connections held across await points or long-running operations.

Process (do not skip):
1. Search for explicit pool acquisitions (`pool.acquire().await`, `begin().await`, `transaction`) in gateway and services.
2. Inspect each usage site and determine if connections are held across potentially long awaits or loops.
3. Identify patterns where a single connection is reused for multiple awaits or streaming operations.
4. Record evidence with file paths and specific functions.
5. Summarize any risks and note where pooling is handled implicitly by SQLx macros.

Deliverables:
- analysis report with connection-holding map + findings.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm on next analysis.
