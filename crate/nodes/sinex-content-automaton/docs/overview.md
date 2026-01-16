# Content Automaton Overview

The content automaton listens for events that carry rich text or media payloads
and performs secondary analysis (classification, enrichment, metadata
extraction). It follows the standard `StatefulStreamProcessor` phases so it can
snapshot historical materials, close any ingestion gaps, and then stream updates
in real time.

- The binary entrypoint (`src/main.rs`) uses `processor_main!` to run the
  `ContentAutomaton` `StatefulStreamProcessor` with the unified CLI/lifecycle.
- Uses annex helpers from `sinex-node-sdk` to retrieve source material.
- Emits synthesized content insights that downstream services (search, PKM)
  consume.
- Shares dashboards and health semantics with the broader automaton family
  documented in `crate/lib/sinex-node-sdk/docs/overview.md`.
