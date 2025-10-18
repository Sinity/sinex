# Content Automaton Overview

The content automaton listens for events that carry rich text or media payloads
and performs secondary analysis (classification, enrichment, metadata
extraction). It follows the standard `StatefulStreamProcessor` phases so it can
snapshot historical materials, close any ingestion gaps, and then stream updates
in real time.

- Uses annex helpers from `sinex-satellite-sdk` to retrieve source material.
- Emits synthesized content insights that downstream services (search, PKM)
  consume.
- Shares dashboards and health semantics with the broader automaton family
  documented in `crate/lib/sinex-satellite-sdk/doc/overview.md`.
