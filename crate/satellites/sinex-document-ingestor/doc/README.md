# sinex-document-ingestor

The document ingestor satellite pulls documents from configured sources,
normalises them, and forwards events into the ingestion pipeline. It leverages
the shared annex storage helpers from `sinex-satellite-sdk`.

- Crawls file systems, remote endpoints, or APIs as configured.
- Normalises metadata and persists source material.
- Emits provenance-rich events with ULIDs.

See `docs/architecture/satellite-implementation.md` for the automaton pattern
and `docs/architecture/Core_Architecture.md` for ingestion topology.
