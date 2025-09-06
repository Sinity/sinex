# Web Archiving (WARC/WACZ)

## Overview
Capture durable snapshots of web pages (HTML, resources, metadata) using WARC/WACZ formats alongside lighter browser activity events. Enables offline access, provenance, and reproducible analysis.

## Goals
- High‑fidelity archival (content + resources) with verifiable hashes
- Play well with lightweight capture paths in the browser extension
- Privacy modes and per‑domain policies

## Architecture
- Triggering: Browser extension (or CLI) emits `web.archive.requested` with URL, scope, and policy; desktop archiver (headless Chromium/Firefox + capture tooling) executes capture.
- Artifacts: Produce WARC or WACZ bundles; store content‑addressed in annex; track strong hashes (BLAKE3/SHA‑256) and byte size.
- Indexing: Extract minimal metadata + text; persist as lightweight `web.archive.indexed` events for discovery and search.
- Provenance: Link `web.archive.captured` → source `browser.navigation.*` (when applicable) via `source_event_ids`.

## Event Types (examples)
- `web.archive.requested`: url, reason (manual|rule), priority
- `web.archive.captured`: url, blob_sha256, format (WARC|WACZ), bytes, extractor_version
- `web.archive.indexed`: url, text_hash, length_chars, index_version

## Data Model & Schemas
- Schema IDs
  - `web/archive-request@v1`
  - `web/archive-captured@v1`
  - `web/archive-indexed@v1`
- Minimal payloads (examples)
  - `web.archive.requested`
    - url: string, policy: { cookies: bool, sandbox: bool, scope: enum(page|site) }
  - `web.archive.captured`
    - url, format, bytes, annex_key, content_hash, capture_started, capture_ended
  - `web.archive.indexed`
    - url, text_hash, length_chars, lang?, extractor: { name, version }

## RPC Surface (proposal)
- `content.store_blob` (existing): store WACZ/WARC artifact; returns `annex_key`.
- `search.search_events` (existing): discover archived pages via `source:web` and event filters.
- `web.request_archive` (new): enqueue URL for capture; params { url, priority?, policy? } → returns request_id.
  - Implementation: gateway → enqueue to local task queue; archiver consumes and emits events above.

## Capture Pipeline Details
- Headless capture: prefer Chromium with deterministic flags; timeout and network idle heuristics.
- WACZ specifics: include `pages/` and `indexes/` (CDXJ) for fast text extraction; store text index checksum.
- Deduplication: compute content hashes for resources; skip re‑capture if identical within retention window.

## Failure Modes & Retries
- Soft failures (timeouts, DNS): backoff retry up to N attempts; emit `web.archive.failed` with reason.
- Integrity mismatches: if post‑upload hash diff, mark artifact invalid and purge annex entry.
- Quotas: enforce concurrent capture and bandwidth limits; queue overflow → oldest low‑priority dropped (emit event).

## Privacy & Controls
- Domain allow/deny lists; redaction for sensitive selectors
- Rate limiting and bandwidth caps
- User‑initiated capture only for protected domains

## Implementation Notes
- Use a headless browser (Chromium/Firefox) plus capture libs; consider open web archiving tools.
- Store content‑addressed blobs; keep indexes minimal and reproducible; prefer BLAKE3 for local checks, SHA‑256 for interchange.
- Integrate with existing browser extension (native messaging) for triggers and status; show capture state in extension UI.
- Metrics: capture duration, bytes, resource count, failed requests, text length.

## Roadmap
- P1: Manual capture + artifact storage (CLI + extension trigger)
- P2: Text extraction + indexing pipeline (searchable text summaries)
- P3: Rules engine (auto‑capture) and deduplication + basic UI
