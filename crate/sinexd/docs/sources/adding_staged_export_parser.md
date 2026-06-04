# Adding a Staged-Export Parser

This guide turns one personal-data export (Spotify history, Raindrop CSV,
Messenger thread, etc.) into a typed source that emits events through
the standard sinex pipeline.

It is the consolidated procedure derived from the first three parsers
landed under this pattern:

- `spotify-extended-history` (#1092 → PR #1261) — JSON array files
- `raindrop-bookmarks` (#1091 → PR #1263) — CSV files
- `facebook-messenger-thread` (#1090 → PR #1264) — per-thread JSON object files

Use it for the remaining #1070 backlog (`#1089` social, `#1088` docs,
`#1075` KB, `#1074` finance, `#1068` AI sessions, `#1053` git, `#1052`
health) and for any new export format from a personal-data provider.

## Mental model

Every staged-export parser is built from four pieces:

1. **A payload type** — one strongly-typed `EventPayload` struct per
   emitted event type. Lives in
   `crate/sinex-primitives/src/events/payloads/<domain>.rs`. Re-use a
   payload-domain module across providers (one `messaging` module for
   Messenger + Signal + IRC private messages, one `music` module for
   Spotify + later platforms, etc.) rather than spawning a domain per
   provider.

2. **A parser** — a `MaterialParser` implementation that turns one
   `SourceRecord` into N `ParsedEventIntent`s. Lives in
   `crate/sinexd/src/sources/source_contracts/<domain>.rs`.

3. **A source contract + binding** — the `register_source_contract!`
   and `register_source_runtime_binding!` macros in the same source file.
   These declare identity, privacy tier, retention, verification
   parser/source identity and runtime shape.

4. **The registration triple** — `register_adapter_ingestor!(source_id,
   <Adapter>, <Parser>)` wires the parser into both the replay dispatch
   registry and the continuous-ingestion source factory.

## Picking an adapter

Adapters live in `crate/sinexd/src/node_sdk/parser/adapters/`. They
determine how source bytes are presented to the parser.

| Export shape | Adapter | When to pick |
|---|---|---|
| One JSON array per file (e.g., Spotify Extended History) | `StaticFileAdapter` | Whole-file one-shot read; parser unpacks the array internally |
| One JSON object per file with messages array (e.g., Messenger thread) | `StaticFileAdapter` | Same |
| CSV file with one row per record (e.g., Raindrop) | `StaticFileAdapter` | Whole-file read; parser uses the `csv` crate |
| Line-by-line append-only log (e.g., WeeChat IRC) | `AppendOnlyFileAdapter` | One `SourceRecord` per line; parser handles one line at a time |
| Hot folder of dropped files | `FileDropAdapter` | Live stream; one record per filesystem event |
| SQLite table (e.g., browser history `places.sqlite`) | `SqliteRowAdapter` | One row per record |
| Directory tree (e.g., document library) | `DirectoryWalkAdapter` | One record per file discovered |

For most export-style sources, `StaticFileAdapter` is the right choice —
exports are committed snapshots, not streams.
Static JSON, CSV, and TSV files now expose adapter-level structural
fingerprints, so upstream export-shape changes feed the shared drift substrate
before parser defaults or nulls silently hide the problem. Composed adapters
preserve child fingerprints through `ChainedAdapter`: the primary leg is used
when present, with secondary as fallback.
Directory walks expose a `directory_manifest` fingerprint: relative file-path
presence plus extension class for every matched entry, with nested JSON/CSV/TSV
child shape hashes folded into the manifest entry. That means a provider can
add/remove files, rename export files, or change a CSV/JSON shape inside a
stable path and the adapter-level drift path will still see the change.

## Step-by-step

### 1. Pick names

Decide:

- **Source id** — kebab-case, scoped: `spotify-extended-history`,
  `raindrop-bookmarks`, `facebook-messenger-thread`. Used in
  `SourceId::from_static(...)`, `register_source_contract!` `id:` field,
  `register_adapter_ingestor!` `source_id:`, and as the binding
  `SubjectRef`.
- **Event source** — one segment, lowercased: `"spotify"`, `"raindrop"`,
  `"messenger"`. Mirrors the provider.
- **Event type** — dot-namespaced, present-tense passive: `"track.played"`,
  `"bookmark.created"`, `"message.sent"`. Choose carefully — once events
  are persisted, renaming requires migration.
- **Payload domain** — used only for the Rust module name. Group by
  conceptual domain (`music`, `bookmark`, `messaging`) not by provider.

### 2. Add the payload

`crate/sinex-primitives/src/events/payloads/<domain>.rs`:

```rust
//! <Domain> payloads. Hosts <provider> exports for now; sibling
//! providers go in this module rather than a new file.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "<source>", event_type = "<event_type>")]
pub struct <Provider><Event>Payload {
    pub <ts_field>: Timestamp,
    pub <required_fields>: <types>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub <optional_fields>: Option<<types>>,
}
```

Then register the module in
`crate/sinex-primitives/src/events/payloads/mod.rs` by adding the
`pub mod <domain>;` and `pub use <domain>::*;` lines alphabetically.

Add a tiny smoke test inside the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventPayload;

    #[test]
    fn declares_source_and_event_type() {
        assert_eq!(<Payload>::SOURCE.as_static_str(), "<source>");
        assert_eq!(<Payload>::EVENT_TYPE.as_static_str(), "<event_type>");
    }
}
```

### 3. Define the parser

`crate/sinexd/src/sources/source_contracts/<domain>.rs`:

```rust
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::node_sdk::parser::{
    MaterialParser, ParserError, ParserResult, StaticFileAdapter,
};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent,
    ParserContext, ParserId, ParserManifest, SourceRecord, SourceId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy,
    RuntimeShape, SourceRuntimeBinding, SourceBuildImpact, SourceContract,
    SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// Raw export shape — mirrors the JSON/CSV/etc. fields verbatim with
// lenient defaults for fields that may be absent across snapshot vintages.
#[derive(Debug, Deserialize)]
struct RawRow {
    // ...
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct <Provider>ParserConfig;

#[derive(Debug, Clone, Default)]
pub struct <Provider>Parser;

#[async_trait]
impl MaterialParser for <Provider>Parser {
    type Config = <Provider>ParserConfig;

    fn manifest(&self) -> ParserManifest { /* ... */ }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        // 1. Parse `record.bytes` into a Vec of raw rows.
        // 2. For each row, build a ParsedEventIntent (see below).
        // 3. Return Ok(intents).
    }
}
```

`ParsedEventIntent` fields you have to populate:

| Field | What to set |
|---|---|
| `id` | `Id::new()` |
| `source_id` | `ctx.source_id.clone()` |
| `parser_id` | `ParserId::from_static("<parser-id>")` |
| `parser_version` | `"1.0.0".into()` to start |
| `event_type` | `EventType::from_static("<event_type>")` |
| `event_source` | `EventSource::from_static("<source>")` |
| `payload` | `serde_json::json!({...})` from the row fields |
| `ts_orig` | Parsed from the row's natural timestamp field |
| `timing` | `TimingEvidence::Intrinsic { field: "<source-field-name>".into(), confidence: TimingConfidence::Intrinsic }` |
| `anchor` | See "Anchoring" below |
| `occurrence_key` | `Some(OccurrenceKey { ... })` — see "Occurrence identity" |
| `privacy_context` | `ProcessingContext::Document` for chat-like, `Metadata` for structured records |
| `derived_parents` | `None` (material provenance) |

### 4. Anchoring

`MaterialAnchor` is the stable real-world identifier for a record.
Pick the variant that matches the export shape:

| Export shape | Anchor |
|---|---|
| JSON array | `ByteRange { start: <array_index>, len: 1 }` — index in the array; stable as long as the export's row order is stable |
| CSV file | `Line { byte_start: 0, line: <csv_row_index> }` — 1-based, excluding the header |
| Per-thread/per-file JSON object with internal collection | `ByteRange { start: <message_index>, len: 1 }` — index in the inner collection |
| SQLite row | `SqliteRow { table: ..., rowid: ... }` |
| File-system record (one event per file) | `DirectoryEntry { path, content_hash: Some(...) }` |

The anchor doesn't have to be a literal byte range — `start: <index>, len: 1`
with documented anchor semantics in the source contract is
fine. Replay correctness only requires that the **same record on the
same source material always gets the same anchor**.

### 5. Occurrence identity

The `occurrence_key` is the natural-key dedup mechanism. It must
uniquely identify a real-world occurrence so that re-imports of an
overlapping export snapshot don't double-publish.

Preferred shape:

```rust
let occurrence_key = OccurrenceKey {
    source_id: SourceId::from_static("<source-id>"),
    fields: vec![
        ("<provider_id_field>".into(), provider_id.to_string()),
        ("<secondary_field>".into(), secondary.to_string()),
        // include the timestamp + a quantity field (played_ms, byte_size, etc.)
        // so distinct events with the same primary id dedupe correctly
    ],
};
```

If the export gives you a stable provider id, use it. If not, build a
tuple from `(natural_timestamp, sender_or_actor, content_hint)` — the
Messenger parser does this with a 64-char text hint.

### 6. Privacy

Pick `PrivacyTier` carefully:

- `PrivacyTier::Public` — telemetry, metadata-only health
- `PrivacyTier::Internal` — operator-visible state, no user content
- `PrivacyTier::Sensitive` — anything with user content, names, URLs,
  free text — **default for personal-data exports**

Pick `ProcessingContext` for `privacy_context`:

- `ProcessingContext::Metadata` — for structured records (track names,
  bookmark URLs)
- `ProcessingContext::Document` — for free text the admission layer
  may want to strip (messages, notes, document bodies)

When in doubt, choose `Sensitive` + `Document` and let the admission
policy widen the surface later if needed. Narrowing later is harder.

### 7. Source contract + binding

Two registration macros:

```rust
register_source_contract! {
    SourceContract {
        id: "<source-id>",
        namespace: "<conceptual-namespace>",  // "music", "web", "messaging"
        event_types: &[("<source>", "<event_type>")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(<tuple-description>)",
        ),
        access_policy: "<access-policy-name>",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:<source-id>"),
        "<source-id>",
        "<namespace>",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("<event_type>")
    .privacy_context("Metadata")  // or "Document"
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_id("<source-id>")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("<source_id>_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}
```

The `implementation("sinexd")` and `runner_pack("sinexd-source")`
strings are descriptor labels retained by the current source inventory.
They do not name a separate crate or binary; source contracts are hosted by
`sinexd`.

### 8. Register with the source host

Two more lines at the bottom of the source file:

```rust
crate::register_adapter_ingestor!(
    source_id: "<source-id>",
    adapter: StaticFileAdapter,
    parser: <Provider>Parser,
);
```

Then add `pub mod <domain>;` to
`crate/sinexd/src/sources/source_contracts/mod.rs`.

### 9. Inline tests

The pattern uses one inline `mod tests` per parser. Cover:

| Test | What it asserts |
|---|---|
| `parses_<thing>_into_N_intents` | Basic happy path: M rows → N intents, correct source + event_type |
| `preserves_<key_fields>` | Semantic fields survive parse |
| `<anchor>_uses_<index>` | Per-row anchor is what you documented |
| `occurrence_key_<shape>` | Full OccurrenceKey field list and order |
| `<sensitive_fields>_dropped` | Privacy filter: dropped fields are absent from payload |
| `<edge>_falls_back_to_<default>` | Missing optional fields |
| `<quoted_or_unicode>_round_trip` | CSV/Unicode edge cases for the format |
| `invalid_<format>_errors` | Bad input surfaces ParserError, not panic |

See `crate/sinexd/src/sources/source_contracts/music.rs::tests`,
`bookmark.rs::tests`, `messaging.rs::tests` for concrete examples.

### 9a. Parser-family acceptance fixture

Every #1070 parser child should include at least one representative
`ParserFixtureHarness` fixture with a `FixtureAcceptanceContract`. That
contract is the shared acceptance vocabulary for parser backlog work:

```rust
FixtureSpec {
    name: "provider representative export".to_string(),
    description: "representative parser-family fixture".to_string(),
    input_shape_kind: InputShapeKind::StaticFile,
    material_bytes: fixture_bytes,
    material_path: None,
    expectations: vec![FixtureExpectation {
        index: 0,
        assertions: vec![
            FixtureAssertion::EventSource { expected: "<source>".to_string() },
            FixtureAssertion::EventType { expected: "<event_type>".to_string() },
            FixtureAssertion::Timestamp { value: expected_ts },
            FixtureAssertion::Timing { expected: expected_timing },
            FixtureAssertion::Anchor { expected: expected_anchor },
            FixtureAssertion::OccurrenceKey {
                expected_fields: vec![("<field>".to_string(), "<value>".to_string())],
            },
            FixtureAssertion::PrivacyContext { expected: ProcessingContext::Document },
            FixtureAssertion::ParserMetadata {
                parser_id: "<parser-id>".to_string(),
                parser_version: "1.0.0".to_string(),
            },
        ],
        golden_artifact: None,
    }],
    acceptance: Some(FixtureAcceptanceContract {
        source_id: "<source-id>".to_string(),
        require_timestamp: true,
        require_timing: true,
        require_anchor: true,
        require_occurrence_identity: true,
        require_privacy_context: true,
        require_parser_metadata: true,
    }),
    expect_no_intents: false,
    expect_error: false,
    expected_error_contains: None,
    tags: vec!["parser-family".to_string()],
}
```

When the source contract is available, call
`spec.acceptance_failures(&parser.manifest(), Some(&SOURCE_CONTRACT))`
and assert that it returns no failures. The harness also runs the same
contract against the parser manifest during fixture execution, so missing
timestamp, occurrence, privacy, or event-pair evidence is visible as a fixture
failure instead of a review-only checklist.

### 10. Verification

```bash
xtask check -p sinexd

# Run only your tests by name filter:
xtask test -p sinexd -E 'test(/<your_test_names_pattern>/)'
```

All inline tests should pass. Parser/source acceptance belongs in fixture
assertions and Rust tests; do not add advisory verification tags in place of
enforced checks.

## What this guide intentionally does not cover

- **NixOS bindings.** New source contracts do not need a NixOS systemd unit
  unless they have a continuous runtime shape. Static-file parsers are
  on-demand (operator runs `sinexctl sources stage` + `sources replay`)
  and require no extra Nix wiring beyond the existing source
  service.
- **Live-deploy parity proof.** Each parser's #1070-style AC includes
  "query parity against Lynchpin export counts." That is operator work:
  stage the export under `/realm/data/exports/<provider>/`, run the
  parser job, compare counts. Not part of the PR.

## See also

- `crate/sinexd/src/node_sdk/parser/mod.rs` — `MaterialParser` trait
- `crate/sinexd/src/node_sdk/parser/adapters/` — adapter implementations
- `crate/sinex-primitives/src/parser/mod.rs` — `ParsedEventIntent`,
  `MaterialAnchor`, `OccurrenceKey`, `ParserManifest`
- `crate/sinexd/src/sources/source_contracts/weechat.rs` — the canonical
  append-only-file example
- `crate/sinexd/src/sources/source_contracts/music.rs` — the canonical
  static-file JSON-array example
- `crate/sinexd/src/sources/source_contracts/bookmark.rs` — the canonical
  CSV example
- `crate/sinexd/src/sources/source_contracts/messaging.rs` — the
  canonical per-file JSON-object example
- #1070 — the live tracker for remaining export parsers
