# Declarative Parser — Design Lock

> Status: locked 2026-05-11. Source-of-truth for the #1100 → #1081 → #1132 wave.
>
> This document fixes design choices that downstream sub-agents must follow without re-deciding. If a downstream finding contradicts this lock, escalate to the top orchestrator before implementing — do not silently re-decide.

## Why this exists

The 6-ingestor-crate fold (#1081) requires source-worker dispatch to be data-driven. Today `crate/core/sinex-source-worker/src/dispatch.rs` is a single match arm that returns `Err("not yet wired")` even for the one wired parser (WeeChat). Folding 15+ source units through that match would create a 200-line dispatch table that #1100 then deletes.

The locked solution: parsers are **declared**, not coded. A `#[derive(SourceRecord)]` macro on a payload struct produces the `MaterialParser` impl, the `ParserManifest`, the per-field privacy invocations, and the timestamp/anchor/occurrence-key derivations. A YAML loader compiles the same shape from operator-authored specs. Both flow through one `DeclarativeParser` evaluator.

Per-field privacy annotations on the macro supersede the field-protection scope of #1042. A new `field_privacy_log` on `ParsedEventIntent` gives #1072 audit a richer surface.

## 1. DSL surface — hybrid

Two front-ends, one evaluator:

- **`#[derive(SourceRecord)]`** (canonical, in-tree) — proc-macro in `crate/lib/sinex-macros/`. Compile-time guarantees, IDE autocomplete, refactor-safe. Used for every parser shipped in tree.
- **YAML spec** (operator-authored, runtime-loadable) — loader in `crate/lib/sinex-node-sdk/src/parser/yaml_loader.rs`. Compiles into the same internal `DeclarativeParser` representation. Used for #1062 workbench-generated proposals and operator-defined sources that don't warrant a code change.

Both compile to one internal type: `DeclarativeParser` in `crate/lib/sinex-node-sdk/src/parser/declarative.rs`. The evaluator processes any `SourceRecord` uniformly; the front-end is just how the parser was authored.

**Escape hatch:** the imperative `MaterialParser` trait stays. If a parser genuinely needs custom state machines (e.g., document chunking with paragraph boundaries), it implements `MaterialParser` directly. The escape hatch must explain in its rustdoc why the DSL didn't fit.

## 2. `InputShapeKind` split — replace `EphemeralStream` with 4 typed variants

**Current** (`crate/lib/sinex-primitives/src/parser/mod.rs:253`): 9 variants, one of them (`EphemeralStream`) lumps "anything live with no durable material" into a generic bucket.

**Locked**: replace `EphemeralStream` with four concrete shapes:

```rust
pub enum InputShapeKind {
    StaticFile,
    Archive,
    DirectoryWalk,
    FileDrop,
    AppendOnlyFile,
    SqliteQuery,
    RepositorySnapshot,
    ApiCursor,
    // EphemeralStream removed; replaced by 4 typed shapes below
    Subprocess,         // NEW: long-lived child process emitting JSON lines (e.g. journalctl -f -o json)
    UnixSocket,         // NEW: line-delimited unix domain socket (e.g. Hyprland IPC)
    DbusSubscription,   // NEW: D-Bus signal subscription, anchor only
    Polling,            // NEW: poll-and-detect-change adapter (e.g. clipboard hash)
}
```

Total: 12 variants.

`as_str()` mappings: `subprocess`, `unix_socket`, `dbus_subscription`, `polling`.

**Migration impact**: the parser-side enum is currently consumed by `WeeChatLogParser::manifest()` only. The schema-side `DeclaredCoverageContractKind` enum in `crate/lib/sinex-schema/src/converge.rs:804` is independent and keeps its own variants. No DB rows reference the parser-side enum value `EphemeralStream` today.

## 3. Per-field privacy annotations — macro lowers to `privacy::process()`

The privacy engine at `crate/lib/sinex-primitives/src/privacy/mod.rs:50` (`pub fn process(...) -> ProcessResult`) stays unchanged. The macro generates per-field invocations rather than relying on the parser to remember to call it.

### Attribute catalog

On the struct (`#[source_record(...)]`):

| Attribute | Required | Notes |
|---|---|---|
| `id = "..."` | yes | `ParserId` value (kebab/dot-separated lowercase) |
| `source_unit_id = "..."` | yes | matches a `SourceUnitDescriptor.id` |
| `input_shape = "..."` | yes | one of the 12 `InputShapeKind` `as_str()` values |
| `version = "..."` | no | parser semver; defaults to `"1.0.0"` |
| `event_source = "..."` | no | defaults to first segment of `source_unit_id` |
| `event_type = "..."` | yes | event type emitted (single per record; for multi-output use the imperative escape hatch) |
| `default_privacy_context = "..."` | no | applied to fields without an explicit `#[privacy(...)]` attribute; defaults to the source unit's `PrivacyTier`-derived context |

On individual fields:

| Attribute | Lowering | Notes |
|---|---|---|
| `#[timestamp(format = "...", fallback = "...")]` | extract field, parse timestamp, fall back to `material_timing` if missing/invalid | `format` ∈ `unix_seconds`, `unix_seconds_nanos`, `unix_millis`, `unix_micros`, `rfc3339`, `iso8601`. `fallback` ∈ `material_timing`, `error`, `default(<rfc3339-literal>)` |
| `#[privacy(context = "...")]` | call `privacy::process(value, ProcessingContext::<context>)` before placing in payload; record `FieldPrivacyDecision` in `field_privacy_log` | `context` ∈ all `ProcessingContext` variants |
| `#[suppress_if(field = "...")]` | check binding-config or environment field at parse time; if true, drop this field from payload (or drop entire event if `whole_event = true`) | enables #1071 runtime private-mode without engine changes |
| `#[occurrence_key]` | include this field in the parser's `OccurrenceKey.fields`; multiple fields concatenate in declaration order | composite keys are normal — annotate each contributing field |
| `#[anchor(kind = "...")]` | set `MaterialAnchor` to derived value from this field | `kind` ∈ `byte_offset`, `line_byte_range`, `sqlite_row`, `directory_entry`, etc. — must match `InputShapeKind`'s anchor model |
| `#[required]` | reject record if field is missing/null | default for all non-Option fields |
| `#[default = "..."]` | use literal value if missing | accepts string, number, bool literals |
| `#[redact_if(rule = "...")]` | apply redaction strategy by name from a privacy-rule registry | rule names are operator-extensible |
| `#[skip]` | exclude field from emitted payload | useful for fields used only for `#[occurrence_key]` or `#[anchor]` |

### Worked example

```rust
use sinex_macros::SourceRecord;

#[derive(SourceRecord)]
#[source_record(
    id = "atuin-history",
    source_unit_id = "terminal.atuin-history",
    input_shape = "sqlite_query",
    event_type = "command.executed",
)]
pub struct AtuinHistoryRecord {
    #[timestamp(format = "unix_seconds_nanos", fallback = "material_timing")]
    pub timestamp: i64,

    #[privacy(context = "Command")]
    #[suppress_if(field = "private_mode_active")]
    pub command: String,

    #[occurrence_key]
    #[anchor(kind = "sqlite_row")]
    #[skip]
    pub rowid: i64,

    #[occurrence_key]
    pub session: String,

    #[default = "0"]
    pub exit: i32,
}
```

Macro output (sketch):

```rust
impl ::sinex_node_sdk::parser::MaterialParser for AtuinHistoryRecord {
    type Config = ();
    fn manifest(&self) -> ::sinex_node_sdk::parser::ParserManifest { /* generated */ }
    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let raw: AtuinHistoryRecordRaw = record.deserialize_json()?;
        let mut field_privacy_log = Vec::new();

        // #[timestamp]
        let ts_orig = parse_unix_seconds_nanos(raw.timestamp)
            .unwrap_or_else(|| record.material_timing());

        // #[privacy(Command)] + #[suppress_if]
        let suppressed = ctx.binding_field_bool("private_mode_active");
        let command = if suppressed {
            field_privacy_log.push(FieldPrivacyDecision::suppressed("command", ProcessingContext::Command));
            None
        } else {
            let result = ::sinex_primitives::privacy::process(&raw.command, ProcessingContext::Command);
            field_privacy_log.push(FieldPrivacyDecision::processed("command", &result));
            Some(result.text.into_owned())
        };

        // #[occurrence_key] composite
        let occurrence_key = OccurrenceKey {
            source_unit_id: source_unit_id_const(),
            fields: vec![
                ("rowid".into(), raw.rowid.to_string()),
                ("session".into(), raw.session.clone()),
            ],
        };

        // #[anchor]
        let anchor = MaterialAnchor::SqliteRow { rowid: raw.rowid };

        // payload (skipping #[skip] fields, applying #[default])
        let payload = serde_json::json!({
            "command": command,
            "session": raw.session,
            "exit": raw.exit,
            "timestamp": raw.timestamp,
        });

        Ok(vec![ParsedEventIntent {
            source_unit_id: source_unit_id_const(),
            parser_id: parser_id_const(),
            parser_version: "1.0.0".into(),
            event_type: EventType::from_static("command.executed"),
            event_source: EventSource::from_static("terminal"),
            payload,
            ts_orig,
            timing: TimingEvidence::IntrinsicField { field: "timestamp".into() },
            anchor,
            occurrence_key: Some(occurrence_key),
            privacy_context: ProcessingContext::Command,
            field_privacy_log: Some(field_privacy_log),
        }])
    }
}
```

## 4. `ParsedEventIntent.field_privacy_log`

Add to `crate/lib/sinex-primitives/src/parser/mod.rs:467`:

```rust
pub struct ParsedEventIntent {
    // ... existing fields ...
    pub privacy_context: crate::privacy::ProcessingContext,

    /// Per-field privacy decisions made during parsing.
    /// `None` for imperative parsers that don't populate it (backward-compat).
    /// `Some(vec)` for declarative parsers; the macro emits one entry per
    /// privacy-relevant field. Consumed by #1072 audit/export/redact CLI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_privacy_log: Option<Vec<FieldPrivacyDecision>>,
}
```

`FieldPrivacyDecision` (new, in `crate/lib/sinex-primitives/src/privacy/field.rs`):

```rust
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FieldPrivacyDecision {
    pub field: String,                  // e.g. "command"
    pub context: ProcessingContext,     // e.g. ProcessingContext::Command
    pub strategy: Option<Strategy>,     // None = no rule matched
    pub matched_rules: Vec<String>,     // rule names that fired
    pub redacted: bool,                 // any value substitution occurred
    pub suppressed: bool,               // field dropped from payload entirely
    pub whole_event_suppressed: bool,   // event itself dropped
}
```

Optional helper (used by macro-generated code):

```rust
pub fn parser_field_privacy(
    field_name: &str,
    value: &str,
    context: ProcessingContext,
) -> (String, FieldPrivacyDecision) { /* wraps privacy::process */ }
```

**Backward-compat invariant:** existing imperative parsers that construct `ParsedEventIntent` without setting `field_privacy_log` continue to compile and behave identically. The field is `Option`, default `None`, `serde(skip_serializing_if = "Option::is_none")` so wire format is unchanged when absent.

## 5. Source-bindings shape — `nixos/modules/source-bindings.nix`

New Nix module. Each source-binding entry produces:

```nix
{
  sourceUnit = "terminal.atuin-history";  # matches SourceUnitDescriptor.id
  parserId   = "atuin-history";            # matches ParserManifest.parser_id
  adapter = {
    kind = "sqlite_query";                 # matches InputShapeKind
    config = {                              # passed to adapter Config struct
      path = "/home/sinity/.local/share/atuin/history.db";
      query = "SELECT id, timestamp, command, session, exit FROM history";
    };
  };
  binding = {                               # passed as ParserContext.binding fields
    private_mode_active = false;            # consumed by #[suppress_if]
  };
  privacyOverrides = {                      # operator-level overrides on top of macro defaults
    # field = "...command"; context = "Suppress";  # if needed
  };
  systemd = {                               # mkSourceWorkerUnit inputs
    afterUnits = [ "atuin.service" ];
    requiresUnits = [ ];
    instances = 1;
  };
}
```

The module renders these into per-source `mkSourceWorkerUnit` invocations + a JSON spec file at `${cfg.stateDir}/source-bindings.json` that source-worker reads at startup. The JSON content matches the shape `SourceBinding` deserializes from in `sinex-source-worker`.

## 6. Migration approach — single feature branch, atomic merge

Branch: `feature/source-worker-fold` (already created from `master`).

All Phase 1–5 work lands on this branch. No partial merges to `master`. Master's deployment-broken state (since commit `39216413d`) is not extended by piecemeal landings — it's resolved in one merge.

If a phase fails verification, roll back commits on the feature branch; do not push intermediate states to `master`.

## DeclarativeParser evaluator architecture

```
SourceRecord (from InputShapeAdapter)
    |
    v
DeclarativeParser::evaluate(record, context)
    |
    +-- decode record bytes per InputShapeKind:
    |     StaticFile/AppendOnlyFile/Subprocess -> JSON / line / framed
    |     SqliteQuery -> already-deserialized row
    |     UnixSocket -> line + Hyprland-event parser
    |     DbusSubscription -> already-deserialized signal
    |     ClipboardPolling -> {hash, timestamp, optional-text}
    |     FileDrop -> {path, op, metadata}
    |
    +-- for each field declaration in the spec:
    |     extract value via JSON Pointer / column name / regex capture
    |     apply conversion (string, integer, timestamp, etc.)
    |     apply privacy::process() if context declared (records FieldPrivacyDecision)
    |     check #[suppress_if] (records suppressed, may drop event)
    |
    +-- derive timestamp + anchor + occurrence_key from annotated fields
    +-- assemble ParsedEventIntent with field_privacy_log
    |
    v
Vec<ParsedEventIntent>  (or empty if whole-event suppressed)
```

The evaluator lives in `crate/lib/sinex-node-sdk/src/parser/declarative.rs`. It is the same code path whether the parser was authored via `#[derive(SourceRecord)]` or YAML — the macro and YAML loader just produce different `DeclarativeParserSpec` values that the same evaluator runs.

## What stays out of scope

- **Document chunking + entity extraction** — these stay in `sinex-process` automata; they consume confirmed `document.ingested` events and emit `document.parsed`/`document.chunked`. Source acquisition is what folds; semantic synthesis is not.
- **Multi-event-type-per-record parsers** — the DSL emits one event type per record. Imperative escape hatch handles N-output parsers (rare).
- **Cross-record aggregation** — parsers see one record at a time. Aggregation belongs in automata.
- **Schema migration of in-flight events** — parser version bumps create new event interpretations; #1058's reverted occurrence-table model is not re-introduced.

## Sequencing

Phase 0 lock fixes the surface. Phase 1 sub-tracks build the substrate. Phase 2 wires source-worker around the substrate. Phase 3 migrates ingestors via the substrate. Phase 4 proves it all through the real binary. Phase 5 closes out.

Sub-orchestrators read this document before fanning out to leaves. Leaves have it in their context bundle.
