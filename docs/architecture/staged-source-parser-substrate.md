# Staged Source Parser Substrate

Status: design direction for new source work. Issue
[#1054](https://github.com/Sinity/sinex/issues/1054) owns the remaining
process-topology decision.

Sinex source ingestion is moving from "one crate per source domain" toward a
smaller substrate:

```text
source material -> input-shape adapter -> parser -> material-provenance events
```

This does not replace the event model, provenance rules, replay, schema
validation, gateway, or automata. It changes where raw source interpretation
lives and what deserves a runtime process boundary.

## Why

`sinexctl sources stage` registers source material. It is not itself an
ingestor. Once material is staged, some runtime still needs to enumerate bytes
or records, dispatch a parser, and persist interpreted events.

The previous mental model bundled unrelated mechanics into source-domain crates.
For example, "terminal ingestor" contains several different source shapes:

- Atuin: growing SQLite database plus shell-command parser
- zsh/bash history: append-only file plus shell-history parser
- Kitty scrollback or OSC: terminal stream plus terminal parser
- Asciinema: staged file or directory plus recording parser

Those are not one runtime category. They are input-shape plus parser pairs that
happen to emit terminal-shaped events.

The sensd lesson is also part of this direction. A universal daemon that merely
acquires bytes and hands them to parsers does not automatically earn a separate
process. Generic acquisition and parser dispatch are useful substrate. The
process boundary must still be justified by a concrete operational need:
privilege separation, live stream isolation, external extensibility, restart
granularity, or transport fan-out.

## Terms

| Term | Responsibility |
| --- | --- |
| Source material | Durable ground truth registered in `raw.source_material_registry`: file, directory snapshot, SQLite DB, API dump, stream segment, or other captured bytes. |
| Stager | Registers source material and records enough identity, checksum, path, and operator intent to make later processing auditable. |
| Input-shape adapter | Knows how to enumerate or tail a material shape: SQLite row cursor, append-only byte cursor, directory file list, static dump, API cursor, IPC frame stream. |
| Parser | Deterministic interpretation from records/bytes to typed payloads, timestamps, anchors, privacy contexts, and natural keys. |
| Source unit | Declarative binding of source identity, input shape, parser, emitted `(source, event_type)` pairs, checkpoint family, horizons, privacy tier, and runtime policy. |
| Runtime topology | Where the source unit runs: in `sinex-ingestd`, in a shared source processor, in a lightweight capture node, or as an external plugin/process. |

## Runtime Topology

Parser locality and process count are independent decisions.

Parser locality asks whether source interpretation lives in bespoke source
crates or in a shared parser registry over input-shape adapters. The target
direction is the registry/substrate model.

Process count asks whether that substrate runs in-process with persistence,
in separate managed nodes, or outside the main codebase. That remains open in
#1054. The default should be conservative:

- staged local files, directories, dumps, and databases should not get a new
  long-running ingestor crate by default
- live or privileged streams may keep a separate capture process when the
  boundary does real work
- external/user-written parsers may publish over NATS or another extension
  boundary rather than linking into `sinex-ingestd`

This yields a likely two-tier shape:

1. Local staged-source processing runs close to persistence, possibly inside
   `sinex-ingestd` or a small shared source processor, and uses the same
   validation/repository code as ordinary ingestion.
2. Live capture processes remain for ephemeral streams, target-user bridges,
   browser extension/native messaging, D-Bus, Hyprland IPC, audio/screen
   capture, and other cases where process isolation or target-session access is
   useful.

## NATS Role

NATS is not automatically load-bearing for staged local material parsing on a
single machine. If staged material is already local and parsing runs inside the
same trust boundary as persistence, an in-process path can be simpler than
publishing event batches only to consume them immediately.

NATS still has plausible roles:

- live external producers publishing event or material frames
- confirmations and DLQ streams
- replay progress, invalidation, and control-plane fan-out
- derived automata subscriptions
- extension boundary for user-written or third-party parsers
- buffering when a producer must continue while persistence is temporarily down

#1054 decides which roles stay mandatory and which become topology choices.
New source work should not assume that every staged parser must publish through
NATS, and also should not assume NATS disappears from the system.

## Provenance And Replay

Parsers over staged source material emit material-provenance events. Each event
must identify:

- `source_material_id`
- stable anchor, normally `anchor_byte` or a shape-specific anchor mapped onto
  the event anchor contract
- parser id and semantics version, either in payload metadata, source-unit
  descriptor data, or a future parser registry table
- natural key when the source domain has one

Replay targets the interpretation, not the material. A replay operation should
archive events created by a given source unit/parser version over a material
scope, then rerun the parser through the normal validation and persistence path.
The original source material remains the ground truth.

Derived automata remain synthesis-provenance processors. The parser substrate is
for translating source material into raw/material events. Later normalization,
summaries, embeddings, entity extraction, and analytics continue to derive from
parent events unless they are explicitly reclassified as material parsing.

Document parsing is the important boundary case. If a document file itself is
the source material, initial text extraction and chunk anchoring belong in this
substrate. Later semantic summaries, embeddings, entities, and retrieval aids
are synthesis.

## Source Classification

| Source shape | Examples | Default architecture |
| --- | --- | --- |
| Static file or export | Spotify GDPR, Reddit GDPR, Samsung Health export, Sleep as Android export, Goodreads CSV | Stage file, dispatch parser, emit material events. |
| Directory of files | screenshots, recordings, document intake, WeeChat logs | Stage directory or file set, enumerate files with directory adapter, dispatch per-format parser. |
| Growing SQLite DB | Atuin, ActivityWatch, browser history | SQLite input adapter plus parser; runtime topology chosen by liveness and locking needs. |
| Append-only log | zsh/bash history, scribe-tap JSONL, IRC logs | Append-only adapter plus parser; continuous mode may share source processor or use a watcher. |
| Local repository | git history | Repository/directory adapter plus git parser; not a `sinex-git-ingestor` crate by default. |
| Ephemeral IPC/API stream | Hyprland IPC, D-Bus signals, browser native messaging, live audio/screen capture | Separate capture process often justified; publish material frames or events through the runtime transport. |

## Migration Rules

1. Do not add new permanent source-domain ingestor crates for staged or
   replayable materials unless the process boundary is explicitly justified.
2. Implement new source issues as source-unit descriptors plus parser modules
   over shared input-shape adapters where possible.
3. Keep existing deployed ingestors working while the substrate is designed.
   They are legacy placement, not proof that every future source belongs in a
   matching crate.
4. When a parser needs shared mechanics that are missing, add the input-shape
   adapter to the shared substrate rather than copying watcher/checkpoint logic.
5. Let #1054 settle whether the first implementation lands inside
   `sinex-ingestd`, a shared source processor, or the node SDK runtime.

## Immediate Issue Impact

- #1051 WeeChat: append-only/directory log parser over staged materials.
- #1052 health exports: static export parser over staged files/directories.
- #1053 git history: repository/directory parser over staged repo material.
- #1033 Asciinema: staged file parser; Kitty OSC and D-Bus remain live stream
  questions.
- #1043 historical screenshot/audio directories: directory parser; live capture
  remains a runtime capture question.
