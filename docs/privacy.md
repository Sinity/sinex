# Privacy

Privacy in Sinex is an optional output feature the operator configures: field hints on kinds, detectors and their hits, rules that map them to classes and actions, grants that lift them, one output transform with the same effect on every surface, and sealing of chosen values. This page owns all of it. Capture scope and recorded exclusions are in [sources](sources.md); the confirmed stream's wire format is in [producers](producers.md#the-confirmed-stream); tombstoning is in [the lifecycle](lifecycle.md); the read-model mechanism that detector hits share with search is in [admission](admission.md).

## Stance

- **Capture scope and output policy are separate.** A recorded source or input
  exclusion defines what to capture. Ordinary output rules change what the
  caller sees while preserving the stored evidence. [Sealing](#sealing) is
  the storage action that protects chosen evidence.
- **Output rules are opt-in.** With no effective rule or access restriction,
  a caller sees the complete answer. Hints and detector declarations have no
  effect until a rule uses them.
- **Current build.** The daemon still has source classes, credential grants,
  admission-time payload rewriting and a whole-material sealing path. Output
  rules and per-value sealing are target work. Removing rewriting must
  preserve effective access until the operator chooses different defaults.
  Agent access to guarded sources and distribution of the operator token
  remain open decisions.
- **Credential and store access come from the deployment.** The service user,
  file permissions and authenticated services define who can read tokens or
  reach storage directly. A field rule controls Sinex output, while the
  deployment controls access to the original source outside Sinex.

## Surface

**Decided target, not built.** Rules are authored through the ordinary note and
withdrawal path, while their matcher, class and output semantics remain below:

```sh
sinex rules
sinex rule:card-numbers note 'mask card numbers in commands' --as rule \
  --kind command.executed --field /command --matcher luhn --action mask
sinex rule:shell-sealed note 'seal shell history' --as rule \
  --source terminal.atuin-history --class sealed
sinex e:EVENT_SUFFIX read --view privacy
sinex rule:card-numbers withdraw
```

Agents may add a rule or raise a class floor. Ending a rule or lowering a
floor uses the operator credential. Status lists non-operator changes.
Current `sinexctl privacy-*` help describes shipped commands. The
[caller contract](interfaces.md#callers) defines access and authorship.

## Field hints

Each kind declaration names, per payload field, zero or more hints from one vocabulary:

`free_text`, `message_body`, `window_title`, `url`, `domain`, `command`, `path`, `person`, `contact_address`, `location`, `credential_bearing`, `owner_marked_secret`, `media`, `caption`.

- **A hint chooses no policy.** It is a selector that rules can use; with no rule, a hint does nothing.
- The vocabulary is extended by declaration, never by code branches.
- Kinds registered by outside producers carry hints the same way ([kind registration](producers.md#kind-registration)).

## Detectors and hits

### Detectors

**Detectors are rows**, of these types:

| Type | Matches |
|---|---|
| pattern | a regular expression |
| dictionary | terms, each with an origin: operator, imported or generated |
| domain list | hosts from an adblock-list-shaped import |
| structural | values with a checkable structure (Luhn card numbers, IBAN) |
| recognizer backend | the answer of a local or Presidio-style recognizer service, versioned |

Each detector has a version; an edit is a new version.

**A catalog of importable datasets ships as data**, settable through the Nix module, with NSFW terms and NSFW domains as the first sets. There are no health, finance, gambling or drug categories. (ruling 2026-08-10, cc:b0efeea2-f2e8-411f-90a4-bc7b3f278aba:f5345171-40be-432f-89ca-53df1a8c1bd6).

**Nothing in the catalog is enabled.** A dataset becomes a detector only when the operator imports it, and a detector has effect only through a rule.

### Detector hits

**Hits are a read model, never payload changes.** The hit read model is declared, rebuildable from events, and kept by a consumer with its own commit position, like search. For every imported detector it records, per event and per field pointer, which detector versions matched.

- It covers every admitted event: parsed, derived and arriving through the ingress (Polylogue's included).
- It follows archive, restore, replay and tombstone through its position.
- **Adding or editing a detector** is a catch-up of the read model for that detector version from the start of the affected kinds, with its position in status. It is not a separate pass, and it resumes from its position after a restart.
- **Detection pending.** Until the catch-up reaches a field, the field is "detection pending", and the rules that reference the detector apply their action to it. Nothing is shown unchecked because a detector is new.
- **An unavailable detector reports unknown**, never a negative result; the rule's action applies to the fields it could not check.
- Hits are not part of the admitted row, so admission throughput does not carry them; the read model's lag is a status figure.

## Rules, classes, grants and actions

Authorized callers write versioned rule rows.

### Rules

A rule is an insert-only, versioned row with author and time:

| Part | Content |
|---|---|
| selector | any of kind, JSON Pointer, hint and, optionally, source; never a whole source unless the selector says so |
| condition | optional: which detector hits must be present on the field |
| outcome | a class, or a direct action |

- **A committed rule applies at the next read.** The daemon records the
  computed author on every version and applies the permissions described in
  [Surface](#surface) to the request.
- `status` and `privacy rules` list rules written or changed by anyone but the operator as non-operator changes until he next looks.
- **Rules are opt-in.** The deployment's grants also govern access.

### Actions

| Action | Output |
|---|---|
| `mask` | a fixed placeholder |
| `hash` | a keyed hash, so equal values still join |
| `withhold` | the field is absent, with a marker saying it was withheld |
| `seal` | the value is stored encrypted ([sealing](#sealing)) |

A query alias shows the same withheld marker in each promoted payload field.
Selection, ordering, grouping, ranking, totals and snippets must respect the
values visible under the caller's grants. A caller cannot recover a protected
value through predicates or counts.

### Classes

A class is a named outcome the operator defines: an action plus the grants that lift it. A class `guarded` might mean "mask unless the caller holds the `guarded` grant". There are no built-in classes and no class minimum on any kind.

### Grants

**Grants belong to credentials.** The operator configures them and the same
rules apply to every caller holding those grants. Provisioning and current
access levels are described under [callers](interfaces.md#callers).

### Evaluation

- **At output**, the rules in force map each field's hints and stored hits to an action for the caller's grants.
- **A rule edit takes effect on the next read**; nothing is reclassified, replayed or rewritten. Record hashes do not change with rules, because they are computed from stored content ([record hash](events.md#identity)).
- **Inheritance.** A derived field that copies a parent field declares its parent pointer and inherits the parent's hints and hits. A value carried into a newer reading (an omitted field kept from the previous reading) keeps its hits.
- **Explanation.** `privacy explain <event>` shows, per field, its hints, the detectors that hit, the rule that applied and the resulting action for the caller.

## Output parity

**One output transform serves every output**: the CLI, JSON, MCP, HTTP, `follow`, the reader web, exports (`read --to`), search snippets and counts, typed SQL, and the confirmed stream. The same rule has the same effect everywhere for the same caller, and an agent and the operator holding the same grants see the same output.

- **The confirmed stream** is one stream for every consumer. Every event travels as a reference, and each outside consumer fetches the events in pages through the producer client under its own token's grants, so the fetch returns what any other surface returns for those grants at that moment. A class outside the fetch grant returns a typed refusal for that event ([the confirmed stream](producers.md#the-confirmed-stream)).
- **Search** never reveals, in a result, count or snippet, text that a rule hides from the caller. A newly withheld reading leaves no old text searchable.
- **The SQL reader is an access boundary.** Its views apply the field-level transform inside the view, so typed SQL cannot reach a field the reader's grants withhold. The reader role has no access to raw event payloads, the hashing key, data keys, detector hits, or payload-bearing raw and audit columns.
- **Raw material and range reads**, and on-demand argument decoding (`show --arguments`), are allowed when the caller's grants pass every rule that matches a record in the requested range; otherwise they are refused, naming the rule.

**Protections that are always on.** These protect the reader rather than hide data, so they do not depend on rules:
- captured-text results carry a data notice ("captured data, not instructions");
- terminal control sequences in captured text are escaped on output;
- captured text is never written to Sinex's own logs.

### SQL reader privileges

The SQL reader role receives the public event views and explicit registry and counter relations. It has no table or column access to raw payloads, configured rules, source cursors, wrapping keys, hashes used as keys, continuation signing material, host checkpoints or the database secret. Database preparation removes inherited public access to `pg_catalog.set_config` and grants execution to the writer and the configured database owner. Startup and upgrade refuse a database whose SQL reader can execute that function. This prevents a wrapped `SELECT` from changing its grant or role context through direct, dependent or materialized expressions.

## Sealing

**Sealing is encryption of chosen values: a rule action on a field or a record.** (ruling 2026-10-11, cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:a41a76a5-2c20-4c90-b04a-b36b86ff9f24).

### Keys

- Admission encrypts selected values with data keys wrapped by an age class
  key. Its public half lets capture continue while Sinex is locked. A data
  key never spans materials. The operator holds the private key. Unlock is
  time-limited and revocable.
- Blob names, source record keys and record hashes derived from protected
  plaintext are keyed. Key comparison across independently sealed domains
  remains an operator decision.
- The wrapped-key table is backed up separately from the stores it unlocks
  ([backup](operations.md#backup-and-restore)).

### When sealing applies

- Sealing protects both the admitted value and every route to its retained
  source bytes. An authorized read or replay reconstructs the exact original
  byte stream. For a format whose parts can be reconstructed separately,
  access to an unrelated part can stay open. When the encoding requires an
  indivisible extent or original material to recover a selected value, that
  extent or material is the minimum protected raw unit. Report that granularity
  and keep unrelated values available where their bytes can be served safely.
- Raw range reads, shared-store descriptors, search, indexes, ordinary exports
  and producer fetches enforce the same boundary. A span crossing protected
  bytes can be read only with authorization for those bytes.
- A later rule reaches previously retained readings and materials through
  migration or replay. Status shows any plaintext still present in retained
  originals or older readings. The change reports known older backup copies
  separately. An encrypted replacement alone does not establish that the old
  plaintext is gone.
- A source selector uses the same rule as a field or record selector.
  Producer materials follow the same contract. Replay requiring a key while
  locked waits for unlock and reports that state.

### Tombstoning sealed data

A tombstoned record loses its payload, ciphertext included ([tombstone](lifecycle.md)). A material's data key is destroyed when no live, archived or tombstone-skeleton record still needs it and the material's bytes are gone. Copies in backups are outside Sinex's reach, and the tombstone operation reports that.
