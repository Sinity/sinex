# Privacy

Privacy in Sinex is an optional output feature the operator configures: field hints on kinds, detectors and their hits, rules that map them to classes and actions, grants that lift them, one output transform with the same effect on every surface, and sealing of chosen values. This page owns all of it. Capture scope and recorded exclusions are in [sources](sources.md); the confirmed stream's wire format is in [producers](producers.md#the-confirmed-stream); tombstoning is in [the lifecycle](lifecycle.md); the read-model mechanism that detector hits share with search is in [admission](admission.md).

## Stance

- **Privacy never limits capture.** Captured bytes and stored payloads are exact and are never rewritten for privacy. "Never this input" is a recorded source or input exclusion ([sources](sources.md)); everything else is an output rule.
- **With no operator configuration, every output is complete and unaltered for every caller.** No rule, class, detector or minimum is active by default, and no secret, credential, personal-data or content category is detected or hidden unless the operator configures it. There is no built-in censoring and no credential masking: a secret in history that matters is edited out of its source by the operator, or by an agent he asks, as an exceptional act rather than a system.
- **Rules act at output, not at admission.** Stored payloads stay raw. A rule change invalidates the output epoch immediately; the next read reports unavailable until its bounded projection build atomically activates. Original events and source bytes are never replayed or rewritten for an output-rule change. Sealing is the one action applied when a reading is stored ([sealing](#sealing)).
- **Confidentiality comes from the deployment**: one machine, the service user, file permissions and authenticated services.
- **Sending data to model providers or archive APIs is acceptable**; privacy is no reason to leave a feature unused. Ruling, 2026-08-12: "absurd rly. why woudl we build a feature to not use it?" (cc:049c6f6c-9252-44c7-a0b2-3a585f63b686:891f917e-080f-4506-835b-5f8eaa94e456).
- **Privacy is low priority** next to capture, storage and performance. Ruling, 2026-06-15: "privacy setup is considered extra feature, not something so fundamental as you always make it to be" (cc:1cb95099-0044-4613-97e1-1bac25f8077a:6a77d81c-0e0f-47fa-9e60-715826cd3e6f).

## Field hints

Each kind declaration names, per payload field, zero or more hints from one vocabulary:

`free_text`, `message_body`, `window_title`, `url`, `domain`, `command`, `path`, `person`, `contact_address`, `location`, `credential_bearing`, `owner_marked_secret`, `media`, `caption`.

- **A hint chooses no policy.** It is a selector that rules can use; with no rule, a hint does nothing.
- The vocabulary is extended by declaration, never by code branches.
- Kinds registered by outside producers carry hints the same way ([kind registration](producers.md#kind-registration)); each kind's hints will be listed in the [planned kinds reference](reference/README.md#planned-pages).

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

**A catalog of importable datasets ships as data**, settable through the Nix module, with NSFW terms and NSFW domains as the first sets, because adult content is the category that matters most. There are no health, finance, gambling or drug categories. Ruling, 2026-08-10: "singling these out would be just weird. the actual most important category tho is NSFW adult stuff" (cc:b0efeea2-f2e8-411f-90a4-bc7b3f278aba:f5345171-40be-432f-89ca-53df1a8c1bd6).

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

**Rules are database rows the operator (or anyone) writes, not code.** Ruling, 2026-06-01: "privacy rules are supposed to be database+user driven. not in imperative code like this" (codex:019e7fe3-c012-7a50-a128-cb9f3ba376d4@2026-06-01T02:23Z).

### Rules

A rule is an insert-only, versioned row with author and time:

| Part | Content |
|---|---|
| selector | any of kind, JSON Pointer, hint and, optionally, source; never a whole source unless the selector says so |
| condition | optional: which detector hits must be present on the field |
| outcome | a class, or a direct action |

- **A rule applies from any author.** Agents write rules with the same commands as the operator, and a rule takes effect when written, whoever wrote it. Authorship, computed by the daemon, is recorded on every version.
- `status` and `privacy rules` list rules written or changed by anyone but the operator as non-operator changes until he next looks.
- **Nothing ships enabled**: with no rule written, nothing is withheld. Rules are opt-in.

### Actions

| Action | Output |
|---|---|
| `mask` | a fixed placeholder |
| `hash` | a keyed hash, so equal values still join |
| `withhold` | the field is absent, with a marker saying it was withheld |
| `seal` | the value is stored encrypted ([sealing](#sealing)) |

### Classes

A class is a named outcome the operator defines: an action plus the grants that lift it. A class `guarded` might mean "mask unless the caller holds the `guarded` grant". There are no built-in classes and no class minimum on any kind.

### Grants

**Grants belong to tokens** and are configured by the operator. A new token starts with every grant. No rule names "agent" as a category and no output is refused because the caller is an agent: a token's grants decide, the same way for every caller ([callers](interfaces.md)).

### Evaluation

- **At output**, the rules in force map each field's hints and stored hits to an action for the caller's grants.
- **A rule edit invalidates the output epoch immediately**. Reads report pending until the transformed projection activates; original events and materials remain unchanged. Record hashes do not change with rules, because they are computed from stored content ([record hash](events.md#identity)).
- **Inheritance.** A derived field that copies a parent field declares its parent pointer and inherits the parent's hints and hits. A value carried into a newer reading (an omitted field kept from the previous reading) keeps its hits.
- **Explanation.** `privacy explain <event>` shows, per field, its hints, the detectors that hit, the rule that applied and the resulting action for the caller.

## Policy epochs and original retention

The configured Rust rule set produces one grant-independent output projection. SQL views apply caller grants to that projection; payload reads, text search and vector visibility use the same active policy epoch. The database invalidates the desired epoch in the transaction that changes a rule or source floor. Until the desired epoch is fully projected, reads report `privacy_projection_stale` rather than returning an older policy result.

A rebuild transfers retained events in bounded pages and transactions while admission continues. Admission preserves the original payload and source key in private storage, records an original-preservation marker and projects the new reading under the desired epoch in the same transaction. The final activation compares the completed position with the current commit head under a short policy lock. Interrupted builds resume from their stored position; no caller receives a partly activated epoch.

Rows admitted by the predecessor may already have lost values to admission-time masking. The first bounded build records a permanent custody decision from retained historical rules, floors and the run's recorded rule position; a rule ending or privileged history deletion cannot later promote an uncertain original. Such a row is `unavailable_original` when historical rules or floors prevent proving its original state. Ending a rule restores new preserved originals; it does not invent missing historical content. Controlled source re-derivation is required for lost originals. Descendants whose input transformation cannot be established are explicitly unavailable. A controlled re-derivation reads the current transformed inputs and records that policy epoch with its read mark. The runner pages through current input keys and previous output keys using ordinary recompute batches; the same writer transaction saves that refresh progress or resets an existing per-event position. Admission certifies a repeated identity only when its complete offered payload, content hash, kind, source and exact parents equal the retained original. Thus a safe recomputation can restore unchanged identities without treating suppression alone as evidence. Their payload and search text are excluded, and vector visibility also excludes changed or unavailable targets, embeddings and ancestors. Original vector components remain private and unchanged.

Stored derivation comparison examples are private audit originals. A preview records its active policy epoch, transformed-output availability revision and canonical caller grants. Listing examples or switching a producer requires that same current scope; older or narrower reads return explicit unavailable status. A live comparison checks its input grants and applies the same Rust rule set to hypothetical new outputs before exposing examples. Stored examples are never processed by an API-specific detector or rewritten when a rule changes.

## Output parity

**One output transform serves every output**: the CLI, JSON, MCP, HTTP, `follow`, the reader web, exports (`read --to`), search snippets and counts, typed SQL, and the confirmed stream. The same rule has the same effect everywhere for the same caller, and an agent and the operator holding the same grants see the same output.

- **The confirmed stream** is one stream for every consumer. It carries only event IDs and original positions; metadata and payload are fetched by event ID through the producer client under the current policy and the token's grants. An unavailable projection or a class outside the fetch grant returns a typed refusal ([the confirmed stream](producers.md#the-confirmed-stream)).
- **Search** never reveals, in a result, count or snippet, text that a rule hides from the caller. A newly withheld reading leaves no old text searchable.
- **The SQL reader is an access boundary.** Its views apply the field-level transform inside the view, so typed SQL cannot reach a field the reader's grants withhold. The reader role has no access to raw event payloads, the hashing key, data keys, detector hits, or payload-bearing raw and audit columns.
- **Raw material and range reads**, and on-demand argument decoding (`show --arguments`), are allowed when the caller's grants pass every rule that matches a record in the requested range; otherwise they are refused, naming the rule.

**Protections that are always on.** These protect the reader rather than hide data, so they do not depend on rules:
- captured-text results carry a data notice ("captured data, not instructions");
- terminal control sequences in captured text are escaped on output;
- captured text is never written to Sinex's own logs.

## Sealing

**Sealing is encryption of chosen values: a rule action on a field or a record.** Ruling, 2026-10-11: "how would privacy design work without encryption?" (cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:a41a76a5-2c20-4c90-b04a-b36b86ff9f24).

### Keys

- Admission encrypts the selected values under the **material's data key**. The data key is wrapped by an **age class key**, whose public half lets capture continue while Sinex is locked.
- **A key never spans materials**, and unrelated records and fields in the same material stay open: sealing one value never seals another alongside it.
- The class key's secret is held by the operator (a YubiKey or a passphrase file); Sinex never stores it. Unlock is a time-limited, revocable act by the key holder.
- Blob names, source record keys and record hashes derived from sealed content are keyed, so plaintext names never reveal sealed data.
- The wrapped-key table is backed up separately from the stores it unlocks ([backup](operations.md)).

### When sealing applies

- **Sealing is applied when a reading is stored.** A later sealing rule reaches older readings by replay, after which the old readings can be archived and tombstoned. While the material stays open, the plaintext remains in its bytes, and status says so.
- **Material bytes are stored sealed only when every record in the material is sealed.** A material that also holds open records stays open.
- Per-source sealing is a rule whose selector is a source; there is no separate per-source mode. Producer materials (Polylogue's) follow the same rule action.
- **Replay of sealed records while locked** is deferred and listed, not failed.

### Tombstoning sealed data

A tombstoned record loses its payload, ciphertext included ([tombstone](lifecycle.md)). A material's data key is destroyed when no live, archived or tombstone-skeleton record still needs it and the material's bytes are gone. Copies in backups are outside Sinex's reach, and the tombstone operation reports that.

## Laws

- **off-by-default**: with no rules written, every output for every token equals the stored payload, and no detector runs.
- **nothing-rewritten**: no rule, detector or class changes a stored payload or material byte, and record hashes are equal before and after any rule change.
- **rule-edit-immediate**: after adding, editing or ending a rule, the next read through every surface observes the new policy or explicit stale-unavailable state, whoever wrote the rule. A completed projection activates atomically; no older policy output is returned while it builds.
- **output-parity**: one configured rule gives the same result through CLI, JSON, MCP, HTTP, `follow`, the reader, exports, search snippets and counts, and SQL views, per token grant; the confirmed stream carries only references and authorized fetches obey those same current output rules.
- **agent-equality**: two tokens with equal grants, one an agent's, receive identical output.
- **any-author**: a rule written by an agent invalidates the current output epoch immediately, like the operator's, and `status` lists it as a non-operator change.
- **detection-pending**: a new detector's catch-up resumes from its position after a restart and is reported; fields it has not reached get the rule's action.
- **inheritance**: a derived field and a carried-over field keep their parent's hits.
- **reader-role-boundary**: the SQL reader role cannot read raw payloads, keys or hits.
- **range-read-refused-by-rule**: a raw range containing a field a rule hides from the caller is refused, naming the rule.
- **sealing-scope**: sealing one field leaves the material's other records readable without a key.
- **locked-replay-deferred**: replay of sealed records while locked is deferred and listed.
