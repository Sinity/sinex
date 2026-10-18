# Guarded published-schema upgrade

The native schema owner recognizes the exact declarations published at
`4b5bf06197ffb1d07d57794f37f7273b634e0efd`, the recorded intermediate
states, and the ordered session, Chromium, metadata/text and signed Atuin declarations. It refuses unrelated fingerprints,
changed definitions, missing objects, unsafe grants and change records that
are not the exact ordered prefix. The protected definition receipt is checked
against the live catalog before any change. No general migration framework or
production column rename is introduced.

`sinexd admin schema --connection-file FILE` creates a fresh database or
inspects an existing one. `--check` only inspects. An existing database with
waiting entries is reported with `pending_changes`; neither route changes
its schema, data, grants or progress. A fresh database invokes the same real entry guards before target creation;
missing old objects are a no-op. It records those entries after target creation
and has the same declared catalog as an upgraded database. Admin output reports
the ordered `checked_changes` and each guard outcome.

The query-free operator route applies a populated supported predecessor:

```
sinexd admin schema --connection-file /absolute/admin-connection \
  --apply --dump-file /absolute/new-backup.dump
```

The admin connection needs the same authority as fresh bootstrap. The route
excludes another schema owner and the normal writer, completes a custom
`pg_dump` into a new mode-0600 file and fsyncs it before the first mutation.
A missing, failed or empty dump refuses all mutations, including progress
metadata creation. Raw dump diagnostics stay in the adjacent mode-0600
`DUMP.stderr` file. Both files and the owning directory are fsynced before DDL. Failed partial dumps stay available for diagnosis; choose
a new path when retrying. A completed upgrade is a no-op on repeat and does
not create another dump.

`schema_changes()` is one fixed Rust list. Entry 1 widens the retained file and
two-parent shell constraints, registers imported zsh events and payloads, and
creates protected applied-change records. Entry 2 adds and fills the dirty
frontier from the existing commit counter, creates the shell progress
singleton, seeds existing eligible raw Atuin heads for normal writer
recomputation, and grants only the declared capabilities. Each entry commits
its schema, progress and definition receipt together. A stopped process
resumes at the checked remaining prefix. Final definition and grants checks
must succeed before normal writer admission.

The published single-source payload schema 3 and its version stay unchanged.
The fusion contract is registered separately as schema 5; new canonical events
cite 5. Disjoint constraint branches admit legacy schema-3 singleton rows and
schema-5 one- or two-parent rows under their actual contracts.

Existing event IDs, payloads, provenance, retained bytes and parser producer
configurations remain intact during the upgrade. Old parser configurations do
not acquire a cross-source comparison claim. The normal writer may replace old
singleton canonical interpretations after restart; retained replay is still
required before an unsupported historical comparison can become available.

While changes wait, `serve` reserves its configured endpoint and opens only
the restricted reader. Status names the waiting entries and unavailable
writer, acquisition, replay, exclusion and canonical/indexed paths. Verified
installed-schema raw queries with an explicit noncanonical source or kind,
and retained show/trace, remain available. No state directory, writer run,
source task or projection is started. An existing pending daemon reports `restart_required` once apply has completed.
Restart after explicit apply to start the ordinary writer. No RPC mutation transport is added for schema changes.

`schema_upgrade_contracts` checks the fixed released declaration fingerprint
and decoded dump connection fields without a database. The dump process receives
resolved connection fields, including password/defaults and TLS settings, through
libpq environment variables. It clears inherited PG variables and disables a
second password-file lookup, so services and hostaddr cannot redirect the dump.
Its argv contains no connection URI or password. TLS certificate/key inputs
for this dump route must name files; inline PEM inputs refuse before mutation.

The declared `schema_upgrade_acceptance` operation takes
`--predecessor-package /nix/store/OUTPUT --predecessor-publication FILE`. It
builds the actual candidate package and uses an owned PostgreSQL cluster. A
real published predecessor admits an independently stated synthetic Atuin
fixture before the candidate inspects it. An independent table lock holds
entry 2 after committed entry 1; the harness observes both the recorded prefix
and waiting DDL, kills only the recorded admin process, then resumes through
the ordinary route. It checks dump failure without mutation, refusal controls,
fresh/upgraded catalog equality, retained rows and trace bytes, reader denials,
repeat no-op, normal writer recomputation and later admission. Raw output,
source/package bindings and failed state remain in the private report.


Pass `--review-controls` to the acceptance operation to select the affected
shared-guard ordering, actual child dump connection, progress resume, fresh
catalog equality and surviving pending-process status controls. Unchanged full
writer and refusal qualification stays separate; this mode does not claim to
repeat it.

Entry 3 registers the [selected shell session](shell-sessions.md) contract and seeds durable logical-day dirty work from existing canonical recorded spans, using the same range rule as admission and archival. Its qualification uses the unchanged runtime of published `88a7a641de4517307a0e723e40ecac8ff50aa7d7`, with the original immutable package and the retained publication binding, rather than synthesizing a predecessor schema.

## Metadata and full-text availability

Ordered change 5 recognizes the exact published stage-4 catalog, makes `core.search_text.tsv` nullable and adds the declared partial unavailable-vector index. Existing vectors and all canonical/raw events, source observations, retained materials and operation evidence remain unchanged. The ordinary guarded `--apply --dump-file` route is required; an ordinary schema check reports the pending entry without applying it. Fresh and upgraded catalogs must agree. The previous writer refuses the new fingerprint. The separate retrieval-basis change rebuilds derived metadata/text readiness and invalidates old indexed cursor generations; it never remints source events or rewrites raw evidence. See [indexed retrieval](indexed-retrieval.md) for independent metadata readiness, exact text refusal and bounded diagnostics.

## Signed Atuin status domain

Ordered change 6 relaxes only registered Atuin payload schema 2 to admit signed provider exit integers. It preserves every earlier stage fingerprint and the retired single-source canonical schema 3. The current fingerprint additionally binds the sorted Atuin payload declaration, so an old writer refuses the relaxed catalog. Fresh creation and guarded populated upgrade have identical catalogs. No event, retained byte, reading order, source identity, query column or limit changes during schema application. Parser recipe `atuin-v3` readmission and retained replay are separate ordinary writer operations; they preserve `-1` as unknown and other signed values exactly. See [Atuin](atuin.md) for the provider meaning and bounded controls.
