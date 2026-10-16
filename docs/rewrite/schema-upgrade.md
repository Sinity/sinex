# Guarded published-schema upgrade

The native schema owner recognizes the exact declarations published at
`4b5bf06197ffb1d07d57794f37f7273b634e0efd`, the recorded intermediate
state, and the current declarations. It refuses unrelated fingerprints,
changed definitions, missing objects, unsafe grants and change records that
are not the exact ordered prefix. The protected definition receipt is checked
against the live catalog before any change. No general migration framework or
production column rename is introduced.

`sinexd admin schema --connection-file FILE` creates a fresh database or
inspects an existing one. `--check` only inspects. An existing database with
waiting entries is reported with `pending_changes`; neither route changes
its schema, data, grants or progress. A fresh database has the same declared
catalog and applied-change records as an upgraded database.

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
source task or projection is started. Restart after explicit apply to start
the ordinary writer. No RPC mutation transport is added for schema changes.

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
