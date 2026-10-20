# Architecture

This describes the current Rust ownership boundaries. The specification and
Beads also contain intended behavior and qualification that remain unfinished.

## Capture and interpretation

`sinex-sources` owns source declarations, shared acquisition drivers, framing
and parsers. Drivers obtain bytes; parsers interpret one framed record without
opening files, networks or clocks. The drain attaches record locations, hashes,
carried state and record failures to the resulting batch. Provider-specific
interpretation belongs here, not in the database or daemon.

`sinexd` connects these pieces: source tasks, backfill and replay submit work
to the writer. A source cursor describes admitted progress; obtaining bytes
alone does not mean their records were interpreted successfully. Archive
capture and replay have additional open acceptance criteria in Beads.

## Persistence and derived work

`sinex-store` owns PostgreSQL access, content storage and admission. The daemon's
writer owns transaction scheduling; admission stores events and source progress
together and marks affected derivation keys dirty. Source and derivation code
do not insert event rows independently. Blob installation and database commit
are different operations; a content hash does not prove semantic completeness.

`sinex-derivations` contains computations over supplied events and configuration.
The daemon's derivation runner reads their inputs and submits replacement output
sets through admission. Analysis methods similarly live apart from their store
and application adapters. The commit reader and NATS publisher handle committed
events separately from source acquisition.

## Interfaces and shared code

`sinex-model` owns identities and payloads; `sinex-protocol` declares public
commands. CLI, MCP and HTTP adapt those commands. `sinex-diagnostics` supports
the separate daemon-down diagnostic path. Schema declarations and application
remain in `sinex-schema`; checked SQL compiles against that real schema.

Shared blob storage, credential masking and development tooling come from
immutable `polylogue-sinex-common` releases. Prefer these existing owners to
another local implementation. Change an interface with its callers and remove
the replaced path. Read [AGENTS.md](AGENTS.md) for test placement and focused
development commands; passing a component test does not qualify a deployment.
