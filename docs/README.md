# Sinex documentation

These pages are the design of record: what Sinex does, its model and formats,
the laws its tests check, and the reasons behind the choices. They describe
Sinex as designed. CLI help, MCP tool schemas and module documentation
describe the current build. The [reference index](reference/README.md)
distinguishes current artifacts from planned generated pages; the work
between design and code is tracked outside the repository. The current generated
[source and derivation catalog](../nix/catalog.json) is also a module and
discovery input; its generator and test are described in
[the reference index](reference/README.md).

## Pages

| Page | Covers |
|---|---|
| [architecture.md](architecture.md) | purpose, scope and principles; data flow from source to answer; components; durability order and crash states; pipelines and throughput targets; configuration and clocks |
| [materials.md](materials.md) | materials, inputs, observations and revisions; bytes before rows; reading bytes back; staging, the one way anything enters |
| [events.md](events.md) | records and readings; the event and its hierarchy; time; identity and reading order; citations; producer versions, runs and manifests |
| [lifecycle.md](lifecycle.md) | live, archived and tombstoned; archive and restore; replay and its undo; whole chains; operations and actors |
| [admission.md](admission.md) | the one admission function and its writer; failures and progress; the commit log and positions; storage in PostgreSQL; schema evolution |
| [producers.md](producers.md) | the public contract for producers and consumers outside the daemon: the NATS ingress, the producer client, refusals, the confirmed stream, kind registration, lifecycle requests on the wire |
| [privacy.md](privacy.md) | field hints, detectors, rules, classes, grants and output actions; output parity; sealing |
| [sources.md](sources.md) | the capture model: capture by default, one declaration per source, inputs and materials, capture survival, gaps and source health |
| [streams.md](streams.md) | the base-layer streams and their spools |
| [source-families.md](source-families.md) | every source family at full fidelity, and the catalog of sources |
| [derivations.md](derivations.md) | the derivation model, fusion, intervals, documents and tags, model calling |
| [higher-layer.md](higher-layer.md) | presence, agency, activity, attention, project attribution, episodes and the resume card, figures, answers, findings, forecasts, context packs |
| [query.md](query.md) | the query surface: tables, clocks and units, joins, paging, saved queries, kept results, citation forms, read, trace, related |
| [recall.md](recall.md) | recall, context, marks, notes, the daily brief, agent arrival |
| [interfaces.md](interfaces.md) | the operation table and its transports, callers and powers, the CLI, MCP, status, signals, follow, latency budgets |
| [web.md](web.md) | Today and the reader web |
| [library.md](library.md) | the media library and its curation |
| [operations.md](operations.md) | deployment, the NixOS and Home Manager modules, configuration, backup and restore |
| [testing.md](testing.md) | the test database, the synthetic world, laws and their tests, crash, VM and fuzz tests, benchmarks |
| [vocabulary.md](vocabulary.md) | each concept and its one name |
| [reference/](reference/README.md) | current catalog and planned generated pages for commands, settings, kinds, sources, query tables and derivations |

Shared mechanics (the segment container, the query grammar, the operation
and failure model, the web shell, signal rules, the development tools) are
documented with their crates in
[polylogue-sinex-common](https://github.com/Sinity/polylogue-sinex-common).
How Polylogue uses Sinex is in Polylogue's
[primary-mode page](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md).

## Conventions

- **One fact, one page.** Each page states what it owns and links to the page
  that owns anything else.
- **Rulings.** Only the operator's own words are labelled: a ruling is cited
  at the rule it binds, with its date, a short verbatim quote where the quote
  carries meaning, and a locator in the source where the words were written.
  Everything unlabelled is the design. The locator schemes are defined in
  Polylogue's [documentation index](https://github.com/Sinity/polylogue/blob/master/docs/README.md),
  shared by both products. A default the operator may still change is marked
  "(default; open operator question)".
- **Laws.** Every area page ends with `## Laws`: properties with stable keys,
  each checked by tests named after the key
  ([testing.md](testing.md#laws-and-their-tests)).
- **Planned generated pages** under [reference/](reference/README.md#planned-pages)
  will be rendered by `cargo xtask docs` from the code's declarations. The
  generator is not implemented yet; existing generated artifacts are changed
  through their declarations, not by hand.
