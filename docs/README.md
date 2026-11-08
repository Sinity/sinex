# Sinex documentation

These pages are the design of record: what Sinex does, its model and formats,
and the reasons behind the choices. They describe
Sinex as designed. CLI help, MCP tool schemas and module documentation
describe the current build; the work between design and code is tracked
outside the repository. The generated
[source and derivation catalog](../nix/catalog.json) is a module and
discovery input, produced by `sinexd admin catalog` and kept current by a
test.

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
| [sources.md](sources.md) | the capture model: capture by default, one declaration per source, inputs and materials, capture survival, gaps, input states and completeness (the coverage read model) |
| [hosts.md](hosts.md) | capture from other hosts and devices: hosts, producer identity, placement, time across hosts, held data, a phone as a producer |
| [streams.md](streams.md) | the base-layer streams and their spools |
| [source-families.md](source-families.md) | every source family at full fidelity, and the catalog of sources |
| [derivations.md](derivations.md) | the derivation model, fusion, intervals, documents and tags, model features |
| [higher-layer.md](higher-layer.md) | presence, agency, activity, attention, project attribution, episodes and the resume card, figures, descriptions, packs and narratives |
| [evidence.md](evidence.md) | personal evidence: constructs and metrics, figure cells, studies, experiments, forecasts and track records, places, money, retiring Lynchpin |
| [query.md](query.md) | the query surface: tables, clocks and units, joins, paging, saved queries, kept results, citation forms, read, trace, related |
| [recall.md](recall.md) | selections, the `recall` verb, recipes and packs, strata, lanes, notes, agent arrival |
| [interfaces.md](interfaces.md) | the operation table and its transports, callers and powers, the CLI, MCP's eight tools, status, signals and the host deliverer, follow, latency budgets |
| [web.md](web.md) | Today, the reader web, keys and counterparts |
| [library.md](library.md) | the library: items with any locator, encounters, page captures, curation, rankings, the link to Polylogue |
| [operations.md](operations.md) | deployment, the NixOS and Home Manager modules, configuration, backup and restore |
| [testing.md](testing.md) | the test database, the synthetic world, the properties tested, crash, VM and fuzz tests, benchmarks |
| [vocabulary.md](vocabulary.md) | each concept and its one name |

Shared mechanics (the segment container, the query grammar, references and
locators, the operation and answer contract, authored-record rules, ranking,
forecast scoring, the web shell, signal rules, the development tools) are
documented with their crates in
[polylogue-sinex-common](https://github.com/Sinity/polylogue-sinex-common).
How Polylogue uses Sinex is in Polylogue's
[primary-mode page](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md).

## Conventions

- **One fact, one page.** Each page states what it owns and links to the page
  that owns anything else.
- **Rulings.** A ruling is cited at the rule it binds as `(ruling DATE,
  LOCATOR)`: its date and a locator in the source where the words were
  written. The page states the decided behaviour in its own words; the
  operator's private sessions are never quoted, because these pages are
  public. Everything unlabelled is the design. The locator schemes are defined in
  Polylogue's [documentation index](https://github.com/Sinity/polylogue/blob/master/docs/README.md),
  shared by both products. A default the operator may still change is marked
  "(default; open operator question)".
- **Decided targets.** A section marked *decided target* states a decision
  (most from the 2026-11-01 feature designs) that the code does not implement yet. Its
  acceptance and the work is tracked outside the repository. Unmarked text
  is the design as the code is meant to hold it.
- **Generated artifacts** are changed through their declarations, not by
  hand.
