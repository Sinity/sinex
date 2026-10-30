# Operations

How Sinex is deployed, configured and backed up: the flake's NixOS and Home Manager modules, the deployment configuration (listen address, authentication, the logical-day setting), and backup and restore with the restore drill. How settings flow through the code is in [architecture.md](architecture.md#configuration-and-clocks); every option, key and environment variable will be listed in [planned reference/settings.md](reference/README.md#planned-pages); the meaning of the logical day is the `DaySetting` of [query-language](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/query-language.md).

## Deployment

Sinex ships its own NixOS and Home Manager modules in its flake, and a host configuration only sets their options. Nothing in Sinex assumes a particular host's layout, paths or accounts (ruling 2026-10-28, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).

### The flake

The flake provides the packages (`sinexd`, `sinexctl`, `sinex-schema`, `sinex-session`, `sinex-capture`), `nixosModules.default`, a Home Manager module, an overlay, and checks that include the NixOS VM tests. Building and testing from a checkout is in [AGENTS.md](../AGENTS.md).

### The NixOS module

`services.sinex` runs the daemon and its stores on the main host:

- **PostgreSQL with TimescaleDB**, its cluster initialized with data checksums, and a setup unit that adds the extensions and the roles (the writer, the API, and the SQL reader role of [admission.md](admission.md#read-models)).

The database superuser runs `sinexd admin prepare-database` for initial setup
and privileged upgrades. In this database, it removes public execution of
PostgreSQL's `set_config` function and retains execution for the database owner
and writer. Public SQL runs as the reader and cannot change its grants or role.
Schema application and daemon startup refuse an unhardened boundary. The NixOS
`sinex-db-setup` unit reruns when the package changes, before dependent daemon
startup; activating that change includes the database preparation and restart.
- **NATS with JetStream** for the ingress and the confirmed stream, with one user per producer identity and per registered outside consumer ([producers.md](producers.md)). The NATS configuration holds only password hashes; each process reads its own password from a file, never from its arguments.
- **`sinexd`**, capturing every source whose input exists ([capture by default](sources.md#capture-by-default)). The per-source options are the source's typed settings and its exclusions, generated from the one source declaration in Rust, so what is captured is configuration, not database state. Inputs must be readable by the daemon's user.
- **Capture units** for streams the daemon does not read itself (journald through `sinex-capture`).
- **A capture-only node** for another host: `sinex-capture` with its own spool, sending to the main host's ingress with its own producer identity.

Kinds, CHECK lists, streams, consumers, derivations and sources are declared once in Rust and exported for the module, so the module never restates them.

### The Home Manager module

The Home Manager module runs `sinex-session` as a user service in the operator's graphical session, with each session stream enabled by its own option. It is an outside producer with its own spool and credential ([producers.md](producers.md)); the streams it captures are in [streams.md](streams.md).

### Session capture state

The session user service creates its declared state directory under its own home in `preStart`, then sets that directory to mode `0700`; it runs with umask `0077`. It does not change permissions on home-directory parents. A state path outside that user's home remains administrator-created through the module's tmpfiles declaration, with the configured user as owner and mode `0700`.

A session binding's identity includes the input path spelling. Moving state preserves `session.stateDir` and the producer's installed arguments, using a bind mount at that same path. First stop capture and verify its children have exited; preserve the state, positions, pending frames and a private inventory. Run `sinex-session run` with the installed producer, exclusions and credentials plus `--drain`, which reads existing spools without starting capture and waits for frame acknowledgments. An undrained included spool holds the move.

After a successful drain, copy the stopped state with ownership, modes and bytes preserved, compare hashes and positions, and mount the copy at the original path before restarting. Drain again through that path, verify unchanged binding names and positions, then restart capture and verify new admitted events and writes at the destination. Keep the original copy until the activation owner accepts the result; cleanup is a separate action. Changing the `--state` spelling creates new bindings even when the bytes were copied.

### Upgrading

Activating a new version does not change the schema. When a version brings a schema upgrade, stop `sinexd`, run `sinex-schema apply`, then start it; until then the daemon refuses to start and names the command ([admission.md](admission.md#schema-evolution)). Producers keep spooling while the daemon is stopped, so an upgrade pauses admission, not capture.

### PostgreSQL tuning

The module keeps PostgreSQL's stock memory and checkpoint defaults and sets `log_temp_files = 16MB`. Buffer sizes, WAL size, checkpoint interval and work memory are host tuning: the deployment chooses them for its hardware, and Sinex's targets are stated independently of them ([architecture.md](architecture.md#throughput-targets)).

## Configuration

Each binary loads one typed configuration ([how it is read](architecture.md#configuration-and-clocks)). The NixOS and Home Manager modules write that file and set those variables; a deployment without Nix sets them directly. Every key will be listed in [planned reference/settings.md](reference/README.md#planned-pages); current flags are described by `sinexd run --help` and Nix options by the [module declarations](../nix/module.nix).

**What deployment configuration holds:** the logical day, the API's listen address and authentication, the optional notification command ([signals](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/signals.md)), the database and NATS endpoints, the content store location, this host's name, and per-user facts such as account names on each service. Settings any author may change at run time (quiet hours, the interrupt budget, briefing switches) are the changeable catalog of [architecture.md](architecture.md#configuration-and-clocks), not deployment configuration.

### Listen address and authentication

The daemon serves its API on a Unix socket and, for HTTP and the reader web, on a configured address (`services.sinex.api.listen`), by default loopback at the one default port declared in the module. Authentication is configured with it:

| Listen address | Authentication |
|---|---|
| loopback (default) | peer-uid check on the connection |
| any other address | a token file; a non-loopback address without one refuses to start |

Reaching Sinex from a phone or another machine (a tailnet address, a reverse proxy) is the deployment's choice, made by setting these options; Sinex has no remote-access feature of its own. Ruling, 2026-10-28: "this should not be part of the project … project should be configurable enough (in nix mainly, btw, tho this should boil donwn to actual config file and possibly envvars I guess)" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).

Clients find the daemon through `SINEX_ENDPOINT` (the socket or HTTP address) and `SINEX_TOKEN_FILE`, which the modules set for login shells. Integrations in other languages (Kitty, Neovim) read the one variable the deployment sets for them. Which callers hold which powers is [interfaces.md](interfaces.md).

## Logical day

One configurable day boundary per deployment, used by every day- and hour-grain value Sinex produces: queries, derivations, analysis, recall and briefs. Ruling, 2026-10-11: "yes it should be configurable, and default should be sth like 4AM maybe … need to design the system so that this works reliably, there are no unexpected exceptions" (cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:d716993a-f5d6-4422-aa5e-97d91e430d93).

| Option | Meaning | Default |
|---|---|---|
| `services.sinex.zone` | the named IANA zone days are cut in | the host's configured zone, resolved to its IANA name at start |
| `services.sinex.dayStart` | when a logical day starts, as `HH:MM` | `04:00` |

- **Validated at start.** A zone that does not resolve to a named zone stops the daemon before connecting to the database, with a clear error; there is no fallback to UTC.
- **NixOS host zone.** An explicit `services.sinex.zone` takes precedence over `time.timeZone`. When only the host zone is configured, the module passes that name to the daemon and declared backfills. When both are null, startup must resolve the actual system zone to an IANA name. NixOS's UTC display default without a configured named zone does not supply that evidence; configure `time.timeZone` or `services.sinex.zone` for such a deployment.
- **One home.** Sinex's configuration holds the setting. Polylogue running on Sinex reads the day from Sinex at connect and on each change notice and has no value of its own; standalone Polylogue keeps its own ([Polylogue operations](https://github.com/Sinity/polylogue/blob/master/docs/operations.md)).
- **Changing it** is a version change of every day-grain derivation, which re-derives them by the ordinary mechanism ([derivations.md](derivations.md)); every day- and hour-grain value records the boundary and zone it was cut with.
- A wake-to-wake view is a separate derived kind shown beside the logical day, never a second definition of it ([higher-layer.md](higher-layer.md)). A coarse source time stays in the calendar the source meant ([events.md](events.md#time)).

## Backup and restore

The stores are the backup unit, and the deployment chooses the backup tool; Sinex supplies the boundary the tool needs and the restore that proves the copy.

### What a backup contains

- a consistent logical dump of PostgreSQL;
- the segment log;
- the wrapped data keys, through their protected route ([privacy.md](privacy.md));
- configuration and credentials, through their owners (the deployment's secret store);
- producer spools not yet acknowledged.

Worktrees, caches and scratch are outside backups. NATS state is not backed up: committed changes are republished from PostgreSQL with the same commit positions, and unacknowledged frames are resent from the producer spools, which are in the backup ([producers.md](producers.md)).

### Holds and the boundary

A backup records its boundary: the dump's commit position and schema version, the segment extents it covers, the producer spool positions, and the backup archive's identity. The key dump and the main dump share the boundary.

```
sinexctl backup hold             # records the boundary, returns HOLD
<copy with the deployment's tool>
sinexctl backup release HOLD
```

- The hold records the boundary before the copy starts and returns a hold ID; release ends it.
- **A hold never expires.** Status shows each open hold with its age, so a forgotten hold is visible rather than silently ended.
- **Tombstoning never removes an extent an open hold covers.** The punch is marked; when the last hold covering the extent is released, the writer punches it at its next group, without waiting for the segment to reopen. The punch when a segment opens covers a crash ([architecture.md](architecture.md#durability-order)).

Backup expiry belongs to the deployment's backup tool and never deletes Sinex data. Any copy that leaves the machine is encrypted.

### Restore

Restore is an ordinary rebuild from a backup plus the retained materials, not a repair path: start Sinex against the restored store, and the restored state answers exactly as the original did at the boundary.

- the same event IDs, payloads, lifecycle states and positions at the boundary;
- exact bytes traced from any event;
- replay works after the original source files are gone;
- producers resume their resends without duplicates.

A missing extent, key or spool is a named failure, never an empty start.

### The restore drill

A restore drill on real-shaped data runs before any operation that could lose data; there is no calendar of drills. The drill restores into a separate deployment and checks every property listed under [restore](#restore); how it runs as a test is in [testing.md](testing.md).

## Laws

- **one-day-setting**: every logical-day consumer (queries, derivations, analysis, recall, briefs, and Polylogue on Sinex) cuts days with the one configured setting; changing it changes every consumer's answer together.
- **day-values-record-boundary**: every day- and hour-grain value records the boundary and zone it was cut with.
- **invalid-zone-stops-daemon**: a zone that does not resolve to a named zone stops the daemon at start with an error naming the setting; nothing falls back to UTC.
- **non-loopback-needs-token**: the daemon refuses to start on a non-loopback listen address without a token file; on loopback, a connection from another user is refused.
- **restore-reproduces-boundary**: a restore drill reproduces event IDs, payloads, states, positions and bytes at the recorded boundary, replays without the original files, and producers resume without duplicates.
- **restore-names-missing-parts**: a restore with a missing extent, key or spool fails naming what is missing and never starts empty.
- **hold-protects-extents**: a punch never removes an extent an open backup hold covers, and is done at the writer's next group after the last covering hold is released.
- **hold-visible-until-released**: an open hold appears in status with its age until it is released; no hold ends by itself.
