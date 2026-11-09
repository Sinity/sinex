# Operations

How Sinex is deployed, configured and backed up: the flake's NixOS and Home Manager modules, the deployment configuration (listen address, authentication, the logical-day setting), and backup and restore with the restore drill. How settings flow through the code is in [architecture.md](architecture.md#configuration-and-clocks); the meaning of the logical day is the `DaySetting` of [query-language](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md).

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
- **A capture-only node** for another host: `sinex-capture` with its own spool, sending to the main host's ingress with its own producer identity ([hosts and placement](#hosts-and-placement)).

Kinds, CHECK lists, streams, consumers, derivations and sources are declared once in Rust and exported for the module, so the module never restates them.

### The Home Manager module

The Home Manager module runs `sinex-session` as a user service in the operator's graphical session, with each session stream enabled by its own option. It is an outside producer with its own spool and credential ([producers.md](producers.md)); the streams it captures are in [streams.md](streams.md).

### Session capture state

The session user service creates its declared state directory under its own home in `preStart`, then sets that directory to mode `0700`; it runs with umask `0077`. It does not change permissions on home-directory parents. A state path outside that user's home remains administrator-created through the module's tmpfiles declaration, with the configured user as owner and mode `0700`.

A session binding's identity includes the input path spelling. Moving state preserves `session.stateDir` and the producer's installed arguments, using a bind mount at that same path. First stop capture and verify its children have exited; preserve the state, positions, pending frames and a private inventory. Run `sinex-session run` with the installed producer, exclusions and credentials plus `--drain`, which reads existing spools without starting capture and waits for frame acknowledgments. An undrained included spool holds the move.

After a successful drain, copy the stopped state with ownership, modes and bytes preserved, compare hashes and positions, and mount the copy at the original path before restarting. Drain again through that path, verify unchanged binding names and positions, then restart capture and verify new admitted events and writes at the destination. Keep the original copy until the activation owner accepts the result; cleanup is a separate action. Changing the `--state` spelling creates new bindings even when the bytes were copied.

### Upgrading

Activating a new version does not change the schema. When a version brings a schema upgrade, `sinexd` refuses to start and names the command ([admission.md](admission.md#schema-evolution)). With `sinexd` stopped, run `systemctl start sinex-schema-upgrade` (`sinexd admin upgrade` as the Sinex user), which returns when the one upgrade transaction commits or rolls back, then start `sinexd`. Producers keep spooling while the daemon is stopped, so an upgrade pauses admission, not capture.

Before stopping the daemon, run `sinexd admin upgrade --prepare` from the new build as the Sinex user. It builds the work the upgrade can swap in while the old daemon serves, and returns when that work is complete; it is safe to run again after an interruption, and `sinexd admin upgrade --unprepare` removes it. Without it the upgrade still succeeds, but rewrites the equivalence keys while admission waits.

After start, the daemon finishes the rest of the upgrade on its own ([admission.md](admission.md#schema-evolution)). `sinexctl status` lists what remains under `upgrade_work_after_start`; the list is empty when the store matches a fresh one. Meanwhile the publisher holds until the publication key exists, and the chunk being indexed waits for its index. While the search trigram index is rebuilt, search maintenance waits (the search position shows the delay) and a tombstone started then waits for the build.

### PostgreSQL tuning

The module keeps PostgreSQL's stock memory and checkpoint defaults and sets `log_temp_files = 16MB`. Buffer sizes, WAL size, checkpoint interval and work memory are host tuning: the deployment chooses them for its hardware, and Sinex's targets are stated independently of them ([architecture.md](architecture.md#throughput-targets)).

## Configuration

Each binary loads one typed configuration ([how it is read](architecture.md#configuration-and-clocks)). The NixOS and Home Manager modules write that file and set those variables; a deployment without Nix sets them directly. Current flags are described by `sinexd run --help` and Nix options by the [module declarations](../nix/module.nix).

The NixOS module writes daemon settings to `/etc/sinex/sources.json`, including `services.sinex.stagingRoots`, which limits agent staging. Session and capture-node producers receive `/etc/sinex/session-sources.json` and `/etc/sinex/capture-sources.json`: source settings, exclusions and discovery inputs, with their own spool and watch paths. Staging authority stays with the daemon.

**What deployment configuration holds:** the logical day, the API's listen address and authentication, desktop notifications and the optional away command ([signals](interfaces.md#the-host-deliverer)), the database and NATS endpoints, the content store location, this host's name, and per-user facts such as account names on each service. Settings any author may change at run time (quiet hours, the interrupt budget, briefing switches) are the changeable catalog of [architecture.md](architecture.md#configuration-and-clocks), not deployment configuration.

### Listen address and authentication

The daemon serves its API on a Unix socket and, for HTTP and the reader web, on a configured address (`services.sinex.api.listen`), by default loopback at the one default port declared in the module. Authentication is configured with it:

| Listen address | Authentication |
|---|---|
| loopback (default) | peer-uid check on the connection |
| any other address | a token file; a non-loopback address without one refuses to start |

Reaching Sinex from a phone or another machine (a tailnet address, a reverse proxy) is the deployment's choice, made by setting these options; Sinex has no remote-access feature of its own. (ruling 2026-10-28, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).

Clients find the daemon through `SINEX_ENDPOINT` (the socket or HTTP address) and `SINEX_TOKEN_FILE`, which the modules set for login shells. Integrations in other languages (Kitty, Neovim) read the one variable the deployment sets for them. Which callers hold which powers is [interfaces.md](interfaces.md).

A producer credential authenticates a transport identity; writes additionally require an explicit [write family](producers.md#explicit-write-families). Decided target (2026-11-01): credentials become per-host nkeys bound to (component, host), and the `attestedHost` and canonical-publisher options go ([hosts and placement](#hosts-and-placement)). Configure `services.sinex.nats.producers.<name>.writeFamily`, `attestedHost`, `writeSourcePrefixes`, `writeKindPrefixes` and `writeClasses` for the intended capture. The session service’s generated password entry preserves those configured capabilities and grants no write authority by itself. A missing grant leaves capture spooling locally with a reported refusal. A file-watch-only producer uses the installed native family `sinex`, source `machine.file-watch`, kind `file.changed`, class `guarded` and its actual host. Discovery also selects other session sources whose bus or runtime inputs are available. Cover those sources explicitly in the grant, or configure reasoned `services.sinex.exclude.<source>.reason` entries for a deliberately narrower capture plan. Read and canonical-publisher grants remain separate.

## Hosts and placement

Decided target (2026-11-01). Hosts, their producers' public keys and the placement of account inputs are deployment configuration; Sinex records the configured set as a host event at start, and its CLI shows these values and never edits them ([hosts](hosts.md)).

```nix
services.sinex.hosts = {
  <server-name> = { kind = "server";
    producers.capture.key   = "<public nkey>";
    producers.polylogue.key = "<public nkey>"; };
  <phone-name> = { kind = "phone"; outOfContactAfter = "48h";
    producers.app.key = "<public nkey>"; };
};
services.sinex.inputs.<source>.<account>.placement = "<server-name>";

# on the capture host
services.sinex.captureNode = { enable = true; nats = "<sinex host>:4222"; keyFile = "<path to this producer's private key>"; };
```

| Parameter | Meaning | Default |
|---|---|---|
| `hosts.<name>.kind` | `workstation`, `server`, `phone` or `other`; chooses defaults only | — |
| `hosts.<name>.producers.<component>.key` | the producer's public nkey; its private half is generated on that host with `sinex-capture key` and never leaves it | — |
| `hosts.<name>.outOfContactAfter` | silence before an out-of-contact signal | server 15 min, workstation 1 h, phone 48 h |
| `hosts.<name>.replaces` | the ID of a host this one continues (a reinstalled app) | none |
| `inputs.<source>.<account>.placement` | the host whose producer polls a host-free input | the Sinex host |
| transfer class per source | `immediate`, `unmetered`, `charging`; whether bulk data may use metered networks | discrete records `immediate`; media and sensor chunks `unmetered` on phones (open operator question) |
| `captureNode.nats`, `captureNode.keyFile` | the Sinex host's ingress address and this producer's private key file | — |

- The deployment's private network must let each capture host reach the Sinex host's NATS port, and nothing more is needed for capture.
- A capture node only uploads over the link. The current mode in which a satellite mounts `sinexd`'s content store, and its `upload` flag, go: a capture host is never coupled to Sinex's storage layout or group IDs.
- Whether a backup copy is kept off the Sinex host is the deployment's choice ([backup and restore](#backup-and-restore)).

## Logical day

One configurable day boundary per deployment, used by every day- and hour-grain value Sinex produces: queries, derivations, analysis, recall and briefs. (ruling 2026-10-11, cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:d716993a-f5d6-4422-aa5e-97d91e430d93).

| Option | Meaning | Default |
|---|---|---|
| `services.sinex.zone` | the named IANA zone days are cut in | the host's configured zone, resolved to its IANA name at start |
| `services.sinex.dayStart` | when a logical day starts, as `HH:MM` | `04:00` |

- **Validated at start.** A zone that does not resolve to a named zone stops the daemon before connecting to the database, with a clear error; there is no fallback to UTC.
- **NixOS host zone.** An explicit `services.sinex.zone` takes precedence over `time.timeZone`. When only the host zone is configured, the module passes that name to the daemon. When both are null, startup must resolve the actual system zone to an IANA name. NixOS's UTC display default without a configured named zone does not supply that evidence; configure `time.timeZone` or `services.sinex.zone` for such a deployment.
- **One home.** Sinex's configuration holds the setting. Polylogue running on Sinex reads the day from Sinex at connect and on each change notice and has no value of its own; standalone Polylogue keeps its own ([Polylogue operations](https://github.com/Sinity/polylogue/blob/master/docs/operations.md)).
- **Changing it** is a version change of every day-grain derivation, which re-derives them by the ordinary mechanism ([derivations.md](derivations.md)); every day- and hour-grain value records the boundary and zone it was cut with.
- A wake-to-wake view is a separate derived kind shown beside the logical day, never a second definition of it ([higher-layer.md](higher-layer.md)). A coarse source time stays in the calendar the source meant ([events.md](events.md#time)).

## Backup and restore

The stores are the backup unit, and the deployment chooses the backup tool; Sinex supplies the boundary the tool needs and the restore that proves the copy.

### What a backup contains

- a consistent logical dump of PostgreSQL, uncompressed so the backup tool's own compression and deduplication store only what changed, and without the rows of the search tables (`core.search_text`, `core.search_refusals`, `core.search_position`), which are rebuilt from the events;
- the segment log;
- the wrapped data keys, through their protected route ([privacy.md](privacy.md));
- configuration and credentials, through their owners (the deployment's secret store);
- producer spools not yet acknowledged.

Worktrees, caches and scratch are outside backups. NATS state is not backed up: committed changes are republished from PostgreSQL with the same commit positions, and unacknowledged frames are resent from the producer spools, which are in the backup ([producers.md](producers.md)).

Preparing a backup pauses publication and spool retirement, then takes the hold and the database snapshot. It copies the daemon's retained source spools and the state of every configured producer with write access. Each producer snapshot keeps its checkpoint, its pending frames and the bytes needed to resume from that checkpoint. Capture continues while publication is paused; media generations in progress finish before their files are copied. The manifest names the required producers, each copied file and each producer that could not be copied; `producers_complete` is true only when every required producer was copied. A backup prepared without a known list of producers is incomplete. The hold protects segment prefixes; producer state is described by the manifest.

### Holds and the boundary

A backup records the dump's commit position and schema version, the blob-only change counter, and the committed segment prefixes it protects. Both the main and wrapped-key dumps belong to one exported PostgreSQL snapshot of the same database; the keys are the `privacy.data_keys` table, not a second database.

```
sinexctl backup hold
sinexctl backup list --page 100
sinexctl backup prepare --output /private/backups/new-run
sinexctl backup release HOLD
```

These operations require the operator token. The daemon computes the operation's actor. `hold` commits the boundary and operation before returning its UUID; `release` records its own operation and preserves the original boundary. Open holds have no expiry. `status` reports their count and oldest creation time, and `backup list` pages through every open hold with `--cursor`.

`prepare` creates a fresh hold and exports its snapshot inside the same writer job, after the hold commits and before another admission advances either counter. It then lets the writer continue while the snapshot exporter stays alive for both real `pg_dump` calls. The new output directory must be absolute, private and writable by the daemon; an existing attempt is refused. The artifact contains:

- `main/sinex.dump`, uncompressed, with wrapped-key table data and the rebuilt search tables' rows excluded;
- `main/segments/`, containing only the committed prefixes, copied with bounded memory while later appends continue;
- `keys/data-keys.dump`, containing the wrapped-key table data from that same snapshot;
- the same versioned `manifest.json` in both parts, with the hold, database identity, source revision, file lengths and byte digests.

Live extent descriptors are checked against the copied encoded bytes, including sealed encodings, before a root `manifest.json` is published and synced. The main dump still contains private database state, so both parts need protected staging and encrypted archives. No plaintext material keys are exported.

Before publication, a lost exporter, short segment, corrupt extent, failed dump or cancellation leaves an incomplete directory and an open hold. The early `boundary.json` names the hold without claiming completion. A fresh SQL snapshot cannot be attached to an older hold, and an incomplete directory cannot silently become a different run. The operator can inspect the retained attempt, start a fresh one, and explicitly release abandoned holds. A prepared local artifact is not an acknowledgment from Borg or from a producer.

Tombstoning and orphan cleanup do not punch any extent covered by an open hold. Multiple holds compose: release of the last covering hold dirties the deferred work, which the writer attempts at its next ordinary group without reopening the segment. Ordinary recovery checks the same durable holds before replaying a punch.

### The deployed archive pair

The NixOS `sinex-backup` service owns preparation and the two Borg calls. `backup.artifactDir` is private to the daemon; the old independently scheduled dump flow is removed. A host supplies separate `backup.repo` and `backup.keysRepo` repositories together and the protected pass command. The service's `backup.startAt` accepts a calendar or a list; `[]` lets an existing host coordinator be the sole scheduler. The sibling Borg jobs have no independent timers. Initialize the two encrypted repositories through the deployment's Borg owner before enabling the schedule; the native jobs do not initialize repositories. Each job compares their actual repository IDs before creation, so aliases cannot put key bytes in the main repository even as a failed archive.

Both archives are named `sinex-core-HOLD` in their respective repositories and carry the identical manifest. The service reads each archive's identity back after it is created, checks both names against the hold, and records the pair in `complete.json`. Only then does it release the hold and remove that run's staging copies. `latest-complete.json` points to that record; it is not a choice of unrelated latest archives. Before both archives complete, a warning, a failed job or an interruption keeps the artifact and hold for inspection. A later release or cleanup failure keeps the completed artifact and its archive records; the recorded hold state says whether release committed. Borg's warning failure and `.failed` archive behavior remain intact.

The privileged Borg process enters the selected part through directory descriptors that reject symlinks in every parent and pin its working directory. Its ordinary Linux walk does not follow file or directory symlinks. Root-owned control files and archive records stay separate from daemon-owned artifacts; metadata writes and staging cleanup through artifact paths run as the daemon user. The keys part is never a main-archive input.

Backup expiry belongs to the deployment's backup tool and never deletes Sinex data. Any copy that leaves the machine is encrypted.

### Restore

Restore is an ordinary rebuild from a backup plus the retained materials, not a repair path. Select the main and key archives by their shared hold and actual archive identities, compare their manifests and restore both dumps and the segment prefixes. The restored core answers exactly as it did at the boundary. Its snapshot includes the then-open hold; that hold remains visible until the restore owner verifies the copy and explicitly releases it. It never expires on restart. Producer state is restored separately; a backup whose producers were not all copied says so in its manifest.

- the same event IDs, payloads, lifecycle states and positions at the boundary;
- search starts empty with no position, and the search reader rebuilds it from the restored events; status shows its progress, and follows wait for it;
- exact bytes traced from any event;
- replay works after the original source files are gone;
- producers resume their resends without duplicates.

A missing extent, key or spool is a named failure, never an empty start.

### The restore drill

A restore drill on real-shaped data runs before any operation that could lose data; there is no calendar of drills. The drill restores into a separate deployment and checks every property listed under [restore](#restore); how it runs as a test is in [testing.md](testing.md).
