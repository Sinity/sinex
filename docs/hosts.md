# Hosts

Capture from every machine and device the operator uses into the one Sinex record: hosts and the producers that run on them, credentials, sending rules for devices that are often offline, accounts placed on one host, time across hosts, contact and held data, how late data flows through derivations and signals, and a phone as a producer. The producer wire contract is [producers](producers.md); sources and their declarations are [sources](sources.md); the coverage read model this page extends is in [sources](sources.md#completeness); deployment options are in [operations](operations.md#hosts-and-placement).

Decided target (2026-11-01). The capture node (`sinex-capture` sending to the main host's ingress) exists and passed a two-machine spool and forward test; everything else on this page is decided and not built. Its laws are the acceptance.

## Purpose

Every machine and device is a capture point into one record, and the record says which device saw what. It stays honest while a device is away: a week without network loses nothing and reads as "held on the phone", never as an empty week or a failure.

**One database.** One user, one database, one trust domain and one place that decides what is live; what expands is capture topology. Additional hosts run capture only and send to the one Sinex host's NATS ingress. A replica, a NATS leaf node or any second database is ruled out; a copy for disaster recovery is a backup, and whether a deployment keeps one off the Sinex host is the deployment's choice ([backup](operations.md#backup-and-restore)).

## A week, as an example

On example hosts `kestrel` (a workstation that runs `sinexd`), `server` (always on) and `phone`:

- **Monday, leaving.** The phone goes into airplane mode. Its producer keeps spooling notifications, usage, location, sensors and audio. Its spool screen reads "held 14 h · 1.1 GB · runway 23 days at this rate".
- **Monday evening, a flaky network.** The phone reconnects. Discrete records go first (transfer class `immediate`); audio and sensor chunks wait for an unmetered network. Its Sources rows switch from `held` to `capturing`.
- **Wednesday, `kestrel`'s database is down for an upgrade.** `server` and the phone keep spooling; nothing is lost and the coverage grid shows no gap, because nothing was missed: the records were held and arrived late.
- **Thursday, no signal for 30 hours.** On reconnection the producer's `hello` measures the phone's clock offset (+2.3 s, round trip 40 ms). Readings with instant precision carry `clock offset +2.3 s (measured)`; cross-host views align on the corrected time and say so; the stored `ts_orig` stays what the phone's clock read.
- **Saturday, a reboot loop.** The phone's wall clock starts years in the past until the network sets it. Records framed then carry monotonic anchors; a later good anchor on the same boot re-anchors them on replay, labelled `captured (re-anchored from monotonic clock)`.
- **Back home.** The Day view for Wednesday has one lane per host plus an operator lane, and a query over Wednesday's notifications answers completely, its coverage facet saying the rows arrived 31 hours late. Day packs were provisional while data was held and recomputed when it landed. No interrupting notice fired for the late notifications, and no "phone stalled" notice fired during the flight, because the phone's out-of-contact deadline is 48 hours; one fired only when the phone's last report said its capture service had died.

## Host

A machine or device that captures. It is already part of input identity and of every run; this page makes it a declared row.

| Field | Meaning |
|---|---|
| `name` | the deployment's name for it; a label from configuration, never in code |
| `kind` | `workstation`, `server`, `phone`, `other`; it chooses defaults only (out-of-contact deadline, transfer classes), and no behaviour is keyed on a particular host |
| `id` | minted at first enrollment (UUIDv7) and stored on the host; a rename keeps it |
| `producers` | the components that run there, each with its public key |
| `out_of_contact_after` | default by kind: server 15 min, workstation 1 h, phone 48 h; overridable |

- **Enrolled** when its key is in configuration; **in contact** or **out of contact** from its last frame; **retired** when removed from configuration: its credential stops working, its history stays, and its host-bound inputs show `outside_span` after the retirement.
- A reinstalled app that generated a new key is the same host only through an explicit configuration entry (`replaces = <old id>`), recorded as a host event. Nothing is guessed from device serials.
- **Hosts are configuration** (`services.sinex.hosts`). `sinexd` records the configured set as a host event at start, so history shows when a host joined or left.

## Producer identity, credentials and sending

The producer contract is unchanged ([producers](producers.md)): one identity with its own credential and frame namespace; save before send; retry forever; only adds. This page fixes three things.

- **Identity is the pair (component, host).** The credential is bound to that pair in configuration; a frame's host is its credential's host. A host named inside a payload is stored as a claim and never used as the host.
- **Keys, not shared secrets.** Each producer generates a NATS nkey (Ed25519) on its own host; configuration holds only the public key, and the private seed never leaves the host. Adding a host is one public string in the deployment's configuration. The Sinex host's own producers use keys too, so there is one mechanism.
- **Transport** is a direct NATS client connection over the deployment's private network. There are no leaf nodes: a leaf's own durable queue would have acknowledgement semantics different from Sinex's commit, and the producer spool already removes data only after commit, so one spool per producer is the only offline store.
- **Connect on demand.** A producer on a metered or battery-powered host connects when it has something to send, drains, and disconnects after an idle period (default 60 s). Servers and workstations stay connected.
- **Transfer class per source**, a sending rule and never a capture rule: `immediate` (send whenever connected), `unmetered` (wait for a non-metered network), `charging` (wait for power). Discrete records default to `immediate`; continuous media and sensor chunks default to `unmetered` on phones and `immediate` elsewhere. Within a class inputs drain round-robin; within an input, in order. Whether bulk data may use metered networks is a deployment setting (default `unmetered` only; open operator question).

Two frame additions are the whole protocol change ([frames](producers.md#frames)):

- `hello` carries `clock: {wall, monotonic}` sampled at send, and the reply carries the server's receive and send times, so each connection yields one offset sample with its round-trip bound (the NTP exchange of RFC 5905 §8). `progress` frames carry the same four timestamps, so a connected producer gives a sample per acknowledgement window.
- `progress` carries a **capture report** per input: `captured_through` (everything up to it is in the spool), `held: {records, bytes, oldest}`, the spool's free space, and the input's capture state on the host (`capturing`, `unavailable(cause)`, `stopped(reason)`). Sinex learns what was captured while away before any byte of the backlog arrives.

## Placement

An input is **host-bound** (a file, a bus, a device, a local database; its identity includes the host) or **host-free** (an account or an API; its identity is `(source, locator)`, because the provider's records are the same wherever they are polled from).

- A host-free input has exactly one **placement**: the host whose producer polls it. Placement is configuration; the default is the Sinex host.
- The cursor lives in Sinex, saved as the producer's committed progress. A newly placed producer asks for it at `hello`; a producer offering a host-free input not placed on its host is refused at `hello` with a typed reason.
- A move may repeat the last poll window; source record keys suppress the repeats, and no record is skipped.
- Placing account pollers whose providers expire their own records (mail history identifiers, recently-played windows, CI logs) on an always-on host wins the [expiry race](sources.md#completeness) whatever the workstation's uptime.
- Placement is also how Polylogue decides which host re-parses a chain captured on a host that was retired: one concept serves both products.

## Time across hosts

The time rules stand ([time](events.md#time)): time is decided once, at framing; the writer never changes `ts_orig`; reading order compares readings only within one producer namespace, never across hosts and never by clock. This page adds clock evidence.

- **Clock identity.** Every captured time names its clock: `wall@<host>`, or `monotonic@<host>/<boot>` for boot-relative clocks (Android's elapsed realtime, `CLOCK_BOOTTIME`, evdev kernel timestamps).
- **Offset samples.** Each `hello` and `progress` round trip gives `(at, offset, rtt)` for the producer's host, kept as append-only self-observation. The host's offset estimate at an instant is the minimum-round-trip sample in the surrounding window (the NTP clock filter); its uncertainty is half that round trip plus the drift since.
- **Labels, never rewrites.** A reading whose kind declares instant or second precision gets `clock offset ±x (measured)` in its time evidence when its host's estimated offset exceeds that precision. `ts_orig` keeps the host's wall time, which lines up with the host's own logs.
- **Aligned time at read.** Cross-host views and queries (`group by host`, the operator lane, time joins across hosts) order on `ts_orig − offset` and print the correction. Single-host views use `ts_orig`.
- **Steps and anchors.** Producers that can record `(monotonic, wall)` anchor pairs record one at least once per material part and at every observed wall-clock step. A record framed while the wall clock was implausible (before the host's first contact, or behind a later anchor on the same boot) has time quality `unknown`; replay with a later good anchor on the same boot re-frames it from its monotonic time with quality `captured`. Evidence is append-only, so this is ordinary replay, not repair.
- **The commit-time check stays local.** The bound on a captured time ahead of commit applies to Sinex's own clock; a remote reading far in the future is kept and labelled `suspect: ahead of commit by …`.

## Contact, held data and coverage

**Contact** is derived from frames: the last `hello` or `progress` per producer, kept in a `producer_contact` row updated in the transaction that saves the progress. The [coverage read model](sources.md#completeness) gains one state, **`held`**: the interval after a producer's last `captured_through` for an input, while the producer is out of contact or still draining.

- `held` is not a gap and not `not_yet`. It becomes `complete` up to the newly reported `captured_through` once the records are admitted, and `gap(reason)` only from the producer's own `gap` frames or reported `stopped` or `unavailable` states. Counts over a `held` interval are marked `lower bound`, as for any coverage that is not complete.
- Out of contact past `out_of_contact_after` raises one interrupting signal per stretch out of contact, with the last report: "phone: no contact for 48 h; last report: capture running, held 3.1 GB, runway 19 d". Within the deadline nothing fires.
- A reported stop is a stall whatever the contact state: if the last report says a capture service died, the [stall rule](sources.md#completeness) applies at once.
- **Spool runway** (free space divided by the recent capture rate) shorter than the out-of-contact deadline raises a passive notice; shorter than one day, an interrupting one.
- A full spool filesystem stops capture with a counted `capture.gap` with reason `overflow`; nothing held is deleted to make room ([full disks](sources.md#capture-survival)).

## Late data

- **Derivations need nothing new.** A keyed derivation whose read ranges intersect newly admitted records is dirtied in the same transaction and recomputes ([derivations](derivations.md#when-a-key-recomputes)). Day packs and Today show their keys as provisional while any input in their range is `held`.
- **Signals use observation age.** A rule fires on arrival, but a triggering fact older than the rule's `fresh_within` is delivered passively and labelled `late (held N h on <host>)`; this is a step of the common route function ([signals](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/signals/README.md#the-route-function)). Current-state rules recheck at handoff, and held data can only make a current-state answer more complete.

## What runs where

| Host kind | Runs | Never runs |
|---|---|---|
| the Sinex host | `sinexd`, PostgreSQL, NATS, `sinex-session`, Polylogue on Sinex | — |
| server | `sinex-capture` with every source declaration that applies to a headless host (journal, shell histories, units, files, netlink, audit), the account pollers placed on it, Polylogue on Sinex as producer and consumer | a database, a replica, derivations, a deliverer |
| laptop (kind `workstation`) | as a server, plus `sinex-session` for its desktop streams | the same |
| phone | its capture app as the phone's producer | anything but capture and its own interface |

Which host runs `sinexd` is a configuration value, so moving it is a deployment change. A source declaration runs unchanged on any host: there is no capture-node-specific or phone-specific source code where a format is shared.

## A phone as a producer

Every phone source is captured in its native form, one record per native record, with every field its API or file provides; privacy applies at output ([privacy](privacy.md)), so no field such as a phone number is dropped at capture.

| Source | Native form | Route | Needs root |
|---|---|---|---|
| notifications | posted and removed notifications with all extras and the removal reason | the platform's notification listener | no |
| notification history | the platform's own notification history files, for backfill after listener outages | file read | yes |
| usage events | the platform's usage events (activity resumed and paused, screen interactive, keyguard, standby bucket) | the usage statistics API | no |
| usage statistics files | the system's raw usage files, including what the API aggregates away; `expires_after` measured on the device | directory capture | yes |
| calls and messages | call log and SMS/MMS provider rows with their provider keys; full passes detect removal | content providers | no |
| call audio | recordings an on-device recorder wrote, where the deployment enables call recording | file drop | no |
| location | fixes with accuracy, speed, bearing and provider; raw GNSS measurements while moving | location APIs | no |
| health records | all granted record types, change tokens and deletions | the platform's health API (root cannot substitute for its grants) | no |
| ambient audio and voice notes | audio chunks with sidecars | microphone foreground service | no |
| sensors | sensor chunks with monotonic and wall anchors | sensor API | no |
| device events | the app's own log: network, Bluetooth, power, thermal, unlocks, do-not-disturb, package changes | the app | no |
| system log | every log record, all buffers, continuously | `READ_LOGS`, granted once over adb | no |
| touch and buttons | the same `input.evdev` declaration as the desktop | `/dev/input` | yes |
| browser histories, shell histories | the same declarations as the desktop, at the phone's locations | root read of app data | yes |
| shared storage | camera, downloads, screenshots and recordings as file drops with full metadata | all-files access | no |
| messaging apps | their local databases, as rows | root read of app data | yes |

AI apps on the phone are not phone capture: Polylogue's provider sources own them.

- **Root use is narrow and visible.** The app reads root-only paths through one root-backed reader, copies bytes into its spool and writes nothing outside it. A root source declares `needs: root`; where root is denied the input shows `unavailable(permission_denied(root))`, never empty. Root never substitutes for a grant the platform enforces.
- **The producer runs in the app**, linking the one producer client (`sinex-transport`) through a generated binding, so its traffic uses the app's network path and the app's existing foreground service and battery exemptions. The app keeps writing its native files; the spool takes ownership of a file the moment it is complete (a rename on the same filesystem), so one copy exists on the phone, held until Sinex commits it.
- **Battery and network:** connect on demand; bulk data by its transfer class; location cadence and sensor rate are deployment settings, reported with their measured battery cost and never presented as continuous observation.
- **Sequence.** First, the deployment's phone relay becomes a stateless relay: it turns each upload into material and event frames under the phone's own producer identity, waits for Sinex's acknowledgement, then answers the phone, keeping no bytes; upload identity gives a deterministic frame identity, so a retried upload is the same frame. Second, the app links the client and the relay goes. The relay belongs to the deployment, not to Sinex.

## Polylogue on several hosts

- Polylogue on each host is the producer `(polylogue, <host>)` under the one `polylogue.*` kind family, with its own key. Session IDs are provider IDs; the same session captured twice converges by stable keys.
- Each host's Polylogue consumes the confirmed stream and holds a full index, so a query on one host sees the other's sessions. While Sinex is unreachable it reads its own index with lag shown, and capture spools.
- Work groups by repository identity (git common directory and root commit), so a repository checked out at different paths on two hosts is one project.
- Re-parse ownership is placement. An act on a session that runs on another host is an authored request that only the owning host's daemon executes.
- There is no canonical publisher per family: with stable keys and per-host producers no host needs to publish summaries for the others.

## Surfaces

```
sinexctl hosts                       # one row per host and producer: contact, held, spool and runway, clock offset
sinexctl hosts show <host>           # producers, inputs, contact history, clock samples, held and spool
sinexctl sources --host <host>       # the Sources grid for one host; by default rows group by host
sinexctl placement                   # host-free inputs and where each is placed
sinex-capture key                    # on a new host: generate this producer's key and print the public half
sinex-capture status                 # on the host: spool, held per input, last acknowledgement, runway
```

```
host     kind         producer   contact       held             spool / runway   clock
kestrel  workstation  sinexd     —             —                —                reference
                      session    live          0                12 MiB / —       +0.000 s
server   server       capture    live          0                0 / 412 d        −0.004 s ±0.011
phone    phone        app        2 h 14 m ago  1.3 GB · 9 in.   1.3 GB / 21 d    +2.31 s ±0.04
```

Placement and enrollment are configuration: the CLI shows them and never edits them. Over MCP, `sinex_status` gains a hosts section and `sinex_query` serves the `hosts` unit, the `host` field, `host:<name>` and `group by host`. The query language gains a `host` field on every event (derived events carry their scope's host or none) and uses aligned time automatically when a query spans hosts. References: `host:<name>` and `producer:<component>@<host>`.

In the web, Sources groups rows by host with a header row for contact, held, runway and clock, and draws `held` hatched with its arrival estimate; `/hosts/<name>` shows a 30-day contact strip, the clock offset with its uncertainty band, spool size over time and inputs with their transfer classes; Day and Timeline get one lane per host plus the operator lane, with late spans marked.

## Laws

Stored with the scope `sx.`.

- **mh.one-host-per-credential**: every frame's host is its credential's configured host; a host named in a payload is stored as a claim and never changes input identity. *(protocol test with a forged claim)*
- **mh.no-shared-secret**: no host other than the Sinex host holds another producer's credential; configuration holds only public keys. *(Nix evaluation check)*
- **mh.order-per-namespace**: reading order never compares readings from two producer namespaces; two hosts' readings of one natural key stay separate unless the key is host-free. *(property)*
- **mh.ts-never-rewritten**: no offset estimate, anchor or replay changes a reading's stored `ts_orig` except by re-framing in a replay, which mints a new reading. *(property)*
- **mh.reanchor**: a record framed during an implausible wall-clock interval with a monotonic anchor is `unknown` at first and, after replay with a later anchor on the same boot, `captured` with a monotonic clock and the re-anchored mark; without an anchor it stays `unknown`. *(replay test with an injected clock step)*
- **mh.offset-labelled**: a reading from a host whose estimated offset exceeds the kind's precision carries the offset and its uncertainty in its time evidence. *(VM test with a skewed clock)*
- **mh.held-not-gap**: intervals after a producer's last `captured_through` read `held` until reported, and become `gap` only from the producer's gap frames or reported stop states. *(integration)*
- **mh.week-offline**: a capture node disconnected for seven simulated days, with a 3 s clock skew and one reboot, loses and duplicates nothing after reconnecting; coverage goes from held to complete with no gap. *(two-VM test)*
- **mh.spool-full-gap**: with the spool's filesystem full, capture stops with a counted gap and no spooled record is removed before it is settled. *(VM test with a small filesystem)*
- **mh.transfer-not-capture**: captured bytes and record counts are identical whatever the network or power state; only send time differs. *(property over a simulated network schedule)*
- **mh.placement-exclusive**: a producer offering a host-free input not placed on its host is refused at `hello`; two hosts never poll one account at once. *(integration)*
- **mh.placement-moves-cursor**: after a placement move, every provider record after the committed position is admitted exactly once. *(integration with a mock provider)*
- **mh.late-recomputes**: admitting held records into closed day keys recomputes those keys once per writer group, and the day pack equals a run that had the data on time. *(derivation test)*
- **mh.same-declaration-any-host**: a source declaration runs unchanged on any host; no host-specific source code exists for a format the desktop shares. *(registry lint)*
- **mh.relay-stateless**: the phone relay keeps no bytes after it replies and replies success only after Sinex's material acknowledgement. *(relay test with Sinex stopped)*
- **mh.root-visible**: every root source on a host without root shows `unavailable(permission_denied(root))`. *(instrumentation test)*

Late facts are delivered passively by the common signals law `late-is-passive`. The fusion law `fusion-scope` ([derivations](derivations.md#laws)), the producer laws, `cov.no-false-empty` and `cap.no-host-coupling` ([sources](sources.md#laws)) also hold across hosts.
