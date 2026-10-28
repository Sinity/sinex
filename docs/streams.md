# Streams

The base-layer streams are Sinex's live captures of the desktop and the machine: the D-Bus buses, evdev input, Wayland selections, the accessibility bus, PipeWire audio, screen video, netlink, journald, Hyprland and the browser. This page states, for each stream, what it captures, the raw format of its spool, the generic event it produces and the checks it must pass, together with the spool format and the segment spool driver they share. The general capture model (sources, inputs, capture survival, gaps and health) is in [sources](sources.md); materials and how spooled bytes become them in [materials](materials.md); the typed events derived from the generic stream events in [derivations](derivations.md); each source family's full-fidelity requirements in [source families](source-families.md); output privacy in [privacy](privacy.md).

## One capture per base layer

A stream is one producer-side capture of a base layer, stored raw in its own format. Each stream produces one compact generic event per natural record; typed events (a notification shown, a key pressed, a clipboard copy, screen text) are [derivations](derivations.md) over those generic events, never separate captures. Notifications, MPRIS players, power, network, Bluetooth, mounts, logind and systemd unit lifecycles are all derivations of the bus stream. Ruling, 2025-06-13: "do capture generic DbusSignal. Don't worry about redundancy for now" (cc:f0bb2bee-4ea1-416c-ac7f-f5d0cf9216c6:0640b66c-a353-4ac9-8883-dc141595187e).

| Stream | Stored raw | Spool | Generic event |
|---|---|---|---|
| [D-Bus](#d-bus) | every message on the system bus and each session bus, through a monitor connection | pcapng, `LINKTYPE_DBUS` | `bus.message` |
| [evdev](#evdev) | every input device's events with kernel timestamps, before any remapping | pcapng, `LINKTYPE_USER0` | `input.event`, one per `SYN_REPORT` packet |
| [Wayland selections](#wayland-selections) | every offered MIME type's exact bytes, clipboard and primary, on every seat | JSON-line journal plus one byte file per transfer | `selection.offer` |
| [AT-SPI](#at-spi) | the accessibility bus, plus trees read on focus change | pcapng, `LINKTYPE_DBUS` | `bus.message` for observed traffic; `a11y.event`, one per node finished in a capture-initiated read |
| [PipeWire](#pipewire) | every source, sink monitor and application stream | encoded audio segments | `audio.segment` |
| [screen](#screen) | continuous video per output | encoded video segments | `screen.segment` |
| [netlink](#netlink) | conntrack `NEW`, `UPDATE` and `DESTROY`; udev uevents; audit records | pcapng, `LINKTYPE_NETLINK` | `net.flow`, `device.uevent`, `audit.record` |
| [journald](#journald) | entries in journal export format | export format | `journal.entry` |
| [Hyprland](#hyprland) | socket2 lines plus `hyprctl -j` snapshots | socket2 text | the window and workspace kinds |
| [browser](#browser) | profile rows and files; the extension producer's observations | rows and files | the profile row kinds |

Every interval in which a stream may have missed data is a `capture.gap` event ([sources](sources.md#gaps)); the spool driver below writes it. Connection events (`bus.connection`, `selection.connection`) record connection states only; a missed interval is always a `capture.gap`.

## Spools

### pcapng

Every binary spool is pcapng, so any standard pcapng reader can parse it. D-Bus and AT-SPI use `LINKTYPE_DBUS` (231); evdev uses `LINKTYPE_USER0` with one interface description block per device open and one enhanced packet block per `SYN_REPORT` packet; netlink uses `LINKTYPE_NETLINK`. Text streams (Hyprland socket2, journal export, the selection journal) keep their own framing.

Every pcapng segment follows the same conventions:

- Each segment is a standard section: a section header block, interface description blocks, then one enhanced packet block per record.
- `SnapLen=0`; captured length equals original length. Nothing is truncated.
- `if_tsresol=9`: block timestamps are receive time in nanoseconds.
- `epb_packetid` holds the record's monotonic sequence within its capture UUID.
- Capture metadata (capture UUID, boot ID, bus ID, bus setting, UID, monotonic receive time, connection starts and ends, gap facts and stream-specific acquisition facts) goes in standard comment options of the section header, interface description and interface statistics blocks. These are metadata records, never fabricated packets.
- A metadata record longer than one comment option (whose length field is 16 bits) continues across ordered continuation options ending with a completion marker. The option length is a transfer granularity, never a reason to truncate a record.
- Each segment repeats the section metadata a reader needs to frame it, so any stored part can be parsed on its own.
- Readers check block lengths, alignment and packet lengths with checked arithmetic. An incomplete tail of an active segment waits; a truncated tail of a closed segment is a record failure with its bytes kept, and reading continues with later segments. Corruption never reads as an empty stream.

### The segment spool driver

One segment spool driver in `sinex-source-core` writes every spool, binary or text:

- **Rotation** at 100 MiB or one hour, whichever comes first, as targets. Rotation happens only between records; an oversized valid record is written whole into its own segment.
- **Sync** at least once a second, including when idle, on close and on shutdown, and a directory sync when a segment is created or rotated. The driver reports the last synced sequence; a crash can lose the unsynced interval, and that window is reported, not hidden.
- **A closed marker** is written after each finished segment's final record. Restart opens the next segment rather than appending over a torn suffix.
- **An interrupted tail is kept,** and its readable records are admitted.
- **Gaps** use the one gap encoding: sequence numbers are assigned before a record is enqueued, so a record lost to a local overflow leaves a counted hole, and the driver writes the `capture.gap` once storage resumes. Gap counters travel on a separate small control path, so a saturated packet queue cannot hide the fault.
- **Removal** of a spool copy happens only for acknowledged, closed segments behind the committed cursor ([capture survival](sources.md#capture-survival)), and only when no stream-specific pin (AT-SPI unfinished work, an open selection transfer) holds them.
- **A writer queue** separates socket draining from spool writes and syncs, so a slow disk never stalls the capture socket. Its packet and byte bounds are sized from the measured burst rate and storage-stall duration with recorded headroom; they are never a limit on the size of a valid record. High-water marks, bytes, wait duration and overflow counts are exposed. A large queue is finite protection, not a claim of losslessness through an unlimited disk stall.

### The spool holds the raw format

The spool format is the raw format. There is no deduplication, debounce, MIME selection, size ceiling or re-encoding at capture. Wrapping lines in a JSON envelope or re-encoding D-Bus as JSON is a lossy transform and is not done. Noise is fixed at its source, never by a capture filter ([retention and redundancy](sources.md#retention-and-redundancy)).

### Placement and modes

- **Session spools** (session bus, AT-SPI, selections) live at `${stateRoot}/sinex-session/<user>`, with 0700 directories and 0600 files. The session producer reads its own spool, so no group access is needed. The Sinex module declares the state-root option and its default; the deployment chooses the host path, normally bulk storage rather than a wear-limited system disk.
- **Daemon-read spools** (evdev, and the system-bus monitor) live under `sinexd`'s state directory: 2750 directories and 0640 files owned by the capture account with group `sinex`, created with explicit modes under `UMask=0027`. Directories are opened without following symlinks and segments are created directory-relative with `O_NOFOLLOW|O_CREAT|O_EXCL`.

## Grain

**One compact generic event per natural record.** The generic event carries header fields and the record's location in the raw capture (material, block offset and length). Arguments, bodies and native records are decoded from the raw bytes on demand and never copied into the payload, because the stored bytes are already the lossless copy and a second copy would multiply database size by the stream rate.

**Traffic a capture causes is not a natural record.** A capture's own query traffic (AT-SPI tree-read calls and replies, D-Bus bootstrap reads and registrations) is kept complete in the raw capture but gets no generic event. A packet is caused by the capture when it is sent by, or addressed to, one of the capture's own ordinary connections. The natural record of a capture-initiated read is its result unit, citing its packet range: for AT-SPI, one node finished. Only traffic the capture observes becomes `bus.message`.

**Packet-range reads.** Derivations read a generic event's bytes as a checked range within its material, through one mechanism in the derivation runner shared by every stream; the bytes read scale with the relevant packet ranges, not with segment length times event count ([derivations](derivations.md)).

**Search.** The generic stream kinds that carry raw content (`bus.message`, the `a11y.event` node records) declare `search: none`; their typed derived events are indexed under their own declarations. Excluding a kind from search restricts neither raw queries nor what is stored.

**Arguments on demand.** `show --arguments` decodes a generic event's arguments from its packet. It has the exposure of reading the raw material: it is gated by the material's class, and field rules are not reapplied to the decoded values ([privacy](privacy.md)).

**Classes.** The session streams' sources and generic kinds (`bus.message`, `desktop.input`, `desktop.selection`, `desktop.a11y`) are declared `Guarded` explicitly, and derived events inherit their parents' classes. Classes govern output only: the buses, input and selections are captured whole, nothing is filtered at capture, and the raw material holds plaintext. Privacy of captured input is applied at output and never limits capture ([privacy](privacy.md)).

## Sampling

Sampling is a last resort and is off by default.

- If measured admission cannot keep up with a stream on a host, that stream may declare deterministic sampling of **events only**. The raw capture stays complete: every record is spooled, stored and acknowledged, and replay at stride 1 restores every event and its derivations.
- The one rule is a versioned hash of `(capture_uuid, sequence)` compared against the declared rate. Every stream uses it.
- Only continuous samples can be sampled: evdev packets with no `EV_KEY` or `EV_SW` record (pointer motion), and sensor samples. Keys, buttons, switches, signals, anything a derivation counts, and any packet a typed-view rule matches are never sampled. The parser decides exemptions and counts them.
- Sampling is applied after parser state has advanced over every record, so a kept record still carries its correlation facts when its partner was sampled out.
- Sampled counts, the rate and the exemptions are reported in status, and derived coverage over sampled parents is reported as sampled. Sampling is never silent and never reduces raw capture, disk or tree-reading load.
- The rate is chosen from measured demand, available admission and runner lag, and recorded in the deployment; there is no default rate.

## Processes and privileges

Capture stays in Sinex's own processes.

- `sinexd` reads files, databases, APIs and journals as its own user, through the module's access bridge: read ACLs with default ACLs on directories, read-only bind mounts, and udev rules for device nodes.
- Session-bound readers (session bus, AT-SPI, selections, PipeWire, screen, Hyprland) run in `sinex-session` as the operator.
- Privileged captures (evdev, netlink, the system-bus monitor) are small separate producers with no database or NATS dependency; `sinexd` reads their spools.
- A satellite node (`sinex-capture`) keeps a local spool, its own credentials and no database, and uploads bytes over chunk frames ([producers](producers.md)).
- A missing grant is reported as `permission denied` with the grant it needs, never as silence or an empty stream.
- Every stream starts capturing before any NATS or database connection and keeps spooling through downstream outages ([capture survival](sources.md#capture-survival)).

## D-Bus

### Capture

Source `machine.dbus`. One monitor connection per actual bus: the system bus and each applicable session bus. Monitors are keyed by bus ID, so an alias of a bus never gets a second monitor, and the configured binding path stays stable so a discovered alias cannot reset its cursor.

- The monitor attaches an unfiltered message stream and calls `org.freedesktop.DBus.Monitoring.BecomeMonitor([], 0)`; an empty rule list is all traffic, including unicast calls and replies. Capture begins at the successful conversion and keeps the queued traffic that follows its reply. The bus ID is read before conversion; a converted monitor sends nothing further and hosts no object server.
- The broker accepts a monitor only from UID 0 or the bus owner. The system-bus monitor therefore runs as root with group `sinex`, an empty capability bounding set, `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateNetwork` and `RestrictAddressFamilies=AF_UNIX`; its only writable path is its spool. The session-bus monitor runs as the operator, who owns that bus.
- A second, ordinary connection per bus holds `org.freedesktop.systemd1.Manager.Subscribe()` where a manager exists, for the whole capture, reissued after manager owner replacement or reconnect. It subscribes after the monitor is ready, then makes the bootstrap reads: `ListNames`, `GetNameOwner` for acquired well-known names, MPRIS root and Player `GetAll` for existing and newly acquired players, and systemd `ListUnits`, with activation disabled. A failed bootstrap read is an error, never empty state; bootstrap replies observe state at their reply, not an atomic history.
- Bootstrap and subscription traffic is capture-caused: it is in the spool and parsed for carry, but gets no `bus.message`. Each bootstrap read's result is one record citing the packet range of its call and reply, and initial-state typed views (players, units, name owners) derive from it.
- Private peer sockets (including systemd's private socket) are not bus traffic. Journald is captured independently; kernel and udev datagrams belong to [netlink](#netlink).

**Unix descriptors.** The monitor negotiates descriptor passing, because the broker disconnects a monitor that cannot accept descriptor-bearing traffic. It keeps the `UNIX_FDS` header, the body's indexes, the advertised count and the received count, copies the packet bytes, and drops the message and every handle-owning clone in the receive task before queuing bytes or waiting on disk. A passed descriptor is never read, held across a sync or copied into parser carry; raw bytes cannot recreate a kernel handle.

### Raw format

Each enhanced packet block holds one complete message as received from the broker: serialized header, signature and body, including unknown header fields. The monitor does no body decoding. Block timestamps are receive time; `epb_packetid` is the sequence within the capture UUID; descriptor counts and monotonic receive time are packet options.

### bus.message

| Field | Content |
|---|---|
| `capture_uuid`, `sequence` | identity; `native_id` is `capture_uuid:sequence` |
| receive time, monotonic nanoseconds | from the capture |
| message type, endianness, protocol version, flags, serial | wire header |
| reply serial, sender, destination, object path, interface, member, signature, error name | each present only when the message has it |
| advertised and received descriptor counts | |
| packet offset and length | within the cited material |
| owner and correlation annotations | below |

Host, bus and boot facts live in source and material metadata, not in each payload. There is no body, value tree or argument JSON in the payload. The safe wire decoder in `sinex-model` reads checked slices and signature-directed values for both the parser and derivations; it never constructs a descriptor from an `h` index. A decode error keeps the generic event and its cited bytes with an explicit error kind, never empty successful arguments.

Event time is the captured receive time with captured quality; replay keeps it. Equivalence uses capture UUID and sequence, never the sender's serial.

### Parser carry

Parser carry follows packet sequence, independent of admission order and wall-clock changes, and holds only headers, receive times, packet references and the few request facts bootstrap replies need:

- **Name ownership.** Name-to-owner and owner-to-name sets. `NameOwnerChanged` moves only the changed name; a successful `GetNameOwner` reply establishes ownership at that reply, and later changes win. Each message is annotated with the sender and destination name sets at its own sequence. Ownership not yet observed stays unresolved; nothing is recomputed retroactively.
- **Call and reply.** Pending calls are keyed by `(caller unique name, serial)` within the capture UUID. A return or error finds its call by destination and reply serial and is annotated with the call's sequence, time, relevant headers and packet reference; its sender is validated against the owner known for the call's destination at the call's sequence, accepting bus-generated errors and marking unresolved or conflicting attribution. Serial reuse starts a new call; ambiguity is explicit and no match is fabricated. No-reply calls are not kept pending. Outstanding calls are cleared when their caller disconnects or the capture ends, never by an elapsed-time cutoff or count cap; the outstanding count and carry bytes are exposed, and memory exhaustion is a reported local fault.

These annotations are part of the reply's generic payload, so a typed view classifies a reply from the call headers attached to it without querying earlier events or fetching call bodies.

### Capture connections and replay

- A new capture UUID begins at every successful monitor conversion: process start, reconnect, restart. The first material of a UUID has no previous part; materials within one UUID link as parts, and rotation keeps the UUID and sequence, so a pending call can cross a segment. Parser carry is cleared only at a new UUID. Cursor recovery never links a new capture to the previous one because the spool path is unchanged.
- Replay of any interval starts at the beginning of its capture UUID, not at the beginning of all stored bus history; its cost is linear in that connection's stored bytes and is reported. There are no parser checkpoints and no reconnects to shorten replay, since a reconnect is a recording gap.
- Parser carry, cursor and events are committed together through the producer acknowledgment path.

### Failures

- When the writer queue is full, the receive task keeps draining, disposes of the packet immediately, and records the exact count and receive interval of locally omitted packets as an `overflow` gap. The monitor never applies backpressure to the broker's socket, because the broker disconnects a monitor that exhausts its quota.
- EOF, a bus restart, permission denial or unavailable monitoring opens a `disconnected` gap (with unknown broker-side loss) and retries with interruptible backoff from 1 to 30 seconds. A reconnect gets a new capture UUID and repeats subscription and bootstrap; other streams continue.
- Local disk exhaustion or a writer failure ends that capture with a visible error; existing segments are never deleted.
- Stop interrupts reads, bootstrap and backoff, then flushes and syncs the last accepted packet.

A bus material mixes notifications, player data and unrelated traffic. Tombstoning a notification removes its records; the segment's bytes remain while other records read from them ([lifecycle](lifecycle.md)).

## evdev

### Capture

Source `desktop.input`. Every `/dev/input/event*` node is captured, including virtual keyboards, mice, touch, tablets, controllers, switches and unknown devices; discovery decides what applies and an exclusion needs a recorded reason. Every native `input_event` the kernel returns is kept with its kernel timestamp, including synchronization and miscellaneous records, before debounce or remapping. Ruling, 2026-03-17: "certainly browser profile including its webhistory, and keylogger, mouse ofc as well" (cc:6c1674bb-e912-438c-9de4-62658fb48cf4:41d66e2e-0047-4e0f-b937-56044c511122).

- **Reading.** `read(2)` into reusable buffers with epoll gives the exact bytes. Device metadata and state queries use a separately opened read-only descriptor, because a state-query ioctl can flush queued events on the descriptor it is issued on. Capture opens read-only, nonblocking and close-on-exec, and never grabs, masks, debounces or reinjects. The default `CLOCK_REALTIME` is kept; `EVIOCSCLOCKID` is never called, since it can flush queued records. The synchronizing device iterator is not used, because it can substitute synthesized events.
- **The scanner** runs as the Sinex account with supplementary group `input`, starts before any graphical session, database or NATS, and is the **one spool writer**: one binding, one part sequence, one cursor. Logind device handover cannot serve here, because it requires the session's exclusive controller and pauses inactive devices.
- **Discovery.** The udev monitor starts before enumeration and queued changes are reconciled afterwards. Handles are keyed by device generation, not `eventN`. Permission failures are retried, removal or `ENODEV` closes the device, and a monitor overflow or reconnect triggers reconciliation. One failed node never stops others. Device metadata (ID, name, phys and uniq as bytes, sysfs path, major and minor, by-id and by-path links, seat, capabilities, properties, absolute-axis ranges) is kept; missing metadata stays missing.
- **Layers.** Each device is nonvirtual, virtual or unknown, from sysfs. A virtual device's parent is known only from an explicit uinput creation report naming its system name and the source open; equal names never establish parentage.
- **State queries.** On open and after a loss, the scanner queries keys, switches, LEDs, absolute axes and multitouch slots where supported, keeping unsupported results and errors, and records the sequence interval the query spans rather than claiming an atomic state. After `SYN_DROPPED` the intervening bytes are kept, transitions are marked unreliable through the next `SYN_REPORT` and state query, and neither missing edges nor a loss count is invented.
- **Back-pressure.** Packet assembly uses fallible allocation and lets the kernel queue absorb a slow writer; allocation or disk failure is a local capture fault, and the resulting `SYN_DROPPED` is visible. Bytes not yet spooled count as unspooled in health and crash-loss reporting.

### Forwarding through remapping pipelines

A device that a remapping pipeline grabs is captured through `sinex-session input-forward --device DEVNODE --socket SOCKET --run-as USER`, placed at the head of the pipeline:

- Launched as root, the helper opens and verifies the device, drops supplementary groups, GID and UID to the configured account before touching locks or the socket, takes the per-device advisory lock, then grabs the device.
- It forwards every original byte to stdout in order, before debounce or remapping, and sends a capture copy with metadata to the scanner over a local Unix stream socket. Stdout never waits for the scanner, a disk sync or NATS. The helper never opens the spool.
- Read, record and packet sequences are assigned by the acquisition owner before sending; the handshake carries the open UUID and next sequences, so a scanner restart cannot renumber a surviving helper's packets.
- If the socket fails or the scanner cannot keep up, forwarding continues; the unsent capture copy is discarded and its known sequence range is sent as a gap on reconnect (unknown after a crash). No unbounded queue grows behind a failed scanner.
- The runtime directory `/run/sinex/evdev` is 0750 and the socket 0660; the scanner checks `SO_PEERCRED` and accepts capture messages only from the configured UID.
- Whoever holds a device's lock owns its acquisition. Without a working helper the scanner takes the lock and reads directly; a helper takes over by handover through the socket, and both record the boundary. Every new descriptor gets a new open UUID, and the interval between owners is reported with an unknown loss extent. A grabbed device with no working helper is unavailable, never proven idle by empty reads.
- The remapping pipeline is supervised as a whole and restarted together, so a new reader never feeds an abandoned uinput device.

### Raw format

The spool is pcapng with `LINKTYPE_USER0`, under `${stateDir}/spool/evdev/`. Interleaved devices share one segment sequence and one ordering.

| Block | Holds | Event |
|---|---|---|
| Interface description, one per device open | the device description: host, boot ID, open UUID, ABI, endianness and native offsets, clock, layer, class, capabilities, optional explicit parent; non-UTF-8 strings as hex | `input.device`; unchanged repeated metadata has the same equivalence key |
| Enhanced packet, one per `SYN_REPORT` packet | the packet layout below | `input.event` |
| Enhanced packet flagged incomplete | the bytes available when a close or capture boundary interrupted a packet | `input.capture` citing the raw range; never an invented complete packet |
| Interface statistics comment | a state query: ioctl, exact returned bytes, query interval and sequence range, reason, errors | `input.state` |
| Interface statistics comment | lifecycle: scanner and device open and close, unavailable and recovered, owner change | `input.capture` |
| Interface statistics comment | local loss, owner-change loss | `capture.gap` |

Active device descriptions are repeated at the start of each segment, so replay needs no list of earlier opens.

The enhanced packet's data is a packet header, its read contributions, and the native records:

- header: `packet_sequence:u64 | first_record_sequence:u64 | record_count:u64 | contribution_count:u64 | flags:u8`; `epb_packetid` repeats `packet_sequence`;
- each contribution: `read_sequence:u64 | record_count:u64 | received_realtime_before_ns:i64 | received_monotonic_ns:i64 | received_boottime_ns:i64 | received_realtime_after_ns:i64`, followed by that read's native records;
- added integers are little-endian; native records keep their ABI and endianness. The clock samples bracket the read; a read covering several packets repeats its samples, and a packet spanning reads keeps every contribution.

Packets are assembled per open across reads, keeping `SYN_MT_REPORT` and every other record inside them. There is no packet-size refusal or truncation. An orderly close or shutdown writes available bytes of an unfinished packet as an incomplete block; an unclean end has an unknown unspooled extent.

### input.event

`input.event` holds compact header facts: device open UUID, packet and first-record sequence, record count, event-family mask, ABI and endianness, layer and device class, descriptor reference, the terminating record's native time and clock, receipt time and sampling interval. A family outside the mask's range sets an unknown-family flag; the mask never excludes records. Codes, values and native records are decoded from stored bytes on demand.

- **Key:** `(device_open, packet_sequence)`. Open UUIDs are globally unique and their descriptions fix host and boot, so identical bytes and timestamps stay distinct. Other evdev records key by open plus sequence, or by scanner, segment and offset where no open exists.
- **Time:** the terminating record's valid kernel time is the packet's event time; an invalid time stays unknown and receipt time never substitutes for it. The bracketing clock samples expose clock steps and suspend.
- Parser carry holds active device descriptions and next sequences only; a scanner-start marker closes stale entries after an unclean restart.
- Unknown blocks and malformed complete blocks are record failures with their bytes kept.

Keys, pointer motion and scrolling, chords and typing cadence are [derivations](derivations.md) of `input.event`.

## Wayland selections

### Capture

Source `desktop.selection`, format `selection`, covers every seat and both selections in one binding. One capture connection on the session's Wayland socket, independent of the publisher, binds exactly one data-control protocol (ext-data-control version 1, otherwise wlr-data-control version 2) on every seat, including seats added later. It never sets a selection or acts as a clipboard manager. A missing protocol is reported unavailable and rediscovered after reconnect; a server with only wlr version 1 permits clipboard capture and reports primary as unavailable.

- Both selections (clipboard and primary) on every seat are captured: every advertised MIME name and the exact bytes each receive returns. Equal offers remain separate records, including re-offers by a clipboard persistence tool; data-control supplies no owner identity, so no offer is labelled as an application copy or suppressed by a guessed identity.
- **Every offered MIME type is read,** including content its owner marks as a password or secret. The owner's hint changes nothing at capture; its presence is kept as a field the operator's rules can select ([privacy](privacy.md)).
- **MIME names are exact bytes.** Names are read and sent back to the owner as their original bytes (a small adapter reads the protocol string argument as bytes, since the generated binding converts it lossily). They are stored as base64 with an optional strictly decoded UTF-8 display field; identity, hashing, distinctness and receive requests compare the bytes. Invalid UTF-8 names are still requested. MIME spelling is not normalized and no type is skipped, including `STRING`, `TEXT`, custom types and the password hint.
- **Receive is issued as soon as an offer arrives,** for every distinct MIME name, against the announced offer object, without waiting for another transfer. Journal records are synced asynchronously. Advertised order and duplicates are kept; each request records every index its name was offered at.
- **Transfers never block each other.** CLOEXEC pipes with nonblocking read ends are multiplexed with Wayland dispatch; file writes and hashing run in capture IO workers, so a slow owner cannot block another MIME, seat or selection. On replacement the previous offer is destroyed and already requested pipes keep draining. Resource failures are recorded per request and retried while the offer is valid. No size, MIME-count, repeat, debounce or duration cutoff applies; IO workers bound active buffers, so capture memory tracks active transfers, not payload sizes. Stalled pipes stay outstanding and visible.
- **Identity.** Each connection gets a UUID and every selection notification an increasing sequence; `offer_id` identifies one notification, including repeated and initial state. A seat is `(connection_id, registry_global)` with its reported name kept separately. Object numbers are never persistent identities.

The protocol cannot recover overwritten offers, identify the owning application, observe another client's receive, or prove an EOF holds everything the application meant to offer; an ignored receive can yield an empty EOF. The observed outcome is stored without claiming completeness, and source-window and paste attribution are derived evidence.

### Raw format

The spool is a JSON-line journal written by the segment spool driver, with each transfer's bytes in its own file:

- the journal holds offer records, request-start records (validated transfer UUID, exact name, timing), terminal transfer records and connection records;
- bytes are read into `bytes/<transfer_id>.partial` with fixed buffers and BLAKE3; at the terminal outcome the bytes are synced, the file is renamed to its transfer UUID and the directory synced, and only then is the terminal record (digest, length, outcome) appended. Paths are never built from MIME names;
- the reader is a thin adapter over the line-stream driver: each newly complete line is a record, and each completed transfer file is attached to that material as an owned file, so the bytes travel with the events that cite them ([producers](producers.md)). Acquisition sees only synced groups, and a terminal record exists only after its file is complete, so acquisition never waits on an active pipe;
- a transfer file is kept until the acknowledgment covering its terminal record and the saved cursor both exist. A closed segment is removed only once its cursor has passed and every request begun in it has a terminal result or a compact saved recovery entry.

### Events

| Kind | Fields beyond `connection_id`, `seat`, `received_at`, `monotonic_ns`, `boot_id` |
|---|---|
| `selection.offer` | `offer_id`, `sequence`, `selection: clipboard\|primary`, `initial`, `state: offered\|cleared`, ordered `mime_types` with exact base64 names and optional UTF-8 display names, `password_manager_hint_offered` |
| `selection.transfer` | `offer_id`, `transfer_id`, `mime_name_base64`, optional UTF-8 `mime_type`, `offered_indexes`, `offer_received_at`, nullable `requested_at`, `ended_at`, `outcome: eof\|read_error\|interrupted\|not_requested`, `bytes`, optional `blob: blake3:<hex>`, optional error kind and errno |
| `selection.connection` | `record_id`, `state: connected\|disconnected\|device_finished\|seat_removed`, protocol and version, primary support `observed\|unavailable\|unknown`, optional error kind |

- A null selection, including an initial one, is `cleared`; an offer with no advertised names is still `offered`.
- Each distinct MIME name produces one terminal transfer. An EOF with zero bytes owns the empty blob; a failure owns its observed prefix; no bytes read means no blob. Error messages that could contain payload are never stored.
- Keys: `offer_id`, `transfer_id`, and the connection record UUID. Hash equality shares bytes, never identity.
- Event time is `offer_received_at` with captured quality; transfer timing stays in `requested_at` and `ended_at`.

Decoded text, `clipboard.copied`, `clipboard.selected`, fused copies and pastes are [derivations](derivations.md).

### Failures and restart

- On restart, the capture task takes the exclusive spool-writer lock, recovers only its local journal and transfer files, and starts a fresh Wayland connection without waiting for NATS, frame resends or cursor recovery; one capture writer and one publisher share the state without a publisher-held capture lock.
- A start record without a terminal result becomes `interrupted`, keeping the actual prefix with its recomputed digest and never an invented EOF. A `.partial` file without a synced start record is also recorded as interrupted. Already journaled terminal IDs prevent finalizing a transfer twice. A partial final journal line is resolved before acquisition can pass it, never spliced into later JSON.
- A process crash and a power loss are distinguished: only synced groups are promised after power loss, and recovered lengths are reported.
- A reconnect creates a new connection ID and fresh initial observations, even when bytes equal the previous connection's; initial state cannot fill the gap. Seat removal stops new requests and drains existing pipes; shutdown closes remaining pipes and finalizes interrupted prefixes.
- A corrupt byte file, failed local write, full disk or unreadable spool is a visible local fault, never an acknowledged omission.

## AT-SPI

### Capture

The accessibility bus is captured through the D-Bus stream's machinery: its address from `org.a11y.Bus.GetAddress` on the session bus, its actual bus ID from `GetId`, one whole-bus monitor (`BecomeMonitor([], 0)`), the same pcapng framing with `LINKTYPE_DBUS`, sync, reconnection, descriptor handling and safe decoder. Source `desktop.a11y`, format `atspi`. A failure to obtain monitoring access is a named coverage fault, never a switch to signal-only capture.

- **Observed traffic** (signals, and calls and replies between other clients) gets the same compact `bus.message` as on the other buses.
- **Registration.** An ordinary connection calls `Registry.RegisterEvent` with an empty application restriction and the explicit interests `object:`, `window:`, `document:`, `focus:`, `terminal:`, `mouse:` and `keyboard:`, after the monitor is capturing. An empty event class is never registered as a wildcard: the empty application restriction, not an empty class, means every application. Unknown signals stay captured whenever emitted.
- **Triggers.** Scheduling scans the synced monitor records, so every trigger has an exact capture UUID and sequence even when signals repeat. Replay reads only the monitor, never scheduling callbacks.
- Ordinary connections request no direct peer connections, since that traffic would bypass the monitored bus. Typed proxies may decode results for traversal, but the complete replies come from the monitor.
- The Sinex module enables the accessibility bus and toolkit accessibility (`services.gnome.at-spi2-core.enable`, `QT_LINUX_ACCESSIBILITY_ALWAYS_ON=1`, `ACCESSIBILITY_ENABLED=1`) where the stream applies; Chromium-based browsers additionally need renderer accessibility enabled. A terminal that publishes no accessibility tree is covered by terminal recording ([source families](source-families.md)).

### Tree reads

The focused window is read on focus gain and every 30 seconds while it stays focused.

- **Startup.** Currently active and focused windows found by enumerating applications, top-level objects and states are read. A missing publisher, failed state read or empty registry is a coverage fact.
- **Triggers** are `Focus.Focus`, focused-state gains and window activation; loss signals are kept without changing the selected window. The root is the nearest frame, window, dialog or alert ancestor, stopping at the application, null, a repeated reference or a recorded lookup failure; without a proven window the read starts at the focused object and records that choice. There is no depth cap.
- **The timer** records each due tick with its scheduled time, even when the work is delayed. It covers scrolling, new content and child changes whose signals carry references rather than text. Raw children, text and property signals stay complete. A transient subtree that disappears between reads keeps only its signals; its unobserved text is not claimed.
- **Merging.** Per `(bus ID, unique owner, root)` there is at most one active and one unstarted read. A trigger joins the unstarted read if one exists; trigger membership is frozen before its first node call, so a trigger arriving during a read belongs to the next one. Different roots never merge, and focus signals are never merged or dropped. Each trigger links to its request through its own small record.
- **Traversal.** Each tree starts with one `Cache.GetItems` per participating application, whose items supply identity, parent, index, child count, interfaces, name, role, description and state for the nodes it lists. `Accessible.GetChildren` runs once per expanded node; `GetAll(Accessible)` covers uncached properties; attributes, relations, component extents, Text `GetAll`, default attributes, text ranges and attribute runs are read separately. An unsupported bulk method is an explicit field failure.
- **No ceilings.** Finite readable trees have no node, depth, text or duration cap. Text is read over the complete character count in transfers of 16,384 characters (character offsets, not bytes), with an explicit empty read for zero. Attribute runs must advance or produce a named failure. Invisible, offscreen and sensitive-role nodes are read.
- **Walk state.** The walk is iterative, with frontier, visited references, trigger links and unfinished calls in `work.sqlite` beside the spool, streamed rather than held in memory. Repeated references are recorded as edges without expanding them again. A provider that cannot list children yields a partial tree, never a zero-child success; field failures let other fields and nodes continue.
- **Scheduling.** One ordinary query connection, at most one outstanding tree call per application owner, rotating after each node and yielding between calls and text transfers so signal draining and stop run. A two-second call deadline is a scheduling safeguard: a timed-out field is marked incomplete and retried with interruptible backoff from 250 ms to 30 seconds while its owner exists, with no attempt limit and without blocking ready work for other roots. Confirmed disappearance or rejection is a terminal field failure; an owner loss closes its outstanding work. A hung application never stops another application's reads or signal capture.
- A tree's status is complete, partial with terminal failures, or interrupted, with outstanding retries counted separately. A timed-out node is never reported as a complete tree. Reported read intervals and delays are the actual ones; a tree is never presented as existing at focus time.

### Read records

Tree-read calls and replies are capture-caused: they stay complete in the spool and get no `bus.message`. The read is recorded as logical records in interface statistics comments, through the shared metadata mechanism:

| Record | Content |
|---|---|
| `TreeStarted` | attempt, request, reason, read start |
| `TriggerLinked` | request, stable trigger reference, trigger time |
| `TreeRoot` | attempt, root, application reference, resolving packet interval |
| `NodeStarted` | attempt, object, query-connection unique name, start time |
| `NodeFinished` | attempt, object, end time, packet interval, call and failure counts, status |
| `TreeFinished` | attempt, status, node, call and failure counts, end time |

- **`NodeFinished` is a node's one observation** and becomes one `a11y.event`; its packet interval, query connection and the caller-serial correlation identify every exact reply, including missing and timed-out ones. `NodeStarted` is context for it, not another event. Tree lifecycle and trigger-link records also become compact `a11y.event` observations.
- Node-start context is written before the node's calls are issued; with one active node per application connection, the parser attaches `(attempt, object)` to calls and replies through ordinary caller-serial carry, and a late reply keeps its original context. A timed-out node is paused with recorded resume context before its connection does other work.
- `NodeFinished` is written only after the writer confirms the node's replies were appended. Records sync in cohorts on the shared one-second sync.
- References use capture UUID and sequence. Host, session and bus are binding and material metadata; an object reference is bus ID, unique owner and path. Receipt time has captured quality; trigger and read times stay separate.
- A monitor loss while ordinary calls succeed interrupts the observation; decoded proxy values alone never complete it.

Focus, text changes, screen text and observed windows are [derivations](derivations.md) of these records and the observed `bus.message` events.

### Work state, restart and cleanup

- `work.sqlite` is rebuildable work, not the only copy of captured replies or triggers. Its checkpoint advances transactionally only after scheduling from synced records, and node completions are recorded only through the writer's last synced position.
- Cleanup honours both publication and the work checkpoint: segments holding unfinished request, context or call evidence are pinned, `TreeFinished` is synced before a request's pins are released, and a missing or unreadable work database blocks cleanup until it is rebuilt from the pinned records. A broken pin is a visible fault, never an empty work queue.
- After a restart, uncertain in-flight attempts close as interrupted. If bus ID and owner still match, a new read linked to the original request runs with its real later times; otherwise the disappearance is recorded. A reconnect gets a new capture UUID and a fresh startup observation; missed ticks or focus are never replayed into supposed past trees.
- A bus disconnection opens a gap, stops ordinary reads and retries discovery with interruptible backoff from 250 ms to 30 seconds. Stop cancels connection, call, sync and retry waits before joining.

## PipeWire

Every PipeWire capture source, sink monitor and application stream available to the user's audio graph is recorded as continuous, independently decodable segments with clock and discontinuity facts, without voice gating, downmixing or resampling; each closed segment is an `audio.segment`. Hotplug, rate changes and device failures are events or gaps, and encoder drops are `encoder` gaps. The full requirements are in [source families](source-families.md).

## screen

Every display is recorded continuously at its actual mode as standard video segments whose every frame is reconstructible; each closed segment is a `screen.segment`. Dropped frames and mode changes are counted and recorded as `encoder` gaps, and an unsupported encoder is an unavailable capability, never a silently lower rate. The full requirements are in [source families](source-families.md).

## netlink

A small privileged producer reads native netlink messages into a pcapng spool with `LINKTYPE_NETLINK`:

- conntrack `NEW`, `UPDATE` and `DESTROY` and the initial state, both directions, as `net.flow`; a snapshot entry is never a new connection;
- udev kernel and processed uevents with all properties, sequence, action and device path, as `device.uevent`; subscription comes before inventory, and inventory means present, never added;
- audit records with their companion records, as `audit.record`, attached to the existing audit owner or its multicast without replacing the system's audit controller.

Socket overflow is an `overflow` gap. The full requirements are in [source families](source-families.md).

## journald

Journald is stored in journal export format: every entry with repeated and binary fields, cursor, realtime, monotonic, boot and source times, namespace and all trusted and user fields, one `journal.entry` per entry. Reading resumes by the actual cursor; vacuum and rotation gaps are reported. The full requirements are in [source families](source-families.md).

## Hyprland

Hyprland's socket2 is stored whole in its own line framing, with `hyprctl -j` snapshots on connection and on relevant changes; the reader subscribes before taking a snapshot and drains events concurrently, and a snapshot is present state, never a fabricated open or start. Its records are the window and workspace kinds ([reference](reference/kinds.md)); unknown event text is kept. Other window managers are ordinary sources ([source families](source-families.md)).

## browser

The browser stream is the profile rows and files of every discovered profile, read as rows and files, plus the extension producer's observations of visits, navigation, tabs, frames, downloads and page content; their records are the profile row kinds. The full requirements are in [source families](source-families.md).

## Laws

- **dbus-packet-bytes-exact**: on a test broker with empty monitor rules, signals, calls, returns, errors and unicast under both bus settings (unknown interfaces, empty bodies, missing optional headers, both endians, maximum-width integers, NaN, nested values, duplicate dictionary keys, descriptor indexes, oversized packets) are spooled and read back byte-identical to what the broker delivered.
- **dbus-monitor-privilege**: the system-bus monitor succeeds as root or the bus owner under the declared hardening, and a missing privilege appears as `permission denied`, never an empty stream.
- **caused-traffic-no-generic-event**: bootstrap, subscription, registration and AT-SPI tree-read packets are present in the spool and produce no `bus.message`; each bootstrap read and each finished node produces exactly one result record citing its packet range.
- **dbus-descriptors-released**: a pipe descriptor passed on a test broker reaches EOF on its other end before the writer is released, and a monitor without descriptor negotiation is shown to be disconnected by the broker.
- **dbus-carry-deterministic**: with several names per owner, owner replacement, bootstrap races, serial reuse, no-reply calls, missing calls, broker errors, replies across midnight and across segments, several capture UUIDs, reversed admission and wall-clock reversal, replay reproduces every annotation and key.
- **capture-uuid-per-connection**: every monitor conversion starts a new capture UUID whose first material has no previous part; replay of a late minute reads only its own capture UUID's parts.
- **overflow-counted**: a full writer queue produces an `overflow` gap with the exact count and interval of omitted packets, and a broker disconnect a `disconnected` gap with an unknown count; the counts reconcile as spooled records equal admitted events plus sampled events plus failures and gaps.
- **spool-sync-and-tail**: an idle spool still syncs every second; a truncated closed tail is a record failure with bytes kept and later segments are read; an active incomplete tail waits.
- **generic-not-searched**: new and rebuilt read models hold zero search rows for `bus.message` and the `a11y.event` node records, while typed derived events are searchable under their classes.
- **arguments-only-on-demand**: default `show` and query paths read no packet body; `show --arguments` decodes exact types and is refused before any read when the caller's grant does not cover the material's class.
- **packet-range-reads**: bytes a derivation reads scale with the cited packet ranges, never with segment length times event count.
- **session-kinds-guarded**: the declarations of `bus.message`, `desktop.input`, `desktop.selection` and `desktop.a11y` are `Guarded`, and derived events inherit it.
- **evdev-pre-remap**: through a synthetic uinput pipeline, the forwarding helper's stdout is byte-identical to the device's output, and the spool holds the bytes before debounce and remapping.
- **evdev-packet-per-syn-report**: every split of reads and blocks yields the same packets, one `input.event` per `SYN_REPORT`, with `SYN_MT_REPORT`, unknown codes and oversized packets kept whole and per-record time preserved.
- **evdev-identical-packets-distinct**: two packets with equal bytes and timestamps, on interleaved opens, are two events.
- **evdev-forwarding-survives-capture**: a scanner, socket or disk fault leaves forwarding running and records the lost capture range as a gap after recovery; terminating a helper releases its grab and the scanner takes over.
- **evdev-state-query-keeps-queue**: state queries on the separate descriptor leave the capture descriptor's queued events intact.
- **evdev-syn-dropped-honest**: after `SYN_DROPPED`, the intervening bytes are kept, transitions are marked unreliable until the next state query, and no edge or loss count is invented.
- **evdev-spool-modes**: under a 0022 umask and the service accounts, spool directories are 2750 and files 0640, and a symlinked spool path is refused.
- **evdev-hotplug**: enumeration races, missed udev events, reused event numbers and permission recovery all end with every present device captured under a new open UUID.
- **selection-exact-bytes**: every advertised MIME type of every offer on every seat, including the password hint, custom types, NUL bytes and trailing newlines, is stored with its exact bytes and hash, and an invalid UTF-8 MIME name is sent back byte for byte (two names that collapse to the same lossy string stay distinct).
- **selection-no-cutoff**: simultaneous large transfers, a slow owner and a nonterminating owner never block other MIME types, seats or selections, nothing is truncated, and capture memory tracks active buffers rather than payload size.
- **selection-repeats-kept**: equal repeated offers, clears and initial state each produce their own `selection.offer`.
- **selection-crash-recovery**: a crash at any step of a transfer (start record, byte rename, directory sync, terminal append, frame preparation, admission, cursor save, acknowledgment) ends with one terminal result per transfer, `interrupted` with its actual prefix where the terminal record was not synced, and replay from stored material and owned bytes after spool cleanup.
- **selection-capture-before-ack**: with NATS down and saved unacknowledged frames, new offers are spooled before any acknowledgment arrives.
- **atspi-one-read-per-pending-root**: many triggers before a read starts produce one traversal linked to every trigger; a trigger after the read starts produces another; different roots never merge.
- **atspi-no-tree-ceiling**: trees deeper and larger than any fixed cap, text spanning several transfers, cycles and failed children yield complete or explicitly partial trees, never a truncated success.
- **atspi-hung-app-isolated**: a hung application does not stop another application's reads or the monitor's capture, and its timed-out fields are retried.
- **atspi-node-cites-replies**: every `NodeFinished` cites a packet interval whose replies, including late and missing ones, resolve to that node's context after rotation, replay, midnight and wall-clock changes.
- **atspi-cleanup-pinned**: segments holding unfinished tree-read evidence survive an acknowledgment, and a missing work database blocks cleanup until rebuilt.
- **atspi-no-empty-wildcard**: registration never sends an empty event class.
