# Source families

This page states, for every family of sources, what full fidelity means: which native records are read, what each record keeps, and which cases the family's tests must cover. It ends with the [catalog](#catalog), the floor of sources Sinex captures. The general capture model (capture by default, staging, the source declaration, inputs and materials, gaps and source health, the common record rules and the checks every source passes) is in [sources](sources.md); the base-layer streams (D-Bus, evdev, Wayland selections, AT-SPI, PipeWire, netlink, journald, Hyprland, the browser) and their spools are in [streams](streams.md); fusion of records seen through several sources is in [derivations](derivations.md#fusion); field hints and detectors are in [privacy](privacy.md).

Every family inherits the rules in [sources](sources.md). Each source reads the format an outside program or service produces; SQLite databases are read as rows, never as file images; a parser error never falls back to another format by guesswork; unknown fields, columns and record types are kept and reported as untyped rather than dropped. Records seen through two sources stay separate records; a [fusion family](derivations.md#fusion) groups them only where it is defined against those actual sources, and elsewhere each source's records stand side by side.

## Shell history and terminal recordings

Family F01.

**History databases.** Atuin and Nushell read every history row, including open commands, revisions and observed soft deletions. A record keeps command bytes, cwd, host, shell, session, exit, duration, IDs and unknown columns. A negative duration or an unknown-exit sentinel stays unknown. A new database or file generation never reuses the earlier generation's IDs.

**History files.** Zsh handles metafied bytes, extended headers and multiline commands; Bash handles optional timestamp records; Fish handles its escaped multiline structure and path lists. An explicitly declared plain-text history removes only LF or CRLF, preserving spaces, backslashes and timestamp-looking text as command content. Empty lines emit no event; whitespace-only lines are commands. Starts and durations stay unknown. It never guesses a timestamp or a shell. When an untimed file is rewritten, its records are aligned by sequence and multiplicity ([alignment](events.md#identity)); an ambiguous alignment is reported rather than collapsing all equal text into one command.

A **standalone shell binding** reads one native history input independently of
Atuin or terminal integration. It is an ordinary source binding with its own
material and progress, not a separate admission path. Zsh, Bash and Fish select
their declared formats; `terminal.text-history` is explicitly untimed text and
supplies no shell identity. A later fusion may relate observations; it never
replaces their original bytes.

**Terminal sessions** are captured automatically.
- Asciicast v2 and v3 keep the full header and one event per native input, output, resize, marker and exit record. V2 absolute offsets and v3 relative intervals resolve with exact decimal arithmetic. No command-span framing replaces native records; plain-text rendering and command spans are derivations.
- Kitty records ANSI bytes of all buffers and windows, geometry, modes, session identity, cwd and title, and OSC 133 command facts. Poll snapshots and continuous terminal output are distinct observations. Repeated full snapshots may share content storage, and equal snapshots taken at different times are separate observations of the buffer. Kitty's complete ANSI snapshot stays readable even when a derived tail is empty; computing a tail never erases the full original.
- Tmux, WezTerm and foot logging read each tool's native record shape.
- A closed window and an interrupted recorder are different facts.

A terminal record carries session and host, native sequence and code, original relative time and its precision, the resolved absolute time when the header supplies it, the exact data or a material byte range, and any supplied resize, exit or marker fields. Unknown codes keep their payload. A plain rendering is optional derived text.

Commands are read through the command fusion family (Atuin and the shell's own history) and through the terminal records directly. Native event counts equal cast record counts.

## Desktop state

Family F02: window managers, desktop activity trackers, clipboard and notification archives, idle state and peripherals. The live Hyprland, D-Bus, selection and AT-SPI captures are [streams](streams.md); this family covers what the streams do not.

**Window managers are ordinary sources** behind one interface, so adding another is a new reader, not a new design. Hyprland's socket2 events are stored whole, with `hyprctl -j` snapshots on connection and on relevant changes ([the Hyprland stream](streams.md)). A snapshot is present state, never a fabricated open or start. Every IPC event, unknown event text, window, workspace and output identity and all properties are kept. The compositor plugin's frame, layout and input-latency observations are one record per native report, with no aggregation at the source. Sway and i3, niri and river, KWin, GNOME and X11 have separate native readers that produce the same domain vocabulary where the semantics match. KWin and GNOME bus traffic comes from the bus stream; only provider instrumentation the bus lacks is a separate reader. An alternative compositor that is not running is marked inapplicable, not failed.

**Activity trackers.** ActivityWatch is read from its native rows: every mutable row, bucket metadata and heartbeat revision; archive, API and native exports keep their source identity. It is a stopgap whose data is still captured whole. Ruling, 2026-03-17: "aw is used more like a stopgap … we will collect all hyprland stuff anyway" (cc:6c1674bb-e912-438c-9de4-62658fb48cf4:41d66e2e-0047-4e0f-b937-56044c511122). Archive databases are opened read-only and never written. ActivityWatch's duplicate-start defect is a reading defect the record itself shows how to correct, so the reader records the corrected interval beside the raw row and leaves the stored row as it is. Arbtt is read from its native binary log or the tool's lossless dump.

**Clipboard and notification archives** are application records independent of the live streams: Clipse and archive exports, Cliphist and CopyQ through their native store interfaces, and mako, swaync or dunst history. They keep MIME bytes, source IDs, actions and lifecycle fields. Repeated bytes stay distinct records.

**Idle, displays and peripherals.** Idle and inhibitor facts describe what the compositor or protocol reported, not attention; an unknown idle state does not imply active. Monitor (DDC/CI) and peripheral queries keep complete replies, units, raw numeric codes, device identifiers, receipt time and errors. Query cadence is declared and visible, and identical samples are legitimate timed observations. Bluetooth and UPower state comes from the bus stream, not from a second native capture. Declared compositor key bindings are captured as configuration snapshots.

Window-focus and presence records from the compositor and from ActivityWatch stay side by side; comparing them never erases either input.

## Browsers and web content

Family F03.

**Profiles.** Every declared browser profile is discovered and read whole.
- Chromium: every History table and column, downloads and ordered URL chains, searches, favicons, bookmarks, sessions and tabs, preferences, permissions, extension state, cookies and local storage, each in its native rows or files. JSON, LevelDB, cache and session files each have their own consistent record reader.
- Firefox-family `places.sqlite`, Safari `History.db`, qutebrowser and extension CSV or JSON exports each have an explicit format.
- The database generation and profile identity, WebKit microseconds since 1601, redirect transition bits, opener and referrer, and the mutable visit duration are kept.

The Chromium row reader accepts a browser user-data root or an explicit `History` file. A roster that cannot be enumerated reports unavailable, rather than an empty profile set. Its observed database generation is the input host plus the filesystem device and inode. Ordinary updates and SQLite `VACUUM` keep that generation. Replacing the database file, including replacing it with an equal copy, creates a new generation and starts a new row-material chain, so reused visit and URL IDs do not overwrite earlier generations. A copy at another profile path is a distinct profile; a cross-host copy has no claimed native identity continuity. An in-place rewrite that preserves the file identity requires an explicit new source identity; Chromium exposes no database UUID that could prove otherwise.

Materials acquired before the generation was recorded retain unknown generation. They are preserved, and the new Chromium parser refuses to invent an identity for them. Before upgrading a deployed source, retain its materials and establish a reviewed original-generation mapping or replay it through an independently preserved original database. No automatic deletion, key coalescing or reinterpretation of those materials is part of opening the source.

- Encrypted values are kept as captured. Sinex does not decrypt them or add key access, and never reports a decryption that did not happen.
- Native tables without known semantics still produce raw row events and an unread-category status.

**The extension** captures visits, navigation, tabs, frames, downloads and the page content and resources actually observed, with browser, profile and document IDs and ordering. Its state is acknowledged producer state ([producers](producers.md)); the stream itself is in [streams](streams.md).

**Page content** is a captured file, HTTP or WARC record with request and response metadata, exact body, redirects, encoding, frame and URL and capture time, not merely a URL row.
- Existing native HTML, PDF and WARC page corpora are acquired before anything is recrawled.
- Fetching a URL again is a new response observation; it cannot reconstruct what was displayed earlier.
- Authenticated pages are captured from the user's own browser response. A background crawler never replays POSTs or form submissions.
- The shared crawling reader covers every visited and saved HTTP(S) URL, pages its queue, honours server backoff and distinguishes blocked from unavailable fetches. A rate limit delays work; it never drops it. No domain allowlist or content budget defines scope, and any limit a provider imposes is visible.
- Dead links are recovered from the Internet Archive through a CDX reader. An acquired CDX or archive-index reply is a record of its own; a lookup result is not proof that page content was acquired.

Visited and saved URLs are counted against captured, queued and failed responses, and the differences are shown. Raw content is addressable through `show`, and document extraction reads the stored bytes.

## Account exports

Family F04: Google Takeout and other account archives, and the acquisition of exports.

**One shared pass** inventories every package, member and product, including products without readers, archive parts, empty members and parse failures. An export set may span ZIP or TGZ parts; identity records the export and the member, not a reader-specific copied directory. One archive read serves every compatible reader. Every package is kept whole as received ([inputs and materials](sources.md)), including packages that contain SQLite members, whose rows are additionally read into a rows part. Every readable native item is a record; an unknown product yields a raw-record event and visible untyped status, and an unknown proprietary member is never reported as parsed. AI conversation members go to Polylogue with the same material references and are parsed only there ([Polylogue in primary mode](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md)).

**Readers.** Chrome history, My Activity service cells, YouTube watches, searches, subscriptions and playlists, Keep notes and lists, Tasks hierarchies, purchases, installs and library records, Calendar, contacts, Photos sidecars and assets, Drive files, Google Chat, Assistant notes and lists, and Fit points.
- My Activity keeps service, URLs, titles, products, locations and supplied details; the verb and title are not an invented identity. Export-day CET/CEST labels resolve to actual instants, and duplicate multiplicity survives.
- Keep prefers a supplied stable ID. Creation time alone is not unique, so without an ID the export and source identity with explicit correspondence identify the note, and colliding notes stay distinct.
- Tasks keep lists, parent links, status and schedules.
- Purchases unwrap every documented variant and keep the exact currency and amount text.
- Contacts are records.
- Native Spotify short and account history stays in scope with its coarse time precision recorded; relating it to extended plays never deletes the source readings.

Other account archives (Samsung account data, Freedom usage, OpenRouter usage and billing, password-manager exports) are listed in the [catalog](#catalog). A password-manager export keeps encrypted items encrypted; fixtures are synthetic.

## Mail, calendars and contacts

Family F05.

**Mail** covers every folder and label of the operator's accounts, including sent, archive, Spam and Trash: complete RFC 5322 and MIME bytes, attachments, duplicate headers, threading, flags, provider IDs, dates and received or internal dates. MIME bytes survive an invalid body encoding. Mbox delimiter quoting and Maildir renames keep message ranges and identity. PST and Outlook CSV are explicit native inputs; a converter is a recorded acquisition tool, not an intermediate format. Message-ID alone is not record identity; a message without one is identified by its real provider or file identity, never a shared empty key. Mail fusion matches Message-ID only within one account and keeps every provider observation.

Outlook conversion retains the native PST as an original material with the acquisition tool, its version and folder identity. Converted mbox materials and their record parts cite that acquisition material. Bindings identify the original PST and folder; private directories under managed scratch belong to their folder readers and are removed when the last reader closes, including after conversion failure. Conversion and storage check the native input's revision, and the next read converts a changed revision again. The PST itself is never rewritten.

**IMAP** uses IDLE where supported, UIDVALIDITY and UID, flags and explicit EXPUNGE and VANISHED observations. A sparse or incomplete UID response never implies deletion.

**Gmail** is read with the account's full permission, and every capture call is read-only.
- A full paginated `messages.list(includeSpamTrash=true)` with RAW message acquisition and complete label and history collection, then `history.list` deltas.
- The starting history ID is persisted throughout a paged walk and advances only after every affected record is admitted.
- Provider metadata and MIME are both kept.
- Expired history (HTTP 404) records a gap and performs one read-only full read; it cannot invent the missing transitions.
- A message that disappears between list and get is an observed race, not a fabricated message.
- Quota and backoff delay work; they never narrow labels, age, attachments or record count.
- OAuth refresh and credentials belong to the acquisition code, not to the parser.

**Gmail configuration.**

Discovery reads the account file declared as `input:gmail`; `--read gmail=PATH` uses the same file. It names the expected account and credential files:

```json
{
  "account": "owner@example.test",
  "credentials": {
    "client_id_file": "secrets/client-id",
    "client_secret_file": "secrets/client-secret",
    "refresh_token_file": "secrets/refresh-token"
  }
}
```

Relative credential paths resolve beside that file. Unknown fields, inline secrets and empty credential files are refused. Credentials are read at refresh time; access tokens stay in memory. Grant the capture process read access to those files without placing their contents in binding settings or Nix store files. OAuth requests the full `https://mail.google.com/` scope and offline access; the operator completes consent and supplies the refresh token. Capture neither opens a browser nor broadens an earlier grant, and the first profile must match the configured account.

After a history 404, capture admits the gap, saves a new profile baseline, lists the complete mailbox, then resumes history from that baseline. Restart resumes from admitted progress; authorization failures, rate limits and temporary provider errors retain the previous progress. Missing credential files report unavailable, awaiting credentials. Raw mail and label records replay without Gmail or credentials. Provider label IDs remain with their observations; RFC mail records carry label names and Gmail thread IDs.

Provider references: [raw message retrieval and scopes](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get), [offline OAuth consent and refresh](https://developers.google.com/identity/protocols/oauth2/web-server).

**Calendars.** ICS and CalDAV keep VEVENT, VTODO, VJOURNAL, recurrence rules, exceptions, UIDs and RECURRENCE-ID, VTIMEZONE, all-day dates, floating times, invitations, cancellations and unknown properties. Definitions are stored; occurrences are expanded over a bounded range at query time, so an infinite recurrence is valid. Sync tokens, ETags and explicit tombstones drive changes.

**Contacts.** vCard, CardDAV and phone contacts keep groups, structured fields, photos, provider IDs, repeated properties and unknown extensions. Calendar and contact imports and live sync are one family with format-specific readers.

Recall cites an exact message, cue or attachment location.

## Finance and billing

Family F06.

**hledger** captures the authored journal and its include tree, resolved by hledger's declared rules. Naming a command, a shell expansion or a remote import in a journal never executes it. A record keeps include relationships, grammar, version and options, directives, every native date form, effective dates, status, code, payee and narration, comments and tags, ordered postings, commodities, exact decimal quantities, implicit amounts, prices and costs, assertions, assignments, account and commodity declarations, and automatic and periodic rules.
- `transaction.posted` is an authored transaction, never a generated forecast. Raw directives and rules are records too.
- Derived balancing or generated postings cite their source rules. Capture never edits the ledger or fills an implicit amount.
- Identity is a supplied transaction ID where present, otherwise the source identity plus the complete block identity and a duplicate number; never the first amount or the description alone.
- A directive the reader does not support is reported as unsupported with its bytes.

The journal reader discovers `LEDGER_FILE`, `~/.hledger.journal` and configured finance roots, and accepts `--read hledger=PATH`. Include paths may be relative, absolute or home-relative, with `journal:` prefixes and globs. Each included file is an ordinary binding; missing files, cycles and unsupported targets are reported. Included files inherit the authored year at their include. Native timeclock and timedot imports stay written `journal.imported` records, not generated accounting transactions.

Partial dates remain partial; a known year resolves a civil date without inventing an original instant. Quantities, commodity quoting, virtual accounts, posting status, prices, balance assertions, lot annotations and valuation expressions retain their written forms. Capture does not balance a transaction or execute its automatic rules. Without a supplied transaction ID, record identity includes the journal/member context, complete block digest and duplicate occurrence, including across scan parts. A changed inherited year causes an unchanged child to be read in that new context; a fully dated transaction does not change identity for an unused default year.

**Bank statements.** CSV, MT940 and CAMT keep the original statement, message and entry identities, debit or credit sign, exact amount and currency, booking and value dates, counterparty, references, pending or reversed state, balances and fees. A provider revision is a newer reading, and correspondence across imports is explicit. Statement IDs are scoped by account.

Provider-native invoices, usage exports and receipts are kept as the original PDF, XML or JSON. OpenRouter usage is billing evidence; AI sessions stay with Polylogue. Nothing in this family computes a tax conclusion.

## Health exports and wearables

Family F07.

**Exports.** Original Samsung in-app CSV with its companion JSON and binary records, Samsung GDPR cloud exports, Sleep as Android native CSV and ZIP, Google Fit, Xiaomi vendor responses, Gadgetbridge rows and GPX, and other raw vendor packages in declared health roots.
- A record keeps the provider record ID, package, device and recording method, revision, time and zone, units, precision and every supplied measure.
- Required categories: sleep sessions and stages, heart rate and HRV, SpO2, stress, respiration, temperature, activity and exercise, steps, floors and calories, ECG, weight and body composition, snoring and vendor wellbeing reports.
- Every binned point, sample and stage is independently queryable and cites its original row or bin range. Vendor classifications are vendor statements, not diagnoses and not Sinex certainty.
- Samsung in-app and GDPR records are separate observations.
- Sleep as Android's per-session header and event columns are read whole; the start ID and local time resolve offsets, and the raw Tz is kept when inconsistent. Long sleep is not capped.
- Fit is one event per point; no day or run replaces the points.
- Xiaomi cloud acquisition keeps the original server responses. A failed vendor request is an acquisition failure and a gap, not an empty health record.
- Gadgetbridge's database is a row source; its exported GPX and XML members are native files.

**Live wearables.** BLE capture records the wearable's available GATT notifications and reads and protocol packets with device, service and characteristic UUID, exact payload bytes, sequence, device time and receipt and monotonic clock anchors.
- Standards are decoded where they apply; unknown proprietary payloads stay bytes with untyped status, never invented measures.
- Authentication uses existing credentials at acquisition and never appears in records.
- Sync history and live measurements carry device and provider IDs, and their overlap is accounted for.
- One connection owner per wearable keeps competing clients from breaking notifications.
- The transport and decoder report which channels the actual hardware and firmware support; an absent channel is unavailable or unsupported, never a measured zero.
- Capture never changes a device setting.

**Wearable configuration** is set outside Sinex by the device deployment, whose target is every sensor at its maximum cadence (continuous heart rate, SpO2, stress, sleep, temperature where supported). The BLE source reads and reports each channel's configured and actual state, so a disabled or slowed channel shows in status as a configuration gap rather than as missing data. Ruling, 2026-08-14: "why not SpO2 for example? Also, do configure the band maximally, to rly capture everything" (cc:c7d56707-950b-4e3e-bd5c-b4c1822d5b5c:13add765-7bb0-41aa-9a84-8a27850068a3).

Health coverage counts unique provider IDs and raw points, and charts gaps by source and device.

## Phone, location and personal devices

Family F08.

**The phone application** is a primary source: its event logs and binary sensor files, media and sidecars, native Android logcat, call, SMS and contact data, app usage, notifications, power and ambient state, instrument answers and Health Connect records, in their native format.
- The phone hub forwards native bytes through the ordinary producer protocol ([producers](producers.md)) and acknowledges an upload only after Sinex acknowledges it. File acquisition still reads existing native phone packages; there is no second, converted phone format.
- Device identity survives reconnects and path changes. Upload identity, content digest and offset make resends safe.
- Sensor arrays yield one event per sample. Compressed chunks keep clock anchors and sample addressability.
- Logcat is one event per native record, including continuation fields, at every priority.
- Every app event category and every new unknown kind is captured.
- Sensor and location sampling uses the producer's actual cadence and reports losses rather than claiming continuous observation.
- Instrument records keep the instrument and version, questions and exact answers; scoring is a derivation.

**Health Connect** reads every granted raw record type with stable IDs, source package and device, client version, modification time and deletion and change tokens. Each contained sample is a record. Token expiry records a gap and one read-only full read; page tokens must progress, and repeated pages never multiply records. Android's access-history and background permissions are explicit health status, not an unexplained short history. Root access does not synthesize read grants.

**Location.** Native GPS, GNSS, OwnTracks, GPX and Timeline points keep accuracy, altitude, speed, source and timezone and clock evidence. Place visits and routes are derivations unless a vendor supplies them as records. Location says nothing about attention or presence during a gap.

**Media.** Ambient audio, voice notes, speech, call recordings and camera or download mirrors keep their original AAC, Opus, WAV, image or video bytes and native sidecars. Loudness is a sample with calibration and window information, never a substitute for the audio.

Continuous webcam recording and passive logging of nearby radios (other people's Bluetooth and Wi-Fi devices) are not captured; existing camera recordings and the operator's own devices' connection facts are (default; open operator question).

## Social and chat

Family F09.

**Providers.** APIs and raw exports of Reddit, Wykop, The Motte, X, Mastodon, Hacker News, Messenger, WeeChat, Teams, Matrix, Discord, Telegram, Signal, WhatsApp and Google Chat.
- A record keeps posts, comments and messages with their revisions and deletion markers, conversation and parent IDs, senders and participants, timestamps and edits, replies, reactions and votes, attachments and media, links, forwards, account settings, bookmarks and notifications.
- A reaction list or an attachment body is never replaced by a count.
- Native message encodings and emoji are kept. Local log times need an explicit zone.
- Encrypted values stay encrypted unless the normal authorized client supplies plaintext.

**Live acquisition** reads the operator's available feeds and the full accessible conversation and comment context, not just the operator's own messages. Provider pagination and sync tokens are used, and edit and delete facts are captured. A failed or partial fetch never removes history. Rate limits, backoff and access restrictions are visible. An export-only provider is still discovered from its native packages and reported as export-only.

**Scope** is whole comment trees with metadata wherever the operator commented, plus linked content, self-posts and directly hosted media; dead links are recovered through the CDX reader ([browsers](#browsers-and-web-content)). Ruling, 2026-08-11: "the whole comment trees, incl metadata. for all places where I commented, at least. and secondarily, crawling certain users of interest" (cc:049c6f6c-9252-44c7-a0b2-3a585f63b686:c7769bf6-b74e-47ee-bdbf-659e784d6374). Only a new paid API or a credential not yet available needs the operator ([discovery and exclusions](sources.md)).

**Users of interest.** Per provider, the operator (or an agent on the operator's behalf) keeps a list of accounts of interest as authored records. Their posts and comment trees are acquired through the provider API or an available public dataset, under the same pagination, revision, deletion and rate-limit rules as the operator's own feeds. Each acquired record names its list entry and the dataset or API it came from; status shows per-account coverage and the dataset's own coverage limit.

Provider observations enter the common domain events; there is no per-provider conversation store.

## Music, video, reading and libraries

Family F10.

**Plays.** Spotify extended and account exports, live recently-played, library and playlists, Last.fm and ListenBrainz scrobbles, YouTube and YouTube Music, mpv, Jellyfin, Plex and Steam each keep their provider records; plays seen by several providers stay side by side.
- Spotify keeps offline timestamp versus sync time, play duration, skip, shuffle and reason, platform, country, network and account metadata, and episode and audiobook fields. A short-history record's minute precision is explicit.
- Saved items and playlist positions and revisions are not plays.
- An API with a restricted history window is polled often enough for its actual throughput and exposes gaps; a successful poll never claims complete playback.
- MPRIS player facts come from the [bus stream](streams.md), not a second player poller.
- A game launch or playtime is not proven attention.

**Reading and libraries.** Raindrop, Pocket, Instapaper, Wallabag, the reading stack, Substack, Goodreads, KOReader, Calibre, Zotero, Anna's Archive acquisitions and RSS readers keep items, collection, folder and order, tags, status, notes and highlights, page and annotation geometry, all archived content and attachments, and provider metadata, read from provider-native files, SQLite rows or API bodies.
- Book status is not a reading session; an acquisition is not a read.
- Media and book files keep their bytes through the source and [library](library.md) contract; curation refers to the same stored bytes. A source reader never overwrites authored library metadata with filename hints.
- Reference datasets for genres or features, if used, are recorded acquisitions, not personal playback sources.

Plays and saved or reading records are queried separately, with provider coverage.

## Code, forges and work records

Family F11.

**Git** captures native commit objects and the tree and blob facts needed to read the changes, refs and reflogs, author, committer and signatures, message bytes, ordered parents, per-file diff and numstat, rename, copy and binary indicators, and worktree and dirty-file observations. Raw object bytes are kept beside decoded metadata. Repository identity is stable across moves, clones and worktrees, accounts for multiple roots and SHA algorithms, and is distinct from a filesystem path. A ref rewrite or an unreachable commit is a state change observed at poll time; the commit's creation remains. SVN reads native XML log and diff records with the repository UUID and revision.

**Forges.** GitHub, GitLab and Forgejo acquire issues, pull requests, comments, inline review threads, reviews, labels, milestones and assignees, commits, statuses and checks, Actions and pipeline jobs and attempts, complete available logs and artifacts, and provider events.
- The actual JSON, GraphQL or HTTP bodies and binary artifacts are stored, never a chosen-field report.
- Nested connections are paged to completion.
- Update scans by the provider's update time must catch old workflow reruns and checks, reviews and comments that change without the pull request's `updatedAt`.
- Log and artifact expiry is a provider loss shown in status, so they are fetched before they expire.
- Capture makes no remote changes.

**Work records.** Optional readers, off by default, read job runners' and task trackers' own records. They are ordinary sources; derivations take job and task identifiers from declared data and never depend on a particular runner.
- Job runners: owner-provided jobs and campaigns with exact revision and workspace, command, lifecycle, queue, setup, build and test durations, result, exit, failures and original artifacts.
- Beads: authoritative issue revisions, dependencies, comments, notes and deletions through `bd` or documented owner interfaces with an explicit actor, never by reading its storage files.
- Devkit and xtask history: every run, step, check and failure, selectors, environment identity, revision, timing, output and artifact references, from native SQLite rows or documented exports.
- Polylogue verification and Nix builds: their original run records and logs.
- Taskwarrior and other work applications: their native stores.
- Editors (Neovim, VS Code, JetBrains) report through an integration socket.

Sinex observes work; it holds no task or job state of its own. A query connects a failed run to its exact commit, task revision and original output. Code analysis is derived from git and the source files.

## Notes, documents and files

Family F12.

**Roots.** All files in the declared notes, journal, document, health-document, download and library roots are captured, including note attachments and configuration metadata. The format-specific reader owns consistent acquisition; the filesystem watcher routes changes to it rather than reading a mutable database as a file. Markdown and front matter, Org and Logseq, Joplin native stores, office documents, PDF, HTML, EPUB, photos and scientific or clinical packages keep their exact input, all file metadata and attributes, and raw path bytes. Acquisition never rewrites originals or executes active document content.

**Extraction** is a derived reading of the stored bytes, independent of whether acquisition parsed anything. A file-only capture is reported as acquired and unparsed until extraction succeeds. DICOM images, lab and vendor XML, JSON and CSV, prescriptions and psychometric responses are source records, not clinical interpretations; Sinex draws no health conclusions.

**Notes** keep text, title, tags, wikilinks, native ID, front-matter status and attachment references. A native ID follows a rename; without one, file identity and correspondence are recorded explicitly, never by asserting that two equal notes are one.

**Removal and time.** Only a complete scan observes removal; an interrupted walk, a permission denial or a configuration change does not. File creation and modification times are filesystem evidence, never authored time.

**Photos** keep the original image or video, EXIF, XMP and sidecars, subsecond, offset or unknown timezone, GPS and edits. Equal content at different paths is shared storage with distinct file observations.

## Filesystem, audit and network

Family F13.

**Filesystem.** The configured roots are watched through native inotify or fanotify within configured mount boundaries; a full walk runs only at start and after a watcher overflow. A record keeps the event mask and cookie, device, inode and generation, paths as bytes, observed permissions, ownership, times, xattrs and ACLs, and acquisition time. Content revisions go to the matching file, directory or row reader. No implicit name exclusion (`.git`, build, cache) stands in for an exclusion the operator recorded with a reason. Sinex's own storage and spools are excluded by an explicit, visible owner exclusion so capture never feeds on itself. Kernel queue overflow and unreadable or unwatched roots become gaps followed by a full walk of the affected roots; a current scan cannot reconstruct lost intermediate revisions.

**Audit** captures every native audit record with its companion SYSCALL, PATH, CWD, PROCTITLE, EXECVE and EOE records, including binary and hex values and unknown types. Each record is queryable; a derivation groups `(host, boot, serial)` into operations, and truncated groups and gaps stay visible. Capture attaches to the existing audit owner (multicast) or its raw log; it never replaces the system's audit controller or adds a reduced rule set.

**Connections.** Conntrack records NEW, UPDATE and DESTROY and the initial state, both directions, namespaces and zones, family, protocol, tuple and ID, counters, kernel timestamps and flags from native netlink messages ([the netlink stream](streams.md)). An existing connection seen in a snapshot is not a new connection. Missing starts and counters stay unknown. Socket overflow and conntrack ID reuse are detected. There are no minute-aggregated flow events.

**DNS** reads resolver dnstap, DNS packets or exact log records with questions, answers, RCODE, TTL, endpoint and time. Encrypted DNS outside that resolver is an explicit coverage gap.

**HTTP(S)** records browser request and response bodies and headers through the browser's native response interface, and packet bytes where available, including redirects and WebSocket frames. Encrypted transport stays encrypted when no authorized plaintext source exists. No new trusted interception CA, routing change or paid proxy is assumed. Capture is passive and never replays user transactions.

**Routers.** Native API or SSH replies keep associations, DHCP, traffic, syslog and the device clock, each row or line a record.

Native raw counts are compared with admitted counts and every difference is explained.

## Host records

Family F14: journald, systemd, udev, machine telemetry and machine records.

**Journald** stores export-format entries with repeated and binary fields, cursor, realtime, monotonic, boot and source times, namespace and every trusted and user field; one `journal.entry` per entry ([the journald stream](streams.md)). Every available namespace and the system and user journals are selected explicitly, with reader identity preventing unintended duplicates. Capture resumes by the actual cursor; vacuum and rotation gaps are reported. Arrays and binary values are decoded without being replaced by empty text. Sinex's own daemon logs are captured without a feedback storm.

**systemd.** The logging unit and the unit described are separate fields. Manager transitions come from MESSAGE_ID and structured fields; an ordinary application's "Started" text is not a unit start. Bus-observed lifecycle comes from the bus stream and is related to journal evidence without collapsing either original record. The first-seen boot record is distinct from the true boot instant.

**udev** captures native kernel and udev messages with all properties, sequence, action and device path. The reader subscribes before taking the inventory, and an inventory entry means present, never added.

**Machine telemetry** reads every row and column of the original collector's tables and Below's native records. A sample keeps resource units, scope, boot, process PID and start identity, cgroup, service and device identity, counters, deltas and reset information, and the sample interval. GPU, thermal, memory, IO and pressure, block, network and filesystem capacity, hardware state and OOM kills are independently queryable. Append optimization checks the collector's generation and sequence; a database rotation keeps ID continuity only when the collector guarantees it. Collector status is a capture-health input, never discarded. Below's native encoding needs a version-aware reader or a recorded decode by the tool.

**Machine records.** NixOS activation and generation receipts, installed package and profile state, backup checks and drills, configuration drift, runtime inventory, usage census, experiments and diagnostic run outputs are acquired with the original tool, version and exact outcome. A success flag means only what it states: a successful backup command is not a restore check. These sources never schedule backups or act on the machine. Environment sensors (Awair) and weather responses keep all channels, device time, units, location and time provenance, and request failures.

Source diagnostics report counts, bytes and gaps through public status.

## Screen video and images

Family F15.

**Captured images.** Manual screenshots, screen recordings and replay files, screen-frame archives and native image sidecars use the shared media material contract. Original JPEG, WebP and PNG bytes are kept as historical inputs; they do not establish video coverage. A referenced image's bytes are stored, not just its path.

**OCR.** Imported vendor OCR keeps engine and run, page, box, text, confidence, coordinate units and source media references. OCR run by Sinex is a derived event over stored frames or images ([derivations](derivations.md)). Enabling capture never enables a model.

### Continuous screen video

Continuous screen video is designed but not captured, because its storage cost is not yet affordable; audio, being cheap, is captured whole. Ruling, 2026-10-28: "video, probalby we won't yet even capture it, not imminently" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).

When enabled, it works as follows.
- The capture is a compositor source: a Hyprland plugin captures inside the compositor, like any other [window-manager source](#desktop-state).
- Every display is captured at its actual mode, with frame timestamps, output identity and geometry, scale and rotation, colour space and HDR, and changes of output topology. The cursor is captured separately rather than burned into frames.
- Terminal regions are left out of the video, because terminal recording reproduces them far more cheaply.
- Storage is standard video segments of keyframes and inter-frame residuals in which every frame stays reconstructible; "residuals" means reconstructible compression, never perceptual deduplication that loses motion or repeats. Full fidelity is lossless; a lossy setting is the operator's choice.
- A hardware encoder is used where it preserves the chosen format. An encoder the GPU does not support is an unavailable capability, never a silently lower frame rate or a JPEG substitute. Software encoding meets the same contract only where its load allows.
- Segment boundaries are work and storage boundaries, never limits on recording length. Encoder buffering, dropped frames and mode changes are counted and gap-marked.
- Frame and OCR derivations read stored frame ranges.

## Audio and speech

Family F16.

**All audio is captured, losslessly.** Ruling, 2026-10-28: "audio is comparatively extremely cheap, so obviously we would capture all of it losslessly" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).
- Every PipeWire capture source, sink monitor and application stream whose content reaches the user's audio graph is recorded; the graph itself is the [PipeWire stream](streams.md).
- Topology, node and port identities, serials across restarts, formats, native sample rate, channels and channel map, timestamps and clock correction are kept.
- Simultaneous input and output streams are recorded separately, including silence and overlapping signals. There is no voice-activity gating, microphone-only selection, 16 kHz downsampling or mono conversion.
- Lossless PCM or FLAC keeps the acquired samples; vendor-native Opus, AAC or WAV stays unchanged. Compression settings and sample-format conversions are declared.
- The recorder's own monitor path is never recorded back into itself; the topology rule that prevents it is visible and tested.
- Segments are independently decodable and continuous; frames carry clock and discontinuity facts so transcripts align later.
- Stream and node hotplug, graph rate changes and device failures are source events or gaps. All known devices are captured at startup, and streams are opened before publishing.

**Imported speech.** JSON, Whisper, WebVTT, SRT and plain-text transcripts keep each cue or segment, exact decimal relative offsets, speaker, language, model, run, confidence and unknown fields, linked to the media material when known. External run IDs are never turned into Sinex parent IDs. New speech recognition, voice-activity detection and diarization are model work in [derivations](derivations.md), off while model-calling features are off, and never replace the audio.

## Operator records and source health

Family F17: the operator's journal, moment marks, self-reports, requests made through the interfaces, steering, and source status.

**The raw journal** is authored primary text. Entries keep surrounding source text and time evidence, including corrections and unknown dates. Claims extracted from it are derived assertions, not proof that the described act occurred.

**Moment marks** take one keystroke and persist a small material with the actual press time, optional text or audio and the observed context. Linking a mark to context is a derivation; missing focus or terminal context never blocks the mark. Repeated marks are distinct.

**Self-reports** keep instrument, question and version, prompt, exact response, units and scale, answer time and corrections, with the operator as author. No mood or attention inference is a source fact.

**Steering** reads its native rows, keeps revisions and generated-versus-authored text, and never assumes a fixed database path.

**Requests.** User actions, ratings, comparisons and forecasts made through the API, CLI or MCP are small request materials through the one intake and the kind registry, with the author computed by the daemon ([callers](interfaces.md)).

**Source health.** Capture status and operational observations are distinct from success claims. Sinex's self-observation is ordinary captured and derived records, read through the same query and provenance interfaces, kept in the separate self-observation store with a bounded write rate, and never triggers itself. Source status shows raw records, accounted headers, failures, sampling, exemptions and event counts, so "nothing happened" is distinguishable from unread data ([gaps and source health](sources.md)).

### Elicitation

Elicitation is how Sinex asks the operator for facts no sensor records.
- An **instrument** is a versioned declaration: questions, answer types and scales with units, and the purpose it serves. Instruments are data that the operator and agents edit; every edit is a new version.
- A **prompt** is an event recording that an instrument was offered: when, through which surface (notice, web, CLI, phone) and what triggered it. Triggers are declared over captured context: a time of day, an episode boundary, a journal mention, a gap in a self-report series, or the operator's request. Prompts are ordinary [signal rules](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/signals.md) within the notices' interrupt budget.
- An **answer** is a request material plus an event: instrument and version, each question's exact response with unit and scale, answer time and the operator as author. A correction is a new answer naming the one it corrects; both stay live.
- Substance use is served by an instrument triggered from journal mentions plus a derivation over the journal; title classifications are a derivation with model provenance when model features are on. Scoring and interpretation are derivations, never source facts.

No prompts are sent until the operator designs instruments for them; spontaneous self-reports are accepted (default; open operator question).

## Sources beyond the catalog

Family F18. The catalog is a floor: it lists what is known to exist, and Sinex captures beyond it. Ruling, 2026-10-14: "stuff I didn't ever mention but is obviuosly in scope" (chatgpt:6ace15d4-c6d0-83ed-ac53-4a870df45514:2b47b1b6-ada1-44ab-a07d-f0080f34ae97). Applicable local records are discovered automatically; a new paid service, a new credential grant or a new kind of third-party observation needs the operator ([discovery and exclusions](sources.md)). Every added source inherits the rules in [sources](sources.md) and its family's requirements here. No source is left out for lacking an earlier reader. New event names are registered through the kind registry ([planned kinds reference](reference/README.md#planned-pages)).

## Catalog

Each entry names the source, its family, the native format and capture mechanism, the records it yields and their grain, and the check it passes on a synthetic fixture beyond the [common checks](sources.md). Kinds marked "derived" are produced by [derivations](derivations.md) from the source's records. Entries without an identifier are named by their input. Until the [planned sources reference](reference/README.md#planned-pages) exists, this table is the list.

| Family | Source | Native format and capture | Records and grain | Check |
| --- | --- | --- | --- | --- |
| [shell](#shell-history-and-terminal-recordings) | `terminal.atuin-history` | SQLite history rows, every column. Committed row polling and open-row re-reads. | command.executed | Open row later finishes; soft delete and DB generation. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.zsh-history` | Metafied extended/plain zsh history. Append/rewrite reader. | command.imported | Metafied bytes, multiline and repeated commands. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.bash-history` | Bash history with optional epoch headers. Append/rewrite reader. | command.imported | Untimed lines and timestamp carry after restart. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.fish-history` | Native Fish history records. Append/rewrite reader. | command.imported | Escaped multiline command and path list. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.nushell-history` | Native history SQLite rows/schema. Committed row polling. | command.executed | Session ID, extra columns and mutable completion. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.text-history` | Explicit untimed text lines. Append/rewrite reader. | command.imported | Literal timestamp-looking command and equal repetitions. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.kitty-osc-live` | Native shell-integration socket records and OSC bytes. Session integration producer. | terminal.reported; command.executed derived | OSC 133 pairing, missing cwd and sequence restart. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.asciinema` | Asciicast v2/v3 header and each event. Automatic native recorder plus append reader. | terminal.session; terminal.output per native record | Count every native input/output/resize/marker/exit. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.kitty-scrollback` | ANSI buffers and native metadata. All-window buffer acquisition; snapshot reader. | scrollback.captured; scrollback.window | Alternate screen, buffer clear and window reuse. |
| [shell](#shell-history-and-terminal-recordings) | `terminal.tmux-log` | Native tmux/WezTerm/foot output bytes. Tool output recorder and append reader. | terminal.output | Original escapes, resizing and final tail. |
| [desktop](#desktop-state) | `desktop.window-manager.hyprland` | Socket2 lines and hyprctl JSON state. Subscribe then snapshot on connection/change. | window.focused/opened/closed/moved/title_changed; workspace.switched; monitor.focused; desktop.other | Startup race and unknown IPC event. |
| [desktop](#desktop-state) | `desktop.window-manager.sway` | Sway/i3 IPC headers and payloads. Native subscribe plus state query. | window.focused/opened/closed/moved/title_changed; workspace.switched; desktop.other | IPC reconnect and snapshot/change order. |
| [desktop](#desktop-state) | `desktop.window-manager.niri` | Niri event JSON; river status protocol. Provider-native subscription/state. | window.focused/opened/closed/moved/title_changed; workspace.switched; desktop.other | Keep provider distinctions and unknown messages. |
| [desktop](#desktop-state) | `desktop.window-manager.kwin` | KWin script reports and shared bus packets. Existing compositor API plus bus derivations. | window.focused/opened/closed/title_changed; desktop.other | No second whole-bus recorder. |
| [desktop](#desktop-state) | `desktop.window-manager.gnome` | Mutter/Shell extension reports and bus packets. Native extension producer plus bus derivations. | window.focused/opened/closed/title_changed; desktop.other | Report extension availability/version. |
| [desktop](#desktop-state) | `desktop.window-manager.x11` | X11 property/event records and state. Native EWMH event listener. | window.focused/opened/closed/title_changed; desktop.other | Window ID reuse and property bytes. |
| [desktop](#desktop-state) | `desktop.hyprland-telemetry` | Native compositor frame/input/layout reports. Compositor plugin/IPC producer. | device.sampled per report | One record per native report; no source-side aggregation. |
| [desktop](#desktop-state) | `desktop.activitywatch` | Events/buckets/key-value SQLite rows; native API/archive records. Mutable-row reader or native export/API. | window.span; presence.span; browser.tab_active; activitywatch.other | Same start distinct row IDs and late heartbeat. |
| [desktop](#desktop-state) | `desktop.arbtt` | Native capture log and lossless dump. File reader with native decode. | window.sampled | Binary log fixture read natively. |
| [desktop](#desktop-state) | `desktop.clipboard-import` | Native Clipse/archive records and bytes. Explicit export reader. | clipboard.imported | Repeated bytes remain distinct records. |
| [desktop](#desktop-state) | `desktop.cliphist` | Cliphist/CopyQ native records and MIME bytes. Store read interface; SQLite rows where applicable. | clipboard.imported | Binary MIME and native IDs. |
| [desktop](#desktop-state) | `desktop.notification-history` | Native mako/swaync/dunst history records. Daemon history/export read interface. | notification.imported | History incomplete versus no notifications. |
| [desktop](#desktop-state) | `desktop.keybinds` | Declared compositor configuration bytes. Configuration snapshot acquisition. | keybind.declared | Changed bindings and exact modifier/command text. |
| [desktop](#desktop-state) | `desktop.idle` | Native idle protocol events and inhibitor facts. Idle protocol listener; bus when supplied there. | idle.changed | Unknown idle does not imply active. |
| [desktop](#desktop-state) | `desktop.monitor` | Complete DDC/CI replies and display metadata. Read-only display API polling. | device.sampled | Timeout versus zero brightness; monitor replacement. |
| [desktop](#desktop-state) | `desktop.peripherals` | Solaar/device replies; shared bus state. Native device API plus bus derivations. | device.sampled; device.changed | Preserve serial/vendor/model/unknown codes. |
| [desktop](#desktop-state) | `desktop.input-log` | Historical keyboard and pointer logger JSONL records. Append reader of the existing capture history. | key.pressed/released; pointer.button/moved/scrolled; input.logger | Every historical pointer record and duplicate. |
| [browser](#browsers-and-web-content) | `browser.history.chromium` | History/profile SQLite rows and all native profile record stores. Whole-profile row/file readers. | browser.visit; browser.url; download and profile records | Download chains plus unknown table/column retained. |
| [browser](#browsers-and-web-content) | `browser.history.qutebrowser` | Native history SQLite rows/config/bookmarks. Profile row/file readers. | browser.visit | Existing history and profile generation. |
| [browser](#browsers-and-web-content) | `browser.history.firefox` | places SQLite rows and native profile stores. Profile row/file readers. | browser.visit; bookmark.saved; download records | Visits/bookmarks plus WAL-consistent rows. |
| [browser](#browsers-and-web-content) | `browser.history.safari` | History.db rows and native exported profile records. Row/export reader. | browser.visit | Native epoch and provider schema version. |
| [browser](#browsers-and-web-content) | `browser.history-export` | Declared extension CSV/JSON exports. Explicit export format readers. | browser.visit | Header/version selection and no parser fallback. |
| [browser](#browsers-and-web-content) | `browser.bookmarks` | Chromium JSON, Firefox rows and Netscape HTML. Profile reader/export pass. | bookmark.saved | Folder tree, order, edits, tags and provider IDs. |
| [browser](#browsers-and-web-content) | `browser.navigation` | Native browser navigation/content producer records. Extension producer to ingress. | Navigation/tab records and page.fetched | Opener/referrer/frame and actual displayed response. |
| [browser](#browsers-and-web-content) | `browser.page-corpus` | Native HTML/PDF/WARC and response metadata. Directory/archive readers. | page.fetched | Original fetched response, date and exact bytes. |
| [browser](#browsers-and-web-content) | `browser.page-crawl` | HTTP response headers/body, redirects, fetch facts. Shared URL queue/API reader. | page.fetched | All visited/saved URLs queued/captured/failed without cap. |
| [browser](#browsers-and-web-content) | Acquired CDX/archive-index responses | Original Wayback/Common Crawl HTTP/CDX replies, not URL-ledger generations | One acquired index row plus request metadata; URL/time/digest/status/length and original response range | Archived response remains citable; a lookup result is not proof that page content was acquired |
| [browser](#browsers-and-web-content), [network](#filesystem-audit-and-network) | `network.http` | Native browser request/response interface, WARC/HAR/body resources and packet bytes where available | Request/document/frame/stream IDs, direction/method/status/headers/timing/body ranges, WebSocket frame records | Binary/large body, redirected response, TLS-only gap; no replayed POST and no assumed trusted CA |
| [exports](#account-exports) | `browser.takeout-history` | Chrome Takeout JSON records. Shared package pass. | browser.history_entry | Exact decimal time, repeated records and cross-export title. |
| [exports](#account-exports) | `video.youtube-takeout` | Native YouTube watch/search HTML/JSON plus playlist records. Shared package pass. | video.watched; video.searched; saved-item records | Missing/deleted video metadata and changed titles. |
| [exports](#account-exports) | `google.my-activity` | Native per-service HTML cells/JSON records. Shared Takeout pass. | action.recorded | Export-day CET/CEST, repeated cells and changed title. |
| [exports](#account-exports) | `google.keep` | Native Keep JSON, lists/labels/media. Shared Takeout pass. | note.observed | Creation-time collision and changed list/attachment. |
| [exports](#account-exports) | `google.tasks` | Native Tasks JSON/.Tasks nested lists. Shared Takeout pass. | task.revised | List hierarchy, completion/schedule and repeated IDs. |
| [exports](#account-exports) | `google.purchases` | Native Play Store/Purchases JSON wrapper variants. Shared Takeout pass. | purchase.observed | Every wrapper/time/amount and account identity. |
| [exports](#account-exports) | `google.assistant` | Native Assistant notes/lists JSON/HTML/media; shared package pass | Note/list/item IDs, order, content, supplied created/edited time, attachments | List edits and omitted optional metadata |
| [exports](#account-exports) | `work.freedom` | Native Freedom CSV/JSON daily usage export records | Domain/app/date/seconds/session and all supplied fields, as vendor observations | Date-only record stays a date; missing day versus recorded zero |
| [exports](#account-exports) | Password-manager export records | Native LastPass/provider CSV/JSON or encrypted export; explicit row/file reader | Original item IDs, folders, properties and exact supplied values; encrypted stays encrypted | No new decryption or key access; fixtures are synthetic |
| [exports](#account-exports), [finance](#finance-and-billing) | `account.openrouter` | Native usage/billing CSV/JSON/API responses | Request/provider IDs, model, token and currency counts as exact values, status, time and native metadata | Corrected usage record, unknown cost versus zero; no AI transcript parser added |
| [exports](#account-exports), [health](#health-exports-and-wearables) | `account.samsung` | Native Samsung account/category CSV/JSON and database rows | `account.row`, health/call/task records according to actual category; all metadata, unknown table columns | New category and credential-bearing encrypted value remain intact as captured; no made-up empty success |
| [exports](#account-exports), [notes](#notes-documents-and-files) | Google Drive/Photos/Pay and other present Takeout products | Native files, sidecars and vendor JSON; shared pass plus document/photo/finance reader | Per file/item/transaction/property record; source IDs, relationships and unknown categories | Inventory every member; unsupported categories visible; SQLite members also read as rows |
| [mail](#mail-calendars-and-contacts) | `email.mailbox` | RFC 5322/MIME mbox/Maildir/IMAP/Gmail RAW+history. Mailbox files and full-account API cursor. | mail.message; provider/change/gap events | All folders/labels, attachments and history gaps. |
| [mail](#mail-calendars-and-contacts) | `email.outlook-csv` | Native Outlook CSV and PST bytes/records. Explicit export/PST reader. | mail.message | Quoted multiline fields, dates and attachments. |
| [mail](#mail-calendars-and-contacts) | `calendar.ics` | Native ICS/CalDAV events/tokens. Shared export and live sync. | calendar.event; occurrences derived | Infinite recurrence, DST, exception and cancellation. |
| [mail](#mail-calendars-and-contacts) | `contacts.vcard` | Native VCF/CardDAV/phone contact records/photos. Export/API/row reader. | contact.observed | Repeated properties, groups and explicit deletion. |
| [finance](#finance-and-billing) | `hledger-journal` | Native complete hledger journals/includes/directives. File/include acquisition and transaction/rule framing. | transaction.posted and native directive/rule records | All date/commodity/price/assertion/include forms. |
| [finance](#finance-and-billing) | `finance.bank-csv` | Native bank CSV/MT940/CAMT statements. Explicit native export reader. | bank.transaction and statement records | Exact decimals, reversals, booking/value dates. |
| [health](#health-exports-and-wearables) | `health.samsung-export` | Native Samsung Health CSV and companion JSON/binary. Shared vendor export reader. | health.record per row/sample/stage | Every category and nested point; sleep stays per session and stage. |
| [health](#health-exports-and-wearables) | `health.samsung-gdpr` | Native Samsung GDPR category rows and binning payloads. Vendor package/row reader. | health.record per bin/sample; account rows separately | HRV/stress/HR bins independently queryable. |
| [health](#health-exports-and-wearables) | `health.sleep-as-android` | Native CSV per-session headers/data/event columns and ZIP. Shared export reader. | health.record per native record/sample | All dynamic columns, long sleep and offset evidence. |
| [health](#health-exports-and-wearables) | `health.xiaomi-cloud` | Original Xiaomi vendor API responses. Native API acquisition; exact vendor payload history. | health.record per vendor item/sample | All sensors and history; the original response is the input. |
| [health](#health-exports-and-wearables) | `health.gadgetbridge` | Native SQLite rows, GPX and preference XML. Row/file/package reader. | health.record and per-sample records | All device sample tables and sports track points. |
| [health](#health-exports-and-wearables) | `google.fit` | Native Takeout Fit JSON/CSV/FIT/TCX/GPX where present; shared package pass, explicit decoder per actual member | One health point, activity or track point; data source/device/type, interval, values/units, accuracy and unknown fields; no run/day substitution | Point-count conservation, large member, exact time and a nested dense series |
| [health](#health-exports-and-wearables) | `health.ble` | Native GATT payloads/HCI packets and device history responses | One packet/sample with UUID/device/protocol/time/sequence and bytes; measures only with implemented decoder | Proprietary unknown frame, device reset, disconnect gap and all available sensor channels |
| [phone](#phone-location-and-personal-devices) | `phone.events` | First-party native phone event logs and Health Connect records. Hub producer or original file acquisition. | phone.event; call/app/health/notification records | All kinds, updates/deletions and resend identity. |
| [phone](#phone-location-and-personal-devices) | `phone.ambient-levels` | Native loudness window/sample records and calibration. Phone producer/native files. | phone.ambient.sample | Window, scale and actual calibration. |
| [phone](#phone-location-and-personal-devices) | `phone.ambient-audio` | Native AAC/M4A media and sidecars. Phone hub/media file reader. | audio.segment | Original chunks/clock and missing upload. |
| [phone](#phone-location-and-personal-devices) | `phone.sensors` | Native sensor-binary chunks with wall/monotonic anchors. Phone hub/binary record reader. | sensor.sampled per native sample | Every sample/channel, corrupt tail and clock drift. |
| [phone](#phone-location-and-personal-devices) | `phone.speech` | Native voice-note/speech WAV/M4A and sidecars. Phone hub/media file reader. | audio.segment | Media bytes separate from transcript. |
| [phone](#phone-location-and-personal-devices) | `phone.logcat` | Native Android logcat records/continuations. Native record pull/stream reader. | log.line per record | All priorities, continuation and restart buffer loss. |
| [phone](#phone-location-and-personal-devices) | `phone.instruments` | Native psychometric/reaction/EMA run and answer records. Phone producer/native files. | instrument.run; self_report.answered | Instrument/version/questions/answers and corrected result. |
| [phone](#phone-location-and-personal-devices) | `location.google-timeline` | Native Timeline/Location History JSON and GPX/KML where supplied. Shared export reader. | location.sample; vendor place records | Raw/semantic points, accuracy, timezone and sparse history. |
| [phone](#phone-location-and-personal-devices) | `location.phone-gps` | Native GNSS/GPS/OwnTracks/GPX records. Phone producer/native files. | location.sample | Precision, altitude, clock drift and unavailable intervals. |
| [social](#social-and-chat) | `email.teams-export` | Native Teams package/JSON/LevelDB and assets. Provider-native package/store reader. | conversation.message | Threads, revisions, reactions and media. |
| [social](#social-and-chat) | `weechat` | Native IRC log lines and metadata. Append/rewrite reader. | chat.message | Explicit log zone, joins/parts/notices and multiline. |
| [social](#social-and-chat) | `facebook-messenger-thread` | Native Messenger JSON/SQLite and attachments. Export/row/file reader. | conversation.message | Reactions/media and thread IDs without text identity. |
| [social](#social-and-chat) | `phone.samsung-sms` | Native Samsung SMS backup records. Export/row reader. | conversation.message | Multipart SMS, direction, attachment and local dates. |
| [social](#social-and-chat) | `chat.matrix` | Native sync JSON/events and media. Sync cursor/export reader. | conversation.message | Edits/redaction/encryption and room token gaps. |
| [social](#social-and-chat) | `chat.discord` | Native data package/API records and assets. Package/API reader. | conversation.message | Threads, edits and provider-limited history visible. |
| [social](#social-and-chat) | `chat.telegram` | Native Telegram Desktop JSON/HTML/media. Export reader. | conversation.message | Replies/forwards/reactions and entity offsets. |
| [social](#social-and-chat) | `chat.signal` | Native Signal rows/backups and attachments. Authorized client/row/file read interface. | conversation.message | Encryption availability, edits and attachment bytes. |
| [social](#social-and-chat) | `chat.whatsapp` | Native chat export/backup records and media. Explicit native export reader. | conversation.message | Multiline text, sender/date ambiguity and media. |
| [social](#social-and-chat) | `reddit-gdpr` | Native GDPR CSV/JSON/ZIP, API trees and acquired archive JSON. Package/API reader. | post.published; vote.cast; bookmark.saved; conversation.message | Complete comment context, metadata and edit/delete. |
| [social](#social-and-chat) | `wykop` | Native Wykop export/API records and media. Package/API reader. | post.published; vote.cast; bookmark.saved; notification.received; account.setting | Observed feeds/context as well as authored records. |
| [social](#social-and-chat) | `social.themotte` | Native authenticated API/JSON/downloads. API/export reader. | post.published | Full comment trees and revisions. |
| [social](#social-and-chat) | `social.x-archive` | Native X archives; Mastodon/ActivityPub; HN JSON. Provider-specific export/API readers. | post.published; conversation.message | Replies/media/edits; provider availability/cost explicit. |
| [social](#social-and-chat) | `google.chat` | Native Google Chat JSON/package media; shared pass | Message/space/thread/member IDs, exact content, revisions, reactions and assets | All thread messages and attachment ranges; empty exported stub reported as stub |
| [media](#music-video-reading-and-libraries) | `raindrop-bookmarks` | Native CSV/JSON/API records, HTML/PDF copies/highlights. Export/API/file acquisition. | bookmark.saved; highlight.created; page.fetched | Highlights and saved content are not count-only. |
| [media](#music-video-reading-and-libraries) | `reading.pocket-export` | Pocket/Instapaper/Wallabag native HTML/CSV/JSON. Export/API reader. | bookmark.saved; highlight.created | Read state, collections, notes and archived bodies. |
| [media](#music-video-reading-and-libraries) | `reading.stack` | Owner reading-stack records. Owner API/native record reader. | reading.item.changed | Reorder, note and completion revisions. |
| [media](#music-video-reading-and-libraries) | `reading.ebook` | KOReader native stats rows; Calibre rows/metadata/files. Row/file reader. | reading.session; book.status | Page position versus elapsed/attention uncertainty. |
| [media](#music-video-reading-and-libraries) | `reading.zotero` | Native SQLite rows, attachments and annotations. Row/file reader. | document.captured; highlight.created | Annotation geometry and changed attachment. |
| [media](#music-video-reading-and-libraries) | `reading.goodreads` | Native library CSV. Shared export reader. | book.status | Dates, read shelves, ratings and repeated titles. |
| [media](#music-video-reading-and-libraries) | `reading.substack` | Native article/download/email records. File/export/API acquisition. | post.published | Full body/assets and subscription/account metadata. |
| [media](#music-video-reading-and-libraries) | `reading.rss` | RSS/Atom XML and reader-native state/API rows. Feed and reader API/row reader. | reading.item.changed | GUID scoped by feed, edits/read/star state. |
| [media](#music-video-reading-and-libraries) | `spotify-extended-history` | Native streaming audio/video JSON arrays. Shared export reader. | music.played | Offline time, podcasts/audiobooks and all metadata. |
| [media](#music-video-reading-and-libraries) | `spotify-account-history` | Native short-history/account JSON, playlists/follows/payments. Shared export reader. | music.played; saved/account records | Minute precision and ambiguous extended-history overlap. |
| [media](#music-video-reading-and-libraries) | `music.spotify-live` | Native playback/library/playlist API JSON. Read-only API cursor/polling. | music.played; library.item.saved | Recently-played window limit and playlist revisions. |
| [media](#music-video-reading-and-libraries) | `music.listenbrainz` | Native ListenBrainz/Last.fm scrobble JSON. API cursor/export reader. | music.played | Corrections, repeated plays and time precision. |
| [media](#music-video-reading-and-libraries) | `video.mpv` | Native mpv/Jellyfin/Plex logs/state. Native log/API producer. | video.watched | Seek/pause/end versus partial watch. |
| [media](#music-video-reading-and-libraries) | `games.steam` | Native Steam playtime/app logs/process observations. Client API/log reader and process events. | game.session | Launch versus playtime and idle game. |
| [media](#music-video-reading-and-libraries) | `reading.annas-archive` | Native account acquisition/download JSON/CSV and acquired files | Acquisition ID/time, provider item/content identifier, URL, file reference and supplied metadata | Acquisition is distinct from a play/read; original download file addressed |
| [code](#code-forges-and-work-records) | `git-commit-history` | Native commit/tree/blob/ref/reflog bytes and diff facts. Repository object/ref reader. | git.commit; git reachability/ref observations | Stable repo across worktrees and exact objects. |
| [code](#code-forges-and-work-records) | `code.svn-history` | Native SVN XML log/diff and repository metadata. Read-only SVN/export reader. | commit.created with provider=svn | Repository UUID, copy history and revision. |
| [code](#code-forges-and-work-records) | `code.github` | Native GraphQL/REST/HTTP responses and CI logs/artifacts. Read-only full paginated API reader. | github.issue/pull_request/review/check/workflow_run plus comments/jobs/artifacts | Old reruns, all comment/review pages and actual outputs. |
| [code](#code-forges-and-work-records) | `code.gitlab-forgejo` | Native forge API/webhook/export and artifacts. Read-only provider API reader. | forge.item.revised; forge.review; ci.run.observed; ci.job; artifact.observed | Provider IDs, old pipeline rerun and full artifacts. |
| [code](#code-forges-and-work-records) | `work.xtask-history` | Native devkit/xtask SQLite run/step/history rows and artifacts. Owner history API/export or committed rows. | tool.run.observed; check/step records | Killed run, exact selectors/revision and full stderr. |
| [code](#code-forges-and-work-records) | Job-runner records (optional adapter, off by default) | Native owner job/campaign records and artifacts. The runner's read API or export. | tool.run.observed; job.changed; campaign.changed | Queue/setup/build/test duration and retry/cancel outcomes. |
| [code](#code-forges-and-work-records) | `work.beads` | Native issue revisions/dependencies/comments from owner API. Read-only bd with explicit actor. | task.revised | Every revision/comment/edge and removed task. |
| [code](#code-forges-and-work-records) | `work.polylogue-verification` | Native verification run/step/output records. Owner history API/export/rows. | tool.run.observed | Exact check/revision/input identity and failures. |
| [code](#code-forges-and-work-records) | `work.nix-builds` | Native Nix daemon/build logs, derivations and run records. Native log/API/history reader. | tool.run.observed | All build phases, failure and substituted versus built. |
| [code](#code-forges-and-work-records) | `editor.neovim` | Native integration/autocommand reports and buffer metadata. Editor producer via integration socket. | editor.reported | Buffer save/mode/rename and record ordering. |
| [code](#code-forges-and-work-records) | `editor.vscode` | Native VS Code/JetBrains extension reports. Editor producer. | editor.reported | Document IDs and edits across window restart. |
| [code](#code-forges-and-work-records) | `work.taskwarrior` | Native owner JSON export/API records | Task UUID, full fields, annotations, dependencies, revision and observed deletion | Corrections and complete history where the owner supplies it; capture never mutates tasks |
| [notes](#notes-documents-and-files) | `knowledgebase-vault` | Markdown/front matter/attachments and native IDs. Directory reader plus note parser. | note.document | Native-ID rename/collision and attachments. |
| [notes](#notes-documents-and-files) | `notes.logseq` | Native Org/Markdown/EDN/Joplin rows/files. Provider-native directory/row readers. | note.document | Block IDs, hierarchy, links and attachment revisions. |
| [notes](#notes-documents-and-files) | `docs-library-index` | Original documents and metadata. Directory/file acquisition shared with library. | document.captured; library.copy | Binary/empty file captured and replay after source gone. |
| [notes](#notes-documents-and-files) | `files.photos` | Original DCIM/images/videos plus EXIF/XMP/sidecars. Native file/row reader. | photo.taken; document.captured | Unknown EXIF zone and all actual media bytes. |
| [notes](#notes-documents-and-files) | `files.drop` | Original download/saved file bytes and metadata. Directory reader and format dispatch. | file.dropped; document.captured | Changed/empty/large file; no extension whitelist loss. |
| [notes](#notes-documents-and-files) | Clinical/scientific records | Native PDF/XML/CSV/JSON/DICOM/genomics/psychometric files and metadata in declared roots | File and native item/image/answer records; observation/provider/time/units and source category; interpretation separate | Multifile relationships, large file, unknown units and replay from the original only |
| [network](#filesystem-audit-and-network) | `fs` | Native watcher events plus original file/row revisions/metadata. Inotify/fanotify, with full walks at start and after overflow. | file.changed; document/row revisions | Root coverage, content dispatch and overflow. |
| [network](#filesystem-audit-and-network) | `system.audit` | Original audit log/netlink records including companions. Existing owner stream/raw-log reader. | audit.record | All syscall/PATH/CWD/PROCTITLE/EOE and groups. |
| [network](#filesystem-audit-and-network) | `network.netflow` | Native conntrack NEW/UPDATE/DESTROY messages and snapshot. Netlink subscriber; history reader for captured records. | net.flow per record | NAT/namespace/counters and socket overflow. |
| [network](#filesystem-audit-and-network) | `network.dns` | Native resolver logs/dnstap/DNS packets. Resolver/packet reader. | dns.query and response records | Answers/RCODE/TTL, repeated IDs and encrypted-DNS gaps. |
| [network](#filesystem-audit-and-network) | `network.router` | Native association/DHCP/traffic/syslog API/SSH replies. Read-only poll/subscription. | device.sampled; net.log.line; router row records | Every row/line, revised closed month and device clock. |
| [host](#host-records) | `system.journald` | Native journal export entries and all fields/namespaces. Cursor-follow capture and historic native-journal read. | journal.entry; lifecycle derived | Binary/repeated fields, cursor vacuum and boot. |
| [host](#host-records) | `system.systemd` | Structured journal facts and full shared bus packets. Journal reading plus bus derivations. | unit.started/stopped/reloaded; boot/sleep/shutdown facts | No false start from ordinary service message. |
| [host](#host-records) | `system.udev` | Native kernel/udev netlink messages and properties. Subscribe then device inventory. | device.uevent; device.changed derived | Present versus add, sequence gaps and unknown action. |
| [host](#host-records) | `system.machine-telemetry` | Original collector SQLite rows and Below native samples. Committed row/native record reader. | resource.sampled; process.killed; hardware.state | Every sample; rotation/ID reset/PID start and status. |
| [host](#host-records) | `system.machine-records` | Native activations/generations/drills/drift/inventory/experiment records. File/owner API reader. | machine.record; generation.changed; backup.checked; experiment.started/run | Actual exit/outcome and original artifact, not health inferred. |
| [host](#host-records) | `environment.awair` | Original device/local/cloud API JSON samples. Native API polling. | sensor.sampled | All channels, device time, units and request failure. |
| [host](#host-records) | `environment.weather` | Original weather API response at an observed location/time. Recorded enrichment acquisition. | sensor.sampled | Location/time provenance, missing values and quota. |
| [screen](#screen-video-and-images) | `media.screen-frames` | Original image files and native sidecars. Existing native-file acquisition. | media.segment; frame records derived | Referenced image bytes stored, not just path. |
| [screen](#screen-video-and-images) | `media.screenshots` | Native PNG/JPEG/WebP and metadata. File-drop/directory reader. | media.segment | Empty/unreadable file and original encoding. |
| [screen](#screen-video-and-images) | `media.screen-recordings` | Native MP4/MKV/WebM/replay files. File-drop/directory reader. | media.segment | PTS, codec and interrupted container. |
| [screen](#screen-video-and-images) | `media.screen-video` | Designed, not captured ([screen video](#continuous-screen-video)). Per-output compositor capture, reconstructible residual encoding. | screen.segment; media.segment shared model | Frame-for-frame timing/pixel replay and topology. |
| [screen](#screen-video-and-images) | `media.screen-ocr` | Vendor-native OCR JSON/TSV/text imports; new OCR is derived. Native import; downstream OCR separately. | ocr.segment; screen.text derived | Box units, confidence scale and model/run identity. |
| [audio](#audio-and-speech) | `media.audio` | Per-stream PCM/FLAC; original native encoded files. All PipeWire sources/sink monitors/application streams. | audio.segment; media.segment shared model | All streams, exact samples and a stop midsegment. |
| [audio](#audio-and-speech) | `media.audio-topology` | Native PipeWire object/event/format replies. Graph monitor shared with recorder. | audio.device.changed | Node serial reuse, rate and channel-map change. |
| [audio](#audio-and-speech) | `media.audio-transcript` | Native JSON/Whisper/WebVTT/SRT/text; ASR derived. Native import; downstream model separately. | transcript.segment | Relative offsets and file-root metadata after restart. |
| [operator](#operator-records-and-source-health) | `operator.raw-log` | Original authored Markdown journal. Append/rewrite record reader. | operator_log.entry | Mixed/multiline entries, correction and unknown date. |
| [operator](#operator-records-and-source-health) | `operator.marks` | Native hotkey request with optional note/media. Tiny client to common intake. | operator.mark | Under 1 s without available context. |
| [operator](#operator-records-and-source-health) | `api` | Native operator request bytes. The operation table's intake. | Typed judgments/comparisons/calibration/forecast records | An unsupported request is refused, never stored as a placeholder success. |
| [operator](#operator-records-and-source-health) | `operator.steering` | Native steering SQLite rows/owner exports. Committed rows/native export reader. | steering.row; comparisons/forecast records | Authored versus generated field, live path and revision. |
| [operator](#operator-records-and-source-health) | `operator.self-report` | Original answer/questionnaire/request records. Common request intake; native instrument import. | self_report.answered | Question/version/scale, corrections and operator provenance. |
| [operator](#operator-records-and-source-health) | `sinex.self` | Native component/lifecycle/gauge/error observations. Common source/intake and derived records. | self.observation; capture.gap; health.status proposals | Self-observation never triggers itself; one authoritative store. |
| [Polylogue](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md) | `polylogue` | Original AI files and attachments and the typed records parsed from them. Polylogue's producer over the ingress. | polylogue.session/message/tool_call/turn/delivery and user-state records | AI files are parsed only by Polylogue. |
| [streams](streams.md) | `desktop.clipboard` | All offered MIME bytes and selection metadata. Selections stream. | desktop.selection; clipboard.copied derived | MIME, primary-selection and repeat checks. |
| [streams](streams.md) | `desktop.notification` | Native bus messages. D-Bus stream, typed views. | notification.shown; notification.closed; notification.action | Restart and replacement/action sequence. |
| [streams](streams.md) | `desktop.a11y` | AT-SPI bus packets and tree-read result units. AT-SPI stream. | desktop.a11y; typed derivations | Self-generated queries do not become observed bus events. |
| [streams](streams.md) | `desktop.evdev` | Exact kernel input records in SYN_REPORT packets. Evdev stream. | desktop.input; typed key/pointer derivations | Discrete keys/switches never sampled. |
| [streams](streams.md) | `desktop.mpris` | Native session-bus player messages. D-Bus capture once; derive player facts. | player.changed | Player restart, seeks, unknown fields. |
| [streams](streams.md) | `system.dbus` | Whole native system/session bus capture. D-Bus stream. | bus.message; bus.connection; capture.gap | All calls/replies/errors; no capture-generated traffic events. |

## Laws

Every law is checked on synthetic fixtures for each source of its family that is implemented, on top of the common checks in [sources](sources.md).

- **catalog-entry-checks**: every implemented catalog entry passes the check in its row on a synthetic fixture.

Shell history and terminal recordings:
- **shell-history-bytes-exact**: metafied non-UTF-8, split UTF-8 and CRLF history read back byte-exact, with no timestamp or shell guessed for untimed plain text.
- **shell-history-rewrite-alignment**: rewriting an old history file with equal commands keeps sequence and multiplicity; an ambiguous alignment is reported, never collapsed.
- **shell-history-exit-unknown**: exit zero and an unknown exit stay distinct, and a negative duration stays unknown.
- **shell-history-open-command**: a running command that finishes after later commands is one record revised by its finish, in its original order.
- **shell-history-new-generation**: a new history database or file generation never reuses the earlier generation's IDs.
- **terminal-cast-record-count**: native event counts equal cast record counts, including literal repeated records and each recorder's final unterminated record.
- **terminal-cast-exact-time**: v2 absolute offsets and v3 cumulative intervals resolve with exact decimal arithmetic.
- **terminal-escapes-not-executed**: replaying ANSI and OSC records never executes escapes.
- **terminal-buffer-states**: buffer clear, alternate screen and scrollback truncation are recorded, and the full ANSI snapshot stays readable when a derived tail is empty.
- **terminal-window-reuse**: PID or window reuse starts a new session identity; a closed window is distinct from an interrupted recorder.

Desktop state:
- **desktop-snapshot-race**: a connection snapshot racing a change loses neither, and a snapshot never fabricates an open or start.
- **desktop-unknown-ipc**: an unrecognized IPC event is kept with its text.
- **desktop-inactive-compositor**: a compositor that is not running is marked inapplicable, not failed.
- **desktop-activitywatch-rows**: equal ActivityWatch starts with distinct row IDs stay distinct, and a late heartbeat outside the incremental window is read.
- **desktop-archive-read-only**: a repeated archived database is read without being written and without double import.
- **desktop-arbtt-native**: arbtt's native binary log is read from a binary fixture.
- **desktop-notification-restart**: a notification daemon restart with ID reuse yields distinct records.
- **desktop-clipboard-repeats**: duplicate clipboard bytes stay distinct records.
- **desktop-device-replies**: a DDC timeout differs from zero brightness, and a device replaced under the same address is a new device.
- **desktop-focus-side-by-side**: comparing compositor and ActivityWatch focus never erases either input.

Browsers and web content:
- **browser-profile-whole**: a profile with visits, URLs, download chains, bookmarks, cookies and an unknown table is read whole, the unknown table as raw rows with unread status.
- **browser-mutable-visits**: a changed old visit is a newer reading; a recreated database is a new generation.
- **browser-paths-and-stores**: non-UTF-8 paths and a LevelDB store under concurrent write are read consistently.
- **browser-response-bodies**: binary, large and redirected responses are stored exactly, and one URL with two bodies yields two response records.
- **browser-content-after-view**: a page whose content changed after viewing keeps the viewed response; refetching adds a new observation.
- **browser-unavailable-response**: an unavailable authenticated response is recorded as unavailable, never replaced by a crawl.
- **browser-crawl-queue**: an extension retry and a full queue restart lose no URL, and visited and saved URLs reconcile with captured, queued and failed responses.
- **browser-unsupported-version**: an unsupported browser version is reported as unsupported.
- **browser-content-addressable**: raw content is reachable through `show`, and document extraction reads the stored bytes.

Account exports:
- **export-multipart-sets**: a multipart set is one export with member identity, and archive or member changes are new readings.
- **export-unsupported-product**: an unsupported product and a damaged item among valid ones are inventoried and visible, and valid items are still read.
- **export-format-edges**: sparse arrays, quoted CSV newlines and a last record without terminator are read exactly.
- **export-equal-timestamps**: identical timestamps with distinct items, including two Keep notes sharing a creation time, stay distinct.
- **export-revisions**: a renamed Keep note, task revisions and changed titles or zone labels across exports are newer readings.
- **export-purchase-wrappers**: every purchases wrapper variant is unwrapped with exact amount and currency text.
- **export-deleted-video**: a deleted YouTube video's omitted title stays absent, not invented.
- **export-replay-without-archive**: replay reproduces the records after the archive input is removed.

Mail, calendars and contacts:
- **mail-imap-changes**: sparse UIDs, a UIDVALIDITY reset, an IDLE disconnect and expunge renumbering never cause false deletions or duplicates.
- **mail-identity**: the same Message-ID in two accounts gives two messages, and a message without Message-ID uses provider or file identity.
- **mail-bytes-exact**: duplicate headers, binary attachments, malformed dates and RFC encodings survive byte-exact.
- **mail-gmail-history**: OAuth expiry mid-page, 429 and 5xx retries, and page replay neither lose nor multiply records, and the history ID advances only after admission.
- **mail-history-expired**: Gmail history 404 records a gap and one read-only full read.
- **mail-account-isolation**: accounts never share records or cursors.
- **calendar-recurrence**: an indefinite RRULE, DST transitions, exceptions moved across query ranges, cancellations and all-day and floating events expand correctly at query time.
- **contacts-revisions**: repeated fields, an updated photo and a removed contact are recorded as revisions and explicit removal.
- **mail-recall-citation**: recall cites an exact message, cue or attachment location.

Finance and billing:
- **finance-hledger-forms**: every native hledger date and posting form, quoted commodities and exact large and small decimals read exactly.
- **finance-includes**: include cycles, missing files and duplicate references are reported, and an included-file change is a new reading.
- **finance-authored-as-written**: an unbalanced authored transaction is kept as written; assertions, prices and periodic rules are read; capture never edits or fills amounts.
- **finance-duplicate-transactions**: equal duplicate transactions stay distinct by duplicate number.
- **finance-no-journal**: no journal on the host differs from an empty journal.
- **finance-bank-statements**: multiline CSV, reversals and multiple currencies are exact, and statement IDs are scoped by account.
- **finance-unsupported-directive**: an unsupported directive is reported with its bytes, never as parsed.

Health exports and wearables:
- **health-categories**: every observed vendor category, CSV preamble, header and unknown column is read, and native binned arrays yield one record per bin.
- **health-precision**: timestamps keep source precision, null differs from zero, and DST or offset inconsistencies keep the raw zone.
- **health-sessions**: session and stage links survive, and long sleep is not capped.
- **health-repeated-exports**: sparse and repeated exports with partial fields are newer readings, not duplicates.
- **health-dense-points**: dense Fit data stays one record per point, and a point stays queryable after daily figures are derived.
- **health-coverage-counts**: coverage counts unique provider IDs and raw points and charts gaps by source and device.
- **health-ble-transport**: a BLE reconnect, duplicate packet, sequence reset and clock drift neither lose nor multiply samples.
- **health-backfill-overlap**: sync backfill and live overlap are accounted, not doubled.
- **health-missing-channel**: an unsupported or disabled channel shows as unavailable or a configuration gap, never zero, and missing credentials are a visible failure.

Phone, location and personal devices:
- **phone-resend-identity**: a retransmitted upload with a changed delivery time is admitted once.
- **phone-log-rewrite**: a rewritten day log aligns with its earlier reading, and two identical app events stay distinct.
- **phone-time-forms**: compact and RFC date forms and a clock jump resolve, and an epoch-zero sentinel is unknown.
- **phone-damaged-input**: a corrupt binary tail and a missing sidecar keep the readable part and report the rest.
- **phone-offline-outbox**: an offline outbox and a phone restart deliver every record once.
- **phone-health-connect**: a repeated page, an expired token, a revoked grant and a removed source are handled without multiplying or losing records, with a gap where history is lost.
- **phone-media-exact**: media is retrievable byte-for-byte.
- **phone-coverage**: per-device provider IDs, time bounds, largest gaps, pending grants and raw-to-admitted ratios are reported.
- **phone-instrument-answers**: instrument records keep instrument, version, questions and exact answers.

Social and chat:
- **social-thread-tree**: a full thread with nested replies is captured, and older edits and deletes are kept.
- **social-reactions**: reactions changed without text change are a new reading; attachments larger than a page are complete.
- **social-unknown-kinds**: unknown message kinds and an invalid byte in one record keep the other records.
- **social-repeated-text**: repeated text stays distinct records.
- **social-local-time**: local timestamps across DST resolve with the explicit zone.
- **social-cursor-recovery**: cursor expiry and a rate-limit restart neither lose nor duplicate records.
- **social-account-isolation**: provider ID reuse across accounts never merges records.
- **social-read-only**: acquisition makes no changes at the provider.
- **social-media-citations**: a conversation is queryable through raw records, with media and citations resolved.

Music, video, reading and libraries:
- **media-offline-play**: an offline play uploaded days later keeps its play time and sync time.
- **media-podcast-audiobook**: podcast and audiobook plays keep their episode fields.
- **media-history-overlap**: basic and extended history overlap and ambiguous matches keep every source reading.
- **media-repeated-plays**: equal repeated tracks stay distinct plays.
- **media-library-revisions**: a playlist reorder, a removed library item and a scrobble correction are newer readings.
- **media-partial-sessions**: an interrupted mpv session and a game left running are recorded as observed, not as attention.
- **media-reading-content**: a deleted webpage keeps its older content; Raindrop highlight edits and archived assets, ebook page positions, Zotero annotation geometry and RSS read-state revisions are kept.
- **media-plays-versus-saved**: plays and saved or reading records are queried separately with provider coverage.

Code, forges and work records:
- **code-repository-identity**: a clone, move or worktree is one repository, and two repositories with the same path name are two.
- **code-ref-changes**: force-push, rebase and then ref deletion are observed state changes; the commits remain.
- **code-history-shapes**: an orphan branch and a root merge are read correctly.
- **code-exact-objects**: binary and non-UTF-8 paths and signed commits keep exact bytes.
- **code-forge-reruns**: an old CI rerun without a new pull request update is captured, and all review and comment pages are read.
- **code-forge-expiry**: an expired log or artifact is a visible provider loss.
- **code-job-outcomes**: job retries, cancellations and failures are recorded with durations.
- **code-task-revisions**: task revisions, dependencies and comments are captured, and a repeated export does not duplicate them.
- **code-killed-run**: a killed devkit run is recorded, and a failure outside the last displayed lines is found in the full output.
- **code-run-to-commit**: a query connects a failed run to its exact commit, task revision and original output.

Notes, documents and files:
- **notes-document-edges**: empty, binary and very large documents and invalid UTF-8 names and text are captured exactly.
- **notes-native-id**: a native-ID rename follows the note, and an ID collision is reported.
- **notes-front-matter**: invalid or unsupported YAML is reported with the bytes kept.
- **notes-attachment-change**: an attachment change is a new reading.
- **notes-boundaries**: external symlinks and mount boundaries are respected.
- **notes-removal**: a file disappearing midscan, an inaccessible root or a selection change never records a removal; only a complete scan does.
- **notes-photo-metadata**: EXIF without offset stays unknown-zone, and a rotated photo keeps its original bytes.
- **notes-multifile-study**: a DICOM multifile study keeps its relationships.
- **notes-extraction-replay**: a malformed document's bytes stay available, and extraction replays after the original is deleted.
- **notes-acquired-unparsed**: a file-only capture is reported as acquired and unparsed until extraction succeeds.

Filesystem, audit and network:
- **fs-rename-pairs**: rename pairs and moves across roots are paired, and non-UTF-8 paths are exact.
- **fs-overflow**: watch overflow and permission loss become gaps followed by a full walk, with no false removals.
- **fs-no-self-capture**: Sinex's own storage never feeds back into capture.
- **audit-groups**: interleaved serials, reboot serial reuse, a large EXECVE and an incomplete group are grouped correctly and stay visible.
- **network-flows**: IPv4, IPv6 and NAT, a long flow with UPDATEs, and a missed NEW with a seen DESTROY are recorded, with missing starts unknown.
- **network-netlink-loss**: netlink loss and namespace or zone reuse are detected and gap-marked.
- **network-dns**: repeated DNS IDs stay distinct, and DoH outside the resolver is a coverage gap.
- **network-encrypted-traffic**: binary responses are exact, and TLS-only traffic stays encrypted with its gap stated.
- **network-router**: a router reconnect, a revised closed month and unknown counters are newer readings or unknown, never zero.
- **network-count-reconciliation**: native raw counts reconcile with admitted counts, and every difference is explained.

Host records:
- **journal-fields**: binary and repeated journal fields and every priority are kept; malformed or missing time is reported.
- **journal-cursor**: cursor rotation, vacuum and namespaces resume correctly and report gaps.
- **journal-own-logs**: Sinex's own daemon logs are captured without a feedback storm.
- **journal-unit-starts**: an ordinary service message never becomes a unit start; timer triggers, suspend and resume, and the first-seen boot versus the true boot instant are distinct.
- **journal-udev-inventory**: the udev startup race loses no event, inventory means present, and an unknown action is kept.
- **telemetry-columns**: an unknown column or type, a reopened or rotated database and an ID reset are handled without loss or false continuity.
- **telemetry-process-identity**: the same PID with a new start is a new process, and counter rollover is detected.
- **telemetry-all-channels**: missing collector status is reported, a native Below fixture reads, and every hardware sample channel is queryable.
- **host-machine-records**: a command failure, a partial backup, a new generation and a missing revision are recorded as their exact outcomes.
- **host-diagnostics**: source diagnostics report counts, bytes and gaps through public status.

Screen video and images:
- **screen-image-bytes**: referenced image bytes are stored, empty or unreadable files are reported, and the original encoding is kept.
- **screen-recording-containers**: screen recordings keep PTS and codec, and an interrupted container is reported.
- **screen-ocr-import**: imported OCR keeps box units, confidence scale and model and run identity.
- **screen-frame-replay**: when continuous video is enabled, a synthetic moving pattern replays frame for frame at source timestamps, with decoded pixel hashes equal for lossless settings.
- **screen-topology**: hotplug, resize and rotation of two outputs, cursor, colour and HDR are recorded correctly.
- **screen-skipped-frames**: skipped frames are detected and gap-marked, and constant-state compression keeps duration.
- **screen-partial-segment**: a restart inside a partial segment and a broker outage during an open segment lose no completed frame.

Audio and speech:
- **audio-all-streams**: two microphones plus two sink or application streams are recorded separately at native multichannel rate, including silence.
- **audio-sample-round-trip**: sample hashes round-trip exactly for lossless settings.
- **audio-no-feedback**: the recorder's own output is never recorded back.
- **audio-graph-changes**: node reuse, hotplug, clock drift and underrun are source events or gaps.
- **audio-partial-segment**: a stop midsegment, a crash with a partial segment and a disconnected broker keep every completed segment decodable.
- **audio-transcript-import**: imported cues keep multiline text, file-root metadata, null versus zero confidence, offsets without an absolute start and exact decimal boundaries, and equal text from different runs stays distinct.

Operator records and source health:
- **operator-mark-latency**: a mark is acknowledged within 1 s at the 95th percentile without available context, and repeated marks are distinct.
- **operator-journal-entries**: journal dates, corrections and multiline entries are kept, and the journal replays.
- **operator-instrument-versions**: instrument version, scale and revision are kept on every answer.
- **operator-steering-schema**: a steering schema change keeps authored and generated fields distinct.
- **operator-request-retry**: a retried request with the same operation identity is admitted once.
- **operator-self-observation**: self-observation never triggers itself, and diagnostics stay truthful after a source failure.
- **elicitation-version-change**: an instrument version change keeps old answers under their version.
- **elicitation-correction**: a correction leaves both answers queryable.
- **elicitation-trigger-once**: a trigger fires once per declared condition.
- **elicitation-unanswered**: an unanswered prompt is distinct from a zero answer.
