# Source families

This page states, for every family of sources, what full fidelity means: which native records are read, what each record keeps, and which cases the family's tests must cover. The general capture model (capture by default, staging, the source declaration, inputs and materials, gaps and source health, the common record rules and the checks every source passes) is in [sources](sources.md); the base-layer streams (D-Bus, evdev, Wayland selections, AT-SPI, PipeWire, netlink, journald, Hyprland, the browser) and their spools are in [streams](streams.md); fusion of records seen through several sources is in [derivations](derivations.md#fusion); field hints and detectors are in [privacy](privacy.md).

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

Commands are read through the command fusion family (the shell integration's reports, Atuin and the shells' own history) and through the terminal records directly. The shell integration mints one execution token per command; kitty records it on the command's start and end, and the report records the Atuin row started for it, so those observers are matched by identifier rather than by time. Native event counts equal cast record counts.

## Desktop state

Family F02: window managers, desktop activity trackers, clipboard and notification archives, idle state and peripherals. The live Hyprland, D-Bus, selection and AT-SPI captures are [streams](streams.md); this family covers what the streams do not.

**Window managers are ordinary sources** behind one interface, so adding another is a new reader, not a new design. Hyprland's socket2 events are stored whole, with `hyprctl -j` snapshots on connection and on relevant changes ([the Hyprland stream](streams.md)). A snapshot is present state, never a fabricated open or start. Every IPC event, unknown event text, window, workspace and output identity and all properties are kept. The compositor plugin's frame, layout and input-latency observations are one record per native report, with no aggregation at the source. Sway and i3, niri and river, KWin, GNOME and X11 have separate native readers that produce the same domain vocabulary where the semantics match. KWin and GNOME bus traffic comes from the bus stream; only provider instrumentation the bus lacks is a separate reader. An alternative compositor that is not running is marked inapplicable, not failed.

**Activity trackers.** ActivityWatch is read from its native rows: every mutable row, bucket metadata and heartbeat revision; archive, API and native exports keep their source identity. It is a stopgap whose data is still captured whole. (ruling 2026-03-17, cc:6c1674bb-e912-438c-9de4-62658fb48cf4:41d66e2e-0047-4e0f-b937-56044c511122). Archive databases are opened read-only and never written. Coding agents animate kitty window titles with a spinner glyph, which defeats ActivityWatch's heartbeat merging; the reader strips the glyph and joins adjacent identical events into one span, and the stored row stays as written. Arbtt is read from its native binary log or the tool's lossless dump.

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

Other account archives (Samsung account data, Freedom usage, OpenRouter usage and billing, password-manager exports) are captured like the archives above. A password-manager export keeps encrypted items encrypted; fixtures are synthetic.

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

Discovery reads the account file declared as `input:gmail`; `stage --parser gmail PATH` uses the same file. It names the expected account and credential files:

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

The journal reader discovers `LEDGER_FILE`, `~/.hledger.journal` and configured finance roots, and accepts `stage --parser hledger PATH`. Include paths may be relative, absolute or home-relative, with `journal:` prefixes and globs. Each included file is an ordinary binding; missing files, cycles and unsupported targets are reported. Included files inherit the authored year at their include. Native timeclock and timedot imports stay written `journal.imported` records, not generated accounting transactions.

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

**Wearable configuration** is set outside Sinex by the device deployment, whose target is every sensor at its maximum cadence (continuous heart rate, SpO2, stress, sleep, temperature where supported). The BLE source reads and reports each channel's configured and actual state, so a disabled or slowed channel shows in status as a configuration gap rather than as missing data. (ruling 2026-08-14, cc:c7d56707-950b-4e3e-bd5c-b4c1822d5b5c:13add765-7bb0-41aa-9a84-8a27850068a3).

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

Phone capture status reports, per device, the provider IDs, time bounds, largest gaps, pending grants and the ratio of raw to admitted records.

## Social and chat

Family F09.

**Providers.** APIs and raw exports of Reddit, Wykop, The Motte, X, Mastodon, Hacker News, Messenger, WeeChat, Teams, Matrix, Discord, Telegram, Signal, WhatsApp and Google Chat.
- A record keeps posts, comments and messages with their revisions and deletion markers, conversation and parent IDs, senders and participants, timestamps and edits, replies, reactions and votes, attachments and media, links, forwards, account settings, bookmarks and notifications.
- A reaction list or an attachment body is never replaced by a count.
- Native message encodings and emoji are kept. Local log times need an explicit zone.
- Encrypted values stay encrypted unless the normal authorized client supplies plaintext.

**Live acquisition** reads the operator's available feeds and the full accessible conversation and comment context, not just the operator's own messages. Provider pagination and sync tokens are used, and edit and delete facts are captured. A failed or partial fetch never removes history. Rate limits, backoff and access restrictions are visible. An export-only provider is still discovered from its native packages and reported as export-only.

**Scope** is whole comment trees with metadata wherever the operator commented, plus linked content, self-posts and directly hosted media; dead links are recovered through the CDX reader ([browsers](#browsers-and-web-content)). (ruling 2026-08-11, cc:049c6f6c-9252-44c7-a0b2-3a585f63b686:c7769bf6-b74e-47ee-bdbf-659e784d6374). Only a new paid API or a credential not yet available needs the operator ([discovery and exclusions](sources.md)).

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

Continuous screen video is designed but not captured, because its storage cost is not yet affordable; audio, being cheap, is captured whole. (ruling 2026-10-28, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).

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

**All audio is captured, losslessly.** (ruling 2026-10-28, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).
- Every PipeWire capture source, sink monitor and application stream whose content reaches the user's audio graph is recorded; the graph itself is the [PipeWire stream](streams.md).
- Topology, node and port identities, serials across restarts, formats, native sample rate, channels and channel map, timestamps and clock correction are kept.
- Simultaneous input and output streams are recorded separately, including silence and overlapping signals. There is no voice-activity gating, microphone-only selection, 16 kHz downsampling or mono conversion.
- Lossless PCM or FLAC keeps the acquired samples; vendor-native Opus, AAC or WAV stays unchanged. Compression settings and sample-format conversions are declared.
- The recorder's own monitor path is never recorded back into itself; the topology rule that prevents it is visible and tested.
- Segments are independently decodable and continuous; frames carry clock and discontinuity facts so transcripts align later.
- Stream and node hotplug, graph rate changes and device failures are source events or gaps. All known devices are captured at startup, and streams are opened before publishing.

**Imported speech.** JSON, Whisper, WebVTT, SRT and plain-text transcripts keep each cue or segment, exact decimal relative offsets, speaker, language, model, run, confidence and unknown fields, linked to the media material when known. External run IDs are never turned into Sinex parent IDs. New speech recognition, voice-activity detection and diarization are model work in [derivations](derivations.md), off while model features are off, and never replace the audio.

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
- A **prompt** is an event recording that an instrument was offered: when, through which surface (notice, web, CLI, phone) and what triggered it. Triggers are declared over captured context: a time of day, an episode boundary, a journal mention, a gap in a self-report series, or the operator's request. Prompts are ordinary [signal rules](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/signals/README.md) within the notices' interrupt budget.
- An **answer** is a request material plus an event: instrument and version, each question's exact response with unit and scale, answer time and the operator as author. A correction is a new answer naming the one it corrects; both stay live.
- Substance use is served by an instrument triggered from journal mentions plus a derivation over the journal; title classifications are a derivation with model provenance when model features are on. Scoring and interpretation are derivations, never source facts.

No prompts are sent until the operator designs instruments for them; spontaneous self-reports are accepted (default; open operator question).

## Sources beyond the catalog

Family F18. The catalog is a floor: it lists what is known to exist, and Sinex captures beyond it. (ruling 2026-10-14, chatgpt:6ace15d4-c6d0-83ed-ac53-4a870df45514:2b47b1b6-ada1-44ab-a07d-f0080f34ae97). Applicable local records are discovered automatically; a new paid service, a new credential grant or a new kind of third-party observation needs the operator ([discovery and exclusions](sources.md)). Every added source inherits the rules in [sources](sources.md) and its family's requirements here. No source is left out for lacking an earlier reader.
