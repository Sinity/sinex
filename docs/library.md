# Media library

The media library is how the operator adds, describes, organizes, finds, compares and uses media of any kind inside Sinex: items and their copies, revisions and renditions, tags and properties, notes, collections, sets and fixed selections, ratings, comparisons and rankings, the library's view of saved and visited pages, and exact opening. This page owns the library's model, its features and its laws. Neighbouring pages own the mechanisms it rests on: [materials](materials.md) and [admission](admission.md) store and admit every library input; [the lifecycle](lifecycle.md) archives and tombstones them; [sources](sources.md) owns acquisition (scans, imports, page capture); [derivations](derivations.md) owns the runner that computes items and shown values; [the query surface](query.md) and [interfaces](interfaces.md) own the query language, the operation table and the CLI and MCP spelling of the library operations.

## Purpose

The library is a native workflow that makes other library applications dispensable. Importing another tool's files alone does not deliver it; adding, describing, organizing, finding, comparing and using media inside Sinex does. It is a general media library, not tied to any one tool. Ruling, 2026-10-18: "Stashbox was just an example of one kind of media library that would be mangeed by sinex. it's more general than stashbopx." (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:afc6b7b2-5100-4255-8ace-7d6b09ec801e).

The principles that shape every feature below:

- **Exact bytes and acquisition context are kept.** Item identity, byte identity and evidence that two copies correspond are three separate things.
- **Primary data is on disk as materials, and the library is rebuildable from them by replay.** Ruling, 2026-06-04: "everything should be primarily on disk. ideally entire stash thingy would be rebuildable to the greatest extent" (cc-queued:6785a302-980f-4cd0-bc7c-b5e4b3f0f1e9@2026-06-04T16:15:54Z).
- **Generated outputs are kept and reused, not regenerated.** Ruling, 2026-06-09: "why would we regen when it's right there?" (cc-queued:70929a29-e133-4051-9646-34e8505cb67f@2026-06-09T12:21:22Z).
- **The library is built from ordinary sources and derivations**, and authored curation is Sinex's own authored input. There is no library service, database or plugin framework beside Sinex.
- **Rendering uses existing media tools; queries use the one query language and its paging.** Nothing truncates a valid selection.
- **Missing content, uncertainty and conflicts are reported, never hidden.** A successful command or an imported catalog is not evidence of a useful library.
- **Nothing expires.** There is no media tiering or retention: media is kept until the operator archives it.

## Curation as authored input

### Requests and authorship

Every library change (add, tag, set, collect, rate, compare, note, withdraw) is a **request**. The request is stored as a material, and its records are admitted through the one writer with that material as their origin, like every other input ([admission](admission.md)).

- The **author** of each record (the operator, a named agent, a named producer) is computed by the daemon from the caller's token and is never supplied by the caller ([callers](interfaces.md#callers)).
- A curation record cites its subject (an item, revision, rendition, part, collection or another record) as a **typed reference in its payload**. It is never a derived event of the reading it describes or corrects, so replaying the subject does not break it; the reference resolves to the subject's new reading by its key.
- Imported values keep their acquisition evidence, and proposals keep their producer; neither is relabelled as the operator's.

### Withdrawals

Untag, unset, uncollect, rating withdrawal, dismissal reversal and undo are each a **withdrawal record**: its own authored act that names the record and revision it withdraws, with its author.

- The withdrawn record stays live as history. The current value is a **view** computed from all live records: a record counts unless a live withdrawal names it.
- Withdrawing a withdrawal restores the earlier record's effect.
- **Undo** is a new withdrawal of the named action's effects. It never restores a snapshot. If a later record depends on those effects, undo reports a conflict and changes nothing.
- A withdrawal is not deletion and does not use the lifecycle. Archiving and tombstoning remain the operator's deletion of materials and readings, and they apply to library request materials like to any other material ([the lifecycle](lifecycle.md)).
- Library kinds never rely on a newer reading of the same key in another material replacing an older one. Records seen through different materials stay separate ([identity](events.md#identity)), and withdrawals carry the meaning a replacement would otherwise have to.

### Revisions and conflicts

Every curation record has a stable opaque ID (independent of digests, paths, labels and event IDs), a revision, and the revision it replaces, or none. A revision's payload never changes.

- These revisions are **domain data in the payload**. They are not a link between readings: the core keeps no column or table pointing from one reading to another.
- A request that changes a value carries `expects: {record, revision}`. The writer checks it against the live view inside the admission transaction. Of two clients replacing the same value from the same revision, the first succeeds and the second receives a structured conflict naming the current revision. No policy picks a winner by timestamp.
- An action that writes several records (tag plus collect, assign plus insert, a bulk action over an explicit selection) commits all its records or none.

### Shown values

The value the library shows for a property or tag is computed from every live assignment by a precedence with two axes, both computed by the daemon:

- **origin**: authored, imported, proposed;
- **author**: the operator, an agent, a named producer.

The default order is the operator's authored value (an explicit empty included), then an agent's authored value, then imported values, then proposals. Each property declares its type, cardinality and selection policy, and a per-property policy can raise or lower agents' level. Agents keep every curation power; only precedence differs, so an agent's tag never counts as the operator's.

- Two different values at the winning level are a **conflict**; no value at any level is **unknown**. Unknown, withheld, conflicting, explicitly empty and a real empty string are five distinguishable states.
- An explicit empty stops fallback to lower levels; withdrawing it permits fallback again.
- A tag applies while at least one live, non-withdrawn assignment supports it: withdrawing the operator's assignment leaves an agent's. Reads show every supporting assignment and its author, even when the values agree.
- Rejecting an imported or proposed value is its own explicit record, not a reserved leading character in a tag label.
- Proposals (classifier or model output) are the lowest level and never overwrite an authored value. Model execution follows [the model-calling contract](derivations.md#model-calling); imported model outputs are captured regardless.

## Items and copies

An **item** is the continuing object the operator curates. A **revision** fixes an item's content. A **rendition** supplies original or transformed bytes of a revision. A **copy** is an observed location of bytes: each keeps host, root, path, observation, content digest, size, optional file time, format and source record.

- Equal bytes share storage, while separately curated items stay separate.
- A digest supplied by another catalog is distinguished from a digest Sinex calculated.
- Unknown times and dimensions stay unknown.

### Item identity

- The **item key** is the host, the root's identity and the relative path captured when the item was first seen. That original key is kept across later moves and never reconstructed from today's path.
- The **item ID** is a deterministic function of the key, so a rebuild from the same scan evidence yields the same IDs. Items come from acquisition evidence; no command creates an item record directly.
- Content hashes are revisions: changed bytes at the same key are the item's next revision.
- Unchanged bytes relocated within the root, with the old path gone in the same completed scan, continue the item. When several equal-byte files move in one scan, a unique match continues and any other case is reported as ambiguous, with curation waiting unattached; a silent swap of tags between items is the worst outcome for curated data.
- A whole root or host moving carries items across only through an explicit recorded relocation. A root identity marker may suggest a mapping, which the operator confirms; curation never moves on a guess.
- Equal bytes alone never merge separately curated items.
- Stored events are never rewritten to change identity. A change of the item algorithm that would alter identities in a populated library is refused rather than applied silently.

### Revisions and availability

- Revisions and the sequence of observations are separate: A→B→A reuses A's byte revision but keeps all three transitions.
- The current state follows recorded capture order, not file time, processing time or whichever row a read returns first.
- A complete scan finding a path gone changes that copy's availability, not the existence of stored originals.
- Inaccessible, incomplete, reference-only, stored-but-unreadable and derivative-only are distinct availability states. A disconnected root or an incomplete traversal never claims deletion.
- Finding matching bytes elsewhere offers a verified location; it never merges separately curated items. A reference-only original is re-found by its actual hash.

## Intake

A file, directory, archive member or outside-producer acquisition goes through the existing reader, material store and writer ([materials](materials.md), [sources](sources.md)); the library has no intake path of its own.

- An ordinary addition keeps the exact bytes. Indexing by reference only is an explicit choice made per addition, and its consequences appear in results.
- Any file can be stored and retrieved even when decoding it is unsupported. Type comes from content, with an explicit unknown state; a misleading extension does not decide it.
- Recorded source bytes and request bytes are immutable.
- Storing bytes and admitting the event agree on the same observed content: a later reopen of a changed source cannot supply different bytes under the earlier digest. A file mutated during acquisition ends as a complete matching acquisition or a visible retry or failure.
- Explicit selection never silently excludes a valid file because of its suffix or a hidden name. Partial scans, unreadable entries, symlinks, changing files and sidecar recognition report their actual coverage. A scan of one file neither reads its siblings nor reports them removed.
- Intake resumes from committed progress after an interruption between preparation and admission.

### Compound artifacts

A compound rendition lists its members, each member's role, relative path, order and exact extent or byte reference. A video with its subtitles, an HTML page with its assets and a multi-file document stay usable together after the source directory is unavailable. Missing members are listed individually. Paths inside an artifact keep their meaning without imposing the library's classification.

## Renditions and parts

Every original, crop, preview or transcode fixes the item revision, its exact content, media type, optional dimensions or duration, its parent rendition, and the tool, settings and version that made it.

- An original request resolves only the original.
- A **part** names one exact rendition and declares its units: a half-open time range, or an image region checked against known bounds. Changed item bytes do not move old regions or time ranges, so an old crop stays attached to the earlier bytes.
- Rendering uses existing decoders and players. A render request records its fixed input before execution and publishes a successful stored output through admission. Transformations are derivations over stored evidence, with the external output recorded when a renderer cannot be rerun deterministically.
- Unsupported decoder, corrupt input, unavailable original, cancellation and incomplete output are distinct outcomes. Partial temporary files are never usable renditions. Rendering progress and extraction failures stay visible.
- Repeated work reuses an output for the same content and settings; an output made for a different request is never silently substituted.

## Tags and properties

- **Property definitions** declare type, cardinality, allowed choices where applicable (a single-choice property allows exactly one) and the selection rule used for shown values.
- **Tags** have stable identity, labels, namespaces and aliases. Renaming a tag keeps its assignments. Real labels that begin with punctuation are ordinary labels.
- **Assignments** target an item, a fixed revision, a rendition or a validated part, and carry author, level, source evidence, stable ID, immutable revision and the revision they replace. Two assignments to two distinct regions of one rendition are two assignments.
- Rescanning, reimporting and rerunning a classifier keep the operator's corrections, because those are authored records ranked above imported and proposed values.
- Filters and queries read shown values with selectable provenance and conflict states; raw assignment search stays separately available.

## Notes and edits

Notes and annotations use the same target model and are ordinary attributed records; free text is a note, not an untyped special property. Notes are created, read, edited and withdrawn like any curation record, and a correction cites its subject in its payload.

An **external edit** starts from a fixed rendition and revision, and the edited content returns as a new rendition, revision or record naming that starting point. Two concurrent edits both keep their results and expose a conflict; timestamps do not decide a winner. An edit returned against an old original is recorded against that original.

## Collections

A **collection** is an ordered list of entries with a stable ID independent of its name.

- Every insertion creates an **entry** with its own ID, even for an item already in the collection, so A, B, A has three entries.
- Each entry fixes the revision and rendition selected at insertion and can include a part. Editing the source item does not change what an existing entry opens.
- Order is explicit and deterministic, and its representation supports repeated insertion between neighbours without silently tying positions.
- Insert, reorder, rename, remove one entry, remove explicitly selected entries and inspect withdrawn entries are all operations; removal addresses entries, not items.
- Concurrent inserts and reorders compare against the observed collection revision, succeed atomically or report a conflict, and keep every successful insertion.
- Looking a collection up by name reports ambiguity rather than picking the first match.

## Sets and fixed selections

- A **live set** is a saved query plus arguments, evaluated in the one query language and cited by its query identity ([saved queries](query.md#saved-queries)). Updating its query text creates a new revision, even when the label stays the same. Setting a property on items is a different operation and is not a set.
- A **fixed selection** (`library.selection`) is its own record, not a kind of collection: the query text and revision, its arguments, the evaluation time, and the ordered references with the revisions they resolved to, including unavailable ones with their reasons. A selection has no entry edits, suggestions or dismissals; a collection can take entries from it.
- Set membership and collection entries mean different things. A collection can hold entries taken from a set and keeps its own order and multiplicity.
- A saved query offers **suggestions** to a collection without changing it. Accepting a suggestion creates an ordinary entry; refreshing adds, removes or reorders no existing entry. A **dismissal** is scoped to the collection, the target and the saved query's revision, so it does not carry over to a new revision.
- Paging and the language's set operations apply before a bulk action, and no hidden maximum truncates a valid selection. A failed member of a bulk action is reported per member.

## Ratings

- A **scale** declares its name, meaning, numeric range or enumerated choices, and units. Redefining a scale incompatibly is refused.
- A **direct rating** is one attributed statement per rater, target and scale. Its revisions, imports and withdrawal stay inspectable; several raters coexist; fractional and zero ratings are values, and an unavailable rating is not zero.
- Non-finite numbers and values outside the scale's range are refused explicitly.
- A site's "content rating" is catalog metadata, independent of the operator's stars or preference. A provider rating or an aggregate count is never relabelled as the operator's own vote.
- Withdrawing a rating lets a lower-level shown value return where the scale's selection policy allows it.

## Comparisons and ranking

A **ranking** orders a chosen set of eligible items on a named scale from attributed comparison records.

- Every comparison records its judge, the daemon-computed author, and its target revisions where fixed. Judgments can be withdrawn and corrected; repeated legitimate judgments are all kept, while a retried action is not another vote.
- A comparison may carry an ordered list of k items (a ranking of k), which the fit expands into its implied pairs. A scale is one dimension; several scales over the same set are several rankings.
- A ranking is computed per scale and per selected judge set: the operator, a named agent, or all judges. The default is the operator's judgments only, which keeps the operator's ranking his own while agents' judgments stay usable.
- The default method is a finite regularized Bradley–Terry fit. Results carry support counts, the method and its version, and the fit status, enough to tell weak support from a settled order. A convergence allowance may stop computation with a reported incomplete result; it never turns an unfinished fit into a completed one or drops input comparisons.
- Empty and one-item sets are valid. Uncompared items stay in the chosen set and can be proposed for their first comparison. Contradictory cycles and disconnected groups are shown as they are. A stable display tie-break does not imply a known preference.
- A proposed next pair is advisory, names its selection rule, and includes eligible items not yet compared.
- Direct ratings and comparisons are separate inputs unless an explicit method combines them. An explicit method can bin a fitted ranking into discrete levels on a named scale; its output is a derived rating attributed to the method and its version, never the operator's own rating.
- Preference is not truth or objective quality, and the library does not present it as such.
- Rebuilding from the same comparisons yields the same ranking.

## Imports and catalog metadata

Imports bring in both content and curation from declared original formats. gallery-dl JSON and Hydrus text sidecars are supported entry formats, not dependencies; the library runs without either application.

- Sidecar bytes and original source identifiers are captured before they are interpreted, so unsupported fields and records stay recoverable from the stored bytes.
- Site URLs, external digests, namespaces, title, description, content classifications, uploader, scores, comments and every other supplied field are kept, or exposed explicitly as unsupported. Imported ratings, view counts and viewing time, notes, order and repeated membership are kept when the export format supplies them.
- Imports read local exports only; nothing is fetched from sites or accounts (default; open operator question).
- A verified digest establishes byte correspondence, not item identity. Metadata is routed to its intended item by the acquired copy and the catalog identity; several separately curated items with equal bytes need an explicit mapping.
- Each independent source's support for an equal value is kept.
- Reimporting the same source record is idempotent. A changed sidecar produces a new reading, and withdrawal or update follows that source's semantics.
- Failed traversal, malformed JSON or UTF-8, absent sidecar, unmatched media, digest disagreement and an ambiguous sidecar stem are distinct results. An import returns per-record outcomes and resumable progress.
- Undoing an import is archiving its material ([the lifecycle](lifecycle.md#archive)).

### Duplicate review

Similarity (perceptual-hash distance for images and video, audio fingerprints) is a derived evidence reading that offers candidate pairs; it is never identity. The operator's or an agent's `same` or `distinct` decision on a pair is an authored library record, attributed and withdrawable, and never an automatic merge. A `same` decision links the items for display and query, and their separate curation stays inspectable.

## Filename hints and extracted text

- Author, title, year and external-ID **hints** are derived from recorded copy paths and kept separate from authored metadata. Inner dashes and syntactically isolated four-digit years are kept, with no fixed upper year. A 32-hex external hint is not a verified MD5. A rename changes the hint's input evidence; it is not an operator correction.
- **Captions, OCR text and face candidates**, imported or produced, are library records targeting a rendition part, with producer, model and run metadata. They are kept and reused, not regenerated. An imported model output is acquired evidence with its supplied model and run metadata; a Sinex extraction is a derivation with its actual parents.
- Document text, imported OCR and speech transcripts cite the exact content and revision and a page, region or time selector, so search results open at the exact page.
- Reference-only items are not fetched just to extract text: extraction and preview show "unavailable without stored content", because reference-only was the operator's explicit choice for that item.
- Unavailable and unsupported extraction are visible states.

## Saved and visited pages

The library keeps the content of pages the operator visited or saved, including existing page archives, rather than relying on a new crawl to recreate history; an old page is not recreated by fetching today's URL. Acquisition (browser profiles, the extension, the crawler, archive readers) belongs to [sources](sources.md); the library owns the view of pages as items, their correspondence and their opening.

- A visit, a bookmark or save, a network fetch and an archived page revision are distinct facts. They are linked by typed correspondence with its evidence (URL normalization, redirects, exact content). Unknown correspondence between a visit and content stays unknown, and a visit with no content carries an explicit missing-content reason.
- One stored response can support several visits without inventing independent confirmations of its content. A later crawl does not establish what the operator saw earlier.
- A captured fetch keeps the requested URL, final URL and redirect chain, response metadata, body, fetch time, body completeness or error state, and members and assets when captured. Extracted text cites those bytes. Successive versions of a page are all kept.
- The target is comprehensive visited and saved page content, with visible, reasoned exclusions. A fixed domain allow-list never silently replaces that target. Excluded, unsupported and blocked URLs are reported; failed and incomplete fetches are retried; an operator-chosen limit is a resumable selection, never a permanent success mark.
- The crawler captures only what is reachable without the operator's credentials; pages that need a logged-in browser, private origins and cross-origin redirects are recorded as missing content with the reason (default; open operator question). Credentials never cross origins.

## Browsing and bulk use

- The library offers a scrolling thumbnail grid and a list view, showing previews, labels, shown tags and properties, ratings, view statistics, collection membership, revision and availability ([the reader web](web.md)).
- Search covers titles, tags, properties and extracted content; results sort by declared metadata or by a fitted ranking; exact provenance is inspectable without leaving the library.
- Paging reaches every matching item, and an explicit selection is kept across pages. A query or page refresh never silently changes a fixed selection, and a delayed thumbnail or playback result never replaces the current selection.
- Keyboard navigation, selection, bulk tagging, rating and collecting, side-by-side comparison and video playback through the exact-target handoff are part of the interface. Audio plays on explicit request, not on hover, because hover audio is noisy while scrolling.
- **Use over time.** Library views, opens and file-change events on an item's copies resolve to the item ID, so a query returns an item's tags together with its usage timeline (items with a tag joined to `library.view` and `file.changed` on their copies), and the activity timeline shows which library item was in use.

## Opening and views

Opening resolves the item revision, rendition, member or part, and the requested action before dispatch, and holds that exact target through retries and delayed responses.

- A local copy is verified against the requested bytes, and the player receives an immutable verified copy or handle; hashing a mutable path and letting the player reopen it later does not fix the target. Concurrent cache writes use independent temporary paths.
- A request for an old rendition reports that rendition's revision.
- Opening and recording a view are two ends of one fixed-target operation. Opening revision A, changing the item to B while it plays and exiting records a view of A with A's bytes.
- Accepted, loaded, usable, failed and stopped are tracked separately where the player reports them. A process launch or an `xdg-open` exit alone establishes neither a view nor attention.
- A **view** fixes the opened target, start, optional end, duration or count, author, player and the origin of those facts. Durations are finite and nonnegative; negative or non-finite values from a caller are refused. Unknown duration is not zero, and known viewing time is never synthesized from absent reports. Imported aggregate view counts and time keep their original scope.
- A remote client receives a content reference, never a daemon-local path.

## Agents

Agents have every library power the operator has: add, import, open, preview, curate, compare. No library operation is operator-only, and every one is exposed over MCP; only the daemon-computed authorship differs, and with it the precedence of shown values. CLI, MCP and web compile the same typed request, so curation rules exist once ([interfaces](interfaces.md)). Responses return stable subject and action references, the state and revision used, progress and completeness where applicable, and structured conflict, unsupported and unavailable outcomes.

## Sources, derivations and the lifecycle

- Library scans, imported catalog records, archive members and fetched pages are **sources of evidence**. Items, current availability, shown values, extracted text, suggestions and rankings are **derived readings** or reconstructible query results over that evidence. Authored notes, overrides, ratings, entries, comparisons and duplicate decisions are **request records**.
- A configured source binding, a backfill, an added file and a replay converge on the same library state; library operations never carry their own item algorithm. Whether recomputation is owed is visible.
- Library scan, import and request materials follow [the one lifecycle](lifecycle.md): live, archived, tombstoned.
- Archiving one scan's material archives the item and availability readings derived from it and removes only that scan's support; another live scan can still support the item. Authored curation stays live and shows "target lacks live support" until support returns. Restoring or replaying the scan reads it again normally.
- Tombstoning an item's original material removes its bytes only when no other material or rendition reads them, so equal-byte items are untouched.
- Withdrawing a curation act is not deletion ([withdrawals](#withdrawals)).

## Privacy

The library is private. Ruling, 2026-10-11: "a media stashbox is not supposed to be public, obviously" (cc-queued:56c7f026-fd50-4480-b484-72f1299a534c@2026-10-11T17:33:01Z). Library content stays out of public artifacts, and test fixtures are synthetic.

- Library kinds declare [field hints](privacy.md#field-hints): `path` on copy paths, `free_text` on notes and descriptions, `caption` on captions, `person` on face labels, `url` on site URLs, `media` on renditions.
- No library field is hidden by default. When the operator configures rules, the one output transform ([output parity](privacy.md#output-parity)) applies to every library read: items, shown values, search, previews, thumbnails, cached handoffs and old references.

## Performance

- Intake, import, page capture and cache materialization stream bytes, with memory governed by buffers and metadata, not by total file or body size. Large valid files and records are processed whole; paging and batching change how work is allocated without losing input.
- Unchanged content is hashed only when the configured observation and integrity policy calls for verification, and results distinguish reused metadata from freshly verified bytes.
- Blocking work (a slow hash, an HTTP request, a decoder) shares the scheduling of other sources, so it never stops the API or the writer.
- Reads meet the interface [latency budgets](interfaces.md#latency-budgets), measured end to end including shown values and metadata.
- The library benchmark ([benchmarks](testing.md#benchmarks)) runs one recorded synthetic corpus scaled in both file count and bytes: small files, an unchanged repeat, one changed file, one very large file, mixed media, independent sidecars, a large collection and many comparisons. It reports scan, hash, store, admission and derivation time separately; bytes read and written; files hashed; peak memory; cold and warm first-page, item-read and open latency; ranking time and fit status; cancellation latency; captured page bytes per day and retry backlog. Its baseline comparisons are a configured source binding against an explicit add, a first against an unchanged or restarted scan, 1× against 10× file size, and 1× against 10× item count.
- No cap on items, bodies or comparisons stands in for this.

## Records

Kind fields are listed in [the kinds reference](reference/kinds.md) once it is generated; until then this table is the record model.

| Record | Holds |
|---|---|
| item | key (host, root identity, relative path at first sight), deterministic ID |
| revision | item, content digest; a byte revision is reused when bytes return |
| copy observation | host, root, path, observation, digest (calculated or supplied), size, optional file time, format, source record, availability state |
| rendition | revision, exact content, media type, optional dimensions or duration, parent rendition, tool, settings, version; members with role, relative path, order, extent or byte reference for compound renditions |
| part | rendition, units, half-open time range or bounded image region |
| property definition | type, cardinality, allowed choices, selection rule, agent level policy |
| tag | stable ID, label, namespace, aliases |
| assignment | target (item, revision, rendition or part), property or tag, value, author, origin, source evidence, ID, revision, replaced revision |
| note | target, text, author, ID, revision, replaced revision |
| withdrawal | withdrawn record and revision, author |
| collection | stable ID, name, revision |
| collection entry | entry ID, collection, pinned revision and rendition, optional part, position |
| saved query | query text, arguments, revision, label |
| fixed selection (`library.selection`) | query text and revision, arguments, evaluation time, ordered references with resolved revisions, unavailable references with reasons |
| suggestion dismissal | collection, target, saved query revision |
| scale | name, meaning, range or choices, units |
| rating | rater, target, scale, value, origin |
| comparison | judge, scale, two or k ordered targets with fixed revisions |
| derived rating | method and version, scale, value, source ranking |
| duplicate decision | pair, `same` or `distinct`, author |
| caption, OCR text, face candidate | rendition part, content, producer, model, run |
| filename hint | copy path evidence, author, title, year, external ID |
| page fetch | requested URL, final URL, redirect chain, response metadata, body, fetch time, completeness or error, members and assets |
| view (`library.view`) | opened target, start, optional end, duration or count, author, player, origin of each fact |

## Laws

- **rebuildable**: replaying the scan, import and request materials of a mixed synthetic library into an empty database, with the input roots unavailable, yields the same item IDs, revisions, shown values, collection orders and rankings.
- **one-path**: a configured source binding, a backfill, an explicit add and a replay converge on the same library state.
- **equal-bytes-stay-separate**: two files with equal bytes added as two items stay two items with distinct IDs, and no curation moves between them when a reference-only original is re-found by hash.
- **revisions-keep-transitions**: A→B→A gives two byte revisions and three recorded transitions, in capture order.
- **move-continues-item**: unchanged bytes moved within a root, with the old path gone in the same completed scan, continue the item; several equal-byte moves with no unique match are reported and leave curation unattached.
- **unavailable-is-not-gone**: a disconnected root or an incomplete traversal never marks a copy deleted.
- **intake-matches-bytes**: a file mutated during acquisition ends as a complete acquisition whose stored bytes match its digest, or as a visible retry or failure.
- **withdrawal-keeps-history**: untag, uncollect and undo leave the withdrawn record live and inspectable, the view excludes it, and withdrawing the withdrawal restores it.
- **no-cross-material-replacement**: the library's views are the same whether or not a newer reading in another material would replace an older one, because no library kind depends on it.
- **conflict-on-same-revision**: two concurrent replacements from one revision, including empty and withdraw operations, give one success and one structured conflict naming the current revision.
- **atomic-actions**: a failed part of tag-plus-collect, or of any multi-record action, leaves none of its records.
- **undo-refuses-dependents**: undo after a later dependent record reports a conflict and changes nothing.
- **computed-authorship**: no request can set its author; by default an agent's value ranks below the operator's and above an import, two agents disagreeing is a conflict shown with both supporters, and reads list every supporter.
- **operator-correction-survives**: a rescan, reimport or classifier rerun leaves an operator's authored value shown.
- **pinned-entries**: in a collection A, B, A with the second A removed, the first A remains, and editing A does not change the rendition either entry opens.
- **old-parts-stay-attached**: after an item's bytes change, an existing crop, region or time range still refers to the earlier revision.
- **live-and-fixed-sets**: after an import changes a query's results, the live set changes, the fixed selection does not, collection order does not change until a suggestion is accepted, and a dismissal holds only under its recorded query revision.
- **no-truncated-selection**: a bulk action over a selection larger than one page acts on every member and reports each failed member.
- **lifecycle-support**: archiving a scan removes only its support, curation shows "target lacks live support", restoring brings support back, and tombstoning an original removes bytes only when no other material reads them.
- **exact-use**: opening revision A, changing the item to B and exiting records a view of A, played from A's bytes.
- **unknown-is-not-zero**: an unknown view duration, rating or dimension is never shown or summed as 0.
- **rankings**: A > B > C, cycles, disconnected groups, unbeaten items, two uncompared items and zero judgments each give a deterministic result with fit status and support; the judge set changes the result only through its judgments, and rebuilding from the same comparisons gives the same ranking.
- **derived-ratings-attributed**: a rating produced from a ranking is attributed to its method and version, never to the operator.
- **import-preserves-unknown-fields**: fields an importer does not interpret survive in the stored sidecar bytes, and reimporting the same record changes nothing.
- **privacy-parity**: with one rule configured, item reads, search, previews, thumbnails and handoffs apply it identically; with none, nothing is hidden.
- **agent-parity**: every library operation is callable over MCP with an agent token, and returns the same values and errors as the CLI and web.
