# Media library

The library is how the operator and agents keep, describe, organize, find, judge and use anything they have seen, saved or made, inside Sinex: files, web pages, documents, images, videos, books and tracks, and catalog objects such as a video or a paper. It covers items and their locators, encounters, copies, revisions and renditions, page captures, tags and properties, notes, collections, ratings, comparisons and rankings, the link to Polylogue's sessions, and exact opening. This page owns the library's model, its features and its laws. Neighbouring pages own the mechanisms it rests on: [materials](materials.md) and [admission](admission.md) store and admit every library input; [the lifecycle](lifecycle.md) archives and tombstones them; [sources](sources.md) owns acquisition (scans, imports, page capture); [derivations](derivations.md) owns the runner that computes items and shown values; [the query surface](query.md) and [interfaces](interfaces.md) own the query language, the operation table and the CLI and MCP spelling of the library operations.

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

Decided target (2026-11-01). The current build is a file library whose curation kinds replace each other by newer readings with a `withdrawn_at` field, writes every caller's curation at the operator's level, positions collection entries as floating-point numbers and stores pairwise comparisons only. The decided design keeps this page's behaviour and changes four things: an item is anything with a native locator; encounters link every event to the item it touched; curation records share their revision rules with Polylogue's assertions through common's `authored` crate; and page capture, ranking and the Polylogue link are specified. Sections marked as decided target describe what is not built; their laws are the acceptance.

## Curation as authored input

### Requests and authorship

Every library change (add, tag, property set, collect, rate, judge, note, withdraw) is a **request**. The request is stored as a material, and its records are admitted through the one writer with that material as their origin, like every other input ([admission](admission.md)).

- The **author** of each record (the operator, a named agent, a named producer) is computed by the daemon from the caller's token and is never supplied by the caller ([callers](interfaces.md#callers)).
- A curation record cites its subject (an item, revision, rendition, part, collection or another record) as a **typed reference in its payload**. It is never a derived event of the reading it describes or corrects, so replaying the subject does not break it; the reference resolves to the subject's new reading by its key.
- Imported values keep their acquisition evidence, and proposals keep their producer; neither is relabelled as the operator's.

### Withdrawals

Untag, unset, uncollect, rating withdrawal, dismissal reversal and undo are each a **withdrawal record**: its own authored act that names the record and revision it withdraws, with its author.

- The withdrawn record stays live as history. The current value is a **view** computed from all live records by the descendant rule of common [`authored`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/authored/README.md): a revision is current when it is not withdrawn and no descendant through `revises` is current. Withdrawing a middle revision of a chain therefore leaves the newest one current.
- Withdrawing a withdrawal restores the earlier record's effect.
- **Undo** is a new withdrawal of the named action's effects. It never restores a snapshot. If a later record depends on those effects, undo reports a conflict and changes nothing.
- A withdrawal is not deletion and does not use the lifecycle. Archiving and tombstoning remain the operator's deletion of materials and readings, and they apply to library request materials like to any other material ([the lifecycle](lifecycle.md)).
- Library kinds never rely on a newer reading of the same key in another material replacing an older one. Records seen through different materials stay separate ([identity](events.md#identity)), and withdrawals carry the meaning a replacement would otherwise have to.

### Revisions and conflicts

Every curation act is one record with common's authored-record header (decided target; the current build carries `replace = newer` and `withdrawn_at` instead):

```
{ root, rev, revises?: [rev…], withdraws?: [rev…],
  kind, target: reference (item, revision, rendition, part, collection, or any Sinex or Polylogue reference),
  key?, value?, origin: authored | imported(source citation) | proposed(method, version),
  author (computed), via, at }
```

Its reference is `sinex:curation/<root>@<rev>`. A revision's payload never changes, and these revisions are domain data in the payload, not a link between readings.

| Kind | `key` / `value` | Uniqueness slot (a write into an occupied slot revises it) |
|---|---|---|
| `tag` | tag ID / optional value | target, tag, author |
| `property` | property ID / typed value, or an explicit empty | target, property, author |
| `note` | — / text, optional selector | none (Sinex's one [note](recall.md#notes) kind, with the target as `about`) |
| `rating` | scale / number or choice | target, scale, author |
| `comparison` | scale / an ordered list of k ≥ 2 targets, ties as groups, a `blind` flag | none: every comparison counts |
| `entry` | collection / order key, pinned target | none: repeats allowed |
| `collection`, `scale`, `property-definition`, `tag-definition` | name / declaration | name (labels are revisable; identity is the root) |
| `tag-relation` | `alias` or `implies` / the two tag IDs | the pair and relation |
| `duplicate` | — / `same` or `distinct` over a pair | the unordered pair, author |
| `dismissal` | collection or saved query revision / — | target, scope |

- **Stale base.** A request that changes a value carries `expects: <revision>`. The writer checks it against the current records inside the admission transaction. Of two clients replacing the same value from the same revision, the first succeeds and the second receives a structured conflict naming the current revision and its author. No policy picks a winner by timestamp; two current heads in one slot are a conflict.
- **Atomic requests.** One request carries several writes, numbered `<request>.<n>`, committed together or not at all (tag plus collect, a bulk act over an explicit selection).
- **Computed authorship.** The author comes from the caller's token; no surface accepts one.
- Uniqueness is enforced in the admission transaction, never by a read before the write.

### Shown values

The value the library shows for a property or tag is computed from every live assignment by a precedence with two axes, both computed by the daemon:

- **origin**: authored, imported, proposed;
- **author**: the operator, an agent, a named producer.

The default order is the operator's authored value (an explicit empty included), then an agent's authored value, then imported values, then proposals. Each property declares its type, cardinality and selection policy, and a per-property policy can raise or lower agents' level. Agents keep every curation power; only precedence differs, so an agent's tag never counts as the operator's.

- Two different values at the winning level are a **conflict**; no value at any level is **unknown**. Unknown, withheld, conflicting, explicitly empty and a real empty string are five distinguishable states.
- An explicit empty stops fallback to lower levels; withdrawing it permits fallback again.
- A tag applies while at least one live, non-withdrawn assignment supports it: withdrawing the operator's assignment leaves an agent's. Reads show every supporting assignment and its author, even when the values agree.
- Rejecting an imported or proposed value is its own explicit record, not a reserved leading character in a tag label.
- Shown-value precedence is the library's own: Polylogue's assertions have no imported level to rank, while booru imports, catalog sidecars and reading-service tags make imported values the library's common case.
- **The shown-value projection** (`library_shown`: item, field, value, level, state, supporters) is maintained by the writer in the admission transaction for the targets a request touches, through the common currency function, so reads never recompute currency over the archive.
- Proposals (classifier or model output) are the lowest level and never overwrite an authored value. Model execution follows [the model-calling contract](derivations.md#model-calling); imported model outputs are captured regardless.

## Items, locators and copies

An **item** is the continuing thing the operator curates. A **revision** fixes an item's content. A **rendition** supplies original or transformed bytes of a revision. A **copy** is an observed location of bytes: each keeps host, root, path, observation, content digest, size, optional file time, format and source record.

- Equal bytes share storage, while separately curated items stay separate.
- A digest supplied by another catalog is distinguished from a digest Sinex calculated.
- Unknown times and dimensions stay unknown.

### Locators

Decided target (2026-11-01). An item is anything with a native **locator**, the native address of a thing at first sight, canonicalized by a declared rule and kept as the key forever:

| Locator | Canonical form | Where the address comes from |
|---|---|---|
| `file:` | `file:<host>/<root-id>/<relative path at first sight>` (the item key below) | library scans, `library add`, file drops |
| `url:` | scheme and host lowercased, default port dropped, path normalized (RFC 3986 §6), query parameters sorted, the fragment removed and kept as a part selector, declared tracking parameters removed | visits, bookmarks, reading services, page captures |
| catalog | `youtube:<video-id>`, `spotify:track:<id>`, `doi:<doi>`, `isbn:<isbn-13>`, `arxiv:<id>`, `booru:<site>/<post-id>`, `github:<owner>/<repo>`, `wikipedia:<lang>/<title>` | declared URL patterns, provider exports, reference managers, sidecars |
| `part:` | `part:<source>/<native message id>/<part path>` | mail and chat attachments; files uploaded to or generated in AI sessions, through Polylogue |

- A URL that matches a catalog pattern canonicalizes to the catalog locator: every URL form of one video and the export record of watching it name one item. This is exact normalization of one identifier, not fusion: no time window and no similarity.
- The canonicalizers are versioned data and small pure functions in common [`references`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/references/README.md), so Polylogue keys its URLs byte-identically. A canonicalizer version change is a recomputation of the item derivation by replay, never an edit in place.
- The original address is kept on every encounter and copy; the canonical form decides identity only.
- **Items never merge.** Two locators that no declared rule connects (a page mirrored on two domains; a file downloaded from a URL) stay two items, related by a [correspondence](#correspondences). An authored `same` decision links them for display and query without moving any curation.
- Every locator an encounter rule produces makes an item, so every visited URL is one. The library's default view shows *kept* items (files, saved, curated, made), and a toggle shows everything; this is a default filter of the `items` unit, not a scope limit.
- A web item's revisions are its captures' main-body digests in capture order; an old highlight stays on the revision it was made on and is re-anchored by its quote where the text still matches. A catalog item may have no bytes at all; its availability is `reference-only`, a normal state.

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

### Encounters

Decided target (2026-11-01). An **encounter** is a derived reading linking an event to the item it touched: `{item, relation, actor, at, until?, attention?, via (the event's citation), locator as seen}`. Encounter rules are declarations over event types, versioned data over a closed set of extraction forms (take field F as a locator of kind K; take the span from fields G and H; take attention from field A when present).

| Relation | Typical events | Actor |
|---|---|---|
| `seen` | browser visits, pages displayed (extension), image viewer focus, `library.view` | operator |
| `played` | tracks played, videos watched, media sessions, views of audio or video | operator |
| `read` | reading sessions from e-readers and the reader | operator |
| `saved` | bookmarks and reading-service saves, the extension's save | operator |
| `downloaded` | browser downloads, file drops | operator |
| `made` | screenshots, camera photos, recordings, files the operator wrote, images generated in AI sessions | operator, or the session's agent or model |
| `fetched` | agent web fetches (through Polylogue), the crawler | agent, or `producer:crawler` |
| `mentioned` | a URL or file in a message, mail or chat | its author |

- **Attention is honest.** `attention` is a known duration only when the source measured it (active-tab time from the extension, the player's playback position, an e-reader's page time). Otherwise it is unknown, and sums over unknowns are marked `lower bound`. An open tab is not attention.
- Encounters are readings, so replay recomputes them and archiving a visit's material archives its encounters. Curation never depends on an encounter.
- **Item statistics** (`first_seen`, `last_seen`, `seen_count`, known attention and the count of unknown, `saved_in`, `made_by`) are views over encounters, kept in the item projection.
- Imported aggregate view counts are encounters with `count: N` and their original scope, never expanded into invented individual events.
- "Pages I read while that build failed" and "the track playing when I wrote this commit" are each one query, because encounters are events with a time and an item.

### Correspondences

A typed, derived relation between two items with its evidence: `same-bytes` (equal content digest), `downloaded-from` (a download record names the URL and the resulting path and digest), `rendition-of`, `canonical` (a page's `rel=canonical`), `looks-like` (perceptual-hash distance). Correspondences show on the item page and in queries (`items where corresponds(same-bytes, REF)`). They never move curation; copying curation from one item to another is an explicit act whose records the caller authors.

### Compound artifacts

A compound rendition lists its members, each member's role, relative path, order and exact extent or byte reference. A video with its subtitles, an HTML page with its assets and a multi-file document stay usable together after the source directory is unavailable. Missing members are listed individually. Paths inside an artifact keep their meaning without imposing the library's classification.

## Renditions and parts

Every original, crop, preview or transcode fixes the item revision, its exact content, media type, optional dimensions or duration, its parent rendition, and the tool, settings and version that made it.

- An original request resolves only the original.
- A **part** names one exact rendition and declares its units: a half-open time range, or an image region checked against known bounds. Selectors use W3C Media Fragments (`#t=start,end`, `#xywh=`) and W3C Web Annotation text selectors (a quote with its position), parsed by common [`references`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/references/README.md); an out-of-range selector is invalid, never clamped. `item:<id>@<rev>#t=95,140` names a clip that tags, notes, entries and Polylogue assertions can target. Changed item bytes do not move old regions or time ranges, so an old crop stays attached to the earlier bytes.
- Rendering uses existing decoders and players. A render request records its fixed input before execution and publishes a successful stored output through admission. Transformations are derivations over stored evidence, with the external output recorded when a renderer cannot be rerun deterministically.
- Unsupported decoder, corrupt input, unavailable original, cancellation and incomplete output are distinct outcomes. Partial temporary files are never usable renditions. Rendering progress and extraction failures stay visible.
- Repeated work reuses an output for the same content and settings; an output made for a different request is never silently substituted.

## Tags and properties

- **Property definitions** declare type, cardinality, allowed choices where applicable (a single-choice property allows exactly one) and the selection rule used for shown values.
- **Tags** have stable identity, labels and namespaces. Renaming a tag is a label revision that keeps its assignments. Any non-empty label is an ordinary label, including one that begins with or consists of punctuation.
- **Tag relations** (decided target): `alias` makes two tag IDs one shown tag, so merging two tags is an alias; `implies` adds the implied tag to the shown set, marked `implied by <tag>`, at the implying assignment's level. A cycle is refused at write. `tag:x` matches direct and implied assignments; `tag_direct:x` matches direct ones only.
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
- Order is explicit and deterministic. Entry positions are common `authored` order keys: base-62 fractions with a key between any two, ties broken by the entry's root, so repeated insertion between neighbours never ties or runs out of precision (decided target; the current build's floating-point positions tie after about fifty halvings). A reorder is a revision of one entry's position.
- Insert, reorder, rename, remove one entry, remove explicitly selected entries and inspect withdrawn entries are all operations; removal addresses entries, not items.
- Concurrent inserts and reorders compare against the observed collection revision, succeed atomically or report a conflict, and keep every successful insertion.
- Looking a collection up by name reports ambiguity rather than picking the first match.

## Saved queries and kept results

- A **live set** or reading list is a [saved query](query.md#saved-queries) plus arguments, evaluated in the one query language and cited by its query identity. Updating its query text creates a new revision, even when the label stays. Setting a property on items is `property set`, a different operation. The shipped saved queries include `query:reading` (over the `status` property, ordered by the `reading` collection when one exists), `query:uncurated` (new items nobody has curated) and `query:resurface` (saved or highly rated items not encountered for a year, one or two a day in Today).
- A **fixed selection** is a [kept result](query.md#kept-results) (`QUERY keep` → `result:<id>`): the query, its read context and the ordered references it resolved to, with unavailable ones and their reasons. Decided target: this replaces the separate `library.selection` record, which was the same object. A collection can take entries from it (`collect --from result:<id>`).
- Set membership and collection entries mean different things. A collection keeps its own order and multiplicity.
- A saved query offers **suggestions** to a collection without changing it. Accepting a suggestion creates an ordinary entry; refreshing adds, removes or reorders no existing entry. A **dismissal** is scoped to the collection, the target and the saved query's revision, so it does not carry over to a new revision.
- Paging and the language's set operations apply before a bulk action, and no hidden maximum truncates a valid selection. A failed member of a bulk action is reported per member. A single-valued property set over more than one target needs `--each`, so one pass cannot silently overwrite many operator values; many-valued tags and notes need no flag.

## Ratings

- A **scale** declares its name, meaning, numeric range or enumerated choices, and units. Redefining a scale incompatibly is refused.
- A **direct rating** is one attributed statement per rater, target and scale. Its revisions, imports and withdrawal stay inspectable; several raters coexist; fractional and zero ratings are values, and an unavailable rating is not zero.
- Non-finite numbers and values outside the scale's range are refused explicitly.
- A site's "content rating" is catalog metadata, independent of the operator's stars or preference. A provider rating or an aggregate count is never relabelled as the operator's own vote.
- Withdrawing a rating lets a lower-level shown value return where the scale's selection policy allows it.

## Comparisons and ranking

A **scale** declares its name and the question a judge answers on it (shown on every comparison card and stored with each comparison's scale revision), and a numeric range or choices. A **ranking** orders a chosen set of eligible items on one scale from attributed comparison records. A ranking is a computed answer, never stored as a record.

- A **comparison** is an ordered list of k ≥ 2 targets with optional tie groups, on one scale, by one computed judge, optionally `blind` (no existing ratings, tags or authors shown). A pair is the case k = 2. `judge` is the verb that writes it; `compare` is only the read. Comparisons are withdrawn and corrected like any curation record; repeated legitimate comparisons are all kept, while a retried request is not another vote.
- **The fit** is common [`ranking`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/ranking/README.md)'s Plackett–Luce model (decided target, replacing the current pairwise Bradley–Terry fit). It reduces exactly to Bradley–Terry for pairs and uses a k-ordering as one observation, without the bias of expanding it into independent pairs. A weak symmetric prior keeps the fit finite for unbeaten items and disconnected groups. Each item reports its score, an interval, its rank range, its support (comparisons involving it) and its component; the answer reports the fit status and the judge set.
- **Judge sets** are a query parameter: the operator (default), a named author, a lens, or all judges. The operator's ranking stays his own; agents' comparisons are usable through their own judge set and never mixed in unless asked.
- **Next set** (`judge next`) proposes the pair or k-set with the largest expected reduction in ranking uncertainty within the chosen set, falling back to an uncompared item against its nearest neighbour. Every proposal names its rule, and `rule: random` and `rule: uncompared-first` are available. Uncompared items stay in the chosen set and are listed as unranked, never hidden.
- Empty and one-item sets are valid. Contradictory cycles and disconnected groups are shown as they are. A stable display tie-break does not imply a known preference.
- Direct ratings and comparisons are separate inputs unless a named method combines them. A method such as quantile levels can bin a fitted ranking into discrete levels on a derived scale; its output is a proposed rating attributed to the method and version, never the operator's own rating.
- Preference is not truth or objective quality, and the library does not present it as such. Rebuilding from the same comparisons yields the same ranking.

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

Similarity (64-bit dHash and pHash for images and sampled video keyframes, audio fingerprints) is a derived evidence reading that offers candidate pairs; it is never identity. It is deterministic and needs no model, and the ranking atom `looks_like:REF` sorts by it. Candidate pairs appear in the same judging queue as comparisons, with `same`, `distinct` and `skip`. The operator's or an agent's `same` or `distinct` decision on a pair is an authored library record, attributed and withdrawable, and never an automatic merge. A `same` decision links the items for display and query, and their separate curation stays inspectable.

## Filename hints and extracted text

- Author, title, year and external-ID **hints** are derived from recorded copy paths and kept separate from authored metadata. Inner dashes and syntactically isolated four-digit years are kept, with no fixed upper year. A 32-hex external hint is not a verified MD5. A rename changes the hint's input evidence; it is not an operator correction.
- **Captions, OCR text and face candidates**, imported or produced, are library records targeting a rendition part, with producer, model and run metadata. They are kept and reused, not regenerated. An imported model output is acquired evidence with its supplied model and run metadata; a Sinex extraction is a derivation with its actual parents.
- Document text, imported OCR and speech transcripts cite the exact content and revision and a page, region or time selector, so search results open at the exact page.
- Reference-only items are not fetched just to extract text: extraction and preview show "unavailable without stored content", because reference-only was the operator's explicit choice for that item.
- Unavailable and unsupported extraction are visible states.

## Saved and visited pages

The library keeps the content of pages the operator visited or saved, including existing page archives, rather than relying on a new crawl to recreate history; an old page is not recreated by fetching today's URL. Acquisition (browser profiles, the extension, the crawler, archive readers) belongs to [sources](sources.md); the library owns the view of pages as items, their correspondence to visits, and their reading.

- A visit, a bookmark or save, a network fetch and an archived page revision are distinct facts, linked by typed correspondence with its evidence.
- A **capture** is one stored copy of a web item: an extension snapshot (DOM serialization with resources), a response the extension observed, a crawler fetch, an imported page archive member (HTML, MHTML, WARC, WACZ, PDF, SingleFile), or an archive-index response. Each keeps the requested and final URL, the redirect chain, response metadata, the exact body, fetch time and completeness, and lists its members. Successive versions of a page are all kept.
- **Capture scope** (decided target): the extension captures every page the operator displays, deduplicated by digest at every level, so an unchanged page re-displayed stores no new body and shared assets are stored once across all pages. Snapshots are stored as WARC-shaped records inside materials. Serialization runs when the browser is idle and is cancelled by navigation, so it never blocks the page. Saving a page (a key in the browser, or `library save URL`) makes it an item with status `unread`, adds it to the `reading` collection, and captures exactly what was on screen.
- The crawler covers visited and saved URLs not already captured during a visit, reachable without the operator's credentials; pages that need a logged-in browser, private origins and cross-origin redirects are recorded as missing content with the reason (default; open operator question). Credentials never cross origins. Excluded, unsupported and blocked URLs are reported; failed and incomplete fetches are retried.

### What was seen

Decided target (2026-11-01). For each visit or save of a web item, the library derives one **grade** for the nearest capture:

| Grade | Meaning |
|---|---|
| `during` | captured by the browser within this visit (same tab and document, or the extension's snapshot at save time) |
| `same-body` | a capture at another time whose main-body digest equals a capture `during` another visit of the same item |
| `after (Δ)`, `before (Δ)` | the nearest capture outside the visit, with the time difference; content may differ |
| `archive (Δ)` | a third-party archive copy (Memento-style), with the difference from the visit |
| `none` | no capture; the visit's title is all there is |

The grade is shown wherever content stands in for a visit: search hits, the reader header, recall and MCP answers. A later crawl never establishes what was seen earlier; it can only be `after (Δ)`.

- **Extraction.** Readable text (main content extracted, the full DOM kept), PDF text, EPUB chapters, subtitles and imported OCR are derivations over stored bytes; each block cites its rendition and a selector, so a search hit opens at its place.
- **The reader** shows a capture in a sandboxed frame with scripts disabled and all network access blocked, or the extracted text; imported highlights are re-anchored on the text; `[` and `]` step through captures of one item with a text diff between revisions.
- **Link rot** (decided target): saved web items are rechecked on a slow schedule; when one starts answering 404 or a different body, a passive notice says the item is gone at its URL and that its capture is intact.

## Browsing and bulk use

- The library offers a scrolling thumbnail grid and a list view, showing previews, labels, shown tags and properties, ratings, view statistics, collection membership, revision and availability ([the reader web](web.md)).
- Search covers titles, tags, properties and extracted content; results sort by declared metadata or by a fitted ranking; exact provenance is inspectable without leaving the library.
- Paging reaches every matching item, and an explicit selection is kept across pages. A query or page refresh never silently changes a fixed selection, and a delayed thumbnail or playback result never replaces the current selection.
- Keyboard navigation, selection, bulk tagging, rating and collecting, side-by-side judging and video playback through the exact-target handoff are part of the interface. Audio plays on explicit request, not on hover, because hover audio is noisy while scrolling.
- **Use over time.** Library views, opens and file-change events on an item's copies resolve to the item ID, so a query returns an item's tags together with its usage timeline (items with a tag joined to `library.view` and `file.changed` on their copies), and the activity timeline shows which library item was in use.

## Opening and views

Opening resolves the item revision, rendition, member or part, and the requested action before dispatch, and holds that exact target through retries and delayed responses.

- A local copy is verified against the requested bytes, and the player receives an immutable verified copy or handle; hashing a mutable path and letting the player reopen it later does not fix the target. Concurrent cache writes use independent temporary paths.
- A request for an old rendition reports that rendition's revision.
- Opening and recording a view are two ends of one fixed-target operation. Opening revision A, changing the item to B while it plays and exiting records a view of A with A's bytes.
- Accepted, loaded, usable, failed and stopped are tracked separately where the player reports them. A process launch or an `xdg-open` exit alone establishes neither a view nor attention.
- A **view** fixes the opened target, start, optional end, duration or count, author, player and the origin of those facts. Durations are finite and nonnegative; negative or non-finite values from a caller are refused. Unknown duration is not zero, and known viewing time is never synthesized from absent reports. Imported aggregate view counts and time keep their original scope.
- A remote client receives a content reference, never a daemon-local path.
- **Resume anywhere** (decided target). Opening a video or audio item in mpv goes through its JSON IPC, which reports load, position and end, so a view records what was watched and where it stopped, and the next open offers to resume there. A document resumes at the reader's scroll anchor and an e-book at its page. The position is stored as a part selector on the view, so it survives renames and moves. If the player crashes, the view records what it knew and marks the rest unknown.

## The link to Polylogue

Decided target (2026-11-01). Agents fetch pages, sessions mention URLs and files, and models generate images; one mechanism shows that reading beside the operator's.

- **Polylogue exposes a unit `locators`**: one row per URL, path or attachment in a message or tool call, with `session`, `message`, `at`, `kind` (`mentioned`, `fetched`, `search-result`, `attachment`, `generated`, `file-written`, `file-read`), the canonical `locator` from the same common canonicalizer, `digest` when the bytes are known, and `by` (`human`, `model`, `tool`).
- **The library reads it by federation, never by copying** ([mixed queries](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md)). The item page's *In AI sessions* panel is `pl.locators where locator = <item locator>`, and `encountered_by:` and `mentioned_in:` accept `pl.` relations. With Polylogue unreachable, that panel shows unavailable with the reason and every other panel is unchanged.
- **The reverse direction.** Polylogue's reader decorates a URL or path with its library state (saved, captured, rated, tags) through one batched federated lookup per rendered page of messages; with Sinex unreachable the decoration is omitted with a footnote, never guessed.
- Generated images and uploaded attachments become `part:` items whose bytes stay in Polylogue as a reference-only rendition opened through Polylogue's content handoff.

## Agents

Agents have every library power the operator has: add, import, open, preview, curate, judge. No library operation is operator-only, and every one is exposed over MCP; only the daemon-computed authorship differs, and with it the precedence of shown values. CLI, MCP and web compile the same typed request, so curation rules exist once ([interfaces](interfaces.md)). Responses return stable subject and action references, the state and revision used, progress and completeness where applicable, and structured conflict, unsupported and unavailable outcomes.

## Sources, derivations and the lifecycle

- Library scans, imported catalog records, archive members and fetched pages are **sources of evidence**. Items, current availability, shown values, extracted text, suggestions and rankings are **derived readings** or reconstructible query results over that evidence. Authored notes, overrides, ratings, entries, comparisons and duplicate decisions are **request records**.
- A configured source binding, a backfill, an added file and a replay converge on the same library state; library operations never carry their own item algorithm. Whether recomputation is owed is visible.
- Library scan, import and request materials follow [the one lifecycle](lifecycle.md): live, archived, tombstoned.
- Archiving one scan's material archives the item and availability readings derived from it and removes only that scan's support; another live scan can still support the item. Authored curation stays live and shows "target lacks live support" until support returns. Restoring or replaying the scan reads it again normally.
- Tombstoning an item's original material removes its bytes only when no other material or rendition reads them, so equal-byte items are untouched.
- Withdrawing a curation act is not deletion ([withdrawals](#withdrawals)).

## Surfaces

Decided target (2026-11-01). Verbs follow a query or a reference, as everywhere in `sinexctl`; `library` keeps only the intake verbs, so curation is never an island command.

```
sinexctl library add PATH… [--reference] [--root NAME]
sinexctl library import PATH… [--format gallery-dl|hydrus|raindrop|pocket|zotero|calibre|netscape|warc|wacz|singlefile]
sinexctl library save URL… [--collection C]
sinexctl QUERY|REF tag add|remove TAG… [--part SEL] [--expects REV]
sinexctl QUERY|REF property set PROP VALUE|--empty [--each] [--expects REV]
sinexctl QUERY|REF note "TEXT" [--part SEL]
sinexctl QUERY|REF rate SCALE VALUE
sinexctl QUERY|REF collect COLLECTION [--after ENTRY|--before ENTRY|--at end] [--from result:ID]
sinexctl ENTRY… uncollect | move --after ENTRY
sinexctl QUERY judge SCALE [--k N] [--judges SET] [--blind] [--rule information|random|uncompared-first]
sinexctl REF,REF… judge SCALE --order A,B,C | --tie
sinexctl QUERY rank SCALE [--judges SET] [--levels N]        # QUERY | sort by rank(SCALE)
sinexctl QUERY duplicates [--distance N]
sinexctl REF open [--player mpv|xdg|web] [--resume|--at SEL]
sinexctl REF read [--to DIR]
sinexctl CURATION withdraw | undo OP
sinexctl tag define NAME [--namespace NS] | tag alias A B | tag implies A B
sinexctl scale define NAME --question "…" (--range 0..5 | --choices a,b,c)
sinexctl collection create NAME [--suggest-from QUERY]
```

Every write prints the request ID, the records written with their revisions, and conflicts with the current revision and its author; exit 3 is refused, 5 is partial in a bulk act.

Over MCP, intake (`add`, `import`, `save`), `open` and `viewed` are rows of `sinex_act`; every curation act is one atomic request through `sinex_write` (`writes: [{act, target or query, …, expects?}]`); `judge next` and `rank` are rows of `sinex_query`; items and collections are read through `sinex_query` and `sinex_read`. The deployed `sinex_library-*` tools fold into these rows.

| Unit | Rows |
|---|---|
| `items` | items, default filter `kept` (`everything` lifts it): `kind` (`file`, `web`, `catalog`, `part`), `locator`, `domain`, `media_type`, `title`, dimensions, `duration`, `pages`, `size`, `availability`, encounter statistics, `status`, `in:collection:<ref>` |
| `copies`, `encounters`, `captures` | copies, encounters and captures with their grades |
| `curation`, `collections`, `entries`, `comparisons`, `scales`, `tags` | curation records and their views |

Curation predicates: `tag:x`, `tag_direct:x`, `tag_by:(operator|agent|import)`, `prop(name) = v`, `rating(scale) >= 4`, `rank(scale) <= 10` with `judges:`, `conflict:any`, `value_state(prop) = unknown|empty|conflict|withheld`. Relations: `encountered_by:ACTOR`, `encountered during(Q)`, `corresponds(rel, REF)`, `captured: during|after|archive|none`. Bare words search titles, tags, notes and extracted content, and hits carry their rendition and selector, so `@1 open` lands at the hit. References: `sinex:item/<id>[@<rev>][#selector]`, `sinex:collection/<id>[@<rev>]`, `sinex:scale/<name>`, `sinex:content/<blake3>`, `sinex:curation/<root>@<rev>`; a pasted URL resolves to its item through the canonicalizer.

Web views come from common `web-shell` and print the CLI and MCP call that reproduces them: the **grid** over any `items` query (`/q?q=…&layout=grid`, thumbnails loaded only for visible tiles, video tiles scrubbing sprite sheets, audio only on explicit request, facets including the origin of shown values); the **item** page (preview, values with level and supporters, the encounter strip, captures with grades, copies and availability, correspondences, collections, *In AI sessions*, notes, history); the **reader** (`/item/<id>/read`); **judge** (`/judge?q=…&scale=…`, two-up or up to six-up with the scale's question on top, `←`/`→` or `1`–`6`, `=` tie, `s` skip, `u` undo, `b` blind, the ranking panel updating after each comparison); **collection** and **duplicates**.

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

Budgets (p95, warm, decided target): grid first page of 60 tiles over 1 M items and 10 M curation records ≤ 100 ms; facet counts for one field ≤ 20 ms; a warm thumbnail tile ≤ 5 ms and immutable by digest; an item page with 10,000 encounters ≤ 100 ms; a curation write visible ≤ 30 ms of writer time; a bulk tag of 10,000 items ≤ 2 s; a warm-started ranking fit over 10,000 items and 200,000 comparisons ≤ 150 ms; `judge next` over 10,000 items ≤ 50 ms; content search over 50 GB of extracted text ≤ 250 ms; open to player launch ≤ 150 ms. They rest on the item projection (one row per item with its encounter statistics, maintained from the commit log), the shown-value projection, roaring value bitmaps per shown tag and closed-set value (facets are bitmap intersections), a rank cache warm-started from the previous scores, and perceptual hashes in a radius index.

## Records

Kind fields will be listed in [the planned kinds reference](reference/README.md#planned-pages); until then this table is the record model.

| Record | Holds |
|---|---|
| item | first locator (for files: host, root identity, relative path at first sight), deterministic ID |
| encounter | item, relation, actor, at, optional end, attention, the event's citation, locator as seen, rule version |
| capture | requested and final URL, redirect chain, response metadata, body, fetch time, completeness, members; grade per visit |
| revision | item, content digest; a byte revision is reused when bytes return |
| copy observation | host, root, path, observation, digest (calculated or supplied), size, optional file time, format, source record, availability state |
| rendition | revision, exact content, media type, optional dimensions or duration, parent rendition, tool, settings, version; members with role, relative path, order, extent or byte reference for compound renditions |
| part | rendition, units, half-open time range or bounded image region |
| property definition | type, cardinality, allowed choices, selection rule, agent level policy |
| tag | stable ID, label, namespace, aliases |
| assignment | target (item, revision, rendition or part), property or tag, value, author, origin, source evidence, root, revision, revised and withdrawn revisions |
| note | Sinex's one note kind, with the target as `about` |
| withdrawal | withdrawn revisions or roots, author |
| tag relation | `alias` or `implies`, the two tags |
| collection | stable ID, name, revision |
| collection entry | entry ID, collection, pinned revision and rendition, optional part, position |
| saved query | query text, arguments, revision, label |
| fixed selection | a kept result (`result:<id>`) |
| suggestion dismissal | collection, target, saved query revision |
| scale | name, meaning, range or choices, units |
| rating | rater, target, scale, value, origin |
| comparison | judge, scale, k ordered targets with tie groups and fixed revisions, `blind` |
| derived rating | method and version, scale, value, source ranking; origin `proposed` |
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
- **rankings**: A > B > C, cycles, disconnected groups, unbeaten items, two uncompared items and zero comparisons each give a deterministic result with fit status and support; the judge set changes the result only through its comparisons, and rebuilding from the same comparisons gives the same ranking.
- **derived-ratings-attributed**: a rating produced from a ranking is attributed to its method and version, never to the operator.
- **import-preserves-unknown-fields**: fields an importer does not interpret survive in the stored sidecar bytes, and reimporting the same record changes nothing.
- **privacy-parity**: with one rule configured, item reads, search, previews, thumbnails and handoffs apply it identically; with none, nothing is hidden.
- **agent-parity**: every library operation is callable over MCP with an agent token, and returns the same values and errors as the CLI and web.

Decided target (2026-11-01):

- **lib.projection.equals-function**: the shown-value projection equals recomputation from all live records after any sequence of writes, withdrawals, archives and replays.
- **lib.tag.implies-acyclic**: an `implies` write that would close a cycle is refused, and shown tags under implication equal the transitive closure at the implying level.
- **lib.tag.any-label**: any non-empty label, including one that starts with or consists of punctuation, is an ordinary tag.
- **lib.bulk.single-valued-needs-each**: a property set over a query of more than one row without `--each` is refused with the `--each` form.
- **lib.order.dense**: 10,000 successive insertions between the same two neighbours give distinct, ordered positions.
- **lib.locator.shared-module**: Sinex and Polylogue produce byte-identical canonical locators for a generated corpus of URLs, paths and catalog identifiers.
- **lib.locator.catalog-folds**: every declared URL form of a catalog object, and its export record, yields one item.
- **lib.locator.no-guess**: two URLs differing only in a parameter not on the declared tracking list are two items.
- **lib.locator.version-replays**: changing a canonicalizer's version and replaying yields the new item set, and curation on items whose locator canonicalizes the same keeps its target.
- **lib.encounter.from-rules-only**: every encounter cites one event and one declared rule version; deleting the rule and replaying removes exactly its encounters.
- **lib.attention.unknown-not-zero**: an item with one measured and one unmeasured encounter shows the measured sum marked `lower bound`.
- **lib.capture.later-is-not-during**: a crawler fetch after a visit never grades that visit `during`, even with an equal body, unless another visit's `during` capture has that body.
- **lib.capture.dedup**: re-displaying an unchanged page stores no new body bytes, and an asset shared by 1,000 pages is stored once.
- **lib.reader.inert**: the reader never executes captured script and makes no network request.
- **lib.rank.pl-reduces-to-bt**: on pair-only comparisons the Plackett–Luce fit equals the Bradley–Terry fit within tolerance.
- **lib.rank.k-not-expanded**: one 4-ordering and its six implied pairs submitted as pairs give different supports, and the answer reports the comparisons actually submitted.
- **lib.rank.judge-isolation**: adding an agent's comparisons changes the operator's ranking not at all.
- **lib.rank.deterministic**: the same comparisons in any arrival order give the same scores, intervals and order.
- **lib.rank.next-includes-unseen**: over a set with uncompared items, `judge next` proposes one within the first ⌈n/k⌉ proposals under every rule.
- **lib.pl.federated-only**: the library holds no copy of Polylogue rows; with Polylogue down, the *In AI sessions* panel shows unavailable with the reason and every other panel is unchanged.
- **lib.pl.split-equals-oracle**: mixed library queries equal the federation oracle of the shared query suite.

Stored with the scope `sx.`. The library also runs the shared law `authored.currency.middle-withdrawal` against its current-record view.
