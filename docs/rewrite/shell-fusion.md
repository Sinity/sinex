# Selected Atuin and zsh shell grouping

This slice implements the selected `shell-command` rule over admitted Atuin `command.executed` and zsh `command.imported` observations. Its current contract basis is specification `b2373e9`, including exact window distance, mutual uniqueness and privacy comparability. It advances slice 1a. General fusion rules, source alignment, sessions, summaries, corpus performance and full M1 acceptance remain open.

## Source and identity

Add this source alongside the existing Atuin configuration:

```toml
sources = ["terminal.atuin-history", "terminal.zsh-history"]
read_model_connection_file = "/absolute/read-model-connection"

[zsh]
path = "/absolute/extended-history"
execution_host = "declared-execution-host"
protocol = "append_only_extended_v1"
poll_interval_ms = 30000
```

The append-only protocol is an independent operator premise. A full retained-prefix digest establishes agreement with the previously retained chain; it cannot prove that an unobserved trim and reappend never happened. Inode equality supplies no stronger correspondence. The adapter retains this limitation in source and canonical evidence. Only extended records `: start:elapsed;command` are interpreted. Zsh metafication and escaped physical newlines are decoded after original-byte retention. Elapsed zero never creates a duration or exit status. Invalid records remain retained failures and make read completeness false. A trailing partial record waits unavailable without advancing the committed prefix. Plain history, rewrite alignment and arbitrary byte encodings refuse.

Each accepted record has a registered input chain and stable natural key `(start_ns, exact command, n)`, where `n` distinguishes repeated equal records in that chain. Its public native label is `chain-record:SOURCE_OFFSET`. Exact material digest and anchor distinguish retained records; timestamps or text hashes alone are never occurrence identity. Selected record counts do not establish a world execution count. An observed prefix mismatch or truncation durably fences the input chain, including after the old bytes reappear. Recovery requires a newly declared input chain; this subset has no correspondence override or implicit rewrite adoption. Missing files remain unavailable and are never created.

Optional ingest privacy rules are unavailable with this zsh capability. Existing Atuin-only masking remains supported. Cross-source grouping requires the retained producer configuration of every relevant admitted parent to declare an untransformed comparable-field basis. Missing or historical transformed bases produce an actionable unavailable fusion status and partial answers; current empty configuration cannot relabel them. Supported retained Atuin replay under untransformed rules can restore comparable interpretations. Matching never reparses original bytes to bypass admitted-parent authority. The mandatory credential presentation floor continues to apply to API, CLI, MCP and decoded retained trace text. Encoded source bytes are not a public masking escape.

## Matching and evidence

The registered closed rule is [shell-rule.json](../../crate/sinex-store/src/shell-rule.json). It compares outer-trimmed command text and known equal execution host. Inner whitespace and case stay significant. The two-source subset can form a pair or a singleton, with at most one observation from either source. The strong strategy greedily selects globally shortest compatible edges within exactly 2,000,000,000 integer nanoseconds. Equal distances use source order and retained material digest plus numeric anchor, independently of interpretation UUIDs. The fallback considers unmatched endpoints within exactly 60,000,000,000 nanoseconds. It freezes candidate connections at strategy entry and accepts only mutual-single endpoints. Removing an accepted pair does not manufacture fallback uniqueness. Unknown times cannot supply window matches.

Every unmatched eligible observation becomes a singleton. Candidate counts and ambiguous strong associations are explicit. Even an unambiguous time/content pair is rule inference, not independent proof of physical co-occurrence. Canonical rows retain exact parent IDs, per-field suppliers, conflicting claims, source continuity and unknown capture coverage. No exact execution count is asserted. Atuin supplies command/cwd/exit/duration when present; zsh cannot invent those missing claims. Temporal selection ranks quality, then declared source order, then retained record digest/anchor. It copies the selected parent's complete time evidence, including precision, clock and bounded end. Precision, earliest start and interpretation ID do not break equal-quality ties. Earliest known parent start supplies the owner hour separately.

`query 'commands'`, ranked search and the six existing MCP read tools share these actual canonical events. Explicit raw-source queries remain separate. `show` and `trace` follow every parent to its retained record; CLI `trace --bytes` returns masked logical text and exact original-byte coordinates, including for archived interpretations. Atuin material replay and exact raw exclusion keep their existing source subset. Excluding an Atuin parent immediately archives its canonical children and fences current reads; zsh remains a distinct eligible observation. Undo performs no admission. Explicit retained replay can restore Atuin without either provider. Zsh replay and zsh exclusion are unavailable.

## Bounded maintenance

The existing writer records dirty seeds atomically with source admission, replay, removal and exclusion. Reconciliation uses PostgreSQL temporary work tables over the complete eligible population at a pinned admission/archive frontier. PostgreSQL can spill these tables; Rust scans and publishes bounded pages of 64. There is no fixture-size or hour-window cutoff. This whole-input baseline evaluates complete cross-hour competition and old owners after splits. Component-targeted efficiency remains unqualified.

Source/control work gets writer turns between bounded pages. Later arrivals retain newer dirty seeds for the next pass; they do not restart the active frontier. Persistent progress records the active basis, phase, scanned/replaced counts and last fully evaluated frontier. Compatible restart reconstructs temporary work at the retained active basis. Already committed identical outputs keep their IDs. Replacement commits precede obsolete-output archive pages, so older qualified evidence can coexist while pending. Current raw liveness always fences handoffs from an older snapshot. Successful zero-output work advances committed progress independently of event presence.

The projection owner still writes only projection tables. Search requires a serving, failure-free projection caught up to the page's committed position. Pending fusion can return usable eligible rows with `complete=false`, current fusion phase/failure and last evaluated basis; it cannot report a complete stale or empty result. Canonical reads and zsh raw reads also expose record failures as incomplete; existing Atuin raw-event reads retain their admitted-event/source-availability semantics and disclose record errors separately. Full source coverage remains unknown even after settlement. A failed comparison basis is retained as actionable unavailable, rather than busy retrying unchanged dirty work.

## Qualification

`fusion_acceptance -- --fusion-spec /absolute/spec-repository` runs the connected development route; `fusion_package_acceptance` selects the actual Nix package. Both use the independent selected journey pinned to the specification Git revision above. The driver prepares real SQLite and original metafied zsh files, binds actual interpretation/material/span facts, then normalizes actual client replies for the independent checker. Hidden fixture execution labels and expected groups are never runtime inputs.

The route covers initial/late grouping, a real committed replacement rendezvous and process kill, same-state restart, exact raw exclusion/replay/undo, provider-free restoration, and CLI/API/real rmcp identity. Separate literal controls distinguish global distance, symmetric uniqueness, exact thresholds, host partitions, same-second duplicates, cross-hour competition/removal, live admission between bounded turns, retained Unicode/continuation and durable rewrite refusal. Historical lossy rules and later empty configuration must refuse comparison until supported replay. These are synthetic controls and process-interruption evidence. They do not establish power-loss behavior, a broad privacy product, general source correspondence or production performance. Exact run receipts, source candidate and residual gates accompany delivery.
