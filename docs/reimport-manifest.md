# Fresh-rebuild reimport manifest

Status: execution manifest for the operator-approved fresh rebuild. This file is a route and scope ledger, not authorization to wipe production.

The inventory was reconciled against `/realm/data` on 2026-08-12. The measured sizes are coarse planning evidence from `du -sxh`; they are not a promise that the lake is immutable. Raw files remain authoritative and must not be replaced by the derived products listed below.

## Route vocabulary

| Route | Meaning during Phase C | Operator surface |
| --- | --- | --- |
| adapter | Existing Sinex source contract and historical parser read the source directly | Source-specific historical scan through the runtime binding |
| staged | Provider/archive file is staged as source material, then parsed by a registered staged binding | `sinexctl sources stage <path> --format json` followed by the binding's scan/parse operation |
| append-log | Append-only capture is replayed as bounded historical material, preserving source offsets | Source-specific scan; one material per bounded file/day batch, not one material per event |
| W1 | No production historical route is currently proved; keep the raw authority and file a route bead before import | Do not silently discard; quarantine from the wipe execution set until the route is real |
| out | Deliberately not an activity-plane Sinex input, with the reason recorded below | Preserve outside the database and document the external authority |

## In-scope activity and evidence sources

| Lake family | Canonical relative roots | Approx. size | Sinex route and source IDs | Historical plan | `ts_orig` quality | Privacy / blocker notes |
| --- | --- | ---: | --- | --- | --- | --- |
| ActivityWatch | `captures/activitywatch/`, `exports/activitywatch/` | 2.9G | adapter: `desktop.activitywatch`; archive-to-SQLite binding must be explicit | Bind the archive or canonical SQLite export, then run the full historical horizon. Do not assume the live DB path points at the lake copy | Event timestamps are source-native | Private activity; `sinex-bdh9` is closed, but archive binding still needs manifest evidence |
| Browser history and bookmarks | `captures/webhistory/`, `exports/google/`, `exports/raindrop/` | 61G combined | adapter: `browser.history`; adapter/API export: `raindrop-bookmarks`; staged Google browser slices | Bind each archived SQLite/JSONL source and scan to the archive end. Verify static imports after deployment (`sinex-2bk`) | Browser/provider timestamps, with export quality recorded per material | Private browsing; `sinex-rkv.16` remains open for Takeout Chrome |
| Git and repository history | `exports/repos/`, repository histories referenced by the manifest | 20G | adapter: `git-commit-history` | Scan each configured repository history once; retain commit hash and repository path as occurrence identity | Commit timestamps are source-native, author/committer quality differs | Code and delivery evidence; verify exact-id parity after import |
| Terminal history and recordings | `captures/shell/`, `captures/asciinema/`, `captures/kitty-scrollback/`, terminal capture roots | 267G asciinema plus small append logs | adapters: `terminal.atuin-history`, `terminal.bash-history`, `terminal.zsh-history`, `terminal.text-history`, `terminal.asciinema`; Kitty history has no content parser | Import append logs by bounded file/day batch. Asciinema currently imports session metadata but skips `.cast` frames. Kitty scrollback needs a staged parser before inclusion | Command/session timestamps vary; capture time is a fallback and must be marked | Highly private; apply terminal redaction and `sinex-h3gy` coverage checks |
| Clipboard and input capture | `captures/clipboard/`, `captures/keylog/`, `exports/clipboard/` | 1.5G keylog; clipboard size currently negligible | adapter: `desktop.clipboard`; keylog historical route requires confirmation | Import clipboard exports through the existing staged/desktop route. Keylog is W1 unless an offset-preserving historical parser is confirmed | Capture timestamps, often exact; content sensitivity is high | Privacy-critical; do not import unredacted keylog material |
| Polylogue and agent sessions | `captures/polylogue/`, `exports/chatlog/`, `self/polylogue-pre-reset-embeddings-2026-07-10/` | 109G combined | `integration.polylogue` plus `ai-session-claude` / `ai-session-chatgpt` where the registered package binding is enabled | Import the raw chat archive through the Polylogue/Sinex bridge, then run the agent-session quarantine preflight before any embedding path | Provider message timestamps are generally strong; archive export time is separate | Exact archive binding and historical command remain undocumented; `sinex-h2x` quarantine and `sinex-2k2` cost fuse are mandatory before embedding |
| Knowledgebase vault | `knowledgebase/` | 112M | adapter: `knowledgebase-vault` | Scan the vault as bounded file materials, preserving front matter and byte anchors | Document front matter, then filesystem fallback | Private notes; preserve confidential subtree policy |
| Audio and screenshots | `captures/audio/`, `captures/screenshot/` | 8.5G | staged/media: `media.audio-transcript`, `media.screen-ocr` where the package binding is enabled | Stage transcript/OCR bundles, not raw binary media as event rows; retain raw audio/images outside the event database | Transcript/OCR timestamps may be derived; record quality per bundle | Sensitive media; use proposed/default profile gates |
| Communications | `captures/comms/`, `exports/comms/` | 2.8G combined | staged: `facebook-messenger-thread`; `email.mailbox` only if explicitly enabled and its source authority is present | Import provider exports by thread/file batch. Email is not assumed in scope merely because the contract exists | Provider message timestamps | Private communications; verify redaction and source enablement |
| Health and sleep | `exports/health/`, `exports/samsung/` | 6.8G combined | staged: `sleep-merged-summary` and the registered health/provider bindings | Stage raw provider exports, then parse source-native measurements; do not import only derived daily summaries when raw evidence exists | Measurement timestamps generally strong; timezone and device quality remain fields | Sensitive health data; no embedding or broad read surface before privacy checks |
| Music and media history | `exports/spotify/` | 350M | staged: `spotify-extended-history` | Stage raw extended-history exports and parse in bounded archive batches | Provider play timestamps | Private consumption data; no external media library import |
| Social exports | `exports/reddit/`, `exports/wykop/`, `exports/themotte/` | 2.7G combined | staged: `reddit-gdpr-posts`, `reddit-gdpr-comments`, `wykop-entries`, `wykop-entry-comments` | Stage each provider family separately; preserve provider IDs and pagination/export metadata | Provider timestamps, with deleted/edit caveats | Public-facing activity can still contain private context; record provider deletion gaps |
| Finance | `self/finance/` | 627M | staged/adapter: `hledger-journal` | Import journal files and broker/bank exports through the finance parser; preserve posting-level anchors | Posting dates are source-native; import time is never substituted silently | `sinex-ztlf` is in scope and must be closed or explicitly triaged before importing transactions |
| System journals and machine telemetry | `exports/syslog-journal/`, `captures/syslog/`, `captures/machine/` | 157G combined | `system.journald` is live `JournalctlStreamAdapter`; machine telemetry needs its SQLite-drain binding | Historical journal exports currently have no valid archive command because the adapter is hardcoded to live `journalctl -f`. Machine SQLite needs the registered sqlite-drain route | Journal record timestamps are strong; machine metric quality varies by table | `sinex-y0o3.3/.4` and the archived-journal route gap block complete coverage |

## In-scope conditional blockers

The following blockers are relevant to the manifest above and cannot be silently skipped:

- `sinex-ztlf` applies because `self/finance/` is in scope. Verify occurrence keys for amount-less postings before the first finance import.
- `sinex-h3gy` applies to terminal commands and must remain enabled in the historical path.
- `sinex-h2x` and `sinex-2k2` apply before any agent-session embedding or semantic worker is started. The rebuild can import raw session evidence without enabling embeddings.
- `sinex-rkv.15` and `sinex-rkv.16` apply to the Google Takeout product families and web-history slices that are actually present in the export root.
- `sinex-2bk` is a post-deploy verification gate for static Git and Raindrop imports, not a reason to omit them.
- `sinex-k4c` must be rechecked before Phase C because its child list is not captured by this document alone.
- `sinex-y0o3.3`, `.4`, and `.5` remain route/scale dependencies for append-log, SQLite-drain, and staged provider-export coverage. Their raw authorities stay preserved while the route is unfinished.

The following are known route gaps, not successful imports: archived journald files, product-by-product Google Takeout bindings, Takeout Chrome archive binding, multi-repository Git coverage beyond the current dev manifest, full Asciinema `.cast` content, Kitty scrollback content, and the exact Polylogue archive command. A complete Phase C claim must either land those routes or record an explicit N/A disposition with the raw authority preserved.

## Explicitly out of the activity-plane import

These roots are preserved and audited separately. They are not silently treated as missing Sinex data. Optional media and uncovered exports remain explicit W1 decisions rather than being silently dropped:

| Root family | Reason |
| --- | --- |
| `self/genome/` and device backup trees | Independent medical/device recovery authorities with their own pipelines; importing raw binary/genotype/device images into the activity event plane would be a category error. |
| `self/photos/` and `self/private/` | Private document/photo authorities. Only an explicitly approved derived OCR/transcript bundle may enter a media route. |
| `self/code-archives/` | Historical archive material, not repository event history. Git repositories under `exports/repos/` are the authoritative code-history input. |
| `exports/career/`, `exports/freedom/`, `exports/ai-reports/`, `exports/goodreads/`, `exports/lastpass/` | No verified Sinex contract or current rebuild requirement. Preserve raw exports and create a route bead before reconsidering. `lastpass` remains credential material and must not enter the event database. |
| `derived/` and pre-reset embedding products | Rebuildable products, not raw witnesses. Do not re-import derived rows; regenerate only after raw source parity and privacy gates pass. |

## Measurement and execution checklist

Before Phase C, refresh this table's size/cardinality columns with a machine-readable manifest generated from the exact files selected for import. During the rebuild, every route must record:

1. selected file list and content hashes;
2. material count and event-intent count;
3. admitted, suppressed, superseded, DLQ, and unresolved counts;
4. `ts_orig` quality distribution and source-material provenance;
5. final source row/event counts and a rerun/idempotence comparison.

For a registered historical source, the runtime shape is:

```bash
sinexd scan-source-driver \
  --source <SOURCE_ID> \
  --runtime-config '<JSON>' \
  scan \
  --from none \
  --until <RFC3339-END-TIME>
```

An ISO `--until` selects the historical horizon. The exact runtime binding and archive path must be recorded for each source in the deployment binding manifest; `xtask run all-sources` starts configured bindings but does not by itself rewind every source to `--from none`.

The database wipe remains separately authorized and must not be inferred from this document. The authoritative pre-wipe sequence is: resolve or triage every applicable `sinex-kn14` dependency, capture and inspect the Phase-A snapshot, rehearse the selected routes against a clean dev stack, then obtain fresh operator authorization immediately before dropping `sinex_prod`.
