# Dependency Hygiene Triage

This document records the current dependency-hygiene classifier for issue #775.
It is intentionally based on tool output plus local source inspection, not on
tool output alone.

Regenerate the evidence with:

```bash
xtask deps unused
xtask deps duplicates --json
xtask deps duplicates --direct-only --json
xtask deps duplicates --transitive-only --json
rg "serde_yaml|serde_yml|yaml-rust|figment|Figment|Env::|Toml::|Yaml::|Json::" Cargo.toml crate xtask tests
```

## Current Classifier

| Area | Dependency finding | Kind | Action | Owner / blocker | Verification |
| --- | --- | --- | --- | --- | --- |
| advisory gate | `atty`, `aws-lc-sys`, `time`, `rustls-webpki`, `rustls-pemfile`, `syntect`/`bincode`/`yaml-rust`, `inquire`/`fxhash`, `portable-pty`/`serial`, `indicatif`/`number_prefix`, `ratatui`/`paste` | direct and transitive | Done. Keep `cargo deny check advisories` as the gate. | Landed through #810-#817. | `cargo deny check advisories` |
| workspace config | `figment` | unused workspace dep | Removed. Re-open only as a deliberate config-loader consolidation issue, not as a dormant dependency. | No live `Figment` call sites. | `xtask deps unused`; inverse lookup through `xtask deps tree --package <owner>` when needed |
| xtask shell helper | `xshell` | unused dev dep | Removed. | No `xshell::` / `cmd!` call sites. | `xtask deps unused`; `rg "xshell::|cmd!"` |
| unused dependency scanner | all workspace manifests | direct production/dev/test | Clean as of 2026-05-23 after removing stale `sinex-source-worker` manifest entries for `notify`, `sinex-db`, `tokio-util`, and `validator`. Do not retain old candidate rows unless `xtask deps unused` reports them again. | Future findings still require source inspection before removal. | `xtask deps unused --format human` |
| library cleanup completed | `sinex-schema`: stale `pretty_assertions`, `proptest`, `rstest`, `tempfile`, and `tokio-test` dev-deps removed | dev/test | Keep `tokio` and `color-eyre`; `#[sinex_test]` expands through those crates directly in the test crate. | Schema crate owner. | `xtask check --fmt -p sinex-schema`; focused schema tests |
| proc-macro crate | `sinex-macros`: stale `xtask-macros` dev-dep removed; `color-eyre` and `tokio` are required by `#[sinex_test]` expansion even without textual call sites | resolved | Keep the harness-required dev-deps unless the test macro expansion changes. | Macro crate owner. | `xtask check -p sinex-macros`; trybuild/proc-macro tests |
| YAML output, manifests, and source front matter | `serde_yml` in `sinexctl`, `xtask`, and `sinex-source-worker` | direct production/devtool | Standardized on the workspace `serde_yml` crate. Do not reintroduce deprecated `serde_yaml` without compatibility evidence and a deliberate policy decision. | #775 / follow-up YAML policy slice. | CLI YAML tests; xtask docs/git-stack tests; knowledgebase front-matter parser tests |
| duplicate versions | crypto/rand stack, `darling`, `hashbrown`/`foldhash`, `thiserror`, `webpki-roots`, `winnow`, and similar upstream stacks | transitive upstream | Direct workspace duplicate debt is currently zero. Do not churn all duplicates at once. Use the `classification` field from `xtask deps duplicates --json` to separate `direct_workspace` action from `transitive_upstream` noise before patching manifests. | Dependency owner per family. | `xtask deps duplicates --json`; `xtask deps duplicates --direct-only --json`; package checks for touched owners |

## Classified Non-Wins

These probes were checked during the 2026-05 dependency-compaction wave and
should not be retried as blind patch bumps:

| Candidate | Result | Why not |
| --- | --- | --- |
| `rusqlite` 0.32 -> 0.39 | rejected | Pulls `libsqlite3-sys` 0.37, which conflicts with SQLx 0.8's `libsqlite3-sys` 0.28 `links = "sqlite3"` requirement. This needs an SQLx-aligned SQLite native-link update, not an isolated rusqlite bump. |
| `async-nats` 0.47 -> 0.48 | rejected for dependency compaction | Compiles, but duplicate count increases from 15 to 17 by adding `rand` 0.10, `rand_core` 0.10, `chacha20` 0.10, and `cpufeatures` 0.3 lanes. Revisit only for async-nats functionality or once the wider random/crypto stack is ready to move. |
| `dashmap` 6.1 -> 6.2 | neutral | Compiles, but still depends on `hashbrown` 0.14 and does not reduce the duplicate graph. A future meaningful cleanup likely needs DashMap 7 after it leaves RC or a different state-map strategy. |

## Policy

- `xtask deps unused` findings are candidates, not proof.
- A dependency can close one of four ways: removed, deliberately adopted,
  isolated behind a feature/crate boundary, or kept with an explicit proof/ignore.
- YAML support is an operator/API compatibility surface. The workspace standard
  is `serde_yml`; changes must preserve the documented YAML output,
  manifest/stack-plan formats, and source front-matter parsing semantics or
  explicitly narrow them in the CLI/docs.
- Duplicate-version cleanup should happen by family. A PR that updates one
  direct dependency should state which duplicate family it reduced and cite the
  `direct_workspace` / `transitive_upstream` split that remains.
