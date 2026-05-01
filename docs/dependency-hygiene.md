# Dependency Hygiene Triage

This document records the current dependency-hygiene classifier for issue #775.
It is intentionally based on tool output plus local source inspection, not on
tool output alone.

Regenerate the evidence with:

```bash
cargo machete --with-metadata
cargo tree --duplicates
rg "serde_yaml|serde_yml|yaml-rust|figment|Figment|Env::|Toml::|Yaml::|Json::" Cargo.toml crate xtask tests
```

## Current Classifier

| Area | Dependency finding | Kind | Action | Owner / blocker | Verification |
| --- | --- | --- | --- | --- | --- |
| advisory gate | `atty`, `aws-lc-sys`, `time`, `rustls-webpki`, `rustls-pemfile`, `syntect`/`bincode`/`yaml-rust`, `inquire`/`fxhash`, `portable-pty`/`serial`, `indicatif`/`number_prefix`, `ratatui`/`paste` | direct and transitive | Done. Keep `cargo deny check advisories` as the gate. | Landed through #810-#817. | `cargo deny check advisories` |
| workspace config | `figment` | unused workspace dep | Removed. Re-open only as a deliberate config-loader consolidation issue, not as a dormant dependency. | No live `Figment` call sites. | `cargo tree -i figment` |
| xtask shell helper | `xshell` | unused dev dep | Removed. | No `xshell::` / `cmd!` call sites. | `cargo tree -i xshell` |
| CLI machete candidates | `blake3`, `dirs`, `rusqlite`, `rustls-native-certs`, `xtask-macros`; test/dev: `insta`, `serial_test` | mixed production/dev | Investigate per dependency before removal. `serial_test` is likely a macro false positive because `#[sinex_serial_test]` expands through the test harness; `rusqlite`/`dirs` may be inherited from old local config/storage paths. | CLI owner. Do not remove in bulk; run focused CLI tests per removal. | `cargo machete --with-metadata`; `xtask check -p sinexctl`; targeted CLI tests |
| core service metadata | `sd-notify`, `shadow-rs`, gateway `once_cell`, ingestd `xtask-macros` | production/build | Remove if current service entrypoints no longer use them; otherwise add narrow machete ignores with the macro/build reason. | Gateway/ingestd owners. | `xtask check -p sinex-gateway`; `xtask check -p sinex-ingestd` |
| library candidates | `sinex-db`: `async-trait`, `camino`, `mockall`, `urlencoding`; `sinex-primitives`: `rand`, `urlencoding`; `sinex-schema`: `blake3`, `pretty_assertions`, `proptest`, `rstest`, `tempfile`, `tokio-test` | mixed production/dev/test | Split by crate. Prefer removal for no-call-site deps; keep dev-only test helpers only with explicit tests or machete ignore. | Owning library crates. | package checks plus package tests for each crate |
| proc-macro crate | `sinex-macros`: `color-eyre`, `tokio`, `xtask-macros` | likely stale dev/runtime deps | Remove if proc-macro tests still compile without them. | Macro crate owner. | `xtask check -p sinex-macros`; trybuild/proc-macro tests |
| node crates | repeated `clap`, `color-eyre`, `human-panic`, `tokio`, `async-trait`, plus source-specific leftovers | production/runtime | Treat as an SDK-entrypoint cleanup train. Many node crates likely retained old standalone CLI deps after `node_entrypoint!`; remove crate by crate with node checks, not all at once. | Node SDK/runtime owners. | `xtask check -p <node-crate>`; node smoke tests where present |
| fuzz/test crates | fuzz `arbitrary`; e2e `parking_lot`, `tokio-stream`; workspace tests `time` | fuzz/test | Verify scanner false positives against fuzz/test targets. Keep with explicit ignore if generated fuzz/test macros need them; otherwise remove. | Test/fuzz owners. | `cargo machete --with-metadata`; targeted test/fuzz compile |
| YAML output and manifests | `serde_yaml` in `sinexctl` and `xtask` | direct production/devtool | Keep for now with policy: YAML is a supported CLI output and xtask manifest/stack-plan format. Replacement is not a blind removal; it needs compatibility tests for supported YAML shape. | #775 / follow-up YAML policy slice. | CLI YAML tests; xtask docs/git-stack tests |
| duplicate versions | crypto/rand stack, `darling`, `dashmap`, `hashbrown`/`foldhash`, HTTP/reqwest/tower stack, `sysinfo`, `thiserror`, `toml`, `unicode-width`, `webpki-roots`, `which`, `whoami` | transitive and some direct | Do not churn all duplicates at once. Classify into direct-upgrade candidates vs transitive-only duplication. Prefer dependency upgrades where there is a single owning direct dependency. | Dependency owner per family. | `cargo tree --duplicates`; package checks for touched owners |

## Policy

- `cargo machete` findings are candidates, not proof.
- A dependency can close one of four ways: removed, deliberately adopted,
  isolated behind a feature/crate boundary, or kept with an explicit proof/ignore.
- YAML support is an operator/API compatibility surface. Replacing `serde_yaml`
  must preserve the documented YAML output and manifest formats or explicitly
  narrow them in the CLI/docs.
- Duplicate-version cleanup should happen by family. A PR that updates one
  direct dependency should state which duplicate family it reduced and which
  duplicates remain transitive-only.
