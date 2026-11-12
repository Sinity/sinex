# Development

`rust-toolchain.toml` selects Rust and its components for rustup, the Nix
shell and the Nix package build. The flake reads it through its pinned
rust-overlay input. Change the compiler version in common and both products
together. Coverage uses that toolchain's LLVM tools; fuzz instrumentation
uses its bundled sanitizer runtime.

`cargo-nextest` comes from the pinned `devtools-nixpkgs` input because the
primary nixpkgs snapshot predates devkit's minimum supported nextest. The
project compiler and LLVM tools come from `rust-toolchain.toml`.

| Command | Does |
|---|---|
| `cargo xtask check` | formatting, boundaries, workspace and reduced-capture Clippy against the schema database, including the fuzz harness |
| `cargo xtask compile -p <crate> [--tests]` | compile selected packages, preparing the schema database when needed |
| `cargo xtask test -p <crate> -E 'test(<name>)'` | selected Rust tests under nextest |
| `cargo xtask test` | every Rust test and bounded fuzz-corpus replay |
| `cargo xtask test --no-run` | build every test binary without running tests |
| `cargo xtask warm` | build every test binary and run the workspace and fuzz Clippy without running tests |
| `cargo xtask run --watch --output DIR` | an isolated development stack and watched rebuilds |
| `cargo xtask vm --output DIR` | the module check and three exported NixOS VM scenarios |
| `cargo xtask coverage --output DIR --format html -- -p <crate>` | LLVM coverage of selected tests |
| `cargo xtask changed` | the nextest filter for packages changed since the merge base, and their dependents |
| `cargo xtask db ensure` | build the database for the current schema and print its `DATABASE_URL`; `db exec` runs a command with it set |
| `cargo xtask bench ...` | backfill and derivation on a fixed sample of real inputs |
| `cargo xtask status`, `cargo xtask history ...` | what failed in this checkout's last run, and the recorded run history |
| `nix build .#checks.x86_64-linux.vm-stack` | one NixOS VM test (`vm-stack`, `vm-topology`, `vm-restore`) |
| `nix flake check` | the locked package plus Nix checks, including the VM tests |
| `nix build` | the packages |

Entering the shell configures `.githooks` when Git has no hooks path; its
commit hook checks staged whitespace and formats staged Rust files. The
xtask command owns any services its run needs.

`direnv allow` loads the same shell through `.envrc`. Its ignored
`.envrc.local` is the place for personal build-output placement, job counts
and compiler wrappers. The shell preserves an explicit `CARGO_TARGET_DIR`.
When it is unset, an available `sinnix-cargo-target` helper may choose the
output location; otherwise it uses `target/`.

For local shared-crate development, run `cargo xtask link-common PATH`
explicitly. Run `cargo xtask unlink-common` before committing either
workspace lock, as described in the shared
[dependency guide](https://github.com/Sinity/polylogue-sinex-common#depending-on-this-repository).

A host may queue these commands through its own job runner.

`cargo xtask test -p PACKAGE` passes native package selection to Cargo.
`-E` and test-name filters narrow execution after compilation; they do not
remove unrelated build targets from a selected package. Target flags can
limit that compilation too:

```sh
cargo xtask test -p sinex-store --lib -E 'test(query::)'
cargo xtask test -p sinex-model -p sinex-store -p sinex-sources -p sinexd --lib --bin sinexd -E 'test(query::) | test(sources::analysis::) | test(analysis::recall::)'
```

The daemon's unit tests live in its binary target, so `--lib` alone omits
them. The second command includes those tests while excluding integration
targets and their separately built executables. Integration tests that use
`CARGO_BIN_EXE_*` still need their integration target selected; the full
workspace run includes them. Database and NATS preparation stay with xtask.

Cargo's selected packages, default features and explicit feature flags own
feature resolution. `sinex-sources` defaults to its full source set, and
`sinexd` explicitly enables that set, archive/SQLite readers and CLI
protocol support. Selecting different packages can produce a different
feature build. The complete workspace run is what CI runs on master.

## PostgreSQL provider

`nix/postgresql.nix` constructs the PostgreSQL provider used by development
and the default deployment package. Host development services import the
same function. It preserves exact UUIDv7 row predicates while widening their
millisecond chunk-pruning bounds conservatively. UUIDs within one
millisecond remain distinct, including at strict range endpoints.

`nix build .#postgresql` builds the provider without building Rust packages.
`nix build .#checks.x86_64-linux.postgresql-uuid-bounds` checks the provider
against ordinary PostgreSQL UUID comparisons in a fresh isolated cluster.
The synthetic matrix covers strict and inclusive ranges, chunk boundaries,
empty ranges, equality and commuted operands. It never connects to a deployed
database. A custom `services.sinex.database.package` must preserve these
semantics.

## Published package

The `package` workflow builds `.#default` for every master commit and pushes
its runtime closure and `.#sinex-dependencies` to `sinity.cachix.org`. The
dependency artifact uses the exact production Cargo graph; the runtime
builds `sinexd`, `sinexctl`, `sinex-session` and `sinex-capture` together and
ships those four binaries. A separately built native schema tool prepares
the read-only PostgreSQL database used by compile-time query checks.
Required CI owns workspace boundaries and tests; packaging does not repeat
them or ship `xtask`. The development shell, package and VM checks use the
same pinned nixpkgs revision as Polylogue and the shared crates.
Sinnix deploys a revision only
after fetching that package, so its switches never compile Sinex. To deploy a
revision master does not contain, such as a deployment branch's head, build
it first with `gh workflow run package -f rev=<commit>`.
The builder uses the explicit Git fetch policy `exportIgnore=1`, including
for module and VM checks. Deployers use that same policy when pinning the
revision. The workflow records the Nix version, resolved source path and NAR
hash, and package derivation before building; a runtime signature alone does
not identify the module source used to evaluate it.
For a package or nixpkgs change, add `-f verify-vms=true` to verify the
deployment module and the stack, topology and restore VMs in that workflow.
