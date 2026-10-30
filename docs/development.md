# Development

`rust-toolchain.toml` selects Rust and its components for rustup, the Nix
shell and both Nix package builds. The flake reads it through its pinned
rust-overlay input. Change the compiler version in common and both products
together. Coverage uses that toolchain's LLVM tools; fuzz instrumentation
uses its bundled sanitizer runtime.

`cargo-nextest` comes from the pinned `devtools-nixpkgs` input because the
primary nixpkgs snapshot predates devkit's minimum supported nextest. The
project compiler and LLVM tools come from `rust-toolchain.toml`.

```sh
nix develop
cargo xtask check
cargo xtask test
cargo xtask compile -p sinex-model
nix build
```

Entering the shell configures `.githooks` when Git has no hooks path. The
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
