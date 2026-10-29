{
  description = "Sinex: a durable record of one person's digital life";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.rust-nixpkgs.url = "github:NixOS/nixpkgs/6510408d147d8c5e6d76043e899e9251756f6a75";
  outputs = { self, nixpkgs, rust-nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfreePredicate = pkg: builtins.elem (pkgs.lib.getName pkg) [ "timescaledb" ];
      };
      # The Rust toolchain comes from a pinned nixpkgs that matches the released shared crates.
      rustPkgs = import rust-nixpkgs { inherit system; };
      compilerTools = rustPkgs.rustc.llvmPackages;
      postgres = pkgs.postgresql_18.withPackages (p: [ p.timescaledb p.pgvector ]);
      server = rustPkgs.rustPlatform.buildRustPackage {
        pname = "sinex";
        version = "0.1.0";
        src = ./.;
        cargoLock = {
          lockFile = ./Cargo.lock;
          # Git dependencies are fetched at the SHA recorded in Cargo.lock.
          allowBuiltinFetchGit = true;
        };
        nativeBuildInputs = [ postgres ];
        # sinex-sources tests start a throwaway NATS server and make git
        # repositories.
        nativeCheckInputs = [ pkgs.nats-server pkgs.git pkgs.gnutar ];
        preBuild = ''
          cargo run --offline --locked -p xtask -- boundaries
          export PGDATA=$(mktemp -d)
          export PGHOST=$(mktemp -d)
          export PGPORT=5432
          initdb --no-locale --auth=trust --data-checksums > /dev/null
          pg_ctl -D "$PGDATA" -o "-k $PGHOST -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on -p $PGPORT" -w start > /dev/null
          trap 'pg_ctl -D "$PGDATA" -m immediate -w stop >/dev/null || true' EXIT
          createdb foundation
          export DATABASE_URL="postgresql:///foundation?host=$PGHOST&port=$PGPORT"
          # The schema tool has no checked queries, so it builds before the
          # database exists and provisions the one the store's queries need.
          cargo build --offline --locked -p sinex-schema --target-dir target --target ${rustPkgs.stdenv.hostPlatform.rust.rustcTarget}
          target/${rustPkgs.stdenv.hostPlatform.rust.rustcTarget}/debug/sinex-schema apply "$DATABASE_URL"
        '';
        cargoBuildFlags = [ "-p" "sinexd" "-p" "sinexctl" "-p" "xtask" ];
        doCheck = true;
        preCheck = ''
          export DEVKIT_PG_SOCKET="$PGHOST"
          export DEVKIT_PG_TEMPLATE=foundation
        '';
        # The memory test measures its process's peak RSS, so it needs a
        # process of its own, which nextest gives it and `cargo test` does not.
        # Nix's syscall filter prevents setting xattrs and ACLs. The native
        # xtask suite runs the GNU tar restoration test with those assertions.
        checkFlags = [
          "--skip" "drivers::tests::backfill_of_a_large_file_peaks_well_under_its_size"
          "--skip" "drivers::dir_snapshot::tests::restore_tar::gnu_tar_restores_native_names_sparse_data_attributes_and_access_acl"
        ];
        # Schema tests clone the builder's prepared database. Store and daemon
        # database tests run under `xtask test`.
        cargoTestFlags = [ "-p" "sinex-model" "-p" "sinex-protocol" "-p" "sinex-schema" "-p" "sinex-sources" "-p" "sinex-source-core" "-p" "sinex-sealing" "-p" "sinex-derivations" ];
      };
      capture = rustPkgs.rustPlatform.buildRustPackage {
        pname = "sinex-capture";
        version = "0.1.0";
        src = ./.;
        cargoLock = {
          lockFile = ./Cargo.lock;
          allowBuiltinFetchGit = true;
        };
        cargoBuildFlags = [ "-p" "sinex-session" "-p" "sinex-capture" ];
        doCheck = false;
      };
      package = pkgs.symlinkJoin {
        name = "sinex-0.1.0";
        paths = [ server capture ];
      };
    in {
      packages.${system} = { default = package; sinexd = package; sinexctl = package; };
      nixosModules.default = import ./nix/module.nix self;
      # VM tests: the deployed stack, a satellite capture node, the restore
      # drill. Built on demand, not by `package`.
      checks.${system} = {
        module = import ./nix/tests/module.nix self pkgs;
        vm-stack = pkgs.testers.runNixOSTest (import ./nix/tests/stack.nix self);
        vm-topology = pkgs.testers.runNixOSTest (import ./nix/tests/topology.nix self);
        vm-restore = pkgs.testers.runNixOSTest (import ./nix/tests/restore.nix self);
      };
      devShells.${system}.default = pkgs.mkShell {
        packages = [ rustPkgs.cargo rustPkgs.cargo-nextest rustPkgs.rustc rustPkgs.rustfmt rustPkgs.clippy compilerTools.llvm pkgs.cargo-llvm-cov pkgs.cargo-fuzz postgres pkgs.nats-server pkgs.python3 pkgs.pkg-config pkgs.mold pkgs.poppler-utils ];
        # mold links the test binaries several times faster than the default
        # linker; linking was a third to a half of each test binary's build.
        DEVKIT_FUZZ_RUSTFLAGS = "-Zexternal-clangrt -Clinker=${compilerTools.clang}/bin/clang -Clink-arg=-fsanitize=address";
        LLVM_COV = "${compilerTools.llvm}/bin/llvm-cov";
        LLVM_PROFDATA = "${compilerTools.llvm}/bin/llvm-profdata";
        CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-C link-arg=-fuse-ld=mold";
        shellHook = ''
          # Optional host placement; an explicit CARGO_TARGET_DIR is left as is.
          if [ -z "''${CARGO_TARGET_DIR:-}" ] && command -v sinnix-cargo-target >/dev/null 2>&1; then
            if cargo_target="$(sinnix-cargo-target)"; then
              export CARGO_TARGET_DIR="$cargo_target"
            fi
          fi
          export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$PWD/target}"
          # Build against a checkout of polylogue-sinex-common when there is
          # one ($POLYLOGUE_SINEX_COMMON, else the sibling directory), unless
          # `cargo xtask unlink-common` turned that off.
          if [ -z "''${LINKING_COMMON:-}" ] && [ -f crates/sinexd/Cargo.toml ] \
            && [ -f "''${POLYLOGUE_SINEX_COMMON:-../polylogue-sinex-common}/Cargo.toml" ] \
            && [ ! -e .cargo/local.toml ] && [ ! -e .cargo/common-link.off ]; then
            LINKING_COMMON=1 cargo xtask link-common \
              || echo "sinex: linking polylogue-sinex-common failed; see cargo xtask link-common" >&2
          fi
        '';
      };
    };
}
