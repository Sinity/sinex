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
      postgres = pkgs.postgresql_18.withPackages (p: [ p.timescaledb ]);
      package = rustPkgs.rustPlatform.buildRustPackage {
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
        nativeCheckInputs = [ pkgs.nats-server pkgs.git ];
        preBuild = ''
          cargo run --offline --locked -p xtask -- boundaries
          export PGDATA=$(mktemp -d)
          export PGHOST=$(mktemp -d)
          export PGPORT=55439
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
        cargoBuildFlags = [ "-p" "sinexd" "-p" "sinexctl" "-p" "sinex-session" "-p" "sinex-capture" "-p" "xtask" ];
        doCheck = true;
        # Captures check the private-mode switch under $XDG_RUNTIME_DIR; the
        # builder has none, and a switch that cannot be checked counts as on.
        preCheck = ''
          export XDG_RUNTIME_DIR=$(mktemp -d)
        '';
        # The memory test measures its process's peak RSS, so it needs a
        # process of its own, which nextest gives it and `cargo test` does not.
        checkFlags = [ "--skip" "drivers::tests::backfill_of_a_large_file_peaks_well_under_its_size" ];
        # Database tests need devkit's server; they run under `xtask test`.
        cargoTestFlags = [ "-p" "sinex-model" "-p" "sinex-protocol" "-p" "sinex-schema" "-p" "sinex-sources" "-p" "sinex-source-core" "-p" "sinex-sealing" "-p" "sinex-derivations" ];
      };
    in {
      packages.${system} = { default = package; sinexd = package; sinexctl = package; };
      nixosModules.default = import ./nix/module.nix self;
      # VM tests: the deployed stack, a satellite capture node, the restore
      # drill. Built on demand, not by `package`.
      checks.${system} = {
        vm-stack = pkgs.testers.runNixOSTest (import ./nix/tests/stack.nix self);
        vm-topology = pkgs.testers.runNixOSTest (import ./nix/tests/topology.nix self);
        vm-restore = pkgs.testers.runNixOSTest (import ./nix/tests/restore.nix self);
      };
      devShells.${system}.default = pkgs.mkShell {
        packages = [ rustPkgs.cargo rustPkgs.cargo-nextest rustPkgs.rustc rustPkgs.rustfmt rustPkgs.clippy postgres pkgs.nats-server pkgs.python3 pkgs.pkg-config pkgs.mold pkgs.poppler-utils ];
        # mold links the test binaries several times faster than the default
        # linker; linking was a third to a half of each test binary's build.
        CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-C link-arg=-fuse-ld=mold";
        shellHook = ''
          # Optional host placement; an explicit CARGO_TARGET_DIR is left as is.
          if [ -z "''${CARGO_TARGET_DIR:-}" ] && command -v sinnix-cargo-target >/dev/null 2>&1; then
            if cargo_target="$(sinnix-cargo-target)"; then
              export CARGO_TARGET_DIR="$cargo_target"
            fi
          fi
          export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$PWD/target}"
        '';
      };
    };
}
