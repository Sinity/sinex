{
  description = "Sinex isolated executable foundation";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.rust-nixpkgs.url = "github:NixOS/nixpkgs/6510408d147d8c5e6d76043e899e9251756f6a75";
  outputs = { self, nixpkgs, rust-nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfreePredicate = pkg: builtins.elem (pkgs.lib.getName pkg) [ "timescaledb" ];
      };
      # Keep the qualified database packages while aligning the released crate’s compiler.
      rustPkgs = import rust-nixpkgs { inherit system; };
      postgres = pkgs.postgresql_18.withPackages (p: [ p.timescaledb ]);
      package = rustPkgs.rustPlatform.buildRustPackage {
        pname = "sinex-foundation";
        version = "0.1.0";
        src = ./.;
        cargoLock = {
          lockFile = ./Cargo.lock;
          # Git dependencies are fetched at the SHA recorded in Cargo.lock.
          allowBuiltinFetchGit = true;
        };
        nativeBuildInputs = [ postgres ];
        # The cluster stays up for the check phase; database tests clone their own databases.
        preBuild = ''
          cargo run --offline --locked -p xtask -- boundaries
          export PGDATA=$(mktemp -d)
          export PGHOST=$(mktemp -d)
          export PGPORT=55439
          initdb --no-locale --auth=trust --data-checksums > /dev/null
          pg_ctl -D "$PGDATA" -o "-k $PGHOST -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on -c timescaledb.max_background_workers=0 -p $PGPORT" -w start > /dev/null
          trap 'pg_ctl -D "$PGDATA" -m immediate -w stop >/dev/null || true' EXIT
          createdb foundation
          createuser sinex_writer
          createuser sinex_reader
          export DATABASE_URL="postgresql:///foundation?host=$PGHOST&port=$PGPORT"
          printf '%s' "$DATABASE_URL" > admin-connection
          cargo build --offline --locked -p sinexd --no-default-features --target-dir target --target ${rustPkgs.stdenv.hostPlatform.rust.rustcTarget}
          target/${rustPkgs.stdenv.hostPlatform.rust.rustcTarget}/debug/sinexd admin schema --connection-file "$PWD/admin-connection"
        '';
        cargoBuildFlags = [ "-p" "sinexd" "-p" "sinexctl" "-p" "xtask" ];
        doCheck = true;
        cargoTestFlags = [ "-p" "sinex-model" "-p" "sinex-protocol" "-p" "sinexd" "-p" "sinex-store" "-p" "xtask" ];
      };
    in {
      packages.${system} = { default = package; sinexd = package; sinexctl = package; };
      devShells.${system}.default = pkgs.mkShell {
        packages = [ rustPkgs.cargo rustPkgs.rustc rustPkgs.rustfmt rustPkgs.clippy pkgs.cargo-nextest postgres pkgs.python3 pkgs.pkg-config ];
        shellHook = "export CARGO_TARGET_DIR=\"$PWD/target\"";
      };
    };
}
