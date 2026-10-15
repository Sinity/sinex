{
  description = "Sinex isolated executable foundation";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfreePredicate = pkg: builtins.elem (pkgs.lib.getName pkg) [ "timescaledb" ];
      };
      postgres = pkgs.postgresql_18.withPackages (p: [ p.timescaledb ]);
      package = pkgs.rustPlatform.buildRustPackage {
        pname = "sinex-foundation";
        version = "0.1.0";
        src = ./.;
        cargoLock = {
          lockFile = ./Cargo.lock;
          outputHashes."hashed-blobs-0.1.0" = "sha256-pXXYpwvsA0xtEhHshHSgatLdf+bFnZmlkQcdpv6HlKw=";
        };
        nativeBuildInputs = [ postgres ];
        preBuild = ''
          export PGDATA=$(mktemp -d)
          export PGHOST=$(mktemp -d)
          export PGPORT=55439
          initdb --no-locale --auth=trust --data-checksums > /dev/null
          pg_ctl -D "$PGDATA" -o "-k $PGHOST -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on -p $PGPORT" -w start > /dev/null
          trap 'pg_ctl -D "$PGDATA" -m immediate -w stop >/dev/null || true' EXIT
          createdb foundation
          createuser sinex_writer
          createuser sinex_reader
          export DATABASE_URL="postgresql:///foundation?host=$PGHOST&port=$PGPORT"
          printf '%s' "$DATABASE_URL" > admin-connection
          cargo build --offline --locked -p sinexd --no-default-features --target-dir target --target ${pkgs.stdenv.hostPlatform.rust.rustcTarget}
          target/${pkgs.stdenv.hostPlatform.rust.rustcTarget}/debug/sinexd admin schema --connection-file "$PWD/admin-connection"
        '';
        cargoBuildFlags = [ "-p" "sinexd" "-p" "sinexctl" "-p" "xtask" ];
        doCheck = true;
        cargoTestFlags = [ "-p" "sinex-model" "-p" "sinex-protocol" "-p" "sinexd" "-p" "sinex-store" ];
      };
    in {
      packages.${system} = { default = package; sinexd = package; sinexctl = package; };
      devShells.${system}.default = pkgs.mkShell {
        packages = [ pkgs.cargo pkgs.rustc pkgs.rustfmt pkgs.clippy postgres pkgs.python3 pkgs.pkg-config ];
        shellHook = "export CARGO_TARGET_DIR=\"$PWD/target\"";
      };
    };
}
