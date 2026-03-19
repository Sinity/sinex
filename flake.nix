{
  description = "Sinex - Universal data capture and query system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    agenix = {
      url = "github:ryantm/agenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      fenix,
      crane,
      agenix,
      flake-utils,
    }:
    let
      # pg_jsonschema - PostgreSQL JSON Schema validation extension
      # Not in nixpkgs; packaged from Supabase binary release
      pgJsonschemaOverlay = final: prev: {
        postgresql18Packages = prev.postgresql18Packages // {
          pg_jsonschema = final.stdenv.mkDerivation rec {
            pname = "pg_jsonschema";
            version = "0.3.4";

            src = final.fetchurl {
              url = "https://github.com/supabase/pg_jsonschema/releases/download/v${version}/pg_jsonschema-v${version}-pg18-amd64-linux-gnu.deb";
              sha256 = "sha256-XH/myBCDXkJC+wNltXWBwACbAVUgDdTxJmzuQ0KVcy8=";
            };

            nativeBuildInputs = [ final.dpkg ];
            dontConfigure = true;
            dontBuild = true;
            dontStrip = true;
            dontFixup = true;

            unpackPhase = ''
              dpkg-deb -x $src .
            '';

            installPhase = ''
              mkdir -p $out/lib $out/share/postgresql/extension
              find . -name "*.so" -type f -exec cp {} $out/lib/ \;
              find . -name "*.sql" -type f -exec cp {} $out/share/postgresql/extension/ \;
              find . -name "*.control" -type f -exec cp {} $out/share/postgresql/extension/ \;
            '';

            meta = with final.lib; {
              description = "PostgreSQL JSON Schema validation extension";
              homepage = "https://github.com/supabase/pg_jsonschema";
              license = licenses.asl20;
              platforms = platforms.linux;
            };
          };
        };
      };

      # System-specific outputs
      systemOutputs = flake-utils.lib.eachDefaultSystem (
        system:
        let
          # Apply pg_jsonschema overlay
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
            overlays = [ pgJsonschemaOverlay ];
          };

          fenixPkgs = fenix.packages.${system}.complete;
          rustToolchain = fenixPkgs.toolchain;

          # Crane with fenix toolchain
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          # Extract git information for version tracking
          gitRev = self.rev or self.dirtyRev or "unknown";
          gitShortRev = builtins.substring 0 8 gitRev;
          version = "0.1.0";

          # PostgreSQL with required extensions for SQLx build-time validation
          postgresForSqlx = pkgs.postgresql_18.withPackages (ps: [
            ps.timescaledb
            ps.pgvector
            pkgs.postgresql18Packages.pg_jsonschema
          ]);

          # Filter source for Rust builds.
          # Extends crane's default Rust filter to include .md files:
          # many crates use `#![doc = include_str!("../docs/README.md")]` which
          # requires .md files to be present at compile time.
          src = pkgs.lib.cleanSourceWith {
            src = craneLib.path ./.;
            filter = path: type: (craneLib.filterCargoSources path type) || (pkgs.lib.hasSuffix ".md" path);
          };

          # Common build arguments
          commonArgs = {
            inherit src;
            strictDeps = true;

            buildInputs = with pkgs; [
              openssl
              dbus
              systemd
            ];

            nativeBuildInputs = with pkgs; [
              pkg-config
              protobuf
              mold # .cargo/config.toml: link-arg=-fuse-ld=mold
            ];

          };

          # Build workspace dependencies once (cached layer).
          # SQLX_OFFLINE=true only here: deps have no live database, so SQLx macros
          # must use offline mode. buildPackage (mkPackage) overrides this via preBuild
          # which starts an ephemeral Postgres and sets DATABASE_URL.
          cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { SQLX_OFFLINE = "true"; });

          # Ephemeral Postgres setup for SQLx query validation
          postgresPreBuild = ''
                        export PGDATA="$TMPDIR/pgdata"
                        mkdir -p "$PGDATA"
                        ${postgresForSqlx}/bin/initdb -D "$PGDATA" --locale=C --encoding=UTF8 --auth=trust --username=postgres

                        export PGHOST="$TMPDIR"
                        export PGPORT=55433
                        echo "unix_socket_directories = '$TMPDIR'" >> "$PGDATA/postgresql.conf"
                        echo "port = $PGPORT" >> "$PGDATA/postgresql.conf"
                        echo "shared_preload_libraries = 'timescaledb'" >> "$PGDATA/postgresql.conf"

                        ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -w start

                        ${postgresForSqlx}/bin/createdb -h "$PGHOST" -p "$PGPORT" -U postgres sinex_dev || true

            # Run schema apply as postgres (superuser) — creates schemas, tables, extensions.
                        # SQLx compile-time query validation only needs the schema to exist; user is irrelevant.
                        export DATABASE_URL="postgresql:///sinex_dev?host=$PGHOST&user=postgres"
                        cargo run -p xtask -- infra schema-apply --database-url "$DATABASE_URL"
          '';

          postgresPostBuild = ''
            if [ -n "''${PGDATA:-}" ]; then
              ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -m fast stop || true
            fi
          '';

          # Build a specific package from the workspace.
          # SQLX_OFFLINE=false: preBuild starts an ephemeral Postgres and sets DATABASE_URL,
          # so sqlx::query! macros validate against a live schema (overrides the "true" in
          # cargoArtifacts/buildDepsOnly which only compiled external deps without project macros).
          mkPackage =
            pname:
            craneLib.buildPackage (
              commonArgs
              // {
                inherit cargoArtifacts pname;
                cargoExtraArgs = "-p ${pname}";
                doCheck = false;
                SQLX_OFFLINE = "false";

                preBuild = postgresPreBuild;
                postBuild = postgresPostBuild;
              }
            );

          # All packages built from Cargo.toml names
          sinexPackages = {
            # Core services
            sinex-ingestd = mkPackage "sinex-ingestd";
            sinex-gateway = mkPackage "sinex-gateway";

            # CLI
            sinexctl = mkPackage "sinexctl";

            # Ingestors (data capture nodes)
            sinex-fs-ingestor = mkPackage "sinex-fs-ingestor";
            sinex-terminal-ingestor = mkPackage "sinex-terminal-ingestor";
            sinex-desktop-ingestor = mkPackage "sinex-desktop-ingestor";
            sinex-system-ingestor = mkPackage "sinex-system-ingestor";
            sinex-document-ingestor = mkPackage "sinex-document-ingestor";

            # Automatons (processing nodes)
            sinex-terminal-command-canonicalizer = mkPackage "sinex-terminal-command-canonicalizer";
            sinex-health-automaton = mkPackage "sinex-health-automaton";
            sinex-analytics-automaton = mkPackage "sinex-analytics-automaton";

            # Node SDK binaries (sinex-preflight lives here)
            sinex-node-sdk = mkPackage "sinex-node-sdk";

            # Developer tooling (used by VM concurrency tests)
            xtask = mkPackage "xtask";

            # NixOS VM test suite (Rust binary replacing Python testScript assertions)
            sinex-vm-test-suite = mkPackage "sinex-vm-test-suite";

            # Aggregated suite with all binaries
            sinex = pkgs.symlinkJoin {
              name = "sinex";
              paths = [
                sinexPackages.sinex-ingestd
                sinexPackages.sinex-gateway
                sinexPackages.sinexctl
                sinexPackages.sinex-fs-ingestor
                sinexPackages.sinex-terminal-ingestor
                sinexPackages.sinex-desktop-ingestor
                sinexPackages.sinex-system-ingestor
                sinexPackages.sinex-document-ingestor
                sinexPackages.sinex-terminal-command-canonicalizer
                sinexPackages.sinex-health-automaton
                sinexPackages.sinex-analytics-automaton
                sinexPackages.sinex-node-sdk
                sinexPackages.xtask
              ];
            };

            # PostgreSQL extension
            pg_jsonschema = pkgs.postgresql18Packages.pg_jsonschema;

            # Default package
            default = sinexPackages.sinex-ingestd;
          };

          # VM tests
          vmTests = import ./tests/e2e/nixos-vm/default.nix {
            inherit pkgs;
            sinex-ingestd = sinexPackages.sinex-ingestd;
            sinex-gateway = sinexPackages.sinex-gateway;
            sinex = sinexPackages.sinex;
            sinexCli = sinexPackages.sinexctl;
            xtask = sinexPackages.xtask;
            sinexVmTestSuite = sinexPackages.sinex-vm-test-suite;
            pg_jsonschema = pkgs.postgresql18Packages.pg_jsonschema;
          };

          limitedVmTests = pkgs.lib.filterAttrs (
            name: _:
            pkgs.lib.elem name [
              "basic"
              "preflight"
            ]
          ) vmTests;

          vmPackagesAll = pkgs.lib.mapAttrs' (
            name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value
          ) vmTests;

          vmPackagesEssential = pkgs.lib.mapAttrs' (
            name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value
          ) limitedVmTests;

        in
        rec {
          packages = sinexPackages // vmPackagesEssential;

          legacyPackages = vmPackagesAll;

          formatter = pkgs.nixpkgs-fmt;

          checks = pkgs.lib.mapAttrs' (name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value) (
            pkgs.lib.filterAttrs (_: value: pkgs.lib.isDerivation value) limitedVmTests
          );

          devShells.default =
            let
              stateDir = ".sinex";
              pgPort = 5432;
            in
            pkgs.mkShell {
              packages = with pkgs; [
                # Rust toolchain (Fenix) — toolchain already includes clippy + rustfmt
                fenixPkgs.toolchain
                fenixPkgs.rust-analyzer
                fenixPkgs.llvm-tools
                fenixPkgs.rust-src

                # Cargo development tools
                cargo-nextest
                cargo-insta
                cargo-llvm-cov
                cargo-fuzz
                cargo-mutants
                cargo-audit
                cargo-deny
                cargo-machete
                cargo-modules
                tokei
                mold
                binutils

                # Infrastructure services
                nats-server
                natscli # nats CLI for stream inspection and admin
                postgresForSqlx

                # Build/runtime dependencies
                jq
                openssl
                pkg-config
                dbus
                dbus.dev
                git-annex
                nsc

                # VM testing
                qemu
                qemu_kvm

                # Shell/Nix tooling
                direnv
                zstd
                git
              ];

              PGUSER = "sinity";
              PGDATABASE = "sinex_dev";
              SINEX_PG_BIN = "${postgresForSqlx}/bin";
              NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";

              shellHook = ''
                export PATH="$PWD/scripts:$PWD/${stateDir}/target/debug:$PATH"
                export LD_LIBRARY_PATH="${
                  pkgs.lib.makeLibraryPath [ pkgs.dbus ]
                }''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                export CLIPPY_CONF_DIR="$PWD/.config"
                export SINEX_DEV_STATE_DIR="$PWD/${stateDir}"
                export SINEX_STATE_DIR="$SINEX_DEV_STATE_DIR/state"
                export SINEX_CACHE_DIR="$SINEX_DEV_STATE_DIR/cache"
                export SINEX_TEST_RESULTS_DIR="$SINEX_CACHE_DIR/test-results"
                export SINEX_NATS_DIR="$SINEX_STATE_DIR/nats"
                export SINEX_DEV_PG_PORT="${toString pgPort}"
                export SINEX_DEV_GATEWAY_PORT="9999"
                export DATABASE_URL="postgresql:///sinex_dev?host=$SINEX_DEV_STATE_DIR/run"
                export PGHOST="$SINEX_DEV_STATE_DIR/run"
                export PGPORT="${toString pgPort}"
                _sinex_checkout_hash_hex="$(printf '%s' "$PWD" | sha256sum | cut -c1-2)"
                _sinex_checkout_hash_byte="$((16#$_sinex_checkout_hash_hex))"
                export SINEX_DEV_NATS_PORT="$((4222 + (_sinex_checkout_hash_byte % 100)))"
                export SINEX_NATS_URL="nats://localhost:$SINEX_DEV_NATS_PORT"
                export SINEX_RPC_URL="https://127.0.0.1:9999"

                # xtask binary path — .sinex/target/debug is on PATH via the export above.
                # Never block direnv: all slow work deferred to first sx/xt invocation.
                _xtask_bin="$PWD/${stateDir}/target/debug/xtask"

                sx() {
                  if [ ! -x "$_xtask_bin" ]; then
                    echo "sinex: building xtask..." >&2
                    cargo build -p xtask </dev/null >&2 || { echo "sinex: xtask build failed" >&2; return 1; }
                  fi
                  "$_xtask_bin" "$@"
                }
                xt() { sx "$@"; }

                # Proactive infra start if binary exists (fire-and-forget).
                # On cold start (no binary), preflight handles it on first sx/xt command.
                # Set SINEX_NO_AUTO_INFRA=1 to skip (useful for remote DB, CI, low-resource machines).
                mkdir -p "$SINEX_DEV_STATE_DIR"
                if [ -x "$_xtask_bin" ] && [ -z "''${SINEX_NO_AUTO_INFRA:-}" ]; then
                  _pg_running=0
                  _nats_running=0

                  pg_isready -q -h "$SINEX_DEV_STATE_DIR/run" -p "${toString pgPort}" 2>/dev/null && _pg_running=1
                  (timeout 1 bash -c ">/dev/tcp/localhost/$SINEX_DEV_NATS_PORT") 2>/dev/null && _nats_running=1

                  if [ "$_pg_running" -eq 1 ] && [ "$_nats_running" -eq 1 ]; then
                    echo "✓  Infrastructure already running (pg:${toString pgPort} nats:$SINEX_DEV_NATS_PORT)" >&2
                  else
                    # Detach from direnv and close inherited extra FDs so long-lived
                    # daemons do not keep direnv's private pipes open.
                    (
                      exec </dev/null >"$SINEX_DEV_STATE_DIR/infra-start.log" 2>&1
                      for _fd_path in /proc/$$/fd/*; do
                        _fd_num="''${_fd_path##*/}"
                        [ "$_fd_num" -le 2 ] && continue
                        eval "exec ''${_fd_num}>&-"
                      done
                      exec setsid "$_xtask_bin" infra start
                    ) &
                    _sinex_infra_starting=1
                    echo "ℹ  Infrastructure starting... (pg:${toString pgPort} nats:$SINEX_DEV_NATS_PORT — log: $SINEX_DEV_STATE_DIR/infra-start.log)" >&2
                  fi
                fi
                # Dev TLS certs are generated lazily by preflight when needed.
                # Set TLS env vars if dev certs exist — enables mTLS automatically.
                if [ -f "$PWD/.sinex/tls/server.pem" ]; then
                  export SINEX_GATEWAY_TLS_CERT="$PWD/.sinex/tls/server.pem"
                  export SINEX_GATEWAY_TLS_KEY="$PWD/.sinex/tls/server-key.pem"
                  export SINEX_GATEWAY_TLS_CLIENT_CA="$PWD/.sinex/tls/ca.pem"
                fi
                # MOTD: show workspace health on shell entry.
                # If infra was just launched, poll for readiness before status
                # so the MOTD reflects actual state instead of racing startup.
                if [ -x "$_xtask_bin" ]; then
                  if [ "''${_sinex_infra_starting:-0}" -eq 1 ]; then
                    _deadline=$((SECONDS + 8))
                    while [ $SECONDS -lt $_deadline ]; do
                      _pg_up=0; _nats_up=0
                      pg_isready -q -h "$SINEX_DEV_STATE_DIR/run" -p "${toString pgPort}" 2>/dev/null && _pg_up=1
                      (timeout 1 bash -c ">/dev/tcp/localhost/$SINEX_DEV_NATS_PORT") 2>/dev/null && _nats_up=1
                      [ "$_pg_up" -eq 1 ] && [ "$_nats_up" -eq 1 ] && break
                      sleep 0.3
                    done
                  fi
                  "$_xtask_bin" status --summary 2>/dev/null || true
                fi
              '';
            };
        }
      );
    in
    systemOutputs
    // {
      # NixOS module
      nixosModules = {
        default =
          { ... }:
          {
            imports = [
              agenix.nixosModules.default
              (import ./nixos)
            ];
          };
        sinex = args: (self.nixosModules.default args);
      };

      nixosConfigurations = {
        example = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                nixpkgs.config.allowUnfree = true;
                # Disable services that require secrets/real infrastructure for evaluation
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.sinex.core.gateway.enable = lib.mkForce false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleMonitoring = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-monitoring.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleDevSandbox = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-dev-sandbox.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleHeadless = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-headless.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleRemoteNode = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-remote-node.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleCoordination = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-coordination.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };
      };

      # Unified overlay: pg_jsonschema + all sinex packages
      overlays.default = nixpkgs.lib.composeExtensions pgJsonschemaOverlay (
        final: prev: {
          inherit (self.packages.${final.system})
            sinex
            sinexctl
            sinex-ingestd
            sinex-gateway
            sinex-fs-ingestor
            sinex-terminal-ingestor
            sinex-desktop-ingestor
            sinex-system-ingestor
            sinex-document-ingestor
            sinex-terminal-command-canonicalizer
            sinex-health-automaton
            sinex-analytics-automaton
            sinex-node-sdk
            ;
        }
      );
    };
}
