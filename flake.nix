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
    };
    agenix = {
      url = "github:ryantm/agenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    inputs@{ self
    , nixpkgs
    , fenix
    , crane
    , agenix
    , flake-utils
    ,
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

          # Build the schema bootstrap binary once, then reuse it in every build
          # that needs a live SQLx validation database.
          schemaApplyBootstrap = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "schema-apply-bootstrap";
              cargoExtraArgs = "-p sinex-schema --bin schema-apply-bootstrap";
              doCheck = false;
              SQLX_OFFLINE = "true";
            }
          );

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
                        #
                        # Build the bootstrap binary once outside the per-package sandbox and
                        # invoke the already-built executable here. Re-running `cargo run` in every
                        # package derivation forces the schema bootstrap path to recompile repeatedly,
                        # which makes the full VM closure builds pathologically slow.
                        export DATABASE_URL="postgresql:///sinex_dev?host=$PGHOST&user=postgres"
                        ${schemaApplyBootstrap}/bin/schema-apply-bootstrap
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
            sinex-session-detector = mkPackage "sinex-session-detector";

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
                sinexPackages.sinex-session-detector
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

          vmCheckOutputs = pkgs.lib.mapAttrs'
            (
              name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value
            )
            (pkgs.lib.filterAttrs (_: value: pkgs.lib.isDerivation value) vmTests);

        in
        rec {
          packages = sinexPackages;

          formatter = pkgs.nixpkgs-fmt;

          checks = vmCheckOutputs;

          devShells.default =
            let
              stateDir = ".sinex";
              pgPort = 5432;
              xtaskCommand = pkgs.writeShellScriptBin "xtask" ''
                set -euo pipefail

                root_dir="''${SINEX_DEV_ROOT:-}"
                if [ -z "$root_dir" ]; then
                  echo "xtask requires the sinex devShell (missing SINEX_DEV_ROOT)" >&2
                  exit 1
                fi

                _sinex_xtask_needs_build() {
                  local bin_path="$root_dir/.sinex/target/debug/xtask"
                  local depfile_path="$root_dir/.sinex/target/debug/xtask.d"
                  local extra_dep

                  [ ! -x "$bin_path" ] && return 0
                  [ ! -r "$depfile_path" ] && return 0

                  for extra_dep in \
                    "$root_dir/Cargo.toml" \
                    "$root_dir/Cargo.lock" \
                    "$root_dir/xtask/Cargo.toml" \
                    "$root_dir/.cargo/config.toml"
                  do
                    if [ -e "$extra_dep" ] && [ "$extra_dep" -nt "$bin_path" ]; then
                      return 0
                    fi
                  done

                  while IFS= read -r dep_path; do
                    [ -z "$dep_path" ] && continue
                    if [ ! -e "$dep_path" ] || [ "$dep_path" -nt "$bin_path" ]; then
                      return 0
                    fi
                  done < <(
                    sed -e 's/^[^:]*: //' -e 's/\\$//' "$depfile_path" \
                      | tr ' ' '\n' \
                      | sed '/^$/d'
                  )

                  return 1
                }

                _sinex_xtask_is_observability_command() {
                  case "''${1:-}" in
                    ""|-h|--help|--version|--list-commands|status|history|analytics|jobs|snapshot)
                      return 0
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                bin_path="$root_dir/.sinex/target/debug/xtask"
                force_rebuild="''${SINEX_XTASK_FORCE_REBUILD:-0}"

                cd "$root_dir"
                if [ -x "$bin_path" ] \
                  && [ "$force_rebuild" != "1" ] \
                  && _sinex_xtask_is_observability_command "$@"
                then
                  if _sinex_xtask_needs_build; then
                    echo "ℹ  Using existing xtask binary for read-only command while sources are newer" >&2
                  fi
                  exec "$bin_path" "$@"
                fi

                if [ "$force_rebuild" = "1" ] || _sinex_xtask_needs_build; then
                  echo "ℹ  Rebuilding checkout-local xtask..." >&2
                  cargo build --quiet -p xtask
                fi
                exec "$bin_path" "$@"
              '';
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
                xtaskCommand
              ];

              PGUSER = "sinity";
              PGDATABASE = "sinex_dev";
              SINEX_PG_BIN = "${postgresForSqlx}/bin";
              NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";

              shellHook = ''
                _sinex_path_append_unique() {
                  case ":$PATH:" in
                    *":$1:"*) ;;
                    *) PATH="''${PATH:+$PATH:}$1" ;;
                  esac
                }
                _sinex_path_append_unique "$PWD/${stateDir}/target/debug"
                export PATH
                export LD_LIBRARY_PATH="${
                  pkgs.lib.makeLibraryPath [ pkgs.dbus ]
                }''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                export CLIPPY_CONF_DIR="$PWD/.config"
                export SINEX_DEV_ROOT="$PWD"
                export SINEX_DEV_STATE_DIR="$PWD/${stateDir}"
                export SINEX_DEV_TOOLCHAIN="${rustToolchain.name}"
                export SINEX_STATE_DIR="$SINEX_DEV_STATE_DIR/state"
                export SINEX_CACHE_DIR="$SINEX_DEV_STATE_DIR/cache"
                export SINEX_TEST_RESULTS_DIR="$SINEX_CACHE_DIR/test-results"
                export SINEX_NATS_DIR="$SINEX_STATE_DIR/nats"
                export SINEX_DEV_PG_PORT="${toString pgPort}"
                export DATABASE_URL="postgresql:///sinex_dev?host=$SINEX_DEV_STATE_DIR/run"
                export PGHOST="$SINEX_DEV_STATE_DIR/run"
                export PGPORT="${toString pgPort}"
                _sinex_checkout_hash_hex="$(printf '%s' "$PWD" | sha256sum | cut -c1-2)"
                _sinex_checkout_hash_byte="$((16#$_sinex_checkout_hash_hex))"
                export SINEX_DEV_GATEWAY_PORT="$((19000 + _sinex_checkout_hash_byte))"
                export SINEX_DEV_NATS_PORT="$((4222 + (_sinex_checkout_hash_byte % 100)))"
                export SINEX_NATS_URL="nats://localhost:$SINEX_DEV_NATS_PORT"
                export SINEX_GATEWAY_TCP_LISTEN="127.0.0.1:$SINEX_DEV_GATEWAY_PORT"
                export SINEX_GATEWAY_URL="https://127.0.0.1:$SINEX_DEV_GATEWAY_PORT"
                export SINEX_RPC_URL="$SINEX_GATEWAY_URL"

                # Dev TLS certs are generated lazily by preflight when needed.
                # Set TLS env vars if dev certs exist — enables mTLS automatically.
                if [ -f "$PWD/.sinex/tls/server.pem" ]; then
                  export SINEX_GATEWAY_TLS_CERT="$PWD/.sinex/tls/server.pem"
                  export SINEX_GATEWAY_TLS_KEY="$PWD/.sinex/tls/server-key.pem"
                  export SINEX_GATEWAY_TLS_CLIENT_CA="$PWD/.sinex/tls/ca.pem"
                fi
                xtask --format silent docs sync >/dev/null 2>&1
                if [ -t 1 ]; then
                  # The plain `xtask` command is provided by the devShell and delegates
                  # to `cargo build -p xtask` plus the checkout-local binary.
                  # Set SINEX_NO_AUTO_INFRA=1 to skip (useful for remote DB or low-resource machines).
                  if [ -z "''${SINEX_NO_AUTO_INFRA:-}" ]; then
                    _pg_running=0
                    _nats_running=0
                    _sinex_infra_start_lock="$SINEX_DEV_STATE_DIR/infra-start.lock"
                    _sinex_infra_start_log="$SINEX_DEV_STATE_DIR/infra-start.log"
                    _sinex_infra_start_current_log="$SINEX_DEV_STATE_DIR/infra-start.current.log"

                    pg_isready -q -h "$SINEX_DEV_STATE_DIR/run" -p "${toString pgPort}" 2>/dev/null && _pg_running=1
                    (timeout 1 bash -c ">/dev/tcp/localhost/$SINEX_DEV_NATS_PORT") 2>/dev/null && _nats_running=1

                    if [ "$_pg_running" -eq 1 ] && [ "$_nats_running" -eq 1 ]; then
                      echo "✓  Infrastructure already running (pg:${toString pgPort} nats:$SINEX_DEV_NATS_PORT)" >&2
                    else
                      if mkdir "$_sinex_infra_start_lock" 2>/dev/null; then
                        # Detach from direnv and close inherited extra FDs so long-lived
                        # daemons do not keep direnv's private pipes open.
                        (
                          trap 'if [ -f "$_sinex_infra_start_current_log" ]; then mv -f "$_sinex_infra_start_current_log" "$_sinex_infra_start_log" 2>/dev/null || cp "$_sinex_infra_start_current_log" "$_sinex_infra_start_log" 2>/dev/null || true; fi; rmdir "$_sinex_infra_start_lock"' EXIT
                          : >"$_sinex_infra_start_current_log"
                          exec </dev/null >>"$_sinex_infra_start_current_log" 2>&1
                          for _fd_path in /proc/$$/fd/*; do
                            _fd_num="''${_fd_path##*/}"
                            [ "$_fd_num" -le 2 ] && continue
                            eval "exec ''${_fd_num}>&-"
                          done
                          # This log is for operators inspecting shell-hook startup,
                          # so keep it human-readable instead of JSON-fragment prone.
                          setsid xtask --format human infra start
                        ) &
                        _sinex_infra_starting=1
                        echo "ℹ  Infrastructure starting... (pg:${toString pgPort} nats:$SINEX_DEV_NATS_PORT — live log: $_sinex_infra_start_current_log)" >&2
                      else
                        _sinex_infra_starting=1
                        echo "ℹ  Infrastructure already starting... (pg:${toString pgPort} nats:$SINEX_DEV_NATS_PORT — live log: $_sinex_infra_start_current_log)" >&2
                      fi
                    fi
                  fi
                  # Show workspace health on shell entry. If infra was just launched,
                  # poll for readiness before status so the summary reflects actual state.
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
                  xtask status --summary || true
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
        default = import ./nixos;
        "with-agenix" =
          { ... }:
          {
            imports = [
              agenix.nixosModules.default
              self.nixosModules.default
            ];
          };
      };

      nixosConfigurations = {
        workstation = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/examples/workstation.nix
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

        monitoring = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/examples/monitoring.nix
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

        devSandbox = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/examples/dev-sandbox.nix
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

        headless = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/examples/headless.nix
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

        remoteNode = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/examples/remote-node.nix
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

        coordination = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/examples/coordination.nix
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
            sinex-session-detector
            sinex-node-sdk
            ;
        }
      );
    };
}
