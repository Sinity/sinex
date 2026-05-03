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
            ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.systemd ];

            nativeBuildInputs = with pkgs; [
              pkg-config
              protobuf
              mold # .cargo/config.toml: link-arg=-fuse-ld=mold
            ];

          };

          # Build workspace dependencies once (cached layer).
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          # Build the schema bootstrap binary once, then reuse it in every build
          # that needs a live SQLx validation database.
          schemaApplyBootstrap = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "schema-apply-bootstrap";
              cargoExtraArgs = "-p sinex-schema --bin schema-apply-bootstrap";
              doCheck = false;
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

            if ! ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -l "$TMPDIR/postgres.log" -w -t 180 start; then
              cat "$TMPDIR/postgres.log" >&2 || true
              exit 1
            fi

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
          # preBuild starts an ephemeral Postgres and sets DATABASE_URL so
          # sqlx::query! macros validate against a live schema.
          mkPackage =
            pname:
            craneLib.buildPackage (
              commonArgs
              // {
                inherit cargoArtifacts pname;
                cargoExtraArgs = "-p ${pname}";
                doCheck = false;

                preBuild = postgresPreBuild;
                postBuild = postgresPostBuild;
              }
            );

          runtimePackageNames = [
            "sinex-ingestd"
            "sinex-gateway"
            "sinexctl"
            "sinex-fs-ingestor"
            "sinex-terminal-ingestor"
            "sinex-browser-ingestor"
            "sinex-desktop-ingestor"
            "sinex-system-ingestor"
            "sinex-document-ingestor"
            "sinex-terminal-command-canonicalizer"
            "sinex-health-automaton"
            "sinex-analytics-automaton"
            "sinex-session-detector"
            "sinex-hourly-summarizer"
            "sinex-daily-summarizer"
            "sinex-node-sdk"
            "xtask"
          ];

          runtimeCargoExtraArgs = pkgs.lib.concatMapStringsSep " " (pname: "-p ${pname}") runtimePackageNames;

          sinexRuntime = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "sinex";
              cargoExtraArgs = runtimeCargoExtraArgs;
              doCheck = false;

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
            sinex-browser-ingestor = mkPackage "sinex-browser-ingestor";
            sinex-desktop-ingestor = mkPackage "sinex-desktop-ingestor";
            sinex-system-ingestor = mkPackage "sinex-system-ingestor";
            sinex-document-ingestor = mkPackage "sinex-document-ingestor";

            # Automatons (processing nodes)
            sinex-terminal-command-canonicalizer = mkPackage "sinex-terminal-command-canonicalizer";
            sinex-health-automaton = mkPackage "sinex-health-automaton";
            sinex-analytics-automaton = mkPackage "sinex-analytics-automaton";
            sinex-session-detector = mkPackage "sinex-session-detector";
            sinex-hourly-summarizer = mkPackage "sinex-hourly-summarizer";
            sinex-daily-summarizer = mkPackage "sinex-daily-summarizer";

            # Node SDK binaries (sinex-preflight lives here)
            sinex-node-sdk = mkPackage "sinex-node-sdk";

            # Developer tooling (used by VM concurrency tests)
            xtask = mkPackage "xtask";

            # NixOS VM test suite (Rust binary replacing Python testScript assertions)
            sinex-vm-test-suite = mkPackage "sinex-vm-test-suite";

            # Aggregated suite with all runtime binaries. Build this as one
            # derivation so SQLx's live Postgres validation runs once for the
            # deployed runtime instead of once per node package.
            sinex = sinexRuntime;

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

          vmCheckOutputs = pkgs.lib.mapAttrs' (name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value) (
            pkgs.lib.filterAttrs (_: value: pkgs.lib.isDerivation value) vmTests
          );

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

                cargo_target_dir="''${CARGO_TARGET_DIR:-''${SINEX_DEV_CACHE_ROOT:-$root_dir/.sinex/cache}/target}"
                bin_path="$cargo_target_dir/debug/xtask"
                build_lock_dir="$root_dir/.sinex/state/xtask-build.lock"
                build_failure_stamp="$root_dir/.sinex/state/xtask-build.failed"
                build_failure_log="$root_dir/.sinex/state/xtask-build.failed.log"
                force_rebuild="''${SINEX_XTASK_FORCE_REBUILD:-0}"

                _sinex_xtask_normalize_global_args() {
                  local global_args=()
                  local command_args=()

                  while [ "$#" -gt 0 ]; do
                    case "$1" in
                      --json|--list-commands|--bg|--fg)
                        global_args+=("$1")
                        shift
                        ;;
                      --format)
                        if [ "$#" -ge 2 ]; then
                          global_args+=("$1" "$2")
                          shift 2
                        else
                          command_args+=("$1")
                          shift
                        fi
                        ;;
                      --format=*)
                        global_args+=("$1")
                        shift
                        ;;
                      -v|-vv|-vvv)
                        global_args+=("$1")
                        shift
                        ;;
                      *)
                        command_args+=("$1")
                        shift
                        ;;
                    esac
                  done

                  set -- "''${global_args[@]}" "''${command_args[@]}"
                  printf '%s\n' "$@"
                }

                _sinex_xtask_sources_newer_than() {
                  local ref_path="$1"
                  local depfile_path="$cargo_target_dir/debug/xtask.d"
                  local extra_dep dep_path

                  [ ! -e "$ref_path" ] && return 0
                  [ ! -r "$depfile_path" ] && return 0

                  for extra_dep in \
                    "$root_dir/Cargo.toml" \
                    "$root_dir/Cargo.lock" \
                    "$root_dir/xtask/Cargo.toml" \
                    "$root_dir/.cargo/config.toml"
                  do
                    if [ ! -e "$extra_dep" ] || [ "$extra_dep" -nt "$ref_path" ]; then
                      return 0
                    fi
                  done

                  while IFS= read -r dep_path; do
                    [ -z "$dep_path" ] && continue
                    if [ ! -e "$dep_path" ] || [ "$dep_path" -nt "$ref_path" ]; then
                      return 0
                    fi
                  done < <(
                    sed -e 's/^[^:]*: //' -e 's/\\$//' "$depfile_path" \
                      | tr ' ' '\n' \
                      | sed '/^$/d'
                  )

                  return 1
                }

                _sinex_xtask_needs_build() {
                  [ ! -x "$bin_path" ] && return 0
                  _sinex_xtask_sources_newer_than "$bin_path"
                }

                _sinex_xtask_failed_build_is_current() {
                  [ "$force_rebuild" = "1" ] && return 1
                  [ ! -e "$build_failure_stamp" ] && return 1
                  ! _sinex_xtask_sources_newer_than "$build_failure_stamp"
                }

                _sinex_xtask_report_current_failure() {
                  echo "✗ checkout-local xtask rebuild is currently broken for these sources; not retrying until sources change or SINEX_XTASK_FORCE_REBUILD=1" >&2
                  if [ -r "$build_failure_log" ]; then
                    echo "  log: $build_failure_log" >&2
                  fi
                }

                _sinex_xtask_is_observability_command() {
                  local command_name
                  command_name="$(_sinex_xtask_command_name "$@")"
                  case "$command_name" in
                    ""|-h|--help|--version|--list-commands|status|history|analytics|jobs|snapshot)
                      return 0
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_xtask_command_name() {
                  while [ "$#" -gt 0 ]; do
                    case "$1" in
                      --json|--list-commands|--bg|--fg|-v|-vv|-vvv)
                        shift
                        ;;
                      --format)
                        if [ "$#" -ge 2 ]; then
                          shift 2
                        else
                          shift
                        fi
                        ;;
                      --format=*)
                        shift
                        ;;
                      *)
                        printf '%s\n' "$1"
                        return 0
                        ;;
                    esac
                  done
                }

                _sinex_xtask_can_use_existing_binary() {
                  local command_name
                  command_name="$(_sinex_xtask_command_name "$@")"
                  case "$command_name" in
                    ""|-h|--help|--version|--list-commands|status|history|analytics|jobs|snapshot|check|test|build|deps|doctor|infra|docs)
                      return 0
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_xtask_wait_for_existing_build() {
                  while [ -d "$build_lock_dir" ]; do
                    if [ -r "$build_lock_dir/pid" ]; then
                      _lock_pid="$(cat "$build_lock_dir/pid" 2>/dev/null || true)"
                      if [ -n "$_lock_pid" ] && ! kill -0 "$_lock_pid" 2>/dev/null; then
                        rm -rf "$build_lock_dir"
                        continue
                      fi
                    fi

                    if [ "$force_rebuild" != "1" ] && [ -x "$bin_path" ] && ! _sinex_xtask_needs_build; then
                      return 0
                    fi
                    sleep 0.1
                  done
                  return 1
                }

                _sinex_xtask_build_with_lock() {
                  mkdir -p "$root_dir/.sinex/state"

                  while ! mkdir "$build_lock_dir" 2>/dev/null; do
                    if _sinex_xtask_wait_for_existing_build; then
                      return 0
                    fi
                  done

                  printf '%s\n' "$$" > "$build_lock_dir/pid"
                  trap 'rm -rf "$build_lock_dir"' EXIT INT TERM

                  if _sinex_xtask_failed_build_is_current; then
                    rm -rf "$build_lock_dir"
                    trap - EXIT INT TERM
                    return 1
                  fi

                  if [ "$force_rebuild" = "1" ] || _sinex_xtask_needs_build; then
                    echo "ℹ  Rebuilding checkout-local xtask..." >&2
                    if _sinex_xtask_build_checkout_binary >"$build_failure_log" 2>&1; then
                      rm -f "$build_failure_stamp" "$build_failure_log"
                    else
                      printf '%s\n' "$(date -Iseconds)" > "$build_failure_stamp"
                      cat "$build_failure_log" >&2 || true
                      rm -rf "$build_lock_dir"
                      trap - EXIT INT TERM
                      return 1
                    fi
                  fi

                  rm -rf "$build_lock_dir"
                  trap - EXIT INT TERM
                }

                _sinex_xtask_build_checkout_binary() {
                  (
                    local sqlx_tmp pgdata pglog build_rc

                    sqlx_tmp="$(mktemp -d "$root_dir/.sinex/state/xtask-sqlx.XXXXXX")"
                    pgdata="$sqlx_tmp/pgdata"
                    pglog="$sqlx_tmp/postgres.log"
                    build_rc=0

                    export PGDATA="$pgdata"
                    export PGHOST="$sqlx_tmp"
                    export PGPORT="$((55433 + ($$ % 1000)))"

                    if ! ${postgresForSqlx}/bin/initdb -D "$PGDATA" --locale=C --encoding=UTF8 --auth=trust --username=postgres; then
                      rm -rf "$sqlx_tmp"
                      return 1
                    fi

                    echo "unix_socket_directories = '$PGHOST'" >> "$PGDATA/postgresql.conf"
                    echo "port = $PGPORT" >> "$PGDATA/postgresql.conf"
                    echo "shared_preload_libraries = 'timescaledb'" >> "$PGDATA/postgresql.conf"

                    if ! ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -l "$pglog" -w -t 180 start; then
                      cat "$pglog" >&2 || true
                      rm -rf "$sqlx_tmp"
                      return 1
                    fi

                    if ! ${postgresForSqlx}/bin/createdb -h "$PGHOST" -p "$PGPORT" -U postgres sinex_dev; then
                      ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -m fast stop || true
                      rm -rf "$sqlx_tmp"
                      return 1
                    fi

                    export DATABASE_URL="postgresql:///sinex_dev?host=$PGHOST&user=postgres"

                    if ! ${schemaApplyBootstrap}/bin/schema-apply-bootstrap; then
                      ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -m fast stop || true
                      rm -rf "$sqlx_tmp"
                      return 1
                    fi

                    cargo build --quiet -p xtask || build_rc=$?
                    ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -m fast stop || true
                    rm -rf "$sqlx_tmp"
                    return "$build_rc"
                  )
                }

                cd "$root_dir"
                _normalized_args=()
                while IFS= read -r _arg; do
                  _normalized_args+=("$_arg")
                done < <(_sinex_xtask_normalize_global_args "$@")
                set -- "''${_normalized_args[@]}"

                if [ -x "$bin_path" ] \
                  && [ "$force_rebuild" != "1" ] \
                  && _sinex_xtask_can_use_existing_binary "$@"
                then
                  if _sinex_xtask_needs_build; then
                    if _sinex_xtask_failed_build_is_current; then
                      echo "ℹ  Using existing xtask binary; local rebuild is currently broken for these sources" >&2
                      if [ -r "$build_failure_log" ]; then
                        echo "  log: $build_failure_log" >&2
                      fi
                    elif _sinex_xtask_is_observability_command "$@"; then
                      echo "ℹ  Using existing xtask binary for read-only command while sources are newer" >&2
                    else
                      if ! _sinex_xtask_build_with_lock; then
                        if _sinex_xtask_failed_build_is_current; then
                          echo "ℹ  Falling back to existing xtask binary after rebuild failure" >&2
                          if [ -r "$build_failure_log" ]; then
                            echo "  log: $build_failure_log" >&2
                          fi
                          exec "$bin_path" "$@"
                        fi
                        exit 1
                      fi
                      exec "$bin_path" "$@"
                    fi
                  fi
                  exec "$bin_path" "$@"
                fi

                if [ "$force_rebuild" = "1" ] || _sinex_xtask_needs_build; then
                  if ! _sinex_xtask_build_with_lock; then
                    if _sinex_xtask_failed_build_is_current; then
                      if [ -x "$bin_path" ] && _sinex_xtask_can_use_existing_binary "$@"; then
                        echo "ℹ  Falling back to existing xtask binary after rebuild failure" >&2
                        if [ -r "$build_failure_log" ]; then
                          echo "  log: $build_failure_log" >&2
                        fi
                        exec "$bin_path" "$@"
                      fi
                      _sinex_xtask_report_current_failure
                      exit 101
                    fi
                    exit 1
                  fi
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
                _sinex_default_cache_root() {
                  local root="$1"
                  local root_name
                  local root_hash
                  local candidate
                  local probe
                  root_name="$(basename "$root" | tr -c 'A-Za-z0-9._-' '-')"
                  root_name="''${root_name%-}"
                  if [ -z "$root_name" ]; then
                    root_name="workspace"
                  fi
                  root_hash="$(printf '%s' "$root" | sha256sum | cut -c1-12)"
                  if [ -d /cache ]; then
                    candidate="/cache/sinex/$root_name-$root_hash"
                    probe="$candidate/.write-probe-$$"
                    if mkdir -p "$candidate" 2>/dev/null && : > "$probe" 2>/dev/null; then
                      rm -f "$probe"
                      printf '%s\n' "$candidate"
                      return
                    fi
                    rm -f "$probe" 2>/dev/null || true
                  fi
                  printf '%s/.sinex/cache\n' "$root"
                }
                export SINEX_DEV_ROOT="$PWD"
                export SINEX_DEV_STATE_DIR="$PWD/${stateDir}"
                export SINEX_DEV_TOOLCHAIN="${rustToolchain.name}"
                if [ -z "''${SINEX_DEV_CACHE_ROOT:-}" ]; then
                  export SINEX_DEV_CACHE_ROOT="$(_sinex_default_cache_root "$SINEX_DEV_ROOT")"
                fi
                mkdir -p "$SINEX_DEV_CACHE_ROOT/target" "$SINEX_DEV_CACHE_ROOT/cache"
                if [ -z "''${CARGO_TARGET_DIR:-}" ]; then
                  export CARGO_TARGET_DIR="$SINEX_DEV_CACHE_ROOT/target"
                fi
                _sinex_path_append_unique "$CARGO_TARGET_DIR/debug"
                export PATH
                export LD_LIBRARY_PATH="${
                  pkgs.lib.makeLibraryPath [ pkgs.dbus ]
                }''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                export CLIPPY_CONF_DIR="$PWD/.config"
                export SINEX_STATE_DIR="$SINEX_DEV_STATE_DIR/state"
                export SINEX_CACHE_DIR="$SINEX_DEV_CACHE_ROOT/cache"
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
                if [ -t 1 ]; then
                  # Keep shell entry cheap by default. Heavy dev conveniences are
                  # opt-in so direnv, one-shot commands, and fresh shells do not
                  # silently compile xtask or launch infra.
                  _sinex_infra_starting=0

                  if [ "''${SINEX_AUTO_DOCS_SYNC:-0}" = 1 ]; then
                    xtask --format silent docs sync >/dev/null 2>&1 || true
                  fi

                  if [ "''${SINEX_AUTO_INFRA:-0}" = 1 ]; then
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

                  if [ "''${SINEX_AUTO_STATUS:-0}" = 1 ]; then
                    # If infra was just launched, poll for readiness before status
                    # so the summary reflects actual state.
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
                  elif [ "''${SINEX_SHELL_BANNER:-1}" = 1 ]; then
                    echo "sinex devshell ready; live status: xtask status --summary; auto status: SINEX_AUTO_STATUS=1" >&2
                  fi
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
            sinex-hourly-summarizer
            sinex-daily-summarizer
            sinex-node-sdk
            ;
        }
      );
    };
}
