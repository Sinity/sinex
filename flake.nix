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

      # pg_jsonschema is sourced from Supabase's amd64 Linux release artifact,
      # and the NixOS VM/check surface is Linux-only. Keeping the flake system
      # set explicit avoids evaluating unsupported Darwin outputs on every
      # broad flake check.
      supportedSystems = [ "x86_64-linux" ];

      runtimePackageNames = [
        "sinexd"
        "sinexctl"
        "xtask"
      ];

      packageOutputNames = runtimePackageNames ++ [
        "sinex-vm-test-suite"
        "sinex"
      ];

      # System-specific outputs
      systemOutputs = flake-utils.lib.eachSystem supportedSystems (
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

          runtimeCargoExtraArgs = pkgs.lib.concatMapStringsSep " " (pname: "-p ${pname}") runtimePackageNames;
          vmRuntimePackageNames = [
            "sinexd"
            "sinexctl"
          ];
          vmRuntimeCargoExtraArgs = pkgs.lib.concatMapStringsSep " " (pname: "-p ${pname}") vmRuntimePackageNames;

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

          sinexVmRuntime = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "sinex-vm-runtime";
              cargoExtraArgs = vmRuntimeCargoExtraArgs;
              doCheck = false;
              # VM checks need deployment-shaped binaries, not optimized
              # production codegen. Keep this local to the VM runtime
              # derivation so restoring exported checks does not make
              # workstation proof runs lose to earlyoom during sinexd lib
              # optimization.
              CARGO_PROFILE_RELEASE_OPT_LEVEL = "0";
              CARGO_PROFILE_RELEASE_CODEGEN_UNITS = "16";
              CARGO_PROFILE_RELEASE_PANIC = "abort";
              CARGO_BUILD_JOBS = "1";

              preBuild = postgresPreBuild;
              postBuild = postgresPostBuild;
            }
          );

          sinexVmPackage = pkgs.symlinkJoin {
            name = "sinex-vm-package";
            paths = [
              sinexVmRuntime
              sinexPackages.xtask
            ];
          };

          # All packages built from Cargo.toml names. Keep the inventory in
          # packageOutputNames/runtimePackageNames so Nix outputs, the aggregate
          # runtime closure, and the overlay cannot drift independently.
          sinexPackages = pkgs.lib.genAttrs (runtimePackageNames ++ [ "sinex-vm-test-suite" ]) mkPackage // {
            sinex = sinexRuntime;

            schema-apply-bootstrap = schemaApplyBootstrap;

            pg_jsonschema = pkgs.postgresql18Packages.pg_jsonschema;

            default = sinexPackages.sinex;
          };

          vmCheckOutputs =
            let
              vmScenarios = import ./tests/e2e/nixos-vm/default.nix {
                inherit pkgs;
                pg_jsonschema = pkgs.postgresql18Packages.pg_jsonschema;
                sinex = sinexVmPackage;
                sinexCli = sinexVmRuntime;
                xtask = sinexPackages.xtask;
                sinexVmTestSuite = sinexPackages.sinex-vm-test-suite;
              };
            in
            pkgs.lib.mapAttrs'
              (name: value: {
                name = "sinex-vm-${name}";
                inherit value;
              })
              vmScenarios;

          nixFormatCheck = pkgs.runCommand "sinex-nix-format-check"
            {
              nativeBuildInputs = [ pkgs.nixpkgs-fmt ];
              src = pkgs.lib.cleanSourceWith {
                src = ./.;
                filter = path: type:
                  let
                    rel = pkgs.lib.removePrefix (toString ./. + "/") (toString path);
                  in
                  type == "directory" || rel == "flake.nix";
              };
            } ''
            nixpkgs-fmt --check "$src/flake.nix"
            touch "$out"
          '';

          sourceCatalogEvalCheck =
            let
              catalog = import ./nixos/modules/lib/source-catalog.nix { lib = pkgs.lib; };
              consumerConfig = nixpkgs.lib.nixosSystem {
                modules = [
                  (
                    { lib, ... }:
                    {
                      nixpkgs.hostPlatform = system;
                      nixpkgs.overlays = [ pgJsonschemaOverlay ];
                      boot.isContainer = true;
                      boot.loader.grub.enable = false;
                      fileSystems."/" = {
                        device = "none";
                        fsType = "tmpfs";
                      };
                      services.sinex = {
                        enable = true;
                        package = pkgs.runCommand "sinexd-catalog-eval-package" { } "mkdir -p $out/bin";
                        adminPackage = pkgs.runCommand "xtask-catalog-eval-package" { } "mkdir -p $out/bin";
                        cliPackage = null;
                        users.target = "catalog-user";
                        database.enable = lib.mkForce false;
                        nats.enable = lib.mkForce false;
                        nats.autoSetup = lib.mkForce false;
                        lifecycle.preflight.enable = false;
                        lifecycle.updates.enable = false;
                        sources.document.runOnBoot = false;
                        sources.document.schedule = null;
                      };
                      users.users.catalog-user = {
                        isNormalUser = true;
                        home = "/home/catalog-user";
                      };
                      system.stateVersion = "24.05";
                    }
                  )
                  ./nixos
                ];
              };
              sinexdServiceConfig = consumerConfig.config.systemd.services.sinexd.serviceConfig;
              sinexdEnv = sinexdServiceConfig.Environment or [ ];
              hasSourceManifestEnv =
                builtins.any
                  (value: pkgs.lib.hasPrefix "SINEX_SOURCE_BINDINGS_PATH=" value)
                  sinexdEnv;
              consumerAssertions =
                if !hasSourceManifestEnv then
                  throw "source catalog consumer did not render SINEX_SOURCE_BINDINGS_PATH"
                else if !(sinexdServiceConfig ? MemoryMax) then
                  throw "source catalog consumer did not render catalog-derived sinexd MemoryMax"
                else { sinexdMemoryMax = sinexdServiceConfig.MemoryMax; };
              requiredSources = catalog.requireFieldsFor [
                "fs"
                "terminal.atuin-history"
                "terminal.bash-history"
                "terminal.fish-history"
                "terminal.monitor"
                "terminal.zsh-history"
                "browser.history"
                "desktop.activitywatch"
                "desktop.clipboard"
                "desktop.window-manager"
                "document.staging"
                "system.dbus"
                "system.journald"
                "system.monitor"
                "system.systemd"
                "system.udev"
              ];
              evalSummary = builtins.toJSON {
                inherit (catalog) entryCount schemaVersion;
                inherit (consumerAssertions) sinexdMemoryMax;
                required = builtins.attrNames requiredSources;
              };
            in
            pkgs.runCommand "source-catalog-eval" { } ''
              printf '%s\n' ${pkgs.lib.escapeShellArg evalSummary} > "$out"
            '';

        in
        rec {
          packages = sinexPackages;

          formatter = pkgs.nixpkgs-fmt;

          checks = vmCheckOutputs // {
            flake-format = nixFormatCheck;
            source-catalog-eval = sourceCatalogEvalCheck;
          };

          devShells.default =
            let
              stateDir = ".sinex";
              pgPort = 5432;
              cargoCommand = pkgs.writeShellScriptBin "cargo" ''
                set -euo pipefail

                root_dir="''${SINEX_DEV_ROOT:-}"
                if [ -z "$root_dir" ]; then
                  exec ${fenixPkgs.toolchain}/bin/cargo "$@"
                fi

                real_cargo="${fenixPkgs.toolchain}/bin/cargo"
                pgdata="$SINEX_DEV_STATE_DIR/data/postgres"
                pgrun="$SINEX_DEV_STATE_DIR/run"
                pglog="$SINEX_DEV_STATE_DIR/run/logs"
                pgport="''${PGPORT:-5432}"
                runtime_conf="$pgdata/sinex-dev.conf"
                include_line="include_if_exists = '$runtime_conf'"
                dev_user="''${USER:-$(id -un)}"
                bootstrap_lock_dir="$SINEX_DEV_STATE_DIR/cargo-sqlx-bootstrap.lock"
                bootstrap_log="$pglog/cargo-sqlx-bootstrap.log"

                _sinex_schema_apply_bootstrap_bin() {
                  local bootstrap_bin bootstrap_out cached_path cache_fingerprint cache_fingerprint_file cache_path_file current_fingerprint

                  if ! command -v nix >/dev/null 2>&1; then
                    echo "nix is required to build schema-apply-bootstrap lazily" >>"$bootstrap_log"
                    return 127
                  fi

                  cache_fingerprint_file="$SINEX_DEV_STATE_DIR/schema-apply-bootstrap.fingerprint"
                  cache_path_file="$SINEX_DEV_STATE_DIR/schema-apply-bootstrap.path"
                  current_fingerprint="$(_sinex_schema_apply_bootstrap_fingerprint || true)"

                  if [ -n "$current_fingerprint" ] \
                    && [ -r "$cache_fingerprint_file" ] \
                    && [ -r "$cache_path_file" ]
                  then
                    cache_fingerprint="$(cat "$cache_fingerprint_file" 2>/dev/null || true)"
                    cached_path="$(cat "$cache_path_file" 2>/dev/null || true)"
                    if [ "$cache_fingerprint" = "$current_fingerprint" ] && [ -x "$cached_path" ]; then
                      printf '%s\n' "$cached_path"
                      return 0
                    fi
                  fi

                  bootstrap_out="$(
                    nix --no-warn-dirty \
                      --extra-experimental-features 'nix-command flakes' \
                      build \
                      --no-link \
                      --print-out-paths \
                      "$root_dir#schema-apply-bootstrap" \
                      2>>"$bootstrap_log"
                  )" || return $?

                  bootstrap_bin="$bootstrap_out/bin/schema-apply-bootstrap"
                  if [ ! -x "$bootstrap_bin" ]; then
                    echo "schema-apply-bootstrap output is missing executable: $bootstrap_bin" >>"$bootstrap_log"
                    return 1
                  fi
                  if [ -n "$current_fingerprint" ]; then
                    mkdir -p "$SINEX_DEV_STATE_DIR"
                    printf '%s\n' "$current_fingerprint" >"$cache_fingerprint_file"
                    printf '%s\n' "$bootstrap_bin" >"$cache_path_file"
                  fi
                  printf '%s\n' "$bootstrap_bin"
                }

                _sinex_schema_apply_bootstrap_fingerprint() {
                  (
                    cd "$root_dir"
                    {
                      printf '%s\n' schema-apply-bootstrap-cache-v1
                      git ls-files \
                        Cargo.toml \
                        Cargo.lock \
                        flake.nix \
                        crate/sinex-schema \
                        crate/sinex-primitives
                      git ls-files --others --exclude-standard \
                        Cargo.toml \
                        Cargo.lock \
                        flake.nix \
                        crate/sinex-schema \
                        crate/sinex-primitives
                    } \
                      | LC_ALL=C sort -u \
                      | while IFS= read -r rel_path; do
                        [ -n "$rel_path" ] || continue
                        [ -f "$rel_path" ] || continue
                        printf '%s\n' "$rel_path"
                        sha256sum "$rel_path"
                      done \
                      | sha256sum \
                      | awk '{print $1}'
                  )
                }

                _sinex_cargo_command_name() {
                  while [ "$#" -gt 0 ]; do
                    case "$1" in
                      +*)
                        shift
                        ;;
                      -Z|--config|--manifest-path|--color)
                        if [ "$#" -ge 2 ]; then
                          shift 2
                        else
                          shift
                        fi
                        ;;
                      -Z*|--config=*|--manifest-path=*|--color=*)
                        shift
                        ;;
                      -h|--help|-V|--version)
                        printf '%s\n' "$1"
                        return 0
                        ;;
                      -*)
                        shift
                        ;;
                      *)
                        printf '%s\n' "$1"
                        return 0
                        ;;
                    esac
                  done
                }

                _sinex_cargo_requires_sqlx_database() {
                  local command_name
                  command_name="$(_sinex_cargo_command_name "$@" || true)"
                  case "$command_name" in
                    build|check|test|clippy|run|doc|bench|nextest)
                      return 0
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_cargo_direct_command_replacement() {
                  local command_name
                  command_name="$(_sinex_cargo_command_name "$@" || true)"
                  case "$command_name" in
                    test|nextest)
                      printf '%s\n' "xtask test"
                      ;;
                    check)
                      printf '%s\n' "xtask check"
                      ;;
                    clippy)
                      printf '%s\n' "xtask check --lint"
                      ;;
                    fmt)
                      printf '%s\n' "xtask check --fmt"
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_cargo_guard_xtask_surface() {
                  if [ "''${SINEX_XTASK_MANAGED_CARGO:-0}" = 1 ] || [ "''${SINEX_ALLOW_DIRECT_CARGO:-0}" = 1 ]; then
                    return 0
                  fi

                  local replacement
                  if replacement="$(_sinex_cargo_direct_command_replacement "$@")"; then
                    echo "✗ Direct cargo $(_sinex_cargo_command_name "$@" || printf command) is disabled in the Sinex devshell." >&2
                    echo "  Use the repo-native gate instead: $replacement" >&2
                    echo "  For xtask/tooling debugging only, rerun with SINEX_ALLOW_DIRECT_CARGO=1." >&2
                    return 2
                  fi

                  return 0
                }

                _sinex_cargo_write_runtime_config() {
                  {
                    printf "unix_socket_directories = '%s'\n" "$pgrun"
                    printf "%s = '%s'\n" "listen_addresses" ""
                    printf "port = %s\n" "$pgport"
                    printf "max_connections = 128\n"
                    printf "max_worker_processes = 6\n"
                    printf "shared_buffers = '32MB'\n"
                    printf "shared_preload_libraries = 'timescaledb'\n"
                    printf "timescaledb.max_background_workers = 2\n"
                    printf "log_destination = 'stderr'\n"
                    printf "logging_collector = on\n"
                    printf "log_directory = '%s'\n" "$pglog"
                    printf "log_filename = 'postgres.log'\n"
                  } >"$runtime_conf"

                  if ! grep -Fqx "$include_line" "$pgdata/postgresql.conf"; then
                    printf '\n%s\n' "$include_line" >>"$pgdata/postgresql.conf"
                  fi
                }

                _sinex_cargo_postgres_log_tail() {
                  local log_path="$1"
                  if [ -r "$log_path" ]; then
                    echo "--- postgres log tail: $log_path ---" >&2
                    tail -n 40 "$log_path" >&2 || true
                    echo "--- end postgres log tail ---" >&2
                  else
                    echo "(postgres log is not readable: $log_path)" >&2
                  fi
                }

                _sinex_cargo_cleanup_stale_postgres_pid() {
                  local pid_file pid cmdline socket lock
                  pid_file="$pgdata/postmaster.pid"
                  [ -e "$pid_file" ] || return 0

                  pid="$(head -n 1 "$pid_file" 2>/dev/null | tr -d '[:space:]' || true)"
                  socket="$pgrun/.s.PGSQL.$pgport"
                  lock="$pgrun/.s.PGSQL.$pgport.lock"

                  case "$pid" in
                    ""|*[!0-9]*)
                      echo "⚠️  Removing malformed checkout-local PostgreSQL pid file: $pid_file" >&2
                      rm -f "$pid_file" "$socket" "$lock"
                      return 0
                      ;;
                  esac

                  if ! kill -0 "$pid" 2>/dev/null; then
                    echo "⚠️  Removing stale checkout-local PostgreSQL pid file for dead PID $pid" >&2
                    rm -f "$pid_file" "$socket" "$lock"
                    return 0
                  fi

                  cmdline="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
                  case "$cmdline" in
                    *"$pgdata"*)
                      return 0
                      ;;
                    *)
                      echo "⚠️  Removing checkout-local PostgreSQL pid file for unrelated live PID $pid" >&2
                      rm -f "$pid_file" "$socket" "$lock"
                      return 0
                      ;;
                  esac
                }

                _sinex_cargo_start_postgres() {
                  local start_log start_rc
                  start_log="$pglog/postgres-start.log"

                  _sinex_cargo_cleanup_stale_postgres_pid

                  echo "ℹ  Starting checkout-local Postgres for SQLx validation..." >&2
                  start_rc=0
                  ${postgresForSqlx}/bin/pg_ctl \
                    -D "$pgdata" \
                    start \
                    -w \
                    -l "$start_log" \
                    -o "-k $pgrun -p $pgport" >>"$bootstrap_log" 2>&1 || start_rc=$?

                  if [ "$start_rc" -ne 0 ]; then
                    echo "✗ checkout-local Postgres failed to start (status $start_rc)" >&2
                    _sinex_cargo_postgres_log_tail "$start_log"
                    return "$start_rc"
                  fi
                }

                _sinex_cargo_bootstrap_sqlx_database_unlocked() {
                  mkdir -p "$pgdata" "$pgrun" "$pglog"

                  if [ ! -f "$pgdata/PG_VERSION" ]; then
                    echo "ℹ  Initializing checkout-local Postgres for SQLx validation..." >&2
                    ${postgresForSqlx}/bin/initdb \
                      --auth=trust \
                      --no-locale \
                      --encoding=UTF8 \
                      -U postgres \
                      -D "$pgdata" >>"$bootstrap_log" 2>&1
                  fi

                  _sinex_cargo_write_runtime_config

                  if ! ${postgresForSqlx}/bin/pg_isready -q -h "$pgrun" -p "$pgport" >/dev/null 2>&1; then
                    _sinex_cargo_start_postgres
                  fi

                  ${postgresForSqlx}/bin/psql \
                    -h "$pgrun" \
                    -p "$pgport" \
                    -U postgres \
                    -d postgres \
                    -v ON_ERROR_STOP=1 \
                    -v dev_user="$dev_user" >>"$bootstrap_log" 2>&1 <<'SQL'
SELECT format('CREATE ROLE %I LOGIN SUPERUSER CREATEDB', :'dev_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'dev_user')\gexec
SELECT format('ALTER ROLE %I WITH SUPERUSER CREATEDB LOGIN', :'dev_user')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'dev_user')\gexec
SELECT format('CREATE DATABASE sinex_dev OWNER %I', :'dev_user')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sinex_dev')\gexec
SQL

                  echo "ℹ  Applying checkout-local schema for SQLx validation..." >&2
                  _sinex_schema_apply_bootstrap_bin="$(_sinex_schema_apply_bootstrap_bin)" || return $?
                  DATABASE_URL="postgresql:///sinex_dev?host=$pgrun&user=postgres" \
                    "$_sinex_schema_apply_bootstrap_bin" >>"$bootstrap_log" 2>&1
                }

                _sinex_cargo_bootstrap_sqlx_database() {
                  mkdir -p "$SINEX_DEV_STATE_DIR" "$pglog"
                  while ! mkdir "$bootstrap_lock_dir" 2>/dev/null; do
                    if [ -r "$bootstrap_lock_dir/pid" ]; then
                      _lock_pid="$(cat "$bootstrap_lock_dir/pid" 2>/dev/null || true)"
                      if [ -n "$_lock_pid" ] && ! kill -0 "$_lock_pid" 2>/dev/null; then
                        rm -rf "$bootstrap_lock_dir"
                        continue
                      fi
                    fi
                    sleep 0.1
                  done
                  printf '%s\n' "$$" > "$bootstrap_lock_dir/pid"
                  trap 'rm -rf "$bootstrap_lock_dir"' EXIT INT TERM

                  : >"$bootstrap_log"
                  if ! _sinex_cargo_bootstrap_sqlx_database_unlocked; then
                    echo "✗ cargo SQLx bootstrap failed; log: $bootstrap_log" >&2
                    cat "$bootstrap_log" >&2 || true
                    rm -rf "$bootstrap_lock_dir"
                    trap - EXIT INT TERM
                    return 1
                  fi

                  rm -rf "$bootstrap_lock_dir"
                  trap - EXIT INT TERM
                }

                _sinex_cargo_stop_bootstrap_postgres() {
                  if [ "''${_sinex_cargo_bootstrap_postgres_owned:-0}" != 1 ]; then
                    return 0
                  fi

                  ${postgresForSqlx}/bin/pg_ctl -D "$pgdata" -m fast stop >/dev/null 2>&1 || true
                }

                _sinex_cargo_guard_xtask_surface "$@"

                _sinex_cargo_bootstrap_postgres_owned=0

                if _sinex_cargo_requires_sqlx_database "$@"; then
                  if [ "''${SINEX_CARGO_SQLX_BOOTSTRAP:-1}" = 1 ]; then
                    echo "ℹ  cargo $(_sinex_cargo_command_name "$@" || printf command) uses SQLx compile-time validation; bootstrapping checkout-local Postgres/schema..." >&2
                    if ! ${postgresForSqlx}/bin/pg_isready -q -h "$pgrun" -p "$pgport" >/dev/null 2>&1; then
                      _sinex_cargo_bootstrap_postgres_owned=1
                    fi
                    _sinex_cargo_bootstrap_sqlx_database
                  fi
                  export PGHOST="$pgrun"
                  export PGPORT="$pgport"
                  export DATABASE_URL="postgresql:///sinex_dev?host=$pgrun&user=postgres"
                fi

                set +e
                "$real_cargo" "$@"
                cargo_status="$?"
                set -e
                _sinex_cargo_stop_bootstrap_postgres
                exit "$cargo_status"
              '';
              xtaskCommand = pkgs.writeShellScriptBin "xtask" ''
                set -euo pipefail

                inherited_root_dir="''${SINEX_DEV_ROOT:-}"
                detected_root_dir="$(git rev-parse --show-toplevel 2>/dev/null || true)"
                root_dir="$inherited_root_dir"
                foreign_devshell_env=0

                if [ -n "$detected_root_dir" ] && [ -f "$detected_root_dir/flake.nix" ] && [ -d "$detected_root_dir/xtask" ]; then
                  if [ -z "$root_dir" ] || [ "$root_dir" != "$detected_root_dir" ]; then
                    root_dir="$detected_root_dir"
                    foreign_devshell_env=1
                  fi
                fi

                if [ -z "$root_dir" ]; then
                  echo "xtask requires the sinex devShell (missing SINEX_DEV_ROOT)" >&2
                  exit 1
                fi
                if [ ! -f "$root_dir/flake.nix" ] || [ ! -d "$root_dir/xtask" ]; then
                  echo "xtask expected a Sinex checkout root, got: $root_dir" >&2
                  exit 1
                fi

                if [ "$foreign_devshell_env" = "1" ]; then
                  checkout_hash="$(printf '%s' "$root_dir" | sha256sum | cut -c1-12)"
                  checkout_user="''${USER:-$(id -un)}"
                  checkout_cache_root="/var/cache/sinex/$checkout_user/$checkout_hash"
                  checkout_dev_state_dir="$checkout_cache_root/dev-state"
                  checkout_pg_run_dir="$checkout_dev_state_dir/run"
                  checkout_first_byte="$((16#''${checkout_hash:0:2}))"
                  checkout_nats_port="$((4222 + (checkout_first_byte % 100)))"
                  checkout_gateway_port="$((19000 + checkout_first_byte))"

                  export SINEX_DEV_ROOT="$root_dir"
                  export SINEX_ROOT="$root_dir"
                  export SINEX_DEV_CACHE_ROOT="$checkout_cache_root"
                  export SINEX_CACHE_DIR="$checkout_cache_root"
                  export CARGO_TARGET_DIR="$checkout_cache_root/target"
                  export SINEX_DEV_STATE_DIR="$checkout_dev_state_dir"
                  export SINEX_STATE_DIR="$root_dir/.sinex/state"
                  export PGHOST="$checkout_pg_run_dir"
                  export PGPORT=5432
                  export DATABASE_URL="postgresql:///sinex_dev?host=$checkout_pg_run_dir"
                  export SINEX_DEV_PG_PORT="$PGPORT"
                  export SINEX_DEV_NATS_PORT="$checkout_nats_port"
                  export SINEX_DEV_GATEWAY_PORT="$checkout_gateway_port"
                  export SINEX_NATS_DIR="$checkout_dev_state_dir/nats"
                  export SINEX_NATS_URL="nats://127.0.0.1:$checkout_nats_port"
                fi

                cargo_target_dir="''${CARGO_TARGET_DIR:-''${SINEX_DEV_CACHE_ROOT:-$root_dir/.sinex/cache}/target}"
                bin_path="$cargo_target_dir/debug/xtask"
                # Keep build-coordination markers with the rest of xtask state.
                # SINEX_STATE_DIR is relocated to NVMe (/var/cache/...) by the
                # sinnix devshell hook; honoring it here keeps the lock, failure
                # stamp, history DB, and job records in one place instead of
                # stranding markers in the checkout's .sinex/state.
                build_state_dir="''${SINEX_STATE_DIR:-$root_dir/.sinex/state}"
                build_lock_dir="$build_state_dir/xtask-build.lock"
                build_failure_stamp="$build_state_dir/xtask-build.failed"
                build_failure_log="$build_state_dir/xtask-build.failed.log"
                build_stage_metrics="$build_state_dir/xtask-build-stages.json"
                build_rebuild_trigger="$build_state_dir/xtask-build-rebuild-trigger.json"
                build_postgres_owned_marker="$build_state_dir/xtask-bootstrap-postgres-owned"
                schema_bootstrap_lock_file="$build_state_dir/xtask-sqlx-bootstrap.lock"
                wrapper_event_log="$build_state_dir/xtask-wrapper-events.jsonl"
                force_rebuild="''${SINEX_XTASK_FORCE_REBUILD:-0}"

                _sinex_xtask_json_string() {
                  ${pkgs.jq}/bin/jq -Rn --arg value "$1" '$value'
                }

                _sinex_xtask_bool_json() {
                  case "$1" in
                    1|true|yes) printf 'true' ;;
                    *) printf 'false' ;;
                  esac
                }

                _sinex_xtask_json_file_or_null() {
                  local path="$1"
                  if [ -r "$path" ]; then
                    ${pkgs.jq}/bin/jq -c . "$path" 2>/dev/null || printf 'null'
                  else
                    printf 'null'
                  fi
                }

                _sinex_xtask_source_trigger_json() {
                  local dep_path="$1"
                  local kind="$2"
                  local ref_path="$3"
                  local rel_path status mtime

                  rel_path="''${dep_path#"$root_dir/"}"
                  if [ ! -e "$dep_path" ]; then
                    status="missing"
                    mtime="null"
                  elif [ "$dep_path" -nt "$ref_path" ]; then
                    status="newer"
                    mtime="$(stat -c %Y "$dep_path" 2>/dev/null || printf null)"
                  else
                    return 1
                  fi

                  ${pkgs.jq}/bin/jq -cn \
                    --arg path "$dep_path" \
                    --arg rel_path "$rel_path" \
                    --arg kind "$kind" \
                    --arg status "$status" \
                    --argjson mtime "$mtime" \
                    '{path:$path, rel_path:$rel_path, kind:$kind, status:$status, mtime_epoch:$mtime}'
                }

                _sinex_xtask_collect_source_triggers() {
                  local ref_path="$1"
                  local depfile_path="$cargo_target_dir/debug/xtask.d"
                  local extra_dep dep_path row first

                  first=1
                  printf '['
                  # Only include files whose changes can affect the checkout-local
                  # Cargo-built xtask binary. flake.nix changes are picked up by
                  # re-entering nix develop; rebuilding target/debug/xtask from an
                  # already-loaded shell cannot apply wrapper/devshell changes.
                  for extra_dep in \
                    "$root_dir/Cargo.toml" \
                    "$root_dir/Cargo.lock" \
                    "$root_dir/xtask/Cargo.toml" \
                    "$root_dir/.cargo/config.toml"
                  do
                    row="$(_sinex_xtask_source_trigger_json "$extra_dep" "extra" "$ref_path" || true)"
                    if [ -n "$row" ]; then
                      [ "$first" -eq 1 ] || printf ','
                      printf '%s' "$row"
                      first=0
                    fi
                  done

                  if [ ! -r "$depfile_path" ]; then
                    row="$(${pkgs.jq}/bin/jq -cn \
                      --arg path "$depfile_path" \
                      --arg rel_path "''${depfile_path#"$root_dir/"}" \
                      '{path:$path, rel_path:$rel_path, kind:"depfile", status:"missing", mtime_epoch:null}')"
                    [ "$first" -eq 1 ] || printf ','
                    printf '%s' "$row"
                    printf ']'
                    return 0
                  fi

                  while IFS= read -r dep_path; do
                    [ -z "$dep_path" ] && continue
                    row="$(_sinex_xtask_source_trigger_json "$dep_path" "depfile" "$ref_path" || true)"
                    if [ -n "$row" ]; then
                      [ "$first" -eq 1 ] || printf ','
                      printf '%s' "$row"
                      first=0
                    fi
                  done < <(
                    sed -e 's/^[^:]*: //' -e 's/\\$//' "$depfile_path" \
                      | tr ' ' '\n' \
                      | sed '/^$/d'
                  )

                  printf ']'
                }

                _sinex_xtask_write_rebuild_trigger() {
                  local reason="$1"
                  local ref_path="''${2:-}"
                  local inputs="[]"
                  mkdir -p "$(dirname "$build_rebuild_trigger")" || return 0
                  if [ -n "$ref_path" ]; then
                    inputs="$(_sinex_xtask_collect_source_triggers "$ref_path" 2>/dev/null || printf '[]')"
                  fi
                  ${pkgs.jq}/bin/jq -cn \
                    --arg reason "$reason" \
                    --arg ref_path "$ref_path" \
                    --argjson inputs "$inputs" \
                    '{reason:$reason, ref_path:(if $ref_path == "" then null else $ref_path end), inputs:$inputs}' \
                    > "$build_rebuild_trigger" 2>/dev/null || true
                }

                _sinex_xtask_write_current_rebuild_trigger() {
                  local depfile_path="$cargo_target_dir/debug/xtask.d"

                  if [ "$force_rebuild" = "1" ]; then
                    _sinex_xtask_write_rebuild_trigger "forced" "$bin_path"
                  elif [ ! -x "$bin_path" ]; then
                    _sinex_xtask_write_rebuild_trigger "missing_binary" ""
                  elif [ ! -r "$depfile_path" ]; then
                    _sinex_xtask_write_rebuild_trigger "missing_depfile" "$bin_path"
                  else
                    _sinex_xtask_write_rebuild_trigger "sources_newer" "$bin_path"
                  fi
                }

                _sinex_xtask_record_wrapper_event() {
                  local event_name="$1"
                  local status="$2"
                  local started_at="$3"
                  local finished_at="$4"
                  local duration_ms="$5"
                  local log_path="$6"
                  local command_name args_text log_value stage_value trigger_value
                  shift 6

                  mkdir -p "$build_state_dir" || return 0
                  command_name="$(_sinex_xtask_command_name "$@" || true)"
                  args_text="$*"
                  if [ -n "$log_path" ]; then
                    log_value="$(_sinex_xtask_json_string "$log_path")"
                  else
                    log_value="null"
                  fi
                  if [ -r "$build_stage_metrics" ]; then
                    stage_value="$(${pkgs.jq}/bin/jq -c . "$build_stage_metrics" 2>/dev/null || printf '{}')"
                  else
                    stage_value="{}"
                  fi
                  trigger_value="$(_sinex_xtask_json_file_or_null "$build_rebuild_trigger")"

                  {
                    printf '{'
                    printf '"schema_version":1'
                    printf ',"event":%s' "$(_sinex_xtask_json_string "$event_name")"
                    printf ',"status":%s' "$(_sinex_xtask_json_string "$status")"
                    printf ',"started_at":%s' "$(_sinex_xtask_json_string "$started_at")"
                    printf ',"finished_at":%s' "$(_sinex_xtask_json_string "$finished_at")"
                    printf ',"duration_ms":%s' "$duration_ms"
                    printf ',"command":%s' "$(_sinex_xtask_json_string "$command_name")"
                    printf ',"args":%s' "$(_sinex_xtask_json_string "$args_text")"
                    printf ',"force_rebuild":%s' "$(_sinex_xtask_bool_json "$force_rebuild")"
                    printf ',"log_path":%s' "$log_value"
                    printf ',"rebuild_trigger":%s' "$trigger_value"
                    printf ',"stage_durations_ms":%s' "$stage_value"
                    printf '}\n'
                  } >> "$wrapper_event_log" || true
                }

                _sinex_xtask_stage_start() {
                  date +%s%N
                }

                _sinex_xtask_stage_record() {
                  local stage_name="$1"
                  local stage_started_ns="$2"
                  local stage_finished_ns stage_duration_ms tmp_file

                  stage_finished_ns="$(date +%s%N)"
                  stage_duration_ms="$(( (stage_finished_ns - stage_started_ns) / 1000000 ))"
                  mkdir -p "$(dirname "$build_stage_metrics")" || return 0
                  tmp_file="$build_stage_metrics.tmp"
                  if [ -r "$build_stage_metrics" ]; then
                    ${pkgs.jq}/bin/jq \
                      --arg stage "$stage_name" \
                      --argjson duration "$stage_duration_ms" \
                      '. + {($stage): $duration}' \
                      "$build_stage_metrics" > "$tmp_file" 2>/dev/null \
                      && mv "$tmp_file" "$build_stage_metrics" \
                      || rm -f "$tmp_file"
                  else
                    ${pkgs.jq}/bin/jq -n \
                      --arg stage "$stage_name" \
                      --argjson duration "$stage_duration_ms" \
                      '{($stage): $duration}' > "$build_stage_metrics" 2>/dev/null \
                      || true
                  fi
                }

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

                  # Keep this list in sync with _sinex_xtask_collect_source_triggers:
                  # these are Cargo-built xtask binary inputs, not devshell inputs.
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
                  if _sinex_xtask_is_help_request "$@"; then
                    return 0
                  fi
                  command_name="$(_sinex_xtask_command_name "$@")"
                  case "$command_name" in
                    ""|-h|--help|--version|--list-commands|status|history|analytics|jobs|snapshot)
                      return 0
                      ;;
                    check)
                      _sinex_xtask_changed_strict_has_no_rust_delta "$@"
                      return $?
                      ;;
                    fix)
                      _sinex_xtask_is_no_compile_subcommand "$@"
                      return $?
                      ;;
                    doctor|infra|run)
                      _sinex_xtask_is_read_only_subcommand "$@"
                      return $?
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_xtask_is_help_request() {
                  while [ "$#" -gt 0 ]; do
                    case "$1" in
                      -h|--help)
                        return 0
                        ;;
                      *)
                        shift
                        ;;
                    esac
                  done
                  return 1
                }

                _sinex_xtask_is_no_compile_subcommand() {
                  local command_name

                  command_name="$(_sinex_xtask_command_name "$@")"
                  case "$command_name" in
                    fix)
                      while [ "$#" -gt 0 ]; do
                        case "$1" in
                          --fmt-only)
                            return 0
                            ;;
                        esac
                        shift
                      done
                      return 1
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_xtask_is_read_only_subcommand() {
                  local command_name subcommand seen_command

                  if _sinex_xtask_is_help_request "$@"; then
                    return 0
                  fi

                  command_name="$(_sinex_xtask_command_name "$@")"
                  subcommand=""
                  seen_command=0
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
                      "$command_name")
                        seen_command=1
                        shift
                        ;;
                      *)
                        if [ "$seen_command" = 1 ]; then
                          subcommand="$1"
                          break
                        fi
                        shift
                        ;;
                    esac
                  done

                  case "$command_name:$subcommand" in
                    infra:status|infra:stop|run:list)
                      return 0
                      ;;
                    infra:smoke)
                      while [ "$#" -gt 0 ]; do
                        case "$1" in
                          --dry-run|--skip-start)
                            return 0
                            ;;
                        esac
                        shift
                      done
                      return 1
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_xtask_is_dependency_bootstrap_subcommand() {
                  local command_name subcommand seen_command

                  command_name="$(_sinex_xtask_command_name "$@")"
                  [ "$command_name" = "deps" ] || return 1

                  subcommand=""
                  seen_command=0
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
                      "$command_name")
                        seen_command=1
                        shift
                        ;;
                      *)
                        if [ "$seen_command" = 1 ]; then
                          subcommand="$1"
                          break
                        fi
                        shift
                        ;;
                    esac
                  done

                  [ "$subcommand" = "update" ]
                }

                _sinex_xtask_is_vm_test_subcommand() {
                  local command_name subcommand seen_command

                  command_name="$(_sinex_xtask_command_name "$@")"
                  [ "$command_name" = "test" ] || return 1

                  subcommand=""
                  seen_command=0
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
                      "$command_name")
                        seen_command=1
                        shift
                        ;;
                      *)
                        if [ "$seen_command" = 1 ]; then
                          subcommand="$1"
                          break
                        fi
                        shift
                        ;;
                    esac
                  done

                  [ "$subcommand" = "vm" ]
                }

                _sinex_xtask_is_launcher_only_background_request() {
                  local arg saw_bg saw_fg

                  if [ -n "''${XTASK_BG_JOB_ID:-}" ] || [ -n "''${XTASK_BG_INVOCATION_ID:-}" ]; then
                    return 1
                  fi

                  saw_bg=0
                  saw_fg=0
                  for arg in "$@"; do
                    case "$arg" in
                      --bg)
                        saw_bg=1
                        ;;
                      --fg)
                        saw_fg=1
                        ;;
                    esac
                  done

                  [ "$saw_bg" = 1 ] && [ "$saw_fg" != 1 ]
                }

                _sinex_xtask_changed_strict_has_no_rust_delta() {
                  local command_name seen_command base_ref next_arg merge_base changed_files

                  command_name="$(_sinex_xtask_command_name "$@")"
                  [ "$command_name" = "check" ] || return 1

                  seen_command=0
                  base_ref=""
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
                      "$command_name")
                        seen_command=1
                        shift
                        ;;
                      --changed-strict)
                        [ "$seen_command" = 1 ] || { shift; continue; }
                        base_ref="origin/master"
                        if [ "$#" -ge 2 ]; then
                          next_arg="$2"
                          case "$next_arg" in
                            -*)
                              ;;
                            *)
                              base_ref="$next_arg"
                              ;;
                          esac
                        fi
                        break
                        ;;
                      --changed-strict=*)
                        [ "$seen_command" = 1 ] || { shift; continue; }
                        base_ref="''${1#--changed-strict=}"
                        [ -n "$base_ref" ] || base_ref="origin/master"
                        break
                        ;;
                      *)
                        shift
                        ;;
                    esac
                  done

                  [ -n "$base_ref" ] || return 1
                  merge_base="$(git -C "$root_dir" merge-base "$base_ref" HEAD 2>/dev/null)" || return 1
                  changed_files="$(git -C "$root_dir" diff --name-only "$merge_base" HEAD -- '*.rs' 2>/dev/null | sed '/^$/d')" || return 1
                  [ -z "$changed_files" ]
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
                    ""|-h|--help|--version|--list-commands|status|history|analytics|jobs|snapshot|schema|check|test|build|deps|doctor|infra|run|docs|fix)
                      return 0
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_xtask_requires_sqlx_database() {
                  local command_name
                  if _sinex_xtask_is_help_request "$@"; then
                    return 1
                  fi
                  if _sinex_xtask_is_launcher_only_background_request "$@"; then
                    return 1
                  fi
                  if _sinex_xtask_is_dependency_bootstrap_subcommand "$@"; then
                    return 1
                  fi
                  if _sinex_xtask_is_vm_test_subcommand "$@"; then
                    return 1
                  fi
                  if _sinex_xtask_is_no_compile_subcommand "$@"; then
                    return 1
                  fi
                  if _sinex_xtask_changed_strict_has_no_rust_delta "$@"; then
                    return 1
                  fi
                  command_name="$(_sinex_xtask_command_name "$@")"
                  case "$command_name" in
                    check|test|build|deps|fix)
                      return 0
                      ;;
                    doctor)
                      _sinex_xtask_is_read_only_subcommand "$@" && return 1
                      return 0
                      ;;
                    *)
                      return 1
                      ;;
                  esac
                }

                _sinex_xtask_exec_checkout_binary() {
                  if [ -f "$build_postgres_owned_marker" ]; then
                    export SINEX_XTASK_BOOTSTRAP_POSTGRES_OWNED=1
                    rm -f "$build_postgres_owned_marker"
                  fi
                  if _sinex_xtask_requires_sqlx_database "$@"; then
                    if ! _sinex_xtask_postgres_ready; then
                      export SINEX_XTASK_BOOTSTRAP_POSTGRES_OWNED=1
                    fi
                    _sinex_xtask_ensure_sqlx_database || exit $?
                  fi
                  exec "$bin_path" "$@"
                }

                _sinex_xtask_exec_nix_readonly_fallback() {
                  if ! command -v nix >/dev/null 2>&1; then
                    echo "✗ no checkout-local xtask binary exists and nix is unavailable for the read-only fallback" >&2
                    return 127
                  fi
                  echo "ℹ  Using Nix-built xtask fallback for read-only command; checkout-local Postgres/NATS will not be started" >&2
                  exec nix run "$root_dir#xtask" -- "$@"
                }

                _sinex_xtask_wait_for_existing_build() {
                  local _lock_pid _lock_age
                  while [ -d "$build_lock_dir" ]; do
                    if [ -r "$build_lock_dir/pid" ]; then
                      _lock_pid="$(cat "$build_lock_dir/pid" 2>/dev/null || true)"
                      if [ -n "$_lock_pid" ] && ! kill -0 "$_lock_pid" 2>/dev/null; then
                        echo "⚠  Removing stale xtask build lock from dead pid $_lock_pid: $build_lock_dir" >&2
                        rm -rf "$build_lock_dir"
                        continue
                      fi
                    else
                      _lock_age="$(($(date +%s) - $(stat -c %Y "$build_lock_dir" 2>/dev/null || date +%s)))"
                      if [ "$_lock_age" -ge 5 ]; then
                        echo "⚠  Removing stale xtask build lock with no pid file: $build_lock_dir" >&2
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
                  mkdir -p "$build_state_dir"

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
                    local rebuild_started_at rebuild_started_ns rebuild_finished_at rebuild_finished_ns rebuild_duration_ms stage_started_ns
                    rebuild_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
                    rebuild_started_ns="$(date +%s%N)"
                    rm -f "$build_stage_metrics" "$build_stage_metrics.tmp" "$build_rebuild_trigger"
                    stage_started_ns="$(_sinex_xtask_stage_start)"
                    _sinex_xtask_write_current_rebuild_trigger
                    _sinex_xtask_stage_record "rebuild_trigger_capture" "$stage_started_ns"
                    echo "ℹ  Rebuilding checkout-local xtask (bootstraps SQLx Postgres/schema first)..." >&2
                    if _sinex_xtask_build_checkout_binary >"$build_failure_log" 2>&1; then
                      rebuild_finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
                      rebuild_finished_ns="$(date +%s%N)"
                      rebuild_duration_ms="$(( (rebuild_finished_ns - rebuild_started_ns) / 1000000 ))"
                      _sinex_xtask_record_wrapper_event "checkout-local-rebuild" "success" "$rebuild_started_at" "$rebuild_finished_at" "$rebuild_duration_ms" "" "$@"
                      rm -f "$build_failure_stamp" "$build_failure_log"
                    else
                      rebuild_finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
                      rebuild_finished_ns="$(date +%s%N)"
                      rebuild_duration_ms="$(( (rebuild_finished_ns - rebuild_started_ns) / 1000000 ))"
                      _sinex_xtask_record_wrapper_event "checkout-local-rebuild" "failed" "$rebuild_started_at" "$rebuild_finished_at" "$rebuild_duration_ms" "$build_failure_log" "$@"
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
                    local build_rc

                    _sinex_xtask_postgres_preexisting=0
                    rm -f "$build_postgres_owned_marker"
                    stage_started_ns="$(_sinex_xtask_stage_start)"
                    if _sinex_xtask_postgres_ready; then
                      _sinex_xtask_postgres_preexisting=1
                    else
                      touch "$build_postgres_owned_marker"
                    fi
                    _sinex_xtask_stage_record "sqlx_postgres_probe" "$stage_started_ns"
                    trap '_sinex_xtask_stop_bootstrap_postgres' EXIT

                    build_rc=0
                    _sinex_xtask_ensure_sqlx_database || return $?
                    _stage_started_ns="$(_sinex_xtask_stage_start)"
                    SINEX_XTASK_MANAGED_CARGO=1 cargo build --quiet -p xtask || build_rc=$?
                    _sinex_xtask_stage_record "xtask_build" "$_stage_started_ns"
                    if [ "$build_rc" -eq 0 ]; then
                      touch "$bin_path" "$cargo_target_dir/debug/xtask.d" 2>/dev/null || true
                    fi
                    stage_started_ns="$(_sinex_xtask_stage_start)"
                    _sinex_xtask_stop_bootstrap_postgres
                    _sinex_xtask_stage_record "sqlx_postgres_stop" "$stage_started_ns"
                    trap - EXIT
                    return "$build_rc"
                  )
                }

                _sinex_xtask_stop_bootstrap_postgres() {
                  if [ "''${_sinex_xtask_postgres_preexisting:-1}" = 1 ]; then
                    return 0
                  fi

                  local pgdata
                  pgdata="$SINEX_DEV_STATE_DIR/data/postgres"
                  ${postgresForSqlx}/bin/pg_ctl -D "$pgdata" -m fast stop >/dev/null 2>&1 || true
                }

                _sinex_xtask_postgres_ready() {
                  local pgrun pgport
                  pgrun="$SINEX_DEV_STATE_DIR/run"
                  pgport="''${PGPORT:-5432}"
                  ${postgresForSqlx}/bin/pg_isready -q -h "$pgrun" -p "$pgport" >/dev/null 2>&1
                }

                _sinex_xtask_schema_apply_bootstrap_bin() {
                  local bootstrap_bin bootstrap_out cached_path cache_fingerprint cache_fingerprint_file cache_path_file current_fingerprint

                  if ! command -v nix >/dev/null 2>&1; then
                    echo "nix is required to build schema-apply-bootstrap lazily" >&2
                    return 127
                  fi

                  cache_fingerprint_file="$SINEX_DEV_STATE_DIR/schema-apply-bootstrap.fingerprint"
                  cache_path_file="$SINEX_DEV_STATE_DIR/schema-apply-bootstrap.path"
                  current_fingerprint="$(_sinex_xtask_schema_apply_bootstrap_fingerprint || true)"

                  if [ -n "$current_fingerprint" ] \
                    && [ -r "$cache_fingerprint_file" ] \
                    && [ -r "$cache_path_file" ]
                  then
                    cache_fingerprint="$(cat "$cache_fingerprint_file" 2>/dev/null || true)"
                    cached_path="$(cat "$cache_path_file" 2>/dev/null || true)"
                    if [ "$cache_fingerprint" = "$current_fingerprint" ] && [ -x "$cached_path" ]; then
                      printf '%s\n' "$cached_path"
                      return 0
                    fi
                  fi

                  bootstrap_out="$(
                    nix --no-warn-dirty \
                      --extra-experimental-features 'nix-command flakes' \
                      build \
                      --no-link \
                      --print-out-paths \
                      "$root_dir#schema-apply-bootstrap"
                  )" || return $?

                  bootstrap_bin="$bootstrap_out/bin/schema-apply-bootstrap"
                  if [ ! -x "$bootstrap_bin" ]; then
                    echo "schema-apply-bootstrap output is missing executable: $bootstrap_bin" >&2
                    return 1
                  fi
                  if [ -n "$current_fingerprint" ]; then
                    mkdir -p "$SINEX_DEV_STATE_DIR"
                    printf '%s\n' "$current_fingerprint" >"$cache_fingerprint_file"
                    printf '%s\n' "$bootstrap_bin" >"$cache_path_file"
                  fi
                  printf '%s\n' "$bootstrap_bin"
                }

                _sinex_xtask_schema_apply_bootstrap_fingerprint() {
                  (
                    cd "$root_dir"
                    {
                      printf '%s\n' schema-apply-bootstrap-cache-v1
                      git ls-files \
                        Cargo.toml \
                        Cargo.lock \
                        flake.nix \
                        crate/sinex-schema \
                        crate/sinex-primitives
                      git ls-files --others --exclude-standard \
                        Cargo.toml \
                        Cargo.lock \
                        flake.nix \
                        crate/sinex-schema \
                        crate/sinex-primitives
                    } \
                      | LC_ALL=C sort -u \
                      | while IFS= read -r rel_path; do
                        [ -n "$rel_path" ] || continue
                        [ -f "$rel_path" ] || continue
                        printf '%s\n' "$rel_path"
                        sha256sum "$rel_path"
                      done \
                      | sha256sum \
                      | awk '{print $1}'
                  )
                }

                _sinex_xtask_postgres_log_tail() {
                  local log_path="$1"
                  if [ -r "$log_path" ]; then
                    echo "--- postgres log tail: $log_path ---" >&2
                    tail -n 40 "$log_path" >&2 || true
                    echo "--- end postgres log tail ---" >&2
                  else
                    echo "(postgres log is not readable: $log_path)" >&2
                  fi
                }

                _sinex_xtask_cleanup_stale_postgres_pid() {
                  local pgdata pgrun pgport pid_file pid cmdline socket lock
                  pgdata="$1"
                  pgrun="$2"
                  pgport="$3"
                  pid_file="$pgdata/postmaster.pid"
                  [ -e "$pid_file" ] || return 0

                  pid="$(head -n 1 "$pid_file" 2>/dev/null | tr -d '[:space:]' || true)"
                  socket="$pgrun/.s.PGSQL.$pgport"
                  lock="$pgrun/.s.PGSQL.$pgport.lock"

                  case "$pid" in
                    ""|*[!0-9]*)
                      echo "⚠️  Removing malformed checkout-local PostgreSQL pid file: $pid_file" >&2
                      rm -f "$pid_file" "$socket" "$lock"
                      return 0
                      ;;
                  esac

                  if ! kill -0 "$pid" 2>/dev/null; then
                    echo "⚠️  Removing stale checkout-local PostgreSQL pid file for dead PID $pid" >&2
                    rm -f "$pid_file" "$socket" "$lock"
                    return 0
                  fi

                  cmdline="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
                  case "$cmdline" in
                    *"$pgdata"*)
                      return 0
                      ;;
                    *)
                      echo "⚠️  Removing checkout-local PostgreSQL pid file for unrelated live PID $pid" >&2
                      rm -f "$pid_file" "$socket" "$lock"
                      return 0
                      ;;
                  esac
                }

                _sinex_xtask_start_postgres() {
                  local pgdata pgrun pglog pgport start_log start_rc
                  pgdata="$1"
                  pgrun="$2"
                  pglog="$3"
                  pgport="$4"
                  start_log="$pglog/postgres-start.log"

                  _sinex_xtask_cleanup_stale_postgres_pid "$pgdata" "$pgrun" "$pgport"

                  echo "ℹ  Starting checkout-local Postgres for SQLx validation..." >&2
                  start_rc=0
                  ${postgresForSqlx}/bin/pg_ctl \
                    -D "$pgdata" \
                    start \
                    -w \
                    -l "$start_log" \
                    -o "-k $pgrun -p $pgport" || start_rc=$?

                  if [ "$start_rc" -ne 0 ]; then
                    echo "✗ checkout-local Postgres failed to start (status $start_rc)" >&2
                    _sinex_xtask_postgres_log_tail "$start_log"
                    return "$start_rc"
                  fi
                }

                _sinex_xtask_ensure_sqlx_database_unlocked() {
                  local pgdata pgrun pglog pgport runtime_conf include_line dev_user schema_apply_bootstrap_bin

                  pgdata="$SINEX_DEV_STATE_DIR/data/postgres"
                  pgrun="$SINEX_DEV_STATE_DIR/run"
                  pglog="$SINEX_DEV_STATE_DIR/run/logs"
                  pgport="''${PGPORT:-5432}"
                  runtime_conf="$pgdata/sinex-dev.conf"
                  include_line="include_if_exists = '$runtime_conf'"
                  dev_user="''${USER:-$(id -un)}"

                  mkdir -p "$pgdata" "$pgrun" "$pglog"

                  if [ ! -f "$pgdata/PG_VERSION" ]; then
                    echo "ℹ  Initializing checkout-local Postgres for SQLx validation..." >&2
                    ${postgresForSqlx}/bin/initdb \
                      --auth=trust \
                      --no-locale \
                      --encoding=UTF8 \
                      -U postgres \
                      -D "$pgdata"
                  fi

                  {
                    printf "unix_socket_directories = '%s'\n" "$pgrun"
                    printf "%s = '%s'\n" "listen_addresses" ""
                    printf "port = %s\n" "$pgport"
                    printf "max_connections = 128\n"
                    printf "max_worker_processes = 6\n"
                    printf "shared_buffers = '32MB'\n"
                    printf "shared_preload_libraries = 'timescaledb'\n"
                    printf "timescaledb.max_background_workers = 2\n"
                    printf "log_destination = 'stderr'\n"
                    printf "logging_collector = on\n"
                    printf "log_directory = '%s'\n" "$pglog"
                    printf "log_filename = 'postgres.log'\n"
                  } >"$runtime_conf"

                  if ! grep -Fqx "$include_line" "$pgdata/postgresql.conf"; then
                    printf '\n%s\n' "$include_line" >>"$pgdata/postgresql.conf"
                  fi

                  if ! ${postgresForSqlx}/bin/pg_isready -q -h "$pgrun" -p "$pgport" >/dev/null 2>&1; then
                    _sinex_xtask_start_postgres "$pgdata" "$pgrun" "$pglog" "$pgport"
                  fi

                  ${postgresForSqlx}/bin/psql \
                    -h "$pgrun" \
                    -p "$pgport" \
                    -U postgres \
                    -d postgres \
                    -v ON_ERROR_STOP=1 \
                    -v dev_user="$dev_user" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN SUPERUSER CREATEDB', :'dev_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'dev_user')\gexec
SELECT format('ALTER ROLE %I WITH SUPERUSER CREATEDB LOGIN', :'dev_user')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'dev_user')\gexec
SELECT format('CREATE DATABASE sinex_dev OWNER %I', :'dev_user')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sinex_dev')\gexec
SQL

                  echo "ℹ  Applying checkout-local schema for SQLx validation..." >&2
                  schema_apply_bootstrap_bin="$(_sinex_xtask_schema_apply_bootstrap_bin)" || return $?
                  DATABASE_URL="postgresql:///sinex_dev?host=$pgrun&user=postgres" \
                    "$schema_apply_bootstrap_bin"

                  export PGHOST="$pgrun"
                  export PGPORT="$pgport"
                  export DATABASE_URL="postgresql:///sinex_dev?host=$pgrun&user=postgres"
                }

                _sinex_xtask_ensure_sqlx_database() {
                  local ensure_rc

                  mkdir -p "$build_state_dir"
                  exec 9>"$schema_bootstrap_lock_file"
                  ${pkgs.util-linux}/bin/flock 9

                  ensure_rc=0
                  _sinex_xtask_ensure_sqlx_database_unlocked || ensure_rc=$?

                  ${pkgs.util-linux}/bin/flock -u 9 >/dev/null 2>&1 || true
                  exec 9>&-
                  return "$ensure_rc"
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
                    elif _sinex_xtask_is_no_compile_subcommand "$@"; then
                      echo "ℹ  Using existing xtask binary for no-compile command while sources are newer" >&2
                    else
                      if ! _sinex_xtask_build_with_lock "$@"; then
                        if _sinex_xtask_failed_build_is_current; then
                          echo "ℹ  Falling back to existing xtask binary after rebuild failure" >&2
                          if [ -r "$build_failure_log" ]; then
                            echo "  log: $build_failure_log" >&2
                          fi
                          _sinex_xtask_exec_checkout_binary "$@"
                        fi
                        exit 1
                      fi
                      _sinex_xtask_exec_checkout_binary "$@"
                    fi
                  fi
                  _sinex_xtask_exec_checkout_binary "$@"
                fi

                if [ "$force_rebuild" != "1" ] && _sinex_xtask_is_observability_command "$@"; then
                  _sinex_xtask_exec_nix_readonly_fallback "$@"
                fi

                if [ "$force_rebuild" = "1" ] || _sinex_xtask_needs_build; then
                  if ! _sinex_xtask_build_with_lock "$@"; then
                    if _sinex_xtask_failed_build_is_current; then
                      if [ -x "$bin_path" ] && _sinex_xtask_can_use_existing_binary "$@"; then
                        echo "ℹ  Falling back to existing xtask binary after rebuild failure" >&2
                        if [ -r "$build_failure_log" ]; then
                          echo "  log: $build_failure_log" >&2
                        fi
                        _sinex_xtask_exec_checkout_binary "$@"
                      fi
                      _sinex_xtask_report_current_failure
                      exit 101
                    fi
                    exit 1
                  fi
                fi
                _sinex_xtask_exec_checkout_binary "$@"
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
                cargo-sweep # reclaim stale dep artifacts from target/ (used by xtask doctor --reclaim)
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
                cargoCommand
                xtaskCommand
              ];

              PGUSER = "sinity";
              PGDATABASE = "sinex_dev";
              SINEX_PG_BIN = "${postgresForSqlx}/bin";
              NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";

              shellHook = ''
                _sinex_path_prepend_unique() {
                  local entry="$1"
                  local old_ifs="$IFS"
                  local part
                  local rest=""

                  IFS=:
                  for part in $PATH; do
                    [ "$part" = "$entry" ] && continue
                    rest="''${rest:+$rest:}$part"
                  done
                  IFS="$old_ifs"

                  PATH="$entry''${rest:+:$rest}"
                }
                export SINEX_DEV_ROOT="$PWD"
                _sinex_checkout_hash="$(printf '%s' "$SINEX_DEV_ROOT" | sha256sum | cut -c1-12)"
                _sinex_user="''${USER:-$(id -un)}"
                _sinex_cache_base="/var/cache/sinex/$_sinex_user/$_sinex_checkout_hash"
                export SINEX_DEV_STATE_DIR="$_sinex_cache_base/dev-state"
                export SINEX_DEV_TOOLCHAIN="${rustToolchain.name}"
                if [ -z "''${SINEX_DEV_CACHE_ROOT:-}" ]; then
                  export SINEX_DEV_CACHE_ROOT="$_sinex_cache_base"
                fi
                if [ -z "''${CARGO_TARGET_DIR:-}" ]; then
                  export CARGO_TARGET_DIR="$SINEX_DEV_CACHE_ROOT/target"
                fi
                mkdir -p "$SINEX_DEV_CACHE_ROOT" "$CARGO_TARGET_DIR" "$SINEX_DEV_STATE_DIR"
                chattr +C "$SINEX_DEV_CACHE_ROOT" "$SINEX_DEV_STATE_DIR" 2>/dev/null || true
                # Disable sccache for the sinex dev loop. The system (sinnix
                # build-policy.nix) exports RUSTC_WRAPPER=sccache globally, but
                # sccache bypasses incremental compilation and gives ~0 on the
                # constantly-changing iterating crate. We opt into incremental
                # builds (Cargo.toml [profile.dev] incremental = true) instead.
                unset RUSTC_WRAPPER
                unset SCCACHE_DIR
                _sinex_path_prepend_unique "${cargoCommand}/bin"
                _sinex_path_prepend_unique "$CARGO_TARGET_DIR/debug"
                _sinex_path_prepend_unique "${xtaskCommand}/bin"
                export PATH
                export LD_LIBRARY_PATH="${
                  pkgs.lib.makeLibraryPath [ pkgs.dbus ]
                }''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                export CLIPPY_CONF_DIR="$PWD/.config"
                # Durable checkout-local state (xtask history DB lives here). Pinned to
                # the checkout — NOT derived from the relocatable SINEX_DEV_STATE_DIR —
                # so the history DB never leaks into the /var/cache scratch tree on
                # re-entry. Matches sinnix-direnvrc's pin; see CLAUDE async-workflow.md
                # ("history must NOT be relocated into the cache-shaped tree").
                export SINEX_STATE_DIR="$SINEX_DEV_ROOT/.sinex/state"
                export SINEX_CACHE_DIR="$SINEX_DEV_CACHE_ROOT"
                export SINEX_TEST_RESULTS_DIR="$SINEX_CACHE_DIR/test-results"
                # NATS runtime scratch (JetStream WAL) stays on the relocated NVMe dir.
                export SINEX_NATS_DIR="$SINEX_DEV_STATE_DIR/nats"
                export SINEX_DEV_PG_PORT="${toString pgPort}"
                export DATABASE_URL="postgresql:///sinex_dev?host=$SINEX_DEV_STATE_DIR/run"
                export PGHOST="$SINEX_DEV_STATE_DIR/run"
                export PGPORT="${toString pgPort}"
                _sinex_checkout_hash_hex="$(printf '%s' "$_sinex_checkout_hash" | cut -c1-2)"
                _sinex_checkout_hash_byte="$((16#$_sinex_checkout_hash_hex))"
                export SINEX_DEV_GATEWAY_PORT="$((19000 + _sinex_checkout_hash_byte))"
                export SINEX_DEV_NATS_PORT="$((4222 + (_sinex_checkout_hash_byte % 100)))"
                export SINEX_NATS_URL="nats://localhost:$SINEX_DEV_NATS_PORT"
                export SINEX_API_TCP_LISTEN="127.0.0.1:$SINEX_DEV_GATEWAY_PORT"
                export SINEX_API_URL="https://127.0.0.1:$SINEX_DEV_GATEWAY_PORT"
                # sinexctl-prod: talk to the live production sinexd (:9999)
                # without overriding the dev-shell SINEX_API_URL.
                sinexctl-prod() { SINEX_API_URL="https://127.0.0.1:9999" sinexctl "$@"; }

                if [ -z "''${SINEX_TEST_TMPDIR:-}" ]; then
                  _sinex_test_tmp_root="$SINEX_DEV_ROOT/.sinex/test-tmp"
                  if [ -d /dev/shm ] && [ -w /dev/shm ] && [ -k /dev/shm ]; then
                    _sinex_shm_available_kb="$(df -Pk /dev/shm 2>/dev/null | awk 'NR == 2 { print $4 }')"
                    if [ "''${_sinex_shm_available_kb:-0}" -ge 1048576 ]; then
                      _sinex_test_tmp_root="/dev/shm/sinex-test-''${USER:-user}-$_sinex_checkout_hash"
                    fi
                  fi
                  export SINEX_TEST_TMPDIR="$_sinex_test_tmp_root"
                fi
                if [ -z "''${SINEX_TEST_PGDATA_DIR:-}" ]; then
                  _sinex_test_pgdata_root="$SINEX_DEV_ROOT/.sinex/test-pgdata"
                  if [ -d /dev/shm ] && [ -w /dev/shm ] && [ -k /dev/shm ]; then
                    _sinex_shm_available_kb="$(df -Pk /dev/shm 2>/dev/null | awk 'NR == 2 { print $4 }')"
                    if [ "''${_sinex_shm_available_kb:-0}" -ge 1048576 ]; then
                      _sinex_test_pgdata_root="/dev/shm/sinex-test-pgdata-''${USER:-user}-$_sinex_checkout_hash"
                    fi
                  fi
                  export SINEX_TEST_PGDATA_DIR="$_sinex_test_pgdata_root"
                fi
                mkdir -p "$SINEX_TEST_TMPDIR"
                chmod 700 "$SINEX_TEST_TMPDIR" 2>/dev/null || true
                if [ -n "''${SINEX_TEST_PGDATA_DIR:-}" ]; then
                  mkdir -p "$SINEX_TEST_PGDATA_DIR"
                  chmod 700 "$SINEX_TEST_PGDATA_DIR" 2>/dev/null || true
                fi

                # Dev TLS certs are generated lazily by preflight when needed.
                # Set TLS env vars if dev certs exist — enables mTLS automatically.
                if [ -f "$PWD/.sinex/tls/server.pem" ]; then
                  export SINEX_API_TLS_CERT="$PWD/.sinex/tls/server.pem"
                  export SINEX_API_TLS_KEY="$PWD/.sinex/tls/server-key.pem"
                  export SINEX_API_TLS_CLIENT_CA="$PWD/.sinex/tls/ca.pem"
                fi

                # Auto-install the pre-push drift guard (.githooks/pre-push)
                # on first devshell entry per checkout. Idempotent — silently
                # skipped if core.hooksPath already points at .githooks.
                if [ -d .git ] || [ -f .git ]; then
                  _current_hooks_path="$(git config --local core.hooksPath 2>/dev/null || true)"
                  if [ "$_current_hooks_path" != ".githooks" ]; then
                    if [ -f .githooks/pre-push ]; then
                      git config --local core.hooksPath .githooks
                      echo "[devshell] installed .githooks (pre-push drift guard)" >&2
                    fi
                  fi
                fi
                if [ -t 1 ]; then
                  _sinex_tcp_ready() {
                    timeout 0.2 bash -c ">/dev/tcp/127.0.0.1/$1" 2>/dev/null
                  }

                  _sinex_recent_history_line() {
                    local db="$SINEX_STATE_DIR/xtask-history.db"
                    local query

                    [ -f "$db" ] || return 0
                    command -v sqlite3 >/dev/null 2>&1 || return 0

                    query="
                      SELECT command || ' ' || status || ' ' || printf('%.1fs', duration_secs) || ' ' || started_at
                      FROM invocations
                      WHERE command IN ('check','test','build','fix')
                        AND status IN ('success','failed','cancelled')
                      ORDER BY started_at DESC
                      LIMIT 1;
                    "

                    timeout 0.25 sqlite3 "file:$db?mode=ro&immutable=1" "$query" 2>/dev/null || true
                  }

                  _sinex_print_motd() {
                    local pg_state="down"
                    local nats_state="down"
                    local history_line
                    local test_tmp="$SINEX_TEST_TMPDIR"
                    local test_pgdata="''${SINEX_TEST_PGDATA_DIR:-unset}"

                    pg_isready -q -h "$SINEX_DEV_STATE_DIR/run" -p "${toString pgPort}" 2>/dev/null && pg_state="up"
                    _sinex_tcp_ready "$SINEX_DEV_NATS_PORT" && nats_state="up"
                    history_line="$(_sinex_recent_history_line)"

                    {
                      printf 'sinex devshell: pg:%s nats:%s target:%s\n' "$pg_state" "$nats_state" "$CARGO_TARGET_DIR"
                      printf '  test tmp: %s\n' "$test_tmp"
                      printf '  test pgdata: %s\n' "$test_pgdata"
                      if [ -n "$history_line" ]; then
                        printf '  last xtask: %s\n' "$history_line"
                      fi
                      printf '  inspect: xtask status --summary | xtask history explain --day today --against yesterday\n'
                      printf '  prod: sinexctl-prod (SINEX_API_URL=:9999) | dev: sinexctl (SINEX_API_URL=:%s)\n' "$SINEX_DEV_GATEWAY_PORT"
                      printf '  controls: SINEX_AUTO_INFRA=1 starts infra; SINEX_AUTO_STATUS=1 runs full status; SINEX_MOTD=0 hides this\n'
                    } >&2
                  }

                  # Keep shell entry cheap by default. Heavy dev conveniences are
                  # opt-in so direnv, one-shot commands, and fresh shells do not
                  # silently compile xtask or launch infra.
                  # When SINEX_AUTO_INFRA=1 does start the stack, it is a
                  # persistent dev service by design: it detaches (setsid below)
                  # and deliberately outlives the launching shell or one-shot
                  # command, listening on loopback only (#1725).
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
                  elif [ "''${SINEX_MOTD:-1}" = 1 ] && [ "''${SINEX_SHELL_BANNER:-1}" = 1 ]; then
                    _sinex_print_motd
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

      nixosConfigurations =
        let
          mkExampleConfig =
            example: extraModules:
            nixpkgs.lib.nixosSystem {
              modules = [
                (
                  { ... }:
                  {
                    nixpkgs.hostPlatform = "x86_64-linux";
                    nixpkgs.overlays = [ self.overlays.default ];
                  }
                )
                example
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
              ] ++ extraModules;
            };
        in
        {
          workstation = mkExampleConfig ./nixos/examples/workstation.nix [
            (
              { lib, ... }:
              {
                nixpkgs.config.allowUnfree = true;
                services.sinex.core.api.enable = lib.mkForce false;
              }
            )
          ];
          monitoring = mkExampleConfig ./nixos/examples/monitoring.nix [ ];
          devSandbox = mkExampleConfig ./nixos/examples/dev-sandbox.nix [ ];
          headless = mkExampleConfig ./nixos/examples/headless.nix [ ];
          remoteRuntime = mkExampleConfig ./nixos/examples/remote-runtime.nix [ ];
          coordination = mkExampleConfig ./nixos/examples/coordination.nix [ ];
        };

      # Unified overlay: pg_jsonschema + all sinex packages
      overlays.default = nixpkgs.lib.composeExtensions pgJsonschemaOverlay (
        final: prev:
          builtins.listToAttrs (
            map
              (name: nixpkgs.lib.nameValuePair name self.packages.${final.stdenv.hostPlatform.system}.${name})
              packageOutputNames
          )
      );
    };
}
