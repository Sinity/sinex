{
  inputs ? { },
  pkgs,
  lib,
  config,
  ...
}:
let
  system = pkgs.stdenv.hostPlatform.system;
  hasFenix = inputs ? fenix;
  fenixPkgs = if hasFenix then inputs.fenix.packages.${system}.complete else null;
  pg_jsonschema = pkgs.callPackage ./nix/pkgs/pg_jsonschema { };
  postgresForSqlx = pkgs.postgresql_16.withPackages (ps: [
    ps.timescaledb
    ps.pgvector
    ps.pgx_ulid
    pg_jsonschema
  ]);

  tlsFixtures = ".devenv/tls";
  basePackages = with pkgs; [
    # Prefer the pinned Fenix toolchain when available (flake path),
    # otherwise fall back to the nixpkgs Rust toolchain when running
    # via plain `devenv` without Sinex flake inputs.
    (if hasFenix then fenixPkgs.toolchain else rustc)
    (if hasFenix then fenixPkgs.rust-analyzer else rust-analyzer)
    (if hasFenix then fenixPkgs.clippy else cargo)
    (if hasFenix then fenixPkgs.rustfmt else rustfmt)
    (if hasFenix then fenixPkgs.llvm-tools else llvmPackages.bintools)
    (if hasFenix then fenixPkgs.rust-src else rustPlatform.rustLibSrc)
    (if hasFenix then fenixPkgs.rustc-codegen-cranelift else rustc)
    cargo-watch
    cargo-nextest
    cargo-llvm-cov
    cargo-tarpaulin
    cargo-modules
    bacon
    tokei
    cargo-audit
    cargo-machete
    mold
    binutils

    nats-server
    postgresForSqlx
    mprocs
    btop
    jq
    coreutils
    protobuf
    openssl
    pkg-config
    dbus
    dbus.dev
    qemu
    qemu_kvm
    git-annex
    fd
    fzf
    bat
    ripgrep
    nsc
  ];
  dbusLibPath = pkgs.lib.makeLibraryPath [ pkgs.dbus ];
  homeDir = builtins.getEnv "HOME";
  xdgStateHome = builtins.getEnv "XDG_STATE_HOME";
  xdgCacheHome = builtins.getEnv "XDG_CACHE_HOME";
  sinexStateDir =
    if xdgStateHome != "" then "${xdgStateHome}/sinex" else "${homeDir}/.local/state/sinex";
  sinexCacheDir = if xdgCacheHome != "" then "${xdgCacheHome}/sinex" else "${homeDir}/.cache/sinex";
in
{
  devenv = {
    root = lib.mkDefault (
      let
        rootEnv = builtins.getEnv "DEVENV_ROOT";
      in
      if rootEnv != "" then rootEnv else toString ./.
    );
    warnOnNewVersion = false;
  };

  cachix = {
    enable = true;
    pull = [
      "sinity"
      "nix-community"
    ];
  };

  packages = basePackages;

  env = {
    DATABASE_NAME = "sinex_dev";
    PGUSER = "sinity";
    PGDATABASE = "sinex_dev";
    SINEX_TEST_OPTIMIZATIONS = "true";
    # Force `cargo xtask ci postgres` to use the Postgres build that includes required extensions.
    SINEX_PG_BIN = "${postgresForSqlx}/bin";
    NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";
    # Keep devenv quiet.
    DEVENV_TASKS_QUIET = "1";
    SINEX_DEVENV_SYSTEM = system;
    SINEX_DEVENV_TOOLCHAIN = if hasFenix then "fenix (${system})" else "nixpkgs (${system})";
    SINEX_STATE_DIR = sinexStateDir;
    SINEX_CACHE_DIR = sinexCacheDir;
    SINEX_TEST_RESULTS_DIR = "${sinexCacheDir}/test-results";
    SINEX_GATEWAY_TLS_CERT = "${tlsFixtures}/server.pem";
    SINEX_GATEWAY_TLS_KEY = "${tlsFixtures}/server-key.pem";
    SINEX_GATEWAY_TLS_CLIENT_CA = "${tlsFixtures}/ca.pem";
    SINEX_RPC_CA_CERT = "${tlsFixtures}/ca.pem";
    SINEX_RPC_CLIENT_CERT = "${tlsFixtures}/client.pem";
    SINEX_RPC_CLIENT_KEY = "${tlsFixtures}/client-key.pem";
    # Per-checkout dev stack configuration (fixed ports, isolated by Unix socket directory)
    SINEX_DEV_PG_PORT = "5432"; # PostgreSQL default
    SINEX_DEV_NATS_PORT = "4222"; # NATS default
    SINEX_DEV_GATEWAY_PORT = "9998";
  };

  enterShell = ''
    export PATH="$PWD/scripts:$PWD/target/debug:$PATH"
    SINEX_STATE_DIR="''${SINEX_STATE_DIR:-''${XDG_STATE_HOME:-$HOME/.local/state}/sinex}"
    SINEX_CACHE_DIR="''${SINEX_CACHE_DIR:-''${XDG_CACHE_HOME:-$HOME/.cache}/sinex}"
    export SINEX_STATE_DIR SINEX_CACHE_DIR
    export SINEX_TEST_RESULTS_DIR="''${SINEX_TEST_RESULTS_DIR:-$SINEX_CACHE_DIR/test-results}"
    export LD_LIBRARY_PATH="${dbusLibPath}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

    # Per-checkout isolated dev stack
    # Each checkout has its own Unix socket directory, so fixed ports don't conflict
    SINEX_DEV_STATE_DIR="$PWD/.devenv/sinex-dev"
    SINEX_DEV_PG_PORT=5432        # PostgreSQL default
    # NATS port uses offset to avoid TCP conflicts
    # Stable fallback calculation in bash matching the 0-99 range logic
    # Note: xtask will refine this precisely once compiled.
    NATS_OFFSET=$(( ( $(echo "$PWD" | cksum | cut -d' ' -f1) % 100 ) ))
    SINEX_DEV_NATS_PORT=$(( 4222 + NATS_OFFSET ))
    SINEX_DEV_GATEWAY_PORT=9998
    export SINEX_DEV_STATE_DIR SINEX_DEV_PG_PORT SINEX_DEV_NATS_PORT SINEX_DEV_GATEWAY_PORT

    # Refine ports via xtask once it is compiled
    if [ -x "$PWD/target/debug/xtask" ]; then
      # This will update SINEX_DEV_NATS_PORT to strictly match Rust's hash
      eval $("$PWD/target/debug/xtask" stack env --export 2>/dev/null)
    fi

    # Database connection via Unix socket (no TCP conflicts between checkouts)
    export DATABASE_URL="postgresql:///sinex_dev?host=$SINEX_DEV_STATE_DIR/run&port=$SINEX_DEV_PG_PORT"
    export PGHOST="$SINEX_DEV_STATE_DIR/run"
    export PGPORT="$SINEX_DEV_PG_PORT"
    export SINEX_NATS_URL="nats://localhost:$SINEX_DEV_NATS_PORT"


    export SINEX_NATS_DIR="$SINEX_DEV_STATE_DIR/data/nats"
    export NATS_CREDS="$SINEX_NATS_DIR/nsc/creds/sinex-dev/sinex-dev/sinex-dev.creds"

    # Git-annex blob storage in per-checkout stack
    export SINEX_ANNEX_PATH="$SINEX_DEV_STATE_DIR/data/annex"

    # Gateway RPC URL
    export SINEX_RPC_URL="https://127.0.0.1:$SINEX_DEV_GATEWAY_PORT"

    tls_dir="$PWD/${tlsFixtures}"
    if [ ! -f "$tls_dir/server.pem" ] || [ ! -f "$tls_dir/client.pem" ]; then
      if [ -x "$PWD/scripts/generate_tls_fixtures.sh" ]; then
        mkdir -p "$tls_dir"
        "$PWD/scripts/generate_tls_fixtures.sh" "$tls_dir" >/dev/null 2>&1 || true
      fi
    fi

    # Keep non-interactive `direnv exec` / scripts quiet and fast.
    if [ -n "''${PS1:-}" ] && [ -t 1 ]; then
      if [ -z "''${SINEX_DEVENV_SHELL_INIT:-}" ]; then
        export SINEX_DEVENV_SHELL_INIT=1

        # Bootstrap NATS Auth once, but never block the shell prompt.
        if [ -z "''${SINEX_DEVENV_SKIP_NATS_BOOTSTRAP:-}" ] && [ -x "$PWD/scripts/bootstrap_nats_auth.sh" ]; then
          nats_lock_dir="$PWD/.nats/.bootstrap.lock"
          mkdir -p "$PWD/.nats"
          if mkdir "$nats_lock_dir" 2>/dev/null; then
            if command -v timeout >/dev/null 2>&1; then
              SINEX_NATS_BOOTSTRAP_QUIET=1 timeout 15s "$PWD/scripts/bootstrap_nats_auth.sh" >/dev/null 2>&1 || true
            fi
            rmdir "$nats_lock_dir" 2>/dev/null || true
          fi
        fi
      fi


      # Ensure sx alias is available in all shells
      # Source project-specific shell config if it exists
      [ -f "$PWD/.zshrc.local" ] && source "$PWD/.zshrc.local"

      alias e2e-test="cargo xtask test -- -p sinex-e2e-tests"
      alias vm-smoke="cargo xtask vm test -c smoke"

      # Show unified banner once per shell session
      if [ -x "$PWD/scripts/dev-env-banner.sh" ] && [ -z "''${SINEX_DEVENV_MOTD_ONCE:-}" ]; then
        "$PWD/scripts/dev-env-banner.sh" || true
        export SINEX_DEVENV_MOTD_ONCE=1
      fi

        if [ -z "''${DIRENV_IN_ENVRC:-}" ] \
          && [ "''${SINEX_DEVENV_COMPLETIONS:-1}" != "0" ] \
          && [ -z "''${SINEX_DEVENV_COMPLETIONS_ONCE:-}" ]; then
          export SINEX_DEVENV_COMPLETIONS_ONCE=1
          # Generate shell completions from the existing xtask binary to avoid an implicit cargo build.
          XTASK_BIN="$PWD/target/debug/xtask"
          COMPLETIONS_CACHE_DIR="$HOME/.cache/sinex-completions"
          mkdir -p "$COMPLETIONS_CACHE_DIR"
          generate_xtask_completions() {
            gen_shell="$1"
            gen_file="$2"
            if [ ! -x "$XTASK_BIN" ]; then
              return
            fi
            if [ ! -f "$gen_file" ] || [ "$XTASK_BIN" -nt "$gen_file" ]; then
              tmp_file="$gen_file.$$"
              timeout 6s "$XTASK_BIN" completions "$gen_shell" > "$tmp_file" 2>/dev/null || true
              if [ -s "$tmp_file" ]; then
                mv "$tmp_file" "$gen_file"
              else
                rm -f "$tmp_file"
              fi
            fi
          }

          if command -v timeout >/dev/null 2>&1; then
            case "''${SHELL:-bash}" in
              *zsh)
                COMPLETIONS_FILE="$COMPLETIONS_CACHE_DIR/xtask-completions.zsh"
                ;;
              *)
                COMPLETIONS_FILE="$COMPLETIONS_CACHE_DIR/xtask-completions.bash"
                ;;
            esac

            if [ -f "$COMPLETIONS_FILE" ]; then
              . "$COMPLETIONS_FILE" 2>/dev/null || true
            elif [ -x "$XTASK_BIN" ]; then
              (
                case "''${SHELL:-bash}" in
                  *zsh) generate_xtask_completions zsh "$COMPLETIONS_FILE" ;;
                  *) generate_xtask_completions bash "$COMPLETIONS_FILE" ;;
                esac
              ) >/dev/null 2>&1 &
              if command -v disown >/dev/null 2>&1; then
                disown
              fi
            fi
          fi
        fi
      fi
  '';

  processes = {
    nats.exec = "${pkgs.nats-server}/bin/nats-server -js -c $SINEX_NATS_DIR/nats.conf";
    ingestd.exec = "cargo run --bin sinex-ingestd";
    gateway.exec = "cargo run --bin sinex-gateway";
    fs-watcher.exec = "cargo run --bin sinex-fs-watcher";
    terminal.exec = "cargo run --bin sinex-terminal-node";
    desktop.exec = "cargo run --bin sinex-desktop-node";
    system.exec = "cargo run --bin sinex-system-node";
    canonicalizer.exec = "cargo run --bin sinex-terminal-command-canonicalizer";
    health.exec = "cargo run --bin sinex-health-aggregator";
    document.exec = "cargo run --bin sinex-document-ingestor";
  };
}
