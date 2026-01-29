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

  # Custom postgres with required extensions
  pg_jsonschema = pkgs.callPackage ./nix/pkgs/pg_jsonschema { };
  postgresForSqlx = pkgs.postgresql_16.withPackages (ps: [
    ps.timescaledb
    ps.pgvector
    ps.pgx_ulid
    pg_jsonschema
  ]);

  basePackages = with pkgs; [
    # Rust toolchain (prefer Fenix if available)
    (if hasFenix then fenixPkgs.toolchain else rustc)
    (if hasFenix then fenixPkgs.rust-analyzer else rust-analyzer)
    (if hasFenix then fenixPkgs.clippy else cargo)
    (if hasFenix then fenixPkgs.rustfmt else rustfmt)
    (if hasFenix then fenixPkgs.llvm-tools else llvmPackages.bintools)
    (if hasFenix then fenixPkgs.rust-src else rustPlatform.rustLibSrc)

    # Cargo tools
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

    # Services (available but not auto-started)
    nats-server
    postgresForSqlx

    # Development utilities
    mprocs
    btop
    jq
    coreutils
    protobuf
    openssl
    pkg-config
    dbus
    dbus.dev
    git-annex
    fd
    fzf
    bat
    ripgrep
    nsc
    qemu
    qemu_kvm
  ];

  dbusLibPath = pkgs.lib.makeLibraryPath [ pkgs.dbus ];
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
    pull = [ "sinity" "nix-community" ];
  };

  packages = basePackages;

  # Static environment variables (non-computed)
  env = {
    DATABASE_NAME = "sinex_dev";
    PGUSER = "sinity";
    PGDATABASE = "sinex_dev";
    SINEX_TEST_OPTIMIZATIONS = "true";
    SINEX_PG_BIN = "${postgresForSqlx}/bin";
    NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";
    DEVENV_TASKS_QUIET = "1";
    SINEX_DEVENV_SYSTEM = system;
    SINEX_DEVENV_TOOLCHAIN = if hasFenix then "fenix (${system})" else "nixpkgs (${system})";
  };

  enterShell = ''
    # Add scripts and target/debug to PATH
    export PATH="$PWD/scripts:$PWD/target/debug:$PATH"

    # DBus library path for desktop nodes
    export LD_LIBRARY_PATH="${dbusLibPath}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

    # XDG-compliant state/cache directories
    export SINEX_STATE_DIR="''${XDG_STATE_HOME:-$HOME/.local/state}/sinex"
    export SINEX_CACHE_DIR="''${XDG_CACHE_HOME:-$HOME/.cache}/sinex"
    export SINEX_TEST_RESULTS_DIR="$SINEX_CACHE_DIR/test-results"

    # Let xtask handle all dynamic configuration (ports, DB paths, TLS, etc.)
    # This only works if xtask is already compiled
    if [ -x "$PWD/target/debug/xtask" ]; then
      eval $("$PWD/target/debug/xtask" stack env --export 2>/dev/null || echo "")
    else
      # Fallback for first-time setup (before xtask is built)
      export SINEX_DEV_STATE_DIR="$PWD/.devenv/sinex-dev"
      export DATABASE_URL="postgresql:///sinex_dev?host=$SINEX_DEV_STATE_DIR/run&port=5432"
      export PGHOST="$SINEX_DEV_STATE_DIR/run"
      export PGPORT="5432"
      export SINEX_NATS_URL="nats://localhost:4222"
      export SINEX_RPC_URL="https://127.0.0.1:9998"
    fi

    # Interactive shell only
    if [ -n "''${PS1:-}" ] && [ -t 1 ]; then
      # Source project shell shortcuts (sx, xt functions)
      [ -f "$PWD/.zshrc.local" ] && source "$PWD/.zshrc.local"

      # Show banner once per shell session
      if [ -z "''${SINEX_DEVENV_MOTD_ONCE:-}" ]; then
        export SINEX_DEVENV_MOTD_ONCE=1
        [ -x "$PWD/scripts/dev-env-banner.sh" ] && "$PWD/scripts/dev-env-banner.sh" || true
      fi
    fi
  '';
}
