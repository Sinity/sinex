{ inputs ? {}, pkgs, lib, config, ... }:
let
  system = pkgs.stdenv.hostPlatform.system;
  fenixInput =
    if inputs ? fenix then inputs.fenix
    else builtins.getFlake "github:nix-community/fenix?rev=d4e14d370b4763c67ea02a39f01f5366297d61cb&narHash=sha256-nx0zy/+yR57FwloXmatf3CaXgzA4zJqIFbplnpaKn/Y=";
  fenixPkgs = fenixInput.packages.${system}.complete;
  pythonDeps = with pkgs.python3Packages; [
    click
    psycopg2
    rich
    pyyaml
  ];
  ttok = pkgs.python3Packages.buildPythonPackage rec {
    pname = "ttok";
    version = "0.3";
    format = "setuptools";
    src = pkgs.python3Packages.fetchPypi {
      inherit pname version;
      sha256 = "sha256-BHSgCldHYNsiTSSur6UOG56t9qV056bBMkZYvZuCSbg=";
    };
    propagatedBuildInputs = with pkgs.python3Packages; [ tiktoken ];
  };
  pgJsonschemaPkg = pkgs.stdenv.mkDerivation rec {
    pname = "pg_jsonschema";
    version = "0.3.3";

    src = pkgs.fetchurl {
      url = "https://github.com/supabase/pg_jsonschema/releases/download/v${version}/pg_jsonschema-v${version}-pg16-amd64-linux-gnu.deb";
      hash = "sha256-6VSbAZrrItYgnpKMhVqffC4fGp9zzPYaMB6/Bf+Ha/g=";
    };

    nativeBuildInputs = [ pkgs.dpkg ];

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
  };
  postgresqlWithExtensions =
    pkgs.postgresql_16.withPackages (ps:
      lib.filter (pkg: pkg != null) [
        (if ps ? timescaledb then ps.timescaledb else null)
        (if ps ? pgvector then ps.pgvector else null)
        pgJsonschemaPkg
        (if ps ? pgx_ulid then ps.pgx_ulid else null)
      ]
    );
  basePackages = with pkgs; [
    fenixPkgs.toolchain
    fenixPkgs.rust-analyzer
    fenixPkgs.clippy
    fenixPkgs.rustfmt
    fenixPkgs.llvm-tools
    fenixPkgs.rust-src
    fenixPkgs.rustc-codegen-cranelift
    cargo-watch
    cargo-nextest
    cargo-llvm-cov
    cargo-tarpaulin
    cargo-modules
    bacon
    tokei
    cargo-audit
    cargo-machete
    sqlx-cli
    mold
    python3
    nats-server
    postgresqlWithExtensions
    mprocs
    btop
    jq
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
    sccache
  ] ++ pythonDeps ++ [ ttok ];
  dbusLibPath = pkgs.lib.makeLibraryPath [ pkgs.dbus ];
in {
  devenv = {
    root = lib.mkDefault (
      let
        rootEnv = builtins.getEnv "DEVENV_ROOT";
      in
      if rootEnv != "" then rootEnv else toString ./.
    );
  };

  packages = basePackages;

  env = {
    DATABASE_NAME = "sinex_dev";
    DATABASE_URL = "postgresql:///sinex_dev?host=/run/postgresql";
    PGHOST = "/run/postgresql";
    PGUSER = "sinity";
    PGDATABASE = "sinex_dev";
    SINEX_TEST_OPTIMIZATIONS = "true";
    NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";
    # Keep devenv tasks quiet unless overridden.
    DEVENV_TASKS_QUIET = "1";
    DEVENV_CMDLINE = "";
    SINEX_DEVENV_SYSTEM = system;
    SINEX_DEVENV_TOOLCHAIN = "fenix (${system})";
    SINEX_DEVENV_PROCESS_HINT = "devenv up nats ingestd gateway";
    SINEX_SCCACHE = "${pkgs.sccache}/bin/sccache";
    SCCACHE_DIR = "$HOME/.cache/sccache";
    SCCACHE_CACHE_SIZE = "2G";
    CARGO_INCREMENTAL = "0";
  };

  enterShell = ''
    export PATH="$PWD/target/debug:$PATH"
    export LD_LIBRARY_PATH="${dbusLibPath}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

    # Auto-refresh or validate sqlx metadata based on schema fingerprint.
    if [ -z "''${SINEX_SKIP_SQLX_AUTO:-}" ]; then
      SCHEMA_HASH="$(cargo run --package xtask --quiet -- sqlx-check 2>/tmp/sinex-sqlx-check.err || true)"
      if [ -s /tmp/sinex-sqlx-check.err ]; then
        echo "sqlx-check reported:" >&2
        cat /tmp/sinex-sqlx-check.err >&2
        # Attempt auto-prepare if Postgres is reachable
        if PGPASSWORD='' psql -h "''${PGHOST:-/run/postgresql}" -U "''${PGUSER:-}" -d "''${PGDATABASE:-}" -c 'select 1' >/dev/null 2>&1; then
          echo "Attempting to refresh .sqlx metadata automatically..."
          if cargo xtask sqlx-prepare; then
            echo "sqlx metadata refreshed."
          else
            echo "Automatic sqlx prepare failed; please run 'cargo xtask sqlx-prepare' manually." >&2
          fi
        else
          echo "Postgres not reachable; run 'cargo xtask sqlx-prepare' once DB is available." >&2
        fi
      fi
      rm -f /tmp/sinex-sqlx-check.err
    fi

    if [ -x "$PWD/scripts/dev-env-banner.sh" ] && [ -z "''${SINEX_DEVENV_MOTD_ONCE:-}" ]; then
      "$PWD/scripts/dev-env-banner.sh" || true
      export SINEX_DEVENV_MOTD_ONCE=1
    fi
    alias sinex-cli="python3 cli/exo.py"
    alias e2e-test="cargo nextest run -p sinex-e2e-tests"
    alias vm-smoke="./tests/e2e/nixos-vm/run-vm-tests.sh -c smoke"
    echo ""
    echo "xtask quick reference:"
    echo "  xtask check        # sqlx check + fmt check + cargo check"
    echo "  xtask lint         # clippy -D warnings"
    echo "  xtask test         # nextest workspace (profile=reliable)"
    echo "  xtask sqlx-prepare # refresh .sqlx after migrations"
    # Generate shell completions once per shell session (writes to /tmp)
    cargo xtask completions bash > /tmp/xtask-completions.bash
    . /tmp/xtask-completions.bash 2>/dev/null || true
  '';

  tasks = { };

  processes = {
    nats.exec = "${pkgs.nats-server}/bin/nats-server -js";
    ingestd.exec = "cargo run --bin sinex-ingestd";
    gateway.exec = "cargo run --bin sinex-gateway";
    fs-watcher.exec = "cargo run --bin sinex-fs-watcher";
    terminal.exec = "cargo run --bin sinex-terminal-satellite";
    desktop.exec = "cargo run --bin sinex-desktop-satellite";
    system.exec = "cargo run --bin sinex-system-satellite";
    canonicalizer.exec = "cargo run --bin sinex-terminal-command-canonicalizer";
    health.exec = "cargo run --bin sinex-health-aggregator";
    document.exec = "cargo run --bin sinex-document-ingestor";
  };
}
