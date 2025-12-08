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
    # Stream task output by default so logs appear immediately; callers can override to quiet if desired.
    DEVENV_TASKS_QUIET = "0";
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

    if [ -x "$PWD/scripts/dev-env-banner.sh" ] && [ -z "''${SINEX_DEVENV_MOTD_ONCE:-}" ]; then
      "$PWD/scripts/dev-env-banner.sh" || true
      export SINEX_DEVENV_MOTD_ONCE=1
    fi
    alias sinex-cli="python3 cli/exo.py"
    alias e2e-test="cargo nextest run -p sinex-e2e-tests"
    alias vm-smoke="./tests/e2e/nixos-vm/run-vm-tests.sh -c smoke"
  '';

  tasks = {
    "dev:fmt".exec = "cargo fmt --all";
    "dev:lint".exec = "cargo clippy --workspace --all-targets --all-features -- -D warnings";
    "dev:check".exec = "cargo check --workspace --all-features";
    "dev:build".exec = "cargo build --workspace";
    "dev:smoke-fixtures".exec = ''
      cargo nextest run -p sinex-test-utils --retries 0 -E "test_empty_database_fixture|test_concurrent_fixture_access|test_populated_checkpoints_fixture|test_fixture_registry_cleanup"
    '';

    # Stream CI harness output and enable verbose postgres setup logs when running locally.
    "dev:test".exec = "CI_VERBOSE=1 stdbuf -oL -eL scripts/ci-postgres.sh ./scripts/run-dev-tests.sh";

    "test:all".exec = ''
      devenv tasks run db:setup
      LD_LIBRARY_PATH="$(pkg-config --variable=libdir dbus-1)''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
      RUST_LOG=''${RUST_LOG:-} cargo nextest run --workspace --profile reliable
    '';

    "test:vm".exec = "./tests/e2e/nixos-vm/run-vm-tests.sh -c smoke";

    "dev:coverage".exec = ''
      cargo tarpaulin --workspace --out Html --output-dir target/coverage
      echo "Coverage report: target/coverage/index.html"
    '';

    "dev:docs".exec = "cargo doc --workspace --no-deps --open";
    "dev:watch".exec = "bacon";
    "dev:stats".exec = "tokei";
    "dev:update".exec = "cargo update";
    "dev:audit".exec = "cargo audit";
    "dev:unused".exec = "cargo machete";

    "db:migrate".exec = ''
      DATABASE_URL="$DATABASE_URL" \
        cargo run \
          --manifest-path crate/lib/sinex-schema/Cargo.toml \
          --bin sinex-schema -- \
          up
    '';

    "db:status".exec = ''
      DATABASE_URL="$DATABASE_URL" \
        cargo run \
          --manifest-path crate/lib/sinex-schema/Cargo.toml \
          --bin sinex-schema -- \
          status
    '';

    "db:reset".exec = ''
      dropdb --if-exists --force "$DATABASE_NAME"
      createdb "$DATABASE_NAME"
      devenv tasks run db:migrate
    '';

    "db:setup".exec = ''
      createdb "$DATABASE_NAME" 2>/dev/null || true
      devenv tasks run db:migrate
    '';

    "db:psql".exec = "psql \"$DATABASE_URL\"";
    "db:doctor".exec = "cargo run -p sinex-test-utils --bin db_doctor";

    "sqlx:prepare".exec = "./scripts/sqlx-prepare.sh";

    "sqlx:check".exec = "cargo sqlx prepare --workspace --check -- --all-targets --all-features";

    "cli:query".exec = ''
      LIMIT="''${LIMIT:-10}" ./cli/exo.py query --limit "$LIMIT"
    '';
  };

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
