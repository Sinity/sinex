{ inputs, pkgs, lib, config, ... }:
let
  system = pkgs.stdenv.hostPlatform.system;
  fenixPkgs = inputs.fenix.packages.${system}.complete;
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
    postgresql_16
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
  ] ++ pythonDeps ++ [ ttok ];
  dbusLibPath = pkgs.lib.makeLibraryPath [ pkgs.dbus ];
in {
  packages = basePackages;

  env = {
    DATABASE_NAME = "sinex_dev";
    DATABASE_URL = "postgresql:///sinex_dev?host=/run/postgresql";
    PGHOST = "/run/postgresql";
    PGUSER = "sinity";
    PGDATABASE = "sinex_dev";
    SINEX_TEST_OPTIMIZATIONS = "true";
    NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";
    DEVENV_TASKS_QUIET = "1";
    DEVENV_CMDLINE = "--quiet";
    SINEX_DEVENV_SYSTEM = system;
    SINEX_DEVENV_TOOLCHAIN = "fenix (${system})";
    SINEX_DEVENV_PROCESS_HINT = "devenv up nats ingestd gateway";
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

    "dev:test".exec = ''
      SINEX_ALLOW_NATIVE_TESTS=1 PROPTEST_CASES=''${PROPTEST_CASES:-64} cargo nextest run --workspace --profile reliable
    '';

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
      (cd crate/lib/sinex-schema && DATABASE_URL="$DATABASE_URL" cargo run -- up)
    '';

    "db:status".exec = ''
      (cd crate/lib/sinex-schema && DATABASE_URL="$DATABASE_URL" cargo run -- status)
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

    "sqlx:prepare".exec = ''
      devenv tasks run db:migrate
      cargo sqlx prepare --workspace -- --all-targets
      echo "✅ SQLX cache updated - commit .sqlx/"
    '';

    "sqlx:check".exec = "cargo sqlx prepare --workspace --check -- --all-targets";

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
