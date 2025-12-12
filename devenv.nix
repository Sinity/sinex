{
  inputs ? { },
  pkgs,
  lib,
  config,
  ...
}:
let
  system = pkgs.stdenv.hostPlatform.system;
  fenixInput =
    if inputs ? fenix then
      inputs.fenix
    else
      builtins.getFlake "github:nix-community/fenix?rev=d4e14d370b4763c67ea02a39f01f5366297d61cb&narHash=sha256-nx0zy/+yR57FwloXmatf3CaXgzA4zJqIFbplnpaKn/Y=";
  fenixPkgs = fenixInput.packages.${system}.complete;
  pythonDeps = with pkgs.python3Packages; [
    click
    psycopg2
    rich
    pyyaml
  ];
  # Postgres extensions are assumed to be installed on the system instance
  # at /run/postgresql; we no longer bundle a Postgres with extensions here.
  basePackages =
    with pkgs;
    [
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
    ]
    ++ pythonDeps;
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
  };

  cachix = {
    enable = true;
    pull = [ "sinity" "nix-community" ];
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
    # Keep devenv quiet.
    DEVENV_TASKS_QUIET = "1";
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

    # sqlx: no auto-check on entry to keep shell startup fast.
    # Run `xt sqlx-prepare` (alias below) when queries/migrations change.

    if [ -x "$PWD/scripts/dev-env-banner.sh" ] && [ -z "''${SINEX_DEVENV_MOTD_ONCE:-}" ]; then
      "$PWD/scripts/dev-env-banner.sh" || true
      export SINEX_DEVENV_MOTD_ONCE=1
    fi
    alias sinex-cli="python3 cli/exo.py"
    xt() { cargo xtask "$@"; }
    alias e2e-test="cargo nextest run -p sinex-e2e-tests"
    alias vm-smoke="./tests/e2e/nixos-vm/run-vm-tests.sh -c smoke"
    if [ -z "''${SINEX_MOTD_SILENT:-}" ]; then
      echo ""
      echo "xtask quick reference:"
      echo "  xtask check        # sqlx check + fmt check + cargo check"
      echo "  xtask lint         # clippy -D warnings"
      echo "  xtask test         # nextest workspace (profile=reliable)"
      echo "  xtask sqlx-prepare # refresh .sqlx after migrations"
    fi
    # Generate shell completions once per shell session (writes to /tmp)
    case "''${SHELL:-bash}" in
      *zsh)
        cargo xtask completions zsh > /tmp/xtask-completions.zsh 2>/dev/null || true
        . /tmp/xtask-completions.zsh 2>/dev/null || true
        ;;
      *)
        cargo xtask completions bash > /tmp/xtask-completions.bash 2>/dev/null || true
        . /tmp/xtask-completions.bash 2>/dev/null || true
        ;;
    esac
  '';

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
