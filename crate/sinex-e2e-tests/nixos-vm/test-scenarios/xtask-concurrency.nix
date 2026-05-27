# xtask coordinator concurrency tests — Rust-driven.
#
# Replaces Python testScript with the typed Rust `sinex-vm-test-suite` binary
# so test logic lives in Rust with full type safety, IDE support, and shared
# infrastructure with the rest of the codebase.
#
# Scenarios (in categories/concurrency.rs):
#   1. Coordinator lock stampede: 5 concurrent `xtask check --bg`, all reach terminal state.
#   2. Zombie reaping: SIGKILL xtask coordinator, orphaned job becomes terminal.
#   3. PID reuse safety: cancel reads /proc/{pid}/cmdline before sending signal.
#   4. History DB consistency: each invocation adds exactly 1 record.
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, xtask
, sinexVmTestSuite ? null
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;
  stateDir = "/var/lib/sinex/xtask-concurrency-test";
in
pkgs.testers.nixosTest {
  name = "sinex-xtask-concurrency";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    environment.systemPackages = with pkgs; [
      xtask
      procps
      util-linux  # for flock(1)
    ];

    # Isolated state directory so xtask history DB is separate from sinex services.
    # These are picked up automatically by the Rust binary via SINEX_STATE_DIR.
    environment.sessionVariables = {
      SINEX_STATE_DIR = stateDir;
      NO_COLOR = "1";
      FORCE_COLOR = "0";
    };

    systemd.tmpfiles.rules = [
      "d ${stateDir} 0755 root root -"
    ];
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")

    with subtest("Rust-driven xtask concurrency suite"):
      machine.succeed(
        "SINEX_STATE_DIR=${stateDir} NO_COLOR=1 FORCE_COLOR=0 "
        "${sinexVmTestSuite}/bin/run-suite --category concurrency"
      )
  '';
}
