# Chaos test: sinex-ingestd process kill during batch ingestion — Rust-driven.
#
# Replaces Python testScript with the typed Rust `sinex-vm-test-suite` binary.
#
# SIGKILLs ingestd mid-batch and verifies:
#   - ingestd restarts (via systemd) and resumes from its checkpoint
#   - no duplicate events in Postgres (idempotent re-processing)
#   - the total event count is monotonically non-decreasing (no data loss)
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinexVmTestSuite ? null
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;
in
pkgs.testers.nixosTest {
  name = "sinex-chaos-process-restart";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex.nodes.filesystem = {
      enable = true;
      watchPaths = [ "/var/lib/sinex/watched" ];
    };

    # ingestd must restart automatically after SIGKILL
    systemd.services.sinex-ingestd.serviceConfig.Restart = lib.mkForce "always";
    systemd.services.sinex-ingestd.serviceConfig.RestartSec = lib.mkForce "2s";

    environment.systemPackages = with pkgs; [ procps ];
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinex-ingestd.service", timeout=60)

    with subtest("Rust-driven chaos-process-restart suite"):
      machine.succeed(
        "DATABASE_URL=postgresql:///sinex "
        "${sinexVmTestSuite}/bin/run-suite --category chaos-process-restart"
      )
  '';
}
