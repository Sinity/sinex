# Chaos test: network partition between event_engine and NATS — Rust-driven.
#
# Injects a deterministic `tc netem` packet-drop rule between sinexd and
# the NATS server, then removes it and verifies:
#   - event_engine survives the partition without crashing
#   - after reconnection, the event pipeline drains (no data loss)
#   - the event count in Postgres grows back to the expected total
{ pkgs
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
  name = "sinex-chaos-network-partition";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib pg_jsonschema sinex sinexCli;
      })
      ../common/chaos-toolkit.nix
    ];

    # Enable NATS for this test (event_engine → NATS → event_engine pipeline)
    services.sinex.nats.enable = true;
    services.sinex.nats.bootstrapStreams.enable = true;

    services.sinex.nodes = {
      filesystem = {
        enable = true;
        watchPaths = [ "/var/lib/sinex/watched" ];
      };
    };

    # iproute2 + iptables for tc/netem packet injection
    environment.systemPackages = with pkgs; [
      iproute2
      iptables
    ];
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinexd.service", timeout=60)

    with subtest("Rust-driven chaos-network-partition suite"):
      machine.succeed(
        "${sinexVmTestSuite}/bin/run-suite --category chaos-network-partition"
      )
  '';
}
