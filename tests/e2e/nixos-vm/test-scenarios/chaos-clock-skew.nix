# Chaos test: system clock skew during event processing — Rust-driven.
#
# Advances the system clock by 1 hour during active event ingestion and verifies:
#   - UUIDv7 IDs remain monotonically ordered (ts_coided never decreases within a batch)
#   - TimescaleDB hypertable chunking doesn't reject late-arriving or future-dated data
#   - No events are silently dropped due to clock-based deduplication false positives
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
  name = "sinex-chaos-clock-skew";
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

    # Allow clock manipulation via date
    environment.systemPackages = with pkgs; [ util-linux ];

    # Disable NTP so we can manipulate the clock freely
    services.timesyncd.enable = lib.mkForce false;
    systemd.services.chronyd.enable = lib.mkDefault false;
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinex-ingestd.service", timeout=60)

    with subtest("Rust-driven chaos-clock-skew suite"):
      machine.succeed(
        "DATABASE_URL=postgresql:///sinex "
        "${sinexVmTestSuite}/bin/run-suite --category chaos-clock-skew"
      )
  '';
}
