# Simplified preflight deployment check: ensure preflight CLI is packaged and
# ingest pipeline still works with preflight features enabled.
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;
in
pkgs.testers.nixosTest {
  name = "sinex-preflight";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ./common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    # The preflight service intentionally verifies production-style resource
    # floors. Give this VM enough headroom for those checks instead of relaxing
    # the deployed preflight thresholds.
    virtualisation = {
      memorySize = 4096;
      diskSize = 16384;
    };

    # common/test-base relaxes filesystem readiness for broad VM tests. This
    # scenario is specifically about deployed preflight contracts, so restore
    # the managed notify/watchdog unit shape before preflight inspects systemd.
    systemd.services.sinex-filesystem-1.serviceConfig.Type = lib.mkOverride 40 "notify";

    services.sinex = {
      # Exercise preflight-enabled wiring but keep other features minimal.
      lifecycle.preflight.enable = lib.mkOverride 60 true;
      lifecycle.updates.enable = lib.mkForce false;
      observability.monitoring.enable = lib.mkForce true;
      nodes.enable = lib.mkForce true;
    };
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("sinex-ingestd.service")
    machine.wait_for_unit("sinex-gateway.service")
    machine.succeed("systemctl start sinex-preflight.service")
    machine.succeed("systemctl show -p Result --value sinex-preflight.service | grep '^success$'")
    machine.succeed("systemctl show -p ExecMainStatus --value sinex-preflight.service | grep '^0$'")
  '';
}
