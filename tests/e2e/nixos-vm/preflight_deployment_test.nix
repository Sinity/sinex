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
    machine.wait_for_unit("sinex-preflight.service")
    machine.succeed("systemctl is-active --quiet sinex-preflight.service")
    machine.succeed("systemctl show -p Result sinex-preflight.service | grep '=success'")
  '';
}
