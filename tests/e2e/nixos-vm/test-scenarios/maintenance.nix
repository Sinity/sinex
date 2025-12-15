# Maintenance flow validation for Sinex
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
  name = "sinex-maintenance";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex = {
      observability.monitoring.enable = lib.mkForce true;
      storage.blob.enable = lib.mkForce true;
      storage.blob.maintenance.gc.enable = lib.mkForce true;
      storage.blob.maintenance.fsck.enable = lib.mkForce true;
    };
  };

  testScript = ''
    machine.start()
    machine.wait_for_unit("multi-user.target")

    required_units = [
        "postgresql.service",
        "sinex-ingestd.service",
        "sinex-gateway.service",
        "sinex-blob-init.service"
    ]
    for unit in required_units:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    timers = [
        "sinex-dlq-cleanup.timer",
        "sinex-blob-gc.timer",
        "sinex-blob-fsck.timer"
    ]
    for timer in timers:
        machine.succeed(f"systemctl list-timers | grep {timer}")

    # Ensure maintenance services are defined and runnable
    maintenance_services = [
        "sinex-dlq-cleanup.service",
        "sinex-blob-gc.service",
        "sinex-blob-fsck.service"
    ]
    for svc in maintenance_services:
        machine.succeed(f"systemctl cat {svc}")

    # Run health check helper to ensure environment and CLI wiring
    machine.succeed("sinex-health-check")
  '';
}
