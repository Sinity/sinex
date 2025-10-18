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
pkgs.nixosTest {
  name = "sinex-maintenance";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex = {
      serviceManagement.serviceGroups = lib.mkForce {
        core = true;
        maintenance = true;
        monitoring = true;
      };

      monitoring.enable = lib.mkForce true;
      blobStorage.enable = lib.mkForce true;
      blobStorage.maintenance = {
        enableAutoGc = lib.mkForce true;
        enablePeriodicFsck = lib.mkForce true;
      };
    };
  };

  testScript = ''
    machine.start()
    machine.wait_for_unit("multi-user.target")

    required_units = [
        "postgresql.service",
        "sinex-ingestd.service",
        "sinex-gateway.service",
        "sinex-git-annex-init.service"
    ]
    for unit in required_units:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    timers = [
        "sinex-dlq-cleanup.timer",
        "sinex-git-annex-gc.timer",
        "sinex-git-annex-fsck.timer",
        "sinex-resource-monitor.timer",
        "sinex-system-health.timer"
    ]
    for timer in timers:
        machine.succeed(f"systemctl list-timers | grep {timer}")

    # Ensure maintenance services are defined and runnable
    maintenance_services = [
        "sinex-dlq-cleanup.service",
        "sinex-git-annex-gc.service",
        "sinex-git-annex-fsck.service",
        "sinex-resource-monitor.service",
        "sinex-system-health.service"
    ]
    for svc in maintenance_services:
        machine.succeed(f"systemctl cat {svc}")

    # Run health check helper to ensure environment and CLI wiring
    machine.succeed("sinex-health-check")
  '';
}
