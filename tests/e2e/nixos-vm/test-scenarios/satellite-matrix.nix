# Satellite constellation coverage test for Sinex
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
  name = "sinex-satellite-matrix";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex = {
      satellites = {
        coordination.enable = lib.mkForce true;

        filesystem = {
          enable = lib.mkForce true;
          instances = lib.mkForce 2;
          watchPaths = lib.mkForce [ "/var/lib/sinex/watched" ];
        };
        terminal = {
          enable = lib.mkForce true;
          instances = lib.mkForce 1;
        };
        desktop = {
          enable = lib.mkForce true;
          instances = lib.mkForce 1;
        };
        system = {
          enable = lib.mkForce true;
          instances = lib.mkForce 1;
        };

        automata = {
          canonicalizer.enable = lib.mkForce true;
          healthAggregator.enable = lib.mkForce true;
        };
      };
    };
  };

  testScript = ''
    machine.start()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("sinex-coordination-setup.service")

    # Core hubs
    for unit in ["sinex-ingestd.service", "sinex-gateway.service", "nats.service"]:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Event source satellites
    satellites = [
        "sinex-fs-watcher-1.service",
        "sinex-fs-watcher-2.service",
        "sinex-terminal-satellite-1.service",
        "sinex-desktop-satellite-1.service",
        "sinex-system-satellite-1.service"
    ]
    for unit in satellites:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Automata
    automata = [
        "sinex-terminal-command-canonicalizer.service",
        "sinex-health-aggregator.service"
    ]
    for unit in automata:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Verify generated units metadata exposed via option
    generated = machine.succeed("nixos-option services.sinex.satellite.generatedUnits")
    assert "sinex-fs-watcher-1" in generated
    assert "sinex-terminal-satellite-1" in generated
  '';
}
