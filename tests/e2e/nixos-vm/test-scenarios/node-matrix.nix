# Node constellation coverage test for Sinex
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
  name = "sinex-node-matrix";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex = {
      nodes = {
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
          analyticsAutomaton.enable = lib.mkForce true;
          sessionDetector.enable = lib.mkForce true;
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

    # Event source nodes
    nodes = [
        "sinex-filesystem-1.service",
        "sinex-filesystem-2.service",
        "sinex-terminal-1.service",
        "sinex-desktop-1.service",
        "sinex-system-1.service"
    ]
    for unit in nodes:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Automata
    automata = [
        "sinex-canonicalizer.service",
        "sinex-health-automaton.service",
        "sinex-analytics-automaton.service",
        "sinex-session-detector.service"
    ]
    for unit in automata:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Verify generated units metadata exposed via option
    generated = machine.succeed("nixos-option sinex._generatedUnits")
    assert "sinex-filesystem-1" in generated
    assert "sinex-terminal-1" in generated
  '';
}
