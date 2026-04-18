# Basic E2E flow test for Sinex — Rust-driven smoke suite.
#
# Keep the test script minimal: start services, then run the typed Rust
# `sinex-vm-test-suite` against the VM.
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, sinexVmTestSuite ? null
, ...
}:

let
  inherit (pkgs) lib;
in
pkgs.testers.nixosTest {
  name = "sinex-basic-flow";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex.nodes = {
      filesystem.watchPaths = lib.mkAfter [ "/var/lib/sinex/watched" ];
      terminal.enable = false;
      desktop.enable = false;
      system.enable = false;
      document = {
        enable = true;
        allowedRoots = [ "/home/test/Documents" ];
      };
      automata = {
        enable = true;
        canonicalizer.enable = true;
        healthAggregator.enable = true;
        analyticsAutomaton.enable = true;
        sessionDetector.enable = true;
      };
    };
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinex-gateway.service", timeout=60)
    machine.wait_for_unit("sinex-ingestd.service", timeout=60)
    machine.wait_for_unit("sinex-filesystem-1.service", timeout=60)
    machine.wait_for_unit("sinex-document-scan.timer", timeout=60)
    machine.wait_for_unit("sinex-canonicalizer.service", timeout=60)
    machine.wait_for_unit("sinex-health-automaton.service", timeout=60)
    machine.wait_for_unit("sinex-analytics-automaton.service", timeout=60)
    machine.wait_for_unit("sinex-session-detector.service", timeout=60)

    with subtest("Rust-driven smoke suite"):
      machine.succeed(
        "${sinexVmTestSuite}/bin/run-suite --category smoke"
      )

    with subtest("Deployment proof via sinexctl verify"):
      machine.succeed("su - test -c 'echo basic-flow > /var/lib/sinex/watched/basic-flow.txt'")
      machine.succeed(
        "sinexctl --insecure verify --gateway-smoke --automata-smoke --document-smoke --source-proof"
      )
  '';
}
