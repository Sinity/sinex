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
    };
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinex-ingestd.service", timeout=60)

    with subtest("Rust-driven smoke suite"):
      machine.succeed(
        "su - postgres -c 'DATABASE_URL=postgresql:///sinex "
        "${sinexVmTestSuite}/bin/run-suite --category smoke'"
      )
  '';
}
