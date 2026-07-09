# NixOS VM tests for Sinex
{ pkgs ? import <nixpkgs> {}
, pg_jsonschema ? null
, sinex ? null
, sinexCli ? null
, xtask ? null
, sinexVmTestSuite ? null
}:

{
  # Basic operational test
  basic = import ./test-scenarios/basic-flow.nix {
    inherit pkgs pg_jsonschema sinex sinexCli sinexVmTestSuite;
  };

  "replay-smoke" = import ./test-scenarios/replay-smoke.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  preflight = import ./preflight_deployment_test.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
    lib = pkgs.lib;
  };

  maintenance = import ./test-scenarios/maintenance.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "runtime-matrix" = import ./test-scenarios/runtime-matrix.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "multi-source" = import ./test-scenarios/multi-source.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "failure-recovery" = import ./test-scenarios/failure-recovery.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  performance = import ./test-scenarios/performance.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "production-scale" = import ./production-scale.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "mtls-enforcement" = import ./test-scenarios/mtls-enforcement.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "kitty-eventsource" = import ./test-scenarios/kitty-eventsource.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "sinexctl-e2e" = import ./test-scenarios/sinexctl-e2e.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  # ─── Chaos scenarios ─────────────────────────────────────────────────────────

  "chaos-network-partition" = import ./test-scenarios/chaos-network-partition.nix {
    inherit pkgs pg_jsonschema sinex sinexCli sinexVmTestSuite;
  };

  "chaos-process-restart" = import ./test-scenarios/chaos-process-restart.nix {
    inherit pkgs pg_jsonschema sinex sinexCli sinexVmTestSuite;
  };

  "chaos-clock-skew" = import ./test-scenarios/chaos-clock-skew.nix {
    inherit pkgs pg_jsonschema sinex sinexCli sinexVmTestSuite;
  };

  "chaos-spool-rename-durability" = import ./test-scenarios/chaos-spool-rename-durability.nix {
    inherit pkgs sinex;
  };

  # ─── xtask concurrency (requires pre-built xtask binary) ─────────────────────

  "xtask-concurrency" = import ./test-scenarios/xtask-concurrency.nix {
    inherit pkgs pg_jsonschema sinex sinexCli xtask sinexVmTestSuite;
  };

  # ─── Environmental hostility ──────────────────────────────────────────────────

  "hostile-host" = import ./test-scenarios/hostile-host.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  "migration-stress" = import ./test-scenarios/migration-stress.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

  # ─── Production-shape proof (#1135) ──────────────────────────────────────

  "production-shape" = import ./test-scenarios/production-shape.nix {
    inherit pkgs pg_jsonschema sinex sinexCli;
  };

}
