# NixOS VM tests for Sinex
{ pkgs ? import <nixpkgs> {}
, sinex-ingestd ? null
, sinex-gateway ? null
, pg_jsonschema ? null
, sinex ? null
, sinexVmFsRuntime ? null
, sinexCli ? null
, xtask ? null
, sinexVmTestSuite ? null
}:

{
  # Basic operational test
  basic = import ./test-scenarios/basic-flow.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinexCli sinexVmTestSuite;
    sinex = if sinexVmFsRuntime != null then sinexVmFsRuntime else sinex;
  };

  "replay-smoke" = import ./test-scenarios/replay-smoke.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinexCli;
    sinex = if sinexVmFsRuntime != null then sinexVmFsRuntime else sinex;
  };

  preflight = import ./preflight_deployment_test.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
    lib = pkgs.lib;
  };

  maintenance = import ./test-scenarios/maintenance.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "node-matrix" = import ./test-scenarios/node-matrix.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "multi-source" = import ./test-scenarios/multi-source.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "failure-recovery" = import ./test-scenarios/failure-recovery.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  performance = import ./test-scenarios/performance.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "production-scale" = import ./production-scale.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "mtls-enforcement" = import ./test-scenarios/mtls-enforcement.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "kitty-eventsource" = import ./test-scenarios/kitty-eventsource.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "sinexctl-e2e" = import ./test-scenarios/sinexctl-e2e.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  # ─── Chaos scenarios ─────────────────────────────────────────────────────────

  "chaos-network-partition" = import ./test-scenarios/chaos-network-partition.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli sinexVmTestSuite;
  };

  "chaos-process-restart" = import ./test-scenarios/chaos-process-restart.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli sinexVmTestSuite;
  };

  "chaos-clock-skew" = import ./test-scenarios/chaos-clock-skew.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli sinexVmTestSuite;
  };

  # ─── xtask concurrency (requires pre-built xtask binary) ─────────────────────

  "xtask-concurrency" = import ./test-scenarios/xtask-concurrency.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli xtask sinexVmTestSuite;
  };

  # ─── Environmental hostility ──────────────────────────────────────────────────

  "hostile-host" = import ./test-scenarios/hostile-host.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

  "migration-stress" = import ./test-scenarios/migration-stress.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
  };

}
