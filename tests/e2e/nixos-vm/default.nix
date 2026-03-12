# NixOS VM tests for Sinex
{ pkgs ? import <nixpkgs> {}
, sinex-ingestd ? null
, sinex-gateway ? null
, pg_jsonschema ? null
, sinex ? null
, sinexCli ? null
}:

{
  # Basic operational test
  basic = import ./test-scenarios/basic-flow.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
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

}
