# NixOS VM tests for Sinex
{ pkgs ? import <nixpkgs> {}
, sinex-ingestd ? null
, sinex-gateway ? null
, pg_jsonschema ? null
}:

{
  # Basic operational test
  basic = import ./test-scenarios/basic-flow.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema;
  };
  
  # Comprehensive multi-source stress testing
  multi-source = import ./test-scenarios/multi-source.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema;
  };
  
  # Failure recovery and resilience testing
  failure-recovery = import ./test-scenarios/failure-recovery.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema;
  };
  
  # Performance validation and load testing
  performance = import ./test-scenarios/performance.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema;
  };
  
  # Advanced testing capabilities
  # Chaos engineering - tests system resilience under failure conditions
  chaos-engineering = import ./chaos-engineering.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema;
  };
  
  # Production scale - tests system performance at production workloads  
  production-scale = import ./production-scale.nix {
    inherit pkgs sinex-ingestd sinex-gateway pg_jsonschema;
  };
}
