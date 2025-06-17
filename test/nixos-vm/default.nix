# NixOS VM tests for Sinex
{ pkgs ? import <nixpkgs> {} }:

{
  # Basic operational test
  basic = import ./test-scenarios/basic-flow.nix { inherit pkgs; };
  
  # Comprehensive multi-source stress testing
  multi-source = import ./test-scenarios/multi-source.nix { inherit pkgs; };
  
  # Failure recovery and resilience testing
  failure-recovery = import ./test-scenarios/failure-recovery.nix { inherit pkgs; };
  
  # Performance validation and load testing
  performance = import ./test-scenarios/performance.nix { inherit pkgs; };
}