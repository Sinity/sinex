# NixOS VM tests for Sinex
{ pkgs ? import <nixpkgs> {} }:

{
  # Basic operational test
  basic = import ./test-scenarios/basic-flow.nix { inherit pkgs; };
  
  # Future tests will be added here:
  # multi-source = import ./test-scenarios/multi-source.nix { inherit pkgs; };
  # failure-recovery = import ./test-scenarios/failure-recovery.nix { inherit pkgs; };
  # performance = import ./test-scenarios/performance.nix { inherit pkgs; };
}