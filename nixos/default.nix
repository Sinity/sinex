{ lib, pkgs, ... }@args:

# Entry point for the Sinex NixOS module.
# Re-export the structured module tree under nixos/modules.

import ./modules/default.nix args
