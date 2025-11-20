{ inputs ? {}, pkgs, lib, config, ... }:
let
  system = pkgs.stdenv.hostPlatform.system;
  fenixInput =
    if inputs ? fenix then inputs.fenix
    else builtins.getFlake (builtins.fetchTarball {
      url = "https://github.com/nix-community/fenix/archive/refs/heads/master.tar.gz";
      sha256 = "1xlziab9wrds2n49mk1q621rf9nwbymrj5ssqb2rwixjzz5k67cz";
    });
  fenixPkgs = fenixInput.packages.${system}.complete;
  ...
}
