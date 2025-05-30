{
  description = "Sinnix Exocortex - Universal data capture and query system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    let
      # System-specific outputs
      systemOutputs = flake-utils.lib.eachDefaultSystem (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" ];
          };
          
          # Build individual ingestors
          hyprlandIngestor = pkgs.rustPlatform.buildRustPackage {
            pname = "hyprland-ingestor";
            version = "0.1.0";
            src = ./ingestors/hyprland;
            
            cargoLock = {
              lockFile = ./ingestors/hyprland/Cargo.lock;
            };
            
            buildInputs = with pkgs; [
              openssl
              pkg-config
            ];
            
            nativeBuildInputs = with pkgs; [
              pkg-config
            ];
          };
        in
        {
          packages = {
            inherit hyprlandIngestor;
            default = hyprlandIngestor;
          };
          
          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # Rust toolchain
              rustToolchain
              cargo-watch
              cargo-nextest
              
              # Database tools
              postgresql_16
              timescaledb
              
              # Python for CLI
              python311
              python311Packages.click
              python311Packages.rich
              python311Packages.psycopg2
              
              # Development tools
              just
              bacon
              sqlx-cli
              
              # Build dependencies
              openssl
              pkg-config
            ];
            
            shellHook = ''
              echo "🧠 Sinnix Exocortex Development Environment"
              echo "Rust: $(rustc --version)"
              echo "PostgreSQL: $(postgres --version)"
              echo "Python: $(python --version)"
            '';
          };
        });
    in
    systemOutputs // {
      # NixOS module
      nixosModules = {
        default = ./nixos;
        sinnix-exocortex = ./nixos;
      };
      
      # Overlay providing our packages
      overlays.default = final: prev: {
        sinnixExocortex = {
          hyprlandIngestor = systemOutputs.packages.${final.system}.hyprlandIngestor;
        };
      };
    };
}