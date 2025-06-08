{
  description = "Sinex - Universal data capture and query system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      flake-utils,
    }:
    let
      # System-specific outputs
      systemOutputs = flake-utils.lib.eachDefaultSystem (
        system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };

          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
            ];
          };

          # Helper to build Rust packages with common configuration
          buildRustPackage = package: pkgs.rustPlatform.buildRustPackage {
            pname = package;
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            buildInputs = with pkgs; [ openssl pkg-config ];
            nativeBuildInputs = with pkgs; [ pkg-config ];
            cargoBuildFlags = [ "-p" package ];
            auditable = false;
            doCheck = false;
            SQLX_OFFLINE = "true";
            preBuild = ''
              if [ ! -d ".sqlx" ]; then
                echo "ERROR: .sqlx directory not found. Run 'cargo sqlx prepare' first."
                exit 1
              fi
            '';
          };
        in
        {
          packages = {
            hyprlandIngestor = buildRustPackage "hyprland-ingestor";
            filesystemIngestor = buildRustPackage "filesystem-ingestor";
            kittyIngestor = buildRustPackage "kitty-ingestor";
            sinexPromoWorker = buildRustPackage "sinex-promo-worker";
            default = buildRustPackage "sinex-promo-worker";
          };

          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # Rust toolchain
              rustToolchain
              cargo-watch
              cargo-nextest

              # Development tools
              just
              bacon
              sqlx-cli

              # Process management and monitoring
              mprocs
              btop
              jq

              # Build dependencies
              openssl
              pkg-config
            ];

            shellHook = ''
              # Ensure default database is set up
              ./script/db.sh dev >/dev/null 2>&1 || true
              echo "📦 Sinex devShell ready. Run 'just' to see available commands."
            '';
          };
        }
      );
    in
    systemOutputs
    // {
      # NixOS module
      nixosModules = {
        default = ./nixos;
        sinex = ./nixos;
      };
    };
}
