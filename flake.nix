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
            config = {
              allowUnfree = true; # Required for TimescaleDB
            };
          };

          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
            ];
          };

          # PostgreSQL with all required extensions
          postgresqlWithExtensions = pkgs.postgresql_16.withPackages (p: [
            p.timescaledb
            p.pgvector
            p.pgx_ulid
          ]);

          # Build individual ingestors
          hyprlandIngestor = pkgs.rustPlatform.buildRustPackage {
            pname = "hyprland-ingestor";
            version = "0.1.0";
            src = ./.;

            cargoHash = "sha256-59AZVhe8dt/5XTPQ7wDAib0P9q66d+QFZrAJyGvSGdI=";

            buildInputs = with pkgs; [
              openssl
              pkg-config
            ];

            nativeBuildInputs = with pkgs; [
              pkg-config
            ];

            cargoBuildFlags = [
              "-p"
              "hyprland-ingestor"
            ];
          };

          # Build filesystem ingestor
          filesystemIngestor = pkgs.rustPlatform.buildRustPackage {
            pname = "filesystem-ingestor";
            version = "0.1.0";
            src = ./.;

            cargoHash = "";

            buildInputs = with pkgs; [
              openssl
              pkg-config
            ];

            nativeBuildInputs = with pkgs; [
              pkg-config
            ];

            cargoBuildFlags = [
              "-p"
              "filesystem-ingestor"
            ];
          };

          # Build kitty ingestor
          kittyIngestor = pkgs.rustPlatform.buildRustPackage {
            pname = "kitty-ingestor";
            version = "0.1.0";
            src = ./.;

            cargoHash = "sha256-59AZVhe8dt/5XTPQ7wDAib0P9q66d+QFZrAJyGvSGdI=";

            buildInputs = with pkgs; [
              openssl
              pkg-config
            ];

            nativeBuildInputs = with pkgs; [
              pkg-config
            ];

            cargoBuildFlags = [
              "-p"
              "kitty-ingestor"
            ];
          };

          # Build promotion worker
          sinexPromoWorker = pkgs.rustPlatform.buildRustPackage {
            pname = "sinex-promo-worker";
            version = "0.1.0";
            src = ./.;

            cargoHash = "sha256-59AZVhe8dt/5XTPQ7wDAib0P9q66d+QFZrAJyGvSGdI=";

            buildInputs = with pkgs; [
              openssl
              pkg-config
              postgresql_16
            ];

            nativeBuildInputs = with pkgs; [
              pkg-config
            ];

            cargoBuildFlags = [
              "-p"
              "sinex-promo-worker"
            ];
          };
        in
        {
          packages = {
            inherit hyprlandIngestor filesystemIngestor kittyIngestor sinexPromoWorker;
            default = sinexPromoWorker;
          };

          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # Rust toolchain
              rustToolchain
              cargo-watch
              cargo-nextest

              # Database tools
              postgresqlWithExtensions

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
              cat <<'EOF'
              ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
              ┃  Sinex Exocortex devShell                                  ┃
              ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
              ┃ psql     : connect → $DATABASE_URL                         ┃
              ┃ test DB  : ./scripts/setup_test_db.sh                      ┃
              ┃ migrate  : sqlx migrate run                                ┃
              ┃ run unit : cargo test --all-features                       ┃
              ┃ run e2e  : cargo test --test e2e                           ┃
              ┃ lint     : nix flake check                                 ┃
              ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
              EOF
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

      # Overlay providing our packages
      overlays.default = final: prev: {
        sinex = {
          hyprlandIngestor = systemOutputs.packages.${final.system}.hyprlandIngestor;
          filesystemIngestor = systemOutputs.packages.${final.system}.filesystemIngestor;
          kittyIngestor = systemOutputs.packages.${final.system}.kittyIngestor;
          promoWorker = systemOutputs.packages.${final.system}.sinexPromoWorker;
        };
      };
    };
}
