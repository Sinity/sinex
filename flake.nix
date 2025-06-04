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

          # Build pgx_ulid from source
          pgx_ulid = pkgs.stdenv.mkDerivation rec {
            pname = "pgx_ulid";
            version = "0.1.5";

            src = pkgs.fetchFromGitHub {
              owner = "pksunkara";
              repo = "pgx_ulid";
              rev = "v${version}";
              sha256 = "sha256-zql7wtZQ+GDEpM0kld7vHCbWNHSpPKjYZgVWhx1GtvU=";
            };

            nativeBuildInputs = with pkgs; [
              cargo
              rustc
              postgresql_16
              pkg-config
            ];

            buildPhase = ''
              export PGRX_HOME=$(mktemp -d)
              cargo install --locked cargo-pgrx --version 0.11.3
              export PATH=$PATH:$HOME/.cargo/bin
              cargo pgrx init --pg16 ${pkgs.postgresql_16}/bin/pg_config
              cargo pgrx package --pg-config ${pkgs.postgresql_16}/bin/pg_config
            '';

            installPhase = ''
              mkdir -p $out/lib $out/share/postgresql/extension
              cp target/release/ulid-pg16/usr/pgsql-16/lib/ulid.so $out/lib/
              cp target/release/ulid-pg16/usr/pgsql-16/share/extension/* $out/share/postgresql/extension/
            '';
          };

          # PostgreSQL with all required extensions
          postgresqlWithExtensions = pkgs.postgresql_16.withPackages (p: [
            p.timescaledb
            p.pgvector
            # pgx_ulid  # Temporarily disabled due to network build issues
          ]);

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

          # Build promotion worker
          sinexPromoWorker = pkgs.rustPlatform.buildRustPackage {
            pname = "sinex-promo-worker";
            version = "0.1.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

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
            inherit hyprlandIngestor sinexPromoWorker;
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
          promoWorker = systemOutputs.packages.${final.system}.sinexPromoWorker;
        };
      };
    };
}
