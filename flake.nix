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

          # For now, create a stub pgx_ulid until we can build it properly
          pgx_ulid = pkgs.stdenv.mkDerivation {
            pname = "pgx_ulid-stub";
            version = "0.1.5";
            
            dontUnpack = true;
            
            installPhase = ''
              mkdir -p $out/lib $out/share/postgresql/extension
              
              # Create stub control file
              cat > $out/share/postgresql/extension/ulid.control << 'EOF'
# ulid extension
comment = 'ULID support for PostgreSQL (stub)'
default_version = '0.1.5'
module_pathname = '$libdir/ulid'
relocatable = true
EOF
              
              # Create stub SQL file
              cat > $out/share/postgresql/extension/ulid--0.1.5.sql << 'EOF'
-- ULID extension stub
-- This is a placeholder until the full pgx_ulid extension can be built
CREATE OR REPLACE FUNCTION ulid_generate()
RETURNS text
LANGUAGE sql
AS $$
  SELECT encode(gen_random_bytes(16), 'hex');
$$;

CREATE OR REPLACE FUNCTION ulid_to_uuid(ulid text)
RETURNS uuid
LANGUAGE sql
AS $$
  SELECT decode(ulid, 'hex')::uuid;
$$;
EOF
              
              # Create stub shared library (empty file)
              touch $out/lib/ulid.so
            '';
          };

          # PostgreSQL with all required extensions
          postgresqlWithExtensions = pkgs.postgresql_16.withPackages (p: [
            p.timescaledb
            p.pgvector
            pgx_ulid
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
