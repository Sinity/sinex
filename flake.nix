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

          # Build cargo-pgrx separately
          cargo-pgrx = pkgs.rustPlatform.buildRustPackage rec {
            pname = "cargo-pgrx";
            version = "0.12.6";

            src = pkgs.fetchCrate {
              inherit pname version;
              hash = "sha256-7aQkrApALZe6EoQGVShGBj0UIATnfOy2DytFj9IWdEA=";
            };

            cargoHash = "sha256-pnMxWWfvr1/AEp8DvG4awig8zjdHizJHoZ5RJA8CL08=";

            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl ];

            doCheck = false; # Tests fail in nix build environment
          };

          # Use pre-built pg_jsonschema deb package
          pg_jsonschema = pkgs.stdenv.mkDerivation rec {
            pname = "pg_jsonschema";
            version = "0.3.3";

            src = pkgs.fetchurl {
              url = "https://github.com/supabase/pg_jsonschema/releases/download/v${version}/pg_jsonschema-v${version}-pg16-amd64-linux-gnu.deb";
              hash = "sha256-6VSbAZrrItYgnpKMhVqffC4fGp9zzPYaMB6/Bf+Ha/g=";
            };

            nativeBuildInputs = [ pkgs.dpkg ];

            dontBuild = true;
            dontStrip = true;
            dontFixup = true;

            unpackPhase = ''
              dpkg-deb -x $src .
            '';

            installPhase = ''
              mkdir -p $out/lib $out/share/postgresql/extension

              # Find and copy the actual files (not symlinks)
              find . -name "*.so" -type f -exec cp {} $out/lib/ \;
              find . -name "*.sql" -type f -exec cp {} $out/share/postgresql/extension/ \;
              find . -name "*.control" -type f -exec cp {} $out/share/postgresql/extension/ \;

              # List what we installed
              echo "Installed files:"
              ls -la $out/lib/
              ls -la $out/share/postgresql/extension/
            '';
          };

          # Note: We expect PostgreSQL with required extensions to be installed at the system level
          # On NixOS, this means configuring services.postgresql with extraPlugins

          # Build individual ingestors
          hyprlandIngestor = pkgs.rustPlatform.buildRustPackage {
            pname = "hyprland-ingestor";
            version = "0.1.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

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

            # Ensure SQLX offline mode for build
            SQLX_OFFLINE = "true";

            # Disable cargo-auditable to avoid version conflicts
            auditable = false;

            # Don't run tests during build
            doCheck = false;

            preBuild = ''
              # Verify .sqlx directory exists
              if [ ! -d ".sqlx" ]; then
                echo "ERROR: .sqlx directory not found. Run 'cargo sqlx prepare' first."
                exit 1
              fi
            '';
          };

          # Build filesystem ingestor
          filesystemIngestor = pkgs.rustPlatform.buildRustPackage {
            pname = "filesystem-ingestor";
            version = "0.1.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

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

            # Ensure SQLX offline mode for build
            SQLX_OFFLINE = "true";

            # Disable cargo-auditable to avoid version conflicts
            auditable = false;

            # Don't run tests during build
            doCheck = false;

            preBuild = ''
              # Verify .sqlx directory exists
              if [ ! -d ".sqlx" ]; then
                echo "ERROR: .sqlx directory not found. Run 'cargo sqlx prepare' first."
                exit 1
              fi
            '';
          };

          # Build kitty ingestor
          kittyIngestor = pkgs.rustPlatform.buildRustPackage {
            pname = "kitty-ingestor";
            version = "0.1.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

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

            # Ensure SQLX offline mode for build
            SQLX_OFFLINE = "true";

            # Disable cargo-auditable to avoid version conflicts
            auditable = false;

            # Don't run tests during build
            doCheck = false;

            preBuild = ''
              # Verify .sqlx directory exists
              if [ ! -d ".sqlx" ]; then
                echo "ERROR: .sqlx directory not found. Run 'cargo sqlx prepare' first."
                exit 1
              fi
            '';
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
            ];

            nativeBuildInputs = with pkgs; [
              pkg-config
            ];

            cargoBuildFlags = [
              "-p"
              "sinex-promo-worker"
            ];

            # Ensure SQLX offline mode for build
            SQLX_OFFLINE = "true";

            # Disable cargo-auditable to avoid version conflicts
            auditable = false;

            # Don't run tests during build
            doCheck = false;

            preBuild = ''
              # Verify .sqlx directory exists
              if [ ! -d ".sqlx" ]; then
                echo "ERROR: .sqlx directory not found. Run 'cargo sqlx prepare' first."
                exit 1
              fi
            '';
          };
        in
        {
          packages = {
            inherit
              hyprlandIngestor
              filesystemIngestor
              kittyIngestor
              sinexPromoWorker
              cargo-pgrx
              pg_jsonschema
              ;
            default = sinexPromoWorker;
          };


          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # Rust toolchain
              rustToolchain
              cargo-watch
              cargo-nextest

              # Database tools
              postgresql_16

              # Python for CLI
              python311
              python311Packages.click
              python311Packages.rich
              python311Packages.psycopg2

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
              # Load saved database state or use default
              if [ -f "$HOME/.sinex_db_state" ]; then
                export DATABASE_URL=$(cat "$HOME/.sinex_db_state")
              else
                export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
              fi

              # Shell aliases for common commands
              alias db='./script/db.sh'
              alias dev='./script/dev.sh'  
              alias monitor='./script/monitor.sh'
              alias test='./script/test.sh'
              alias sqlx-prepare='./script/sqlx-prepare.sh'
              
              # Update DATABASE_URL after db operations
              update_db_env() {
                if [ -f "$HOME/.sinex_db_state" ]; then
                  export DATABASE_URL=$(cat "$HOME/.sinex_db_state")
                fi
              }
              
              # Enhanced db function that updates environment
              db() {
                ./script/db.sh "$@"
                update_db_env
              }

              # Auto-setup development database if PostgreSQL is running
              if pg_isready -h /run/postgresql >/dev/null 2>&1; then
                if ! psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw sinex_dev; then
                  echo "🗄️ Setting up development database..."
                  createdb -h /run/postgresql sinex_dev >/dev/null 2>&1 || true
                  sqlx migrate run --source migration >/dev/null 2>&1 || true
                  echo "✅ Database ready"
                fi
              fi

              cat <<'EOF'
              ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
              ┃  Sinex Exocortex devShell                                  ┃
              ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
              ┃ 🚀 QUICK START                                             ┃
              ┃   dev        : Start full development environment          ┃
              ┃   monitor    : Open monitoring dashboard                   ┃
              ┃   test       : Run test suite                              ┃
              ┃                                                            ┃
              ┃ 📡 INGESTORS (uses current database)                        ┃
              ┃   filesystem: cargo run --bin filesystem-ingestor          ┃
              ┃   hyprland  : cargo run --bin hyprland-ingestor            ┃
              ┃   kitty     : cargo run --bin kitty-ingestor               ┃
              ┃   unified   : cargo run --bin unified-ingestor             ┃
              ┃   dry run   : cargo run --bin <ingestor> -- --dry-run      ┃
              ┃                                                            ┃
              ┃ 🗄️  DATABASE MANAGEMENT                                     ┃
              ┃   db         : Show current database                       ┃
              ┃   db setup   : db setup [dev|prod]                         ┃
              ┃   db shell   : Connect to current database                 ┃
              ┃   db switch  : db [dev|prod|tmp|tmp_0-9]                   ┃
              ┃   db reset   : Reset current database                      ┃
              ┃   sqlx-prepare: Update SQLX offline cache                  ┃
              ┃                                                            ┃
              ┃ 🧪 TESTING                                                  ┃
              ┃   run      : nix run .#test [unit|integration|all]         ┃
              ┃   isolated : db tmp && cargo test [test-name] -- [flags]   ┃
              ┃   watch    : cargo watch -x test                           ┃
              ┃                                                            ┃
              ┃ 🔧 BUILD & CHECK                                            ┃
              ┃   build    : nix run .#build                               ┃
              ┃   check    : nix run .#check                               ┃
              ┃   watch    : cargo watch -x check                          ┃
              ┃                                                            ┃
              ┃ 📊 MONITORING (uses current database)                       ┃
              ┃   dashboard: nix run .#monitor                             ┃
              ┃   live tail: nix run .#monitor live                        ┃
              ┃   events   : nix run .#monitor events                      ┃
              ┃   query cli: ./cli/exo.py query --limit 10                 ┃
              ┃                                                            ┃
              ┃ 📦 ALL APPS: nix flake show                                ┃
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
          pg_jsonschema = systemOutputs.packages.${final.system}.pg_jsonschema;
        };
        # Add pg_jsonschema to PostgreSQL extension packages
        postgresqlPackages = prev.postgresqlPackages // {
          pg_jsonschema = systemOutputs.packages.${final.system}.pg_jsonschema;
        };
        postgresql16Packages = prev.postgresql16Packages // {
          pg_jsonschema = systemOutputs.packages.${final.system}.pg_jsonschema;
        };
        # Also make pg_jsonschema available at top level
        pg_jsonschema = systemOutputs.packages.${final.system}.pg_jsonschema;
      };
    };
}
