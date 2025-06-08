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

          # PostgreSQL with all required extensions
          postgresqlWithExtensions = pkgs.postgresql_16.withPackages (p: [
            p.timescaledb
            p.pgvector
            p.pgx_ulid
            pg_jsonschema
          ]);

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
              postgresql_16
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

          # Flake apps for development workflows
          apps = {
            # Database management
            db-setup = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "db-setup" ''
                  set -euo pipefail

                  # Colors
                  RED='\033[0;31m'
                  GREEN='\033[0;32m'
                  BLUE='\033[0;34m'
                  NC='\033[0m'

                  log() { echo -e "''${BLUE}🗄️''${NC}  $*"; }
                  success() { echo -e "''${GREEN}✅''${NC} $*"; }
                  error() { echo -e "''${RED}❌''${NC} $*" >&2; }

                  MODE="''${1:-dev}"

                  case "$MODE" in
                    dev)
                      log "Setting up development database"
                      export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"

                      # Check if PostgreSQL is running
                      if ! ${postgresqlWithExtensions}/bin/pg_isready -h /run/postgresql >/dev/null 2>&1; then
                        error "PostgreSQL is not running on /run/postgresql"
                        error "Please ensure PostgreSQL is installed and running"
                        error "On NixOS: services.postgresql.enable = true;"
                        exit 1
                      fi

                      # Create database if it doesn't exist
                      if ! ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw sinex_dev; then
                        log "Creating database sinex_dev"
                        ${postgresqlWithExtensions}/bin/createdb -h /run/postgresql sinex_dev || {
                          error "Failed to create database. You may need to run:"
                          error "  sudo -u postgres createdb sinex_dev"
                          error "  sudo -u postgres psql -c \"GRANT ALL ON DATABASE sinex_dev TO $USER;\""
                          exit 1
                        }
                      fi

                      # Create extensions
                      log "Ensuring extensions are available"
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS ulid;" 2>/dev/null || {
                        warning "Could not create ulid extension. May need superuser privileges."
                      }
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS vector;" 2>/dev/null || {
                        warning "Could not create vector extension. May need superuser privileges."
                      }
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" 2>/dev/null || {
                        warning "Could not create timescaledb extension. May need superuser privileges."
                      }
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" 2>/dev/null || {
                        warning "Could not create pg_jsonschema extension. May need superuser privileges."
                      }

                      log "Running migrations"
                      DATABASE_URL="$DATABASE_URL" ${pkgs.sqlx-cli}/bin/sqlx migrate run
                      success "Development database ready at $DATABASE_URL"
                      ;;
                    prod)
                      log "Setting up production database"
                      export DATABASE_URL="postgresql:///sinex?host=/run/postgresql"

                      # Check PostgreSQL
                      if ! ${postgresqlWithExtensions}/bin/pg_isready -h /run/postgresql >/dev/null 2>&1; then
                        error "PostgreSQL is not running on /run/postgresql"
                        exit 1
                      fi

                      # Create production database
                      if ! ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw sinex; then
                        log "Creating database sinex"
                        ${postgresqlWithExtensions}/bin/createdb -h /run/postgresql sinex || {
                          error "Failed to create production database"
                          exit 1
                        }
                      fi

                      # Extensions
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS ulid;" 2>/dev/null || true
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS vector;" 2>/dev/null || true
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" 2>/dev/null || true
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" 2>/dev/null || true

                      # Migrations
                      DATABASE_URL="$DATABASE_URL" ${pkgs.sqlx-cli}/bin/sqlx migrate run
                      success "Production database ready at $DATABASE_URL"
                      ;;
                  reset)
                    log "Resetting development database"
                      export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"

                      warning "This will DROP and recreate the sinex_dev database"
                      read -p "Continue? [y/N] " -n 1 -r
                      echo
                      if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                        log "Reset cancelled"
                        exit 0
                      fi

                      # Drop and recreate
                      ${postgresqlWithExtensions}/bin/dropdb -h /run/postgresql sinex_dev 2>/dev/null || true
                      exec "$0" dev
                      ;;
                    check)
                      log "Checking database connectivity"

                      # Check PostgreSQL server
                      if ${postgresqlWithExtensions}/bin/pg_isready -h /run/postgresql >/dev/null 2>&1; then
                        success "PostgreSQL server: Running on /run/postgresql"
                      else
                        error "PostgreSQL server: Not available on /run/postgresql"
                        exit 1
                      fi

                      # Check dev database
                      if ${postgresqlWithExtensions}/bin/psql -h /run/postgresql sinex_dev -c "SELECT 1;" >/dev/null 2>&1; then
                        success "Development database: sinex_dev accessible"
                      else
                        warning "Development database: sinex_dev not found (run 'nix run .#db-setup dev')"
                      fi

                      # Check production database
                      if ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -c "SELECT 1;" >/dev/null 2>&1; then
                        success "Production database: sinex accessible"
                      else
                        warning "Production database: sinex not found (run 'nix run .#db-setup prod')"
                      fi
                      ;;
                    *)
                      error "Usage: nix run .#db-setup [dev|prod|reset|check]"
                      exit 1
                      ;;
                  esac
                ''
              );
            };

            # Database switching
            db = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "db-switcher" ''
                  set -euo pipefail

                  # Colors
                  RED='\033[0;31m'
                  GREEN='\033[0;32m'
                  BLUE='\033[0;34m'
                  YELLOW='\033[1;33m'
                  NC='\033[0m'

                  log() { echo -e "''${BLUE}🗄️''${NC}  $*"; }
                  success() { echo -e "''${GREEN}✅''${NC} $*"; }
                  warning() { echo -e "''${YELLOW}⚠️''${NC}  $*"; }
                  error() { echo -e "''${RED}❌''${NC} $*" >&2; }

                  STATE_FILE="$HOME/.sinex_current_db"
                  EPHEMERAL_BASE="/tmp/sinex_ephemeral"

                  # Function to create ephemeral database
                  create_ephemeral() {
                    local NUM="$1"
                    local EPHEMERAL_DIR="''${EPHEMERAL_BASE}_$NUM"
                    local EPHEMERAL_URL="postgresql:///sinex_ephemeral_$NUM?host=$EPHEMERAL_DIR&port=5432$NUM"
                    
                    if [ -d "$EPHEMERAL_DIR" ] && ${postgresqlWithExtensions}/bin/pg_isready -h "$EPHEMERAL_DIR" -p "5432$NUM" >/dev/null 2>&1; then
                      log "Reusing existing ephemeral database $NUM"
                    else
                      log "Creating ephemeral database $NUM"
                      mkdir -p "$EPHEMERAL_DIR"/{data,logs}
                      
                      # Initialize database
                      ${postgresqlWithExtensions}/bin/initdb -D "$EPHEMERAL_DIR/data" --no-locale --encoding=UTF8 >/dev/null
                      
                      # Configure
                      echo "unix_socket_directories = '$EPHEMERAL_DIR'" >> "$EPHEMERAL_DIR/data/postgresql.conf"
                      echo "shared_preload_libraries = 'timescaledb'" >> "$EPHEMERAL_DIR/data/postgresql.conf"
                      echo "port = 5432$NUM" >> "$EPHEMERAL_DIR/data/postgresql.conf"
                      echo "listen_addresses = ''" >> "$EPHEMERAL_DIR/data/postgresql.conf"
                      
                      # Start PostgreSQL
                      ${postgresqlWithExtensions}/bin/pg_ctl -D "$EPHEMERAL_DIR/data" -l "$EPHEMERAL_DIR/logs/postgres.log" start >/dev/null
                      
                      # Wait for startup
                      for i in {1..10}; do
                        if ${postgresqlWithExtensions}/bin/pg_isready -h "$EPHEMERAL_DIR" -p "5432$NUM" >/dev/null 2>&1; then
                          break
                        fi
                        sleep 0.5
                      done
                      
                      # Create database and extensions
                      ${postgresqlWithExtensions}/bin/createdb -h "$EPHEMERAL_DIR" -p "5432$NUM" "sinex_ephemeral_$NUM"
                      ${postgresqlWithExtensions}/bin/psql -h "$EPHEMERAL_DIR" -p "5432$NUM" -d "sinex_ephemeral_$NUM" -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null
                      ${postgresqlWithExtensions}/bin/psql -h "$EPHEMERAL_DIR" -p "5432$NUM" -d "sinex_ephemeral_$NUM" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
                      ${postgresqlWithExtensions}/bin/psql -h "$EPHEMERAL_DIR" -p "5432$NUM" -d "sinex_ephemeral_$NUM" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null
                      ${postgresqlWithExtensions}/bin/psql -h "$EPHEMERAL_DIR" -p "5432$NUM" -d "sinex_ephemeral_$NUM" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" >/dev/null
                      
                      # Run migrations
                      DATABASE_URL="$EPHEMERAL_URL" ${pkgs.sqlx-cli}/bin/sqlx migrate run >/dev/null
                    fi
                    
                    echo "$EPHEMERAL_URL"
                  }

                  # Function to show current database
                  show_current() {
                    if [ -f "$STATE_FILE" ]; then
                      local CURRENT=$(cat "$STATE_FILE")
                      echo "$CURRENT"
                    else
                      echo "sinex_dev"
                    fi
                  }

                  # Main command handling
                  TARGET="''${1:-}"
                  
                  if [ -z "$TARGET" ]; then
                    # Show current database
                    CURRENT=$(show_current)
                    log "Current database: $CURRENT"
                    case "$CURRENT" in
                      sinex_dev) echo "  URL: postgresql:///sinex_dev?host=/run/postgresql" ;;
                      sinex) echo "  URL: postgresql:///sinex?host=/run/postgresql" ;;
                      tmp*) 
                        NUM="''${CURRENT#tmp}"
                        NUM="''${NUM:-0}"
                        echo "  URL: postgresql:///sinex_ephemeral_$NUM?host=/tmp/sinex_ephemeral_$NUM&port=5432$NUM" 
                        ;;
                    esac
                    exit 0
                  fi

                  case "$TARGET" in
                    dev)
                      log "Switching to development database"
                      export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
                      echo "sinex_dev" > "$STATE_FILE"
                      echo "export DATABASE_URL=\"$DATABASE_URL\""
                      success "Switched to sinex_dev"
                      ;;
                    
                    prod)
                      log "Switching to production database"
                      export DATABASE_URL="postgresql:///sinex?host=/run/postgresql"
                      echo "sinex" > "$STATE_FILE"
                      echo "export DATABASE_URL=\"$DATABASE_URL\""
                      success "Switched to sinex (production)"
                      ;;
                    
                    tmp|tmp_*)
                      # Handle tmp (alias for tmp_0) and tmp_N
                      if [ "$TARGET" = "tmp" ]; then
                        NUM=0
                      else
                        NUM="''${TARGET#tmp_}"
                        if ! [[ "$NUM" =~ ^[0-9]$ ]]; then
                          error "Invalid ephemeral database number. Use tmp or tmp_0 through tmp_9"
                          exit 1
                        fi
                      fi
                      
                      log "Switching to ephemeral database $NUM"
                      URL=$(create_ephemeral "$NUM")
                      echo "tmp_$NUM" > "$STATE_FILE"
                      echo "export DATABASE_URL=\"$URL\""
                      success "Switched to ephemeral database $NUM"
                      ;;
                    
                    reset)
                      CURRENT=$(show_current)
                      warning "This will reset the current database ($CURRENT)"
                      read -p "Continue? [y/N] " -n 1 -r
                      echo
                      if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                        log "Reset cancelled"
                        exit 0
                      fi
                      
                      case "$CURRENT" in
                        sinex_dev|sinex)
                          ${postgresqlWithExtensions}/bin/dropdb -h /run/postgresql "$CURRENT" 2>/dev/null || true
                          nix run .#db-setup "''${CURRENT#sinex_}"
                          ;;
                        tmp*)
                          NUM="''${CURRENT#tmp_}"
                          EPHEMERAL_DIR="''${EPHEMERAL_BASE}_$NUM"
                          ${postgresqlWithExtensions}/bin/pg_ctl -D "$EPHEMERAL_DIR/data" stop 2>/dev/null || true
                          rm -rf "$EPHEMERAL_DIR"
                          create_ephemeral "$NUM" >/dev/null
                          success "Reset ephemeral database $NUM"
                          ;;
                      esac
                      ;;
                    
                    destroy)
                      CURRENT=$(show_current)
                      if [[ "$CURRENT" =~ ^tmp ]]; then
                        NUM="''${CURRENT#tmp_}"
                        EPHEMERAL_DIR="''${EPHEMERAL_BASE}_$NUM"
                        warning "This will destroy ephemeral database $NUM"
                        read -p "Continue? [y/N] " -n 1 -r
                        echo
                        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                          log "Destroy cancelled"
                          exit 0
                        fi
                        ${postgresqlWithExtensions}/bin/pg_ctl -D "$EPHEMERAL_DIR/data" stop 2>/dev/null || true
                        rm -rf "$EPHEMERAL_DIR"
                        success "Destroyed ephemeral database $NUM"
                        # Switch to dev
                        echo "sinex_dev" > "$STATE_FILE"
                        echo "export DATABASE_URL=\"postgresql:///sinex_dev?host=/run/postgresql\""
                        success "Switched to sinex_dev"
                      else
                        error "Can only destroy ephemeral databases"
                        exit 1
                      fi
                      ;;
                    
                    setup)
                      # Setup dev or prod database
                      DB_TYPE="''${2:-dev}"
                      case "$DB_TYPE" in
                        dev|prod)
                          nix run .#db-setup "$DB_TYPE"
                          ;;
                        *)
                          error "Usage: db setup [dev|prod]"
                          exit 1
                          ;;
                      esac
                      ;;
                    
                    shell|psql)
                      # Connect to current database
                      CURRENT=$(show_current)
                      case "$CURRENT" in
                        sinex_dev) 
                          DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
                          ;;
                        sinex) 
                          DATABASE_URL="postgresql:///sinex?host=/run/postgresql"
                          ;;
                        tmp*) 
                          NUM="''${CURRENT#tmp_}"
                          DATABASE_URL="postgresql:///sinex_ephemeral_$NUM?host=/tmp/sinex_ephemeral_$NUM&port=5432$NUM"
                          ;;
                      esac
                      log "Connecting to $CURRENT database"
                      ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL"
                      ;;
                    
                    *)
                      error "Usage: db [command] [args]"
                      echo "Commands:"
                      echo "  db              - Show current database"
                      echo "  db dev          - Switch to development database"
                      echo "  db prod         - Switch to production database"
                      echo "  db tmp          - Switch to ephemeral database 0"
                      echo "  db tmp_N        - Switch to ephemeral database N (0-9)"
                      echo "  db reset        - Reset current database"
                      echo "  db destroy      - Destroy current ephemeral database"
                      echo "  db setup [dev|prod] - Initialize dev or prod database"
                      echo "  db shell        - Connect to current database with psql"
                      exit 1
                      ;;
                  esac
                ''
              );
            };

            # Testing
            test = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "test-runner" ''
                  set -euo pipefail

                  BLUE='\033[0;34m'
                  GREEN='\033[0;32m'
                  NC='\033[0m'

                  log() { echo -e "''${BLUE}🧪''${NC}  $*"; }
                  success() { echo -e "''${GREEN}✅''${NC} $*"; }

                  TEST_TYPE="''${1:-unit}"

                  case "$TEST_TYPE" in
                    unit)
                      log "Running unit tests"
                      ${rustToolchain}/bin/cargo test --all-features
                      ;;
                    integration)
                      log "Running integration tests"
                      ${rustToolchain}/bin/cargo test --all-features database_integration_tests
                      ${rustToolchain}/bin/cargo test --all-features event_pipeline_integration_tests
                      ${rustToolchain}/bin/cargo test --all-features promotion_worker_integration
                      ;;
                    all)
                      log "Running all tests"
                      ${rustToolchain}/bin/cargo test --all-features
                      ;;
                    *)
                      echo "Usage: nix run .#test [unit|integration|all]"
                      exit 1
                      ;;
                  esac
                  success "Tests completed"
                ''
              );
            };

            # Development build
            build = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "build" ''
                  set -euo pipefail
                  echo "🔧 Building all workspace members..."
                  ${rustToolchain}/bin/cargo build --all-features
                  echo "✅ Build completed"
                ''
              );
            };

            # Check/lint
            check = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "check" ''
                  set -euo pipefail
                  echo "🔍 Checking code..."
                  ${rustToolchain}/bin/cargo check --all-features
                  ${rustToolchain}/bin/cargo clippy --all-features -- -D warnings
                  echo "✅ Check completed"
                ''
              );
            };

            # SQLX cache management
            sqlx-prepare = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "sqlx-prepare" ''
                  set -euo pipefail

                  BLUE='\033[0;34m'
                  GREEN='\033[0;32m'
                  YELLOW='\033[1;33m'
                  RED='\033[0;31m'
                  NC='\033[0m'

                  log() { echo -e "''${BLUE}🗄️''${NC}  $*"; }
                  success() { echo -e "''${GREEN}✅''${NC} $*"; }
                  warning() { echo -e "''${YELLOW}⚠️''${NC}  $*"; }
                  error() { echo -e "''${RED}❌''${NC} $*" >&2; }

                  log "Updating SQLX offline cache..."

                  # Check if DATABASE_URL is set
                  if [ -z "''${DATABASE_URL:-}" ]; then
                    warning "DATABASE_URL not set, using default"
                    export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
                  fi

                  # Check database connectivity
                  if ! ${postgresqlWithExtensions}/bin/pg_isready -h /run/postgresql >/dev/null 2>&1; then
                    error "PostgreSQL is not running on /run/postgresql"
                    error "Please start PostgreSQL or run: nix run .#db-setup dev"
                    exit 1
                  fi

                  # Check if database exists
                  if ! ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "SELECT 1;" >/dev/null 2>&1; then
                    warning "Database not accessible, trying to set it up"

                    # Run db setup
                    log "Setting up database..."
                    nix run .#db-setup dev || {
                      error "Failed to setup database"
                      exit 1
                    }
                  fi

                  # Ensure migrations are up to date
                  log "Running migrations..."
                  DATABASE_URL="$DATABASE_URL" ${pkgs.sqlx-cli}/bin/sqlx migrate run || {
                    error "Failed to run migrations"
                    exit 1
                  }

                  # Update the cache
                  log "Preparing SQLX offline cache..."
                  DATABASE_URL="$DATABASE_URL" ${pkgs.sqlx-cli}/bin/sqlx prepare --workspace -- --all-targets --all-features || {
                    error "Failed to prepare SQLX cache"
                    exit 1
                  }

                  success "SQLX cache updated successfully"
                  warning "Don't forget to commit the changes in .sqlx/"

                  # Show what changed
                  if command -v git >/dev/null 2>&1; then
                    echo ""
                    log "Changes to commit:"
                    git status --porcelain .sqlx/ | sed 's/^/  /'
                  fi
                ''
              );
            };

            # Real-time monitoring TUI
            monitor = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "monitor" ''
                  set -euo pipefail

                  # Colors and formatting
                  BLUE='\033[0;34m'
                  GREEN='\033[0;32m'
                  YELLOW='\033[1;33m'
                  RED='\033[0;31m'
                  NC='\033[0m'
                  BOLD='\033[1m'

                  log() { echo -e "''${BLUE}📊''${NC} $*"; }
                  success() { echo -e "''${GREEN}✅''${NC} $*"; }
                  warning() { echo -e "''${YELLOW}⚠️''${NC} $*"; }
                  error() { echo -e "''${RED}❌''${NC} $*" >&2; }

                  MODE="''${1:-dashboard}"

                  check_database() {
                    if ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -c "SELECT 1;" >/dev/null 2>&1; then
                      return 0
                    else
                      return 1
                    fi
                  }

                  show_dashboard() {
                    clear
                    echo -e "''${BOLD}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓''${NC}"
                    echo -e "''${BOLD}┃  Sinex Live Dashboard                                      ┃''${NC}"
                    echo -e "''${BOLD}┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫''${NC}"

                    # Database status
                    if check_database; then
                      echo -e "┃ 🗄️  Database: ''${GREEN}●''${NC} CONNECTED                              ┃"

                      # Event counts
                      local total_events=$(${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -t -c "SELECT COUNT(*) FROM raw.events;" 2>/dev/null | xargs)
                      local recent_events=$(${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -t -c "SELECT COUNT(*) FROM raw.events WHERE ts_ingest > NOW() - INTERVAL '1 hour';" 2>/dev/null | xargs)

                      echo "┃ 📈 Total Events: $total_events                                  ┃"
                      echo "┃ 🕐 Last Hour: $recent_events                                     ┃"

                      # Event sources breakdown
                      echo "┃                                                            ┃"
                      echo "┃ 📁 Recent Events by Source:                               ┃"
                      ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -t -c "
                        SELECT '┃   ' || RPAD(source, 15) || ': ' || LPAD(count::text, 8) || '                         ┃'
                        FROM (
                          SELECT source, COUNT(*) as count
                          FROM raw.events
                          WHERE ts_ingest > NOW() - INTERVAL '1 hour'
                          GROUP BY source
                          ORDER BY count DESC
                          LIMIT 5
                        ) t
                      " 2>/dev/null || echo "┃   No recent events                                         ┃"
                    else
                      echo -e "┃ 🗄️  Database: ''${RED}●''${NC} DISCONNECTED                           ┃"
                      echo "┃                                                            ┃"
                      echo -e "┃ ''${YELLOW}💡 Run: nix run .#db-setup dev''${NC}                            ┃"
                    fi

                    echo "┃                                                            ┃"
                    echo "┃ ⌨️  Commands:                                              ┃"
                    echo "┃   [r] Refresh   [e] Recent Events   [l] Live Tail         ┃"
                    echo "┃   [p] Processes [s] System Stats    [q] Quit               ┃"
                    echo -e "''${BOLD}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛''${NC}"
                  }

                  show_recent_events() {
                    clear
                    echo -e "''${BOLD}Recent Events (Last 10):''${NC}"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                    if check_database; then
                      ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -c "
                        SELECT
                          LEFT(id::text, 8) as id,
                          source,
                          event_type,
                          ts_ingest::timestamp(0)
                        FROM raw.events
                        ORDER BY ts_ingest DESC
                        LIMIT 10
                      " 2>/dev/null || echo "No events found"
                    else
                      error "Database not connected"
                    fi

                    echo ""
                    echo "Press any key to return to dashboard..."
                    read -n 1
                  }

                  live_tail() {
                    clear
                    echo -e "''${BOLD}Live Event Stream (Ctrl+C to exit):''${NC}"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                    if check_database; then
                      # Simple polling implementation
                      local last_id=""
                      while true; do
                        local new_events=$(${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -t -c "
                          SELECT COUNT(*) FROM raw.events WHERE id > '''$last_id'''
                        " 2>/dev/null | xargs)

                        if [[ "$new_events" -gt 0 ]]; then
                          ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -c "
                            SELECT
                              '[' || ts_ingest::timestamp(0) || '] ' ||
                              source || ':' || event_type ||
                              ' | ' || LEFT(payload::text, 50) || '...'
                            FROM raw.events
                            WHERE id > '''$last_id'''
                            ORDER BY ts_ingest DESC
                          " 2>/dev/null

                          last_id=$(${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -t -c "
                            SELECT id FROM raw.events ORDER BY ts_ingest DESC LIMIT 1
                          " 2>/dev/null | xargs)
                        fi

                        sleep 2
                      done
                    else
                      error "Database not connected"
                      exit 1
                    fi
                  }

                  show_processes() {
                    clear
                    echo -e "''${BOLD}System Processes:''${NC}"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                    # Show postgres status
                    if ${postgresqlWithExtensions}/bin/pg_isready -h /run/postgresql >/dev/null 2>&1; then
                      success "PostgreSQL: Running"
                    else
                      warning "PostgreSQL: Not running"
                    fi

                    # Show any running ingestors
                    echo ""
                    echo -e "''${BOLD}Running Ingestors:''${NC}"
                    ps aux | grep -E "(filesystem|kitty|hyprland)-ingestor" | grep -v grep || echo "No ingestors running"

                    echo ""
                    echo "Press any key to return to dashboard..."
                    read -n 1
                  }

                  show_system_stats() {
                    clear
                    echo -e "''${BOLD}System Statistics:''${NC}"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                    # Basic system info
                    echo "💾 Disk Usage:"
                    df -h . | tail -n +2

                    echo ""
                    echo "🧠 Memory Usage:"
                    free -h | head -n 2

                    if check_database; then
                      echo ""
                      echo "🗄️  Database Size:"
                      ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -d sinex -c "
                        SELECT
                          schemaname,
                          tablename,
                          pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                        FROM pg_tables
                        WHERE schemaname IN ('raw', 'sinex_schemas')
                        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                      " 2>/dev/null
                    fi

                    echo ""
                    echo "Press any key to return to dashboard..."
                    read -n 1
                  }

                  case "$MODE" in
                    dashboard|"")
                      # Interactive dashboard
                      trap 'echo "Exiting..."; exit 0' INT
                      while true; do
                        show_dashboard
                        read -n 1 -t 5 key || key=""
                        case "$key" in
                          r|R) continue ;;
                          e|E) show_recent_events ;;
                          l|L) live_tail ;;
                          p|P) show_processes ;;
                          s|S) show_system_stats ;;
                          q|Q) exit 0 ;;
                        esac
                      done
                      ;;
                    events)
                      show_recent_events
                      ;;
                    live)
                      live_tail
                      ;;
                    *)
                      error "Usage: nix run .#monitor [dashboard|events|live]"
                      exit 1
                      ;;
                  esac
                ''
              );
            };

            # Development server with process management
            dev = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "dev-server" ''
                  set -euo pipefail

                  BLUE='\033[0;34m'
                  GREEN='\033[0;32m'
                  YELLOW='\033[1;33m'
                  NC='\033[0m'

                  log() { echo -e "''${BLUE}🚀''${NC} $*"; }
                  success() { echo -e "''${GREEN}✅''${NC} $*"; }
                  warning() { echo -e "''${YELLOW}⚠️''${NC} $*"; }

                  MODE="''${1:-full}"

                  # Create mprocs configuration
                  create_mprocs_config() {
                    cat > .mprocs.yaml << EOF
                  procs:
                    database:
                      cmd: ["nix", "run", ".#db-setup", "dev"]
                      cwd: "$PWD"
                      env:
                        RUST_LOG: info

                    filesystem:
                      cmd: ["${filesystemIngestor}/bin/filesystem-ingestor", "run"]
                      cwd: "$PWD"
                      env:
                        RUST_LOG: info
                        DATABASE_URL: "postgresql:///sinex_dev?host=/run/postgresql"
                      autostart: false

                    kitty:
                      cmd: ["${kittyIngestor}/bin/kitty-ingestor", "run"]
                      cwd: "$PWD"
                      env:
                        RUST_LOG: info
                        DATABASE_URL: "postgresql:///sinex_dev?host=/run/postgresql"
                      autostart: false

                    hyprland:
                      cmd: ["${hyprlandIngestor}/bin/hyprland-ingestor", "run"]
                      cwd: "$PWD"
                      env:
                        RUST_LOG: info
                        DATABASE_URL: "postgresql:///sinex_dev?host=/run/postgresql"
                      autostart: false

                    monitor:
                      cmd: ["nix", "run", ".#monitor", "dashboard"]
                      cwd: "$PWD"
                      autostart: false

                  display:
                    app: "Sinex Development Environment"
                    begin_show: 4

                  keymap_procs:
                    # Database management
                    'd': database
                    # Individual ingestors
                    'f': filesystem
                    'k': kitty
                    'h': hyprland
                    # Monitoring
                    'm': monitor
                  EOF
                  }

                  case "$MODE" in
                    full)
                      log "Starting full development environment with mprocs"
                      create_mprocs_config
                      warning "Database will start automatically. Start other services manually with keys:"
                      warning "  [d] Database   [f] Filesystem   [k] Kitty   [h] Hyprland   [m] Monitor"
                      warning "  [Ctrl+A] then [q] to quit"
                      echo ""
                      ${pkgs.mprocs}/bin/mprocs --config .mprocs.yaml
                      ;;
                    db-only)
                      log "Setting up database only"
                      exec nix run .#db-setup dev
                      ;;
                    background)
                      log "Starting services in background"

                      # Setup database first
                      nix run .#db-setup dev

                      # Start ingestors in background
                      log "Starting filesystem ingestor..."
                      RUST_LOG=info DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql" \
                        ${filesystemIngestor}/bin/filesystem-ingestor run &

                      if command -v kitty >/dev/null 2>&1; then
                        log "Starting kitty ingestor..."
                        RUST_LOG=info DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql" \
                          ${kittyIngestor}/bin/kitty-ingestor run &
                      fi

                      if [[ -n "''${HYPRLAND_INSTANCE_SIGNATURE:-}" ]]; then
                        log "Starting hyprland ingestor..."
                        RUST_LOG=info DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql" \
                          ${hyprlandIngestor}/bin/hyprland-ingestor run &
                      fi

                      success "Services started in background"
                      warning "Monitor with: nix run .#monitor"
                      warning "Stop with: pkill -f ingestor"
                      ;;
                    *)
                      echo "Usage: nix run .#dev [full|db-only|background]"
                      echo ""
                      echo "Modes:"
                      echo "  full       - Interactive mprocs session (default)"
                      echo "  db-only    - Just setup database"
                      echo "  background - Start all services in background"
                      exit 1
                      ;;
                  esac
                ''
              );
            };

            # Ephemeral isolated test environment
            ephemeral = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "ephemeral-test" ''
                  set -euo pipefail

                  # Colors
                  GREEN='\033[0;32m'
                  RED='\033[0;31m'
                  YELLOW='\033[1;33m'
                  BLUE='\033[0;34m'
                  CYAN='\033[0;36m'
                  NC='\033[0m'

                  TEST_ID="sinex_ephemeral_$(date +%s)"
                  TEST_DIR="/tmp/$TEST_ID"
                  TEST_DB_URL="postgresql:///sinex_test?host=$TEST_DIR&port=54321"

                  log() { echo -e "''${BLUE}🧪''${NC} $*"; }
                  success() { echo -e "''${GREEN}✅''${NC} $*"; }
                  warning() { echo -e "''${YELLOW}⚠️''${NC} $*"; }
                  error() { echo -e "''${RED}❌''${NC} $*" >&2; }

                  echo -e "''${CYAN}=== Sinex Ephemeral Test Environment ===''${NC}"
                  echo "Test ID: $TEST_ID"
                  echo "Socket Path: $TEST_DIR"
                  echo "Test Directory: $TEST_DIR"
                  echo ""

                  cleanup() {
                    warning "Cleaning up ephemeral environment..."

                    # Stop all background processes
                    jobs -p | xargs -r kill 2>/dev/null || true

                    # Stop test database
                    if [[ -n "''${POSTGRES_PID:-}" ]]; then
                      kill $POSTGRES_PID 2>/dev/null || true
                      wait $POSTGRES_PID 2>/dev/null || true
                    fi

                    # Cleanup directories
                    rm -rf "$TEST_DIR" 2>/dev/null || true

                    success "Cleanup complete"
                  }

                  trap cleanup EXIT INT TERM

                  # Setup ephemeral environment
                  setup_environment() {
                    log "Setting up ephemeral environment..."

                    mkdir -p "$TEST_DIR"/{data,logs,watch}

                    # Start ephemeral PostgreSQL instance
                    log "Starting ephemeral PostgreSQL with socket in $TEST_DIR"
                    ${postgresqlWithExtensions}/bin/initdb -D "$TEST_DIR/data" --no-locale --encoding=UTF8 >/dev/null

                    # Configure PostgreSQL
                    echo "unix_socket_directories = '$TEST_DIR'" >> "$TEST_DIR/data/postgresql.conf"
                    echo "shared_preload_libraries = 'timescaledb'" >> "$TEST_DIR/data/postgresql.conf"
                    echo "max_connections = 50" >> "$TEST_DIR/data/postgresql.conf"
                    echo "shared_buffers = 128MB" >> "$TEST_DIR/data/postgresql.conf"
                    echo "port = 54321" >> "$TEST_DIR/data/postgresql.conf"
                    echo "listen_addresses = " >> "$TEST_DIR/data/postgresql.conf"

                    # Start PostgreSQL
                    if ! ${postgresqlWithExtensions}/bin/pg_ctl -D "$TEST_DIR/data" -l "$TEST_DIR/logs/postgres.log" start >/dev/null; then
                      error "Failed to start PostgreSQL. Log output:"
                      cat "$TEST_DIR/logs/postgres.log" >&2
                      return 1
                    fi
                    POSTGRES_PID=$(cat "$TEST_DIR/data/postmaster.pid" | head -n 1)

                    # Wait for startup
                    for i in {1..10}; do
                      if ${postgresqlWithExtensions}/bin/pg_isready -h "$TEST_DIR" -p 54321 >/dev/null 2>&1; then
                        break
                      fi
                      sleep 0.5
                    done

                    # Create database and extensions
                    ${postgresqlWithExtensions}/bin/createdb -h "$TEST_DIR" -p 54321 sinex_test
                    ${postgresqlWithExtensions}/bin/psql -h "$TEST_DIR" -p 54321 -d sinex_test -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null
                    ${postgresqlWithExtensions}/bin/psql -h "$TEST_DIR" -p 54321 -d sinex_test -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
                    ${postgresqlWithExtensions}/bin/psql -h "$TEST_DIR" -p 54321 -d sinex_test -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null
                    ${postgresqlWithExtensions}/bin/psql -h "$TEST_DIR" -p 54321 -d sinex_test -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" >/dev/null

                    # Run migrations
                    log "Running migrations on ephemeral database"
                    DATABASE_URL="$TEST_DB_URL" ${pkgs.sqlx-cli}/bin/sqlx migrate run

                    success "Ephemeral environment ready"
                    echo ""
                    echo "Environment Details:"
                    echo "  Database URL: $TEST_DB_URL"
                    echo "  Test Directory: $TEST_DIR"
                    echo "  PostgreSQL PID: $POSTGRES_PID"
                    echo ""
                  }

                  run_tests() {
                    log "Running tests in ephemeral environment"

                    # Set environment for tests
                    export TEST_DATABASE_URL="$TEST_DB_URL"
                    export RUST_LOG="''${RUST_LOG:-info}"

                    # Run tests
                    ${rustToolchain}/bin/cargo test --all-features "''${@}"

                    success "Tests completed"
                  }

                  interactive_mode() {
                    echo -e "''${CYAN}Ephemeral environment is ready!''${NC}"
                    echo ""
                    echo "Available commands:"
                    echo "  Export database URL: export TEST_DATABASE_URL='$TEST_DB_URL'"
                    echo "  Connect to database: ${postgresqlWithExtensions}/bin/psql '$TEST_DB_URL'"
                    echo "  Run tests: TEST_DATABASE_URL='$TEST_DB_URL' cargo test"
                    echo "  Monitor: nix run .#monitor"
                    echo ""
                    echo "Press Ctrl+C to cleanup and exit"

                    # Keep running until interrupted
                    while true; do
                      sleep 1
                    done
                  }

                  # Main execution
                  setup_environment

                  MODE="''${1:-interactive}"
                  shift || true

                  case "$MODE" in
                    test)
                      run_tests "''${@}"
                      ;;
                    interactive|shell)
                      interactive_mode
                      ;;
                    *)
                      error "Usage: nix run .#ephemeral [test|interactive] [test-args...]"
                      exit 1
                      ;;
                  esac
                ''
              );
            };
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

              # Process management and monitoring
              mprocs
              btop
              jq

              # Build dependencies
              openssl
              pkg-config
            ];

            shellHook = ''
              # Use local peer authentication via Unix socket
              export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
              export SQLX_OFFLINE=true
              
              # Database switching function
              db() {
                local output=$(nix run .#db -- "$@")
                if [[ "$output" =~ ^export ]]; then
                  eval "$output"
                else
                  echo "$output"
                fi
              }

              # Auto-setup development database if PostgreSQL is running
              if ${postgresqlWithExtensions}/bin/pg_isready -h /run/postgresql >/dev/null 2>&1; then
                if ! ${postgresqlWithExtensions}/bin/psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw sinex_dev; then
                  echo "🗄️ Setting up development database..."
                  ${postgresqlWithExtensions}/bin/createdb -h /run/postgresql sinex_dev >/dev/null 2>&1 || true
                  ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null 2>&1 || true
                  ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1 || true
                  ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null 2>&1 || true
                  ${postgresqlWithExtensions}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" >/dev/null 2>&1 || true
                  sqlx migrate run >/dev/null 2>&1 || true
                  echo "✅ Database ready"
                fi
              fi

              cat <<'EOF'
              ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
              ┃  Sinex Exocortex devShell                                  ┃
              ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
              ┃ 🚀 QUICK START                                             ┃
              ┃   dev env  : nix run .#dev                                 ┃
              ┃   monitor  : nix run .#monitor                             ┃
              ┃                                                            ┃
              ┃ 📡 INGESTORS (uses current database)                        ┃
              ┃   filesystem: cargo run --bin filesystem-ingestor          ┃
              ┃   hyprland  : cargo run --bin hyprland-ingestor            ┃
              ┃   kitty     : cargo run --bin kitty-ingestor               ┃
              ┃   unified   : cargo run --bin unified-ingestor             ┃
              ┃   dry run   : cargo run --bin <ingestor> -- --dry-run      ┃
              ┃                                                            ┃
              ┃ 🗄️  DATABASE SWITCHING                                     ┃
              ┃   setup    : db setup [dev|prod]                           ┃
              ┃   shell    : db shell (connect to current database)        ┃
              ┃   sqlx     : nix run .#sqlx-prepare                        ┃
              ┃   current  : db                                            ┃
              ┃   switch   : db [dev|prod|tmp|tmp_0-9]                     ┃
              ┃   reset    : db reset (reset current database)             ┃
              ┃   destroy  : db destroy (remove ephemeral database)        ┃
              ┃                                                            ┃
              ┃ 🧪 TESTING                                                  ┃
              ┃   run      : nix run .#test [unit|integration|all]         ┃
              ┃   isolated : nix run .#ephemeral [test|interactive]        ┃
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
