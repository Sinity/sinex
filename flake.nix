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

            cargoHash = "sha256-eLjONo10zuqdkFrUzd3nlrgJ9FEJePxXlFGvuB7MRQE=";

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

            cargoHash = "sha256-eLjONo10zuqdkFrUzd3nlrgJ9FEJePxXlFGvuB7MRQE=";

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

            cargoHash = "sha256-eLjONo10zuqdkFrUzd3nlrgJ9FEJePxXlFGvuB7MRQE=";

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

            cargoHash = "sha256-eLjONo10zuqdkFrUzd3nlrgJ9FEJePxXlFGvuB7MRQE=";

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
            inherit
              hyprlandIngestor
              filesystemIngestor
              kittyIngestor
              sinexPromoWorker
              ;
            default = sinexPromoWorker;
          };

          # Flake apps for development workflows
          apps = {
            # Database management
            db-setup = {
              type = "app";
              program = toString (pkgs.writeShellScript "db-setup" ''
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
                    export PGDATA="$PWD/.postgres"
                    export DATABASE_URL="postgresql:///sinex?host=$PGDATA"
                    
                    if [[ ! -d "$PGDATA" ]]; then
                      log "Initializing PostgreSQL"
                      ${postgresqlWithExtensions}/bin/initdb --no-locale --encoding=UTF8 -D "$PGDATA" >/dev/null
                      
                      cat >> "$PGDATA/postgresql.conf" << EOL
                shared_preload_libraries = 'timescaledb'
                max_connections = 100
                shared_buffers = 256MB
                effective_cache_size = 1GB
                EOL
                    fi
                    
                    if ! ${postgresqlWithExtensions}/bin/pg_ctl status -D "$PGDATA" >/dev/null 2>&1; then
                      log "Starting PostgreSQL"
                      ${postgresqlWithExtensions}/bin/pg_ctl -D "$PGDATA" -l "$PGDATA/logfile" -o "--unix_socket_directories='$PGDATA'" start >/dev/null
                      sleep 1
                    fi
                    
                    ${postgresqlWithExtensions}/bin/createdb -h "$PGDATA" sinex 2>/dev/null || true
                    ${postgresqlWithExtensions}/bin/psql -h "$PGDATA" -d postgres -c "CREATE ROLE sinex WITH LOGIN;" 2>/dev/null || true
                    
                    ${postgresqlWithExtensions}/bin/psql -h "$PGDATA" -d sinex -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null
                    ${postgresqlWithExtensions}/bin/psql -h "$PGDATA" -d sinex -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
                    ${postgresqlWithExtensions}/bin/psql -h "$PGDATA" -d sinex -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null
                    
                    log "Running migrations"
                    ${pkgs.sqlx-cli}/bin/sqlx migrate run
                    success "Development database ready"
                    ;;
                  test)
                    log "Setting up test database"
                    export TEST_DATABASE_URL="postgres://sinex_test:testpass@localhost:5433/sinex_test"
                    ${postgresqlWithExtensions}/bin/createdb --if-not-exists sinex_test 2>/dev/null || true
                    ${postgresqlWithExtensions}/bin/psql "$TEST_DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS ulid;" >/dev/null 2>&1 || true
                    ${postgresqlWithExtensions}/bin/psql "$TEST_DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1 || true
                    ${postgresqlWithExtensions}/bin/psql "$TEST_DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null 2>&1 || true
                    DATABASE_URL="$TEST_DATABASE_URL" ${pkgs.sqlx-cli}/bin/sqlx migrate run
                    success "Test database ready"
                    ;;
                  reset)
                    log "Resetting development database"
                    rm -rf "$PWD/.postgres"
                    exec "$0" dev
                    ;;
                  check)
                    log "Checking database connectivity"
                    PGDATA="$PWD/.postgres"
                    if ${postgresqlWithExtensions}/bin/psql -h "$PGDATA" -d sinex -c "SELECT 1;" >/dev/null 2>&1; then
                      success "Database connection OK"
                    else
                      error "Database connection failed"
                      exit 1
                    fi
                    ;;
                  *)
                    error "Usage: nix run .#db-setup [dev|test|reset|check]"
                    exit 1
                    ;;
                esac
              '');
            };
            
            # Testing
            test = {
              type = "app";
              program = toString (pkgs.writeShellScript "test-runner" ''
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
              '');
            };
            
            # Development build
            build = {
              type = "app";
              program = toString (pkgs.writeShellScript "build" ''
                set -euo pipefail
                echo "🔧 Building all workspace members..."
                ${rustToolchain}/bin/cargo build --all-features
                echo "✅ Build completed"
              '');
            };
            
            # Check/lint
            check = {
              type = "app";
              program = toString (pkgs.writeShellScript "check" ''
                set -euo pipefail
                echo "🔍 Checking code..."
                ${rustToolchain}/bin/cargo check --all-features
                ${rustToolchain}/bin/cargo clippy --all-features -- -D warnings
                echo "✅ Check completed"
              '');
            };
            
            # Real-time monitoring TUI
            monitor = {
              type = "app";
              program = toString (pkgs.writeShellScript "monitor" ''
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
                  local pgdata="$PWD/.postgres"
                  if ${postgresqlWithExtensions}/bin/psql -h "$pgdata" -d sinex -c "SELECT 1;" >/dev/null 2>&1; then
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
                    local total_events=$(${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -t -c "SELECT COUNT(*) FROM raw.events;" 2>/dev/null | xargs)
                    local recent_events=$(${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -t -c "SELECT COUNT(*) FROM raw.events WHERE ts_ingest > NOW() - INTERVAL '1 hour';" 2>/dev/null | xargs)
                    
                    echo "┃ 📈 Total Events: $total_events                                  ┃"
                    echo "┃ 🕐 Last Hour: $recent_events                                     ┃"
                    
                    # Event sources breakdown
                    echo "┃                                                            ┃"
                    echo "┃ 📁 Recent Events by Source:                               ┃"
                    ${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -t -c "
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
                    ${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -c "
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
                      local new_events=$(${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -t -c "
                        SELECT COUNT(*) FROM raw.events WHERE id > '''$last_id'''
                      " 2>/dev/null | xargs)
                      
                      if [[ "$new_events" -gt 0 ]]; then
                        ${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -c "
                          SELECT 
                            '[' || ts_ingest::timestamp(0) || '] ' ||
                            source || ':' || event_type ||
                            ' | ' || LEFT(payload::text, 50) || '...'
                          FROM raw.events 
                          WHERE id > '''$last_id'''
                          ORDER BY ts_ingest DESC
                        " 2>/dev/null
                        
                        last_id=$(${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -t -c "
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
                  if ${postgresqlWithExtensions}/bin/pg_ctl status -D "$PWD/.postgres" >/dev/null 2>&1; then
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
                    ${postgresqlWithExtensions}/bin/psql -h "$PWD/.postgres" -d sinex -c "
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
              '');
            };
            
            # Development server with process management
            dev = {
              type = "app";
              program = toString (pkgs.writeShellScript "dev-server" ''
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
                      DATABASE_URL: "postgresql:///sinex?host=$PWD/.postgres"
                    autostart: false
                  
                  kitty:
                    cmd: ["${kittyIngestor}/bin/kitty-ingestor", "run"]
                    cwd: "$PWD"
                    env:
                      RUST_LOG: info
                      DATABASE_URL: "postgresql:///sinex?host=$PWD/.postgres"
                    autostart: false
                  
                  hyprland:
                    cmd: ["${hyprlandIngestor}/bin/hyprland-ingestor", "run"]
                    cwd: "$PWD"
                    env:
                      RUST_LOG: info
                      DATABASE_URL: "postgresql:///sinex?host=$PWD/.postgres"
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
                    RUST_LOG=info DATABASE_URL="postgresql:///sinex?host=$PWD/.postgres" \
                      ${filesystemIngestor}/bin/filesystem-ingestor run &
                    
                    if command -v kitty >/dev/null 2>&1; then
                      log "Starting kitty ingestor..."
                      RUST_LOG=info DATABASE_URL="postgresql:///sinex?host=$PWD/.postgres" \
                        ${kittyIngestor}/bin/kitty-ingestor run &
                    fi
                    
                    if [[ -n "''${HYPRLAND_INSTANCE_SIGNATURE:-}" ]]; then
                      log "Starting hyprland ingestor..."
                      RUST_LOG=info DATABASE_URL="postgresql:///sinex?host=$PWD/.postgres" \
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
              '');
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
              export PGDATA="$PWD/.postgres"
              export PGHOST="$PGDATA"
              export DATABASE_URL="postgresql:///sinex?host=$PGDATA"
              export TEST_DATABASE_URL="postgres://sinex_test:testpass@localhost:5433/sinex_test"
              
              cat <<'EOF'
              ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
              ┃  Sinex Exocortex devShell                                  ┃
              ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
              ┃ 🚀 QUICK START                                             ┃
              ┃   dev env  : nix run .#dev                                 ┃
              ┃   monitor  : nix run .#monitor                             ┃
              ┃                                                            ┃
              ┃ 🗄️  DATABASE                                               ┃
              ┃   setup    : nix run .#db-setup [dev|test|reset|check]     ┃
              ┃   connect  : psql $DATABASE_URL                            ┃
              ┃                                                            ┃
              ┃ 🧪 TESTING                                                  ┃
              ┃   run      : nix run .#test [unit|integration|all]         ┃
              ┃   watch    : cargo watch -x test                           ┃
              ┃                                                            ┃
              ┃ 🔧 BUILD & CHECK                                            ┃
              ┃   build    : nix run .#build                               ┃
              ┃   check    : nix run .#check                               ┃
              ┃                                                            ┃
              ┃ 📊 MONITORING                                               ┃
              ┃   dashboard: nix run .#monitor                             ┃
              ┃   live tail: nix run .#monitor live                        ┃
              ┃   events   : nix run .#monitor events                      ┃
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
        };
      };
    };
}
