{
  description = "Sinex - Universal data capture and query system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      flake-utils,
    }:
    let
      # System-specific outputs
      systemOutputs = flake-utils.lib.eachDefaultSystem (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true; # For TimescaleDB in VM tests
          };

          rustToolchain = fenix.packages.${system}.complete.withComponents [
            "cargo"
            "clippy"
            "rust-src"
            "rust-analyzer"
            "rustc"
            "rustfmt"
            "llvm-tools-preview"
            "rustc-codegen-cranelift"
          ];

          # Extract git information for version tracking
          gitRev = self.rev or self.dirtyRev or "unknown";
          gitShortRev = builtins.substring 0 8 gitRev;
          version = "0.1.0"; # TODO: Extract from workspace
          buildTime = "unknown"; # builtins.currentTime not available in pure mode

          # Helper to build Rust packages with common configuration
          buildRustPackage =
            package:
            pkgs.rustPlatform.buildRustPackage {
              pname = package;
              version = version;
              src = ./.;
              cargoLock.lockFile = ./Cargo.lock;
              buildInputs = with pkgs; [
                openssl
                dbus
                systemd
              ];
              nativeBuildInputs = with pkgs; [
                pkg-config
                protobuf
              ];
              cargoBuildFlags = [
                "-p"
                package
              ];
              auditable = false;
              doCheck = false;
              SQLX_OFFLINE = "true";
              preBuild = ''
                if [ ! -d ".sqlx" ]; then
                  echo "ERROR: .sqlx directory not found. Run 'cargo sqlx prepare' first."
                  exit 1
                fi

                # Create build info for version tracking
                mkdir -p src/generated
                cat > src/generated/build_info.rs << EOF
                pub const VERSION: &str = "${version}";
                pub const GIT_HASH: &str = "${gitRev}";
                pub const GIT_SHORT_HASH: &str = "${gitShortRev}";
                pub const BUILD_TIME: &str = "${buildTime}";
                pub const BUILD_HOST: &str = "${system}";
                EOF
              '';
              postInstall = ''
                # Include migrations in the package
                mkdir -p $out/share/sinex
                cp -r migrations $out/share/sinex/
              '';
            };
          # Package the Python CLI tool
          sinex-cli = pkgs.python3Packages.buildPythonApplication rec {
            pname = "sinex-cli";
            version = "0.1.0";
            format = "other";

            src = ./cli;

            propagatedBuildInputs = with pkgs.python3Packages; [
              click
              psycopg2
              rich
              pyyaml
            ];

            installPhase = ''
              mkdir -p $out/bin
              cp exo.py $out/bin/sinex-cli
              chmod +x $out/bin/sinex-cli

              # Also provide 'exo' as an alias
              ln -s $out/bin/sinex-cli $out/bin/exo
            '';

            # Add a simple check to ensure the CLI can import dependencies
            checkPhase = ''
              $out/bin/sinex-cli --help > /dev/null
            '';

            meta = with pkgs.lib; {
              description = "Sinex CLI - Query your digital memory";
              license = licenses.mit;
              maintainers = [ ];
            };
          };

          # Build pg_jsonschema from pre-built deb
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
            '';
          };
        in
        {
          packages = {
            # Core satellite services
            sinexIngestd = buildRustPackage "sinex-ingestd";
            sinexGateway = buildRustPackage "sinex-gateway";

            # Event source satellites
            sinexFsWatcher = buildRustPackage "sinex-fs-watcher";
            sinexTerminalSatellite = buildRustPackage "sinex-terminal-satellite";
            sinexDesktopSatellite = buildRustPackage "sinex-desktop-satellite";
            sinexSystemSatellite = buildRustPackage "sinex-system-satellite";
            sinexDocumentIngestor = buildRustPackage "sinex-document-ingestor";

            # Automaton satellites
            sinexTerminalCommandCanonicalizer = buildRustPackage "sinex-terminal-command-canonicalizer";

            # Support services
            healthAggregator = buildRustPackage "sinex-health-aggregator";
            sinexHealthAggregator = buildRustPackage "sinex-health-aggregator";
            sinexPreflight = buildRustPackage "sinex-preflight";
            sinexCli = sinex-cli;

            # Default package is now the ingestion daemon
            default = buildRustPackage "sinex-ingestd";
            inherit pg_jsonschema;
          };

          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # Rust toolchain with cranelift backend
              rustToolchain
              cargo-watch
              cargo-nextest
              cargo-llvm-cov

              # Development tools
              just
              sqlx-cli
              mold # Fast linker for compilation speed
              sccache # Compilation cache for dependencies
              python3Packages.rich-cli # Rich CLI formatting

              # Python and testing
              python3
              python3Packages.click
              python3Packages.psycopg2
              python3Packages.rich
              python3Packages.pyyaml

              # Process management and monitoring
              mprocs
              btop
              jq

              # VM testing tools (Agent Alpha)
              qemu
              qemu_kvm

              # Build dependencies
              openssl
              pkg-config
              dbus
              dbus.dev
              protobuf
            ];

            shellHook = ''
              # Database configuration
              export DATABASE_NAME="sinex_dev"
              export DATABASE_URL="postgresql:///$DATABASE_NAME?host=/run/postgresql"
              export SINEX_TEST_OPTIMIZATIONS="true"
              export SINEX_ANALYTICS_DIR="$HOME/.sinex-analytics"
              mkdir -p "$SINEX_ANALYTICS_DIR"

              # Setup sccache for faster builds
              export RUSTC_WRAPPER="sccache"
              export SCCACHE_DIR="$HOME/.cache/sccache"
              export SCCACHE_CACHE_SIZE="10G"

              # Create cargo wrapper for analytics
              mkdir -p .nix-shell-bin
              cat > .nix-shell-bin/cargo << 'CARGO_WRAPPER'
              #!/usr/bin/env bash
              # Wrapper to add analytics to cargo commands

              # Commands that should have analytics
              case "$1" in
                  build|check|test|bench|run|clippy)
                      if [ -x "scripts/compile-analytics.sh" ]; then
                          exec scripts/compile-analytics.sh $(which cargo) "$@"
                      else
                          exec $(which cargo) "$@"
                      fi
                      ;;
                  *)
                      # Other commands run normally
                      exec $(which cargo) "$@"
                      ;;
              esac
              CARGO_WRAPPER
              chmod +x .nix-shell-bin/cargo
              export PATH="$PWD/.nix-shell-bin:$PATH"


              # Clear screen for clean start
              clear

              # Header
              echo -e "\033[1;36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
              echo -e "\033[1;36m   🚀 SINEX Development Environment\033[0m"
              echo -e "\033[1;36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
              echo

              # Database setup
              if command -v pg_isready >/dev/null 2>&1 && pg_isready -h /run/postgresql >/dev/null 2>&1; then
                DB_STATUS="🟢"
                if ! psql -h /run/postgresql -lqt | cut -d \| -f 1 | grep -qw "$DATABASE_NAME"; then
                  createdb -h /run/postgresql "$DATABASE_NAME" 2>/dev/null || DB_STATUS="🟡"
                fi
                
                # Run migrations silently
                if [ -d "migrations" ] && [ "$DB_STATUS" = "🟢" ]; then
                  if sqlx migrate run --source migrations >/dev/null 2>&1; then
                    MIGRATION_COUNT=$(sqlx migrate info --source migrations 2>/dev/null | grep -c "applied" || echo "0")
                    MIGRATION_INFO="$MIGRATION_COUNT migrations"
                  else
                    DB_STATUS="🟡"
                    MIGRATION_INFO="run 'just migrate'"
                  fi
                fi
              else
                DB_STATUS="🔴"
                MIGRATION_INFO="PostgreSQL not running"
              fi

              # Build cache status
              if [ -d "$SCCACHE_DIR" ]; then
                CACHE_SIZE=$(du -sh "$SCCACHE_DIR" 2>/dev/null | cut -f1 || echo "0")
                CACHE_INFO="🟢 $CACHE_SIZE"
              else
                CACHE_INFO="🟢 initializing"
              fi

              # Auto-start daemons silently (capture output for MOTD)
              GIT_DAEMON_MSG=""
              COMPILE_DAEMON_MSG=""
              
              if [ -f "scripts/git-state-tracker.sh" ]; then
                TRACKER_STATUS=$(timeout 2s ./scripts/git-state-tracker.sh status 2>/dev/null || echo '{"status":"not_running"}')
                if ! echo "$TRACKER_STATUS" | jq -e '.status == "running"' >/dev/null 2>&1; then
                  if timeout 5s ./scripts/git-state-tracker.sh start >/dev/null 2>&1; then
                    GIT_DAEMON_MSG="started"
                  else
                    GIT_DAEMON_MSG="failed to start"
                  fi
                fi
              fi

              if [ -f "scripts/compile-daemon.sh" ]; then
                DAEMON_STATUS=$(timeout 2s ./scripts/compile-daemon.sh status 2>/dev/null || echo '{"status":"not_running"}')
                if ! echo "$DAEMON_STATUS" | jq -e '.status != "no_data"' >/dev/null 2>&1; then
                  if timeout 5s ./scripts/compile-daemon.sh start >/dev/null 2>&1; then
                    COMPILE_DAEMON_MSG="started"
                  else
                    COMPILE_DAEMON_MSG="failed to start"
                  fi
                fi
              fi

              # Export daemon messages for MOTD
              export GIT_DAEMON_MSG
              export COMPILE_DAEMON_MSG

              # Show MOTD using rich
              MOTD_SCRIPT="$PWD/scripts/motd-rich.sh"
              if [ -x "$MOTD_SCRIPT" ]; then
                "$MOTD_SCRIPT"
              else
                echo -e "\033[1;36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
                echo -e "\033[1;36m   🚀 SINEX Development Environment\033[0m"
                echo -e "\033[1;36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
                echo
                echo "Run 'just' to see available commands"
              fi
            '';
          };

          # NixOS VM tests
          checks = {
            # VM tests need to be updated for the new satellite architecture
            # Temporarily disabled until test scenarios are rewritten

            # sinex-vm-basic = pkgs.callPackage ./test/nixos-vm/test-scenarios/basic-flow.nix {
            #   sinex-ingestd = self.packages.${system}.sinexIngestd;
            #   sinex-gateway = self.packages.${system}.sinexGateway;
            #   sinex-fs-watcher = self.packages.${system}.sinexFsWatcher;
            #   pg_jsonschema = self.packages.${system}.pg_jsonschema;
            # };
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

      # Overlay providing pg_jsonschema
      overlays.default = final: prev: {
        postgresql16Packages = prev.postgresql16Packages // {
          pg_jsonschema = self.packages.${final.system}.pg_jsonschema;
        };
      };
    };
}
