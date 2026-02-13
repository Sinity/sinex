{
  description = "Sinex - Universal data capture and query system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
    };
    agenix = {
      url = "github:ryantm/agenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      fenix,
      crane,
      agenix,
      flake-utils,
    }:
    let
      # pg_jsonschema - PostgreSQL JSON Schema validation extension
      # Not in nixpkgs; packaged from Supabase binary release
      pgJsonschemaOverlay = final: prev: {
        postgresql16Packages = prev.postgresql16Packages // {
          pg_jsonschema = final.stdenv.mkDerivation rec {
            pname = "pg_jsonschema";
            version = "0.3.3";

            src = final.fetchurl {
              url = "https://github.com/supabase/pg_jsonschema/releases/download/v${version}/pg_jsonschema-v${version}-pg16-amd64-linux-gnu.deb";
              sha256 = "sha256-6VSbAZrrItYgnpKMhVqffC4fGp9zzPYaMB6/Bf+Ha/g=";
            };

            nativeBuildInputs = [ final.dpkg ];
            dontConfigure = true;
            dontBuild = true;
            dontStrip = true;
            dontFixup = true;

            unpackPhase = ''
              dpkg-deb -x $src .
            '';

            installPhase = ''
              mkdir -p $out/lib $out/share/postgresql/extension
              find . -name "*.so" -type f -exec cp {} $out/lib/ \;
              find . -name "*.sql" -type f -exec cp {} $out/share/postgresql/extension/ \;
              find . -name "*.control" -type f -exec cp {} $out/share/postgresql/extension/ \;
            '';

            meta = with final.lib; {
              description = "PostgreSQL JSON Schema validation extension";
              homepage = "https://github.com/supabase/pg_jsonschema";
              license = licenses.asl20;
              platforms = platforms.linux;
            };
          };
        };
      };

      # System-specific outputs
      systemOutputs = flake-utils.lib.eachDefaultSystem (
        system:
        let
          # Apply pg_jsonschema overlay
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
            overlays = [ pgJsonschemaOverlay ];
          };

          fenixPkgs = fenix.packages.${system}.complete;
          rustToolchain = fenixPkgs.toolchain;

          # Crane with fenix toolchain
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          # Extract git information for version tracking
          gitRev = self.rev or self.dirtyRev or "unknown";
          gitShortRev = builtins.substring 0 8 gitRev;
          version = "0.1.0";

          # PostgreSQL with required extensions for SQLx build-time validation
          postgresForSqlx = pkgs.postgresql_16.withPackages (ps: [
            ps.timescaledb
            ps.pgvector
            ps.pgx_ulid
            pkgs.postgresql16Packages.pg_jsonschema
          ]);

          # Filter source for Rust builds
          src = craneLib.cleanCargoSource ./.;

          # Common build arguments
          commonArgs = {
            inherit src;
            strictDeps = true;

            buildInputs = with pkgs; [
              openssl
              dbus
              systemd
            ];

            nativeBuildInputs = with pkgs; [
              pkg-config
              protobuf
            ];

            # Environment for SQLx offline mode fallback
            SQLX_OFFLINE = "true";
          };

          # Build workspace dependencies once (cached layer)
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          # Ephemeral Postgres setup for SQLx query validation
          postgresPreBuild = ''
            export PGDATA="$TMPDIR/pgdata"
            mkdir -p "$PGDATA"
            ${postgresForSqlx}/bin/initdb -D "$PGDATA" --locale=C --encoding=UTF8 --auth=trust

            export PGHOST="$TMPDIR"
            export PGPORT=55433
            echo "unix_socket_directories = '$TMPDIR'" >> "$PGDATA/postgresql.conf"
            echo "port = $PGPORT" >> "$PGDATA/postgresql.conf"

            ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -w start

            ${postgresForSqlx}/bin/createdb -h "$PGHOST" -p "$PGPORT" sinex_dev || true

            ${postgresForSqlx}/bin/psql -h "$PGHOST" -p "$PGPORT" -d postgres -U postgres -v ON_ERROR_STOP=1 -c "DO \$\$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sinity') THEN
                CREATE ROLE sinity LOGIN CREATEDB;
              END IF;
            END
            \$\$;"

            ${postgresForSqlx}/bin/psql -h "$PGHOST" -p "$PGPORT" -d sinex_dev -U postgres -v ON_ERROR_STOP=1 -c "GRANT ALL ON SCHEMA public TO sinity;"

            export PGUSER="sinity"
            export DATABASE_URL="postgresql:///sinex_dev?host=$PGHOST&port=$PGPORT"

            # Run migrations to create schema for SQLx query validation
            cargo run --manifest-path crate/lib/sinex-schema/Cargo.toml --bin sinex-schema -- up
          '';

          postgresPostBuild = ''
            if [ -n "''${PGDATA:-}" ]; then
              ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -m fast stop || true
            fi
          '';

          # Build a specific package from the workspace
          mkPackage = pname: craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts pname;
            cargoExtraArgs = "-p ${pname}";
            doCheck = false;

            preBuild = postgresPreBuild;
            postBuild = postgresPostBuild;
          });

          # All packages built from Cargo.toml names
          sinexPackages = {
            # Core services
            sinex-ingestd = mkPackage "sinex-ingestd";
            sinex-gateway = mkPackage "sinex-gateway";

            # CLI
            sinexctl = mkPackage "sinexctl";

            # Ingestors (data capture nodes)
            sinex-fs-ingestor = mkPackage "sinex-fs-ingestor";
            sinex-terminal-ingestor = mkPackage "sinex-terminal-ingestor";
            sinex-desktop-ingestor = mkPackage "sinex-desktop-ingestor";
            sinex-system-ingestor = mkPackage "sinex-system-ingestor";
            sinex-document-ingestor = mkPackage "sinex-document-ingestor";

            # Automatons (processing nodes)
            sinex-terminal-command-canonicalizer = mkPackage "sinex-terminal-command-canonicalizer";
            sinex-health-automaton = mkPackage "sinex-health-automaton";
            sinex-analytics-automaton = mkPackage "sinex-analytics-automaton";
            sinex-search-automaton = mkPackage "sinex-search-automaton";
            sinex-content-automaton = mkPackage "sinex-content-automaton";
            sinex-pkm-automaton = mkPackage "sinex-pkm-automaton";

            # Schema management CLI
            sinex-schema = mkPackage "sinex-schema";

            # Aggregated suite with all binaries
            sinex = pkgs.symlinkJoin {
              name = "sinex";
              paths = [
                sinexPackages.sinex-ingestd
                sinexPackages.sinex-gateway
                sinexPackages.sinexctl
                sinexPackages.sinex-fs-ingestor
                sinexPackages.sinex-terminal-ingestor
                sinexPackages.sinex-desktop-ingestor
                sinexPackages.sinex-system-ingestor
                sinexPackages.sinex-document-ingestor
                sinexPackages.sinex-terminal-command-canonicalizer
                sinexPackages.sinex-health-automaton
                sinexPackages.sinex-analytics-automaton
                sinexPackages.sinex-search-automaton
                sinexPackages.sinex-content-automaton
                sinexPackages.sinex-pkm-automaton
                sinexPackages.sinex-schema
              ];
            };

            # PostgreSQL extension
            pg_jsonschema = pkgs.postgresql16Packages.pg_jsonschema;

            # Default package
            default = sinexPackages.sinex-ingestd;
          };

          # VM tests
          vmTests = import ./tests/e2e/nixos-vm/default.nix {
            inherit pkgs;
            sinex-ingestd = sinexPackages.sinex-ingestd;
            sinex-gateway = sinexPackages.sinex-gateway;
            sinex = sinexPackages.sinex;
            sinexCli = sinexPackages.sinexctl;
            pg_jsonschema = pkgs.postgresql16Packages.pg_jsonschema;
          };

          limitedVmTests = pkgs.lib.filterAttrs (
            name: _:
            pkgs.lib.elem name [
              "basic"
              "preflight"
            ]
          ) vmTests;

          vmPackagesAll = pkgs.lib.mapAttrs' (
            name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value
          ) vmTests;

          vmPackagesEssential = pkgs.lib.mapAttrs' (
            name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value
          ) limitedVmTests;

        in
        rec {
          packages = sinexPackages // vmPackagesEssential;

          legacyPackages = vmPackagesAll;

          formatter = pkgs.nixpkgs-fmt;

          checks = pkgs.lib.mapAttrs' (name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value) (
            pkgs.lib.filterAttrs (_: value: pkgs.lib.isDerivation value) limitedVmTests
          );

          devShells.default =
            let
              pathHash = builtins.hashString "sha256" (toString ./.);
              hexPair = builtins.substring 0 2 pathHash;
              offsetRaw = builtins.fromTOML "v = 0x${hexPair}";
              natsOffset = offsetRaw.v - (offsetRaw.v / 100 * 100);
              natsPort = 4222 + natsOffset;

              stateDir = ".sinex";
              pgPort = 5432;
            in
            pkgs.mkShell {
              packages = with pkgs; [
                # Rust toolchain (Fenix)
                fenixPkgs.toolchain
                fenixPkgs.rust-analyzer
                fenixPkgs.clippy
                fenixPkgs.rustfmt
                fenixPkgs.llvm-tools
                fenixPkgs.rust-src

                # Cargo development tools
                cargo-nextest
                cargo-insta
                cargo-llvm-cov
                cargo-audit
                cargo-machete
                cargo-modules
                tokei
                mold
                binutils

                # Infrastructure services
                nats-server
                postgresForSqlx

                # Build/runtime dependencies
                jq
                openssl
                pkg-config
                dbus dbus.dev
                git-annex
                nsc

                # VM testing
                qemu qemu_kvm

                # Shell/Nix tooling
                direnv
                zstd
                git
              ];

              PGUSER = "sinity";
              PGDATABASE = "sinex_dev";
              SINEX_PG_BIN = "${postgresForSqlx}/bin";
              NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";

              shellHook = ''
                export PATH="$PWD/scripts:$PWD/${stateDir}/target/debug:$PATH"
                export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.dbus ]}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                export CLIPPY_CONF_DIR="$PWD/.config"
                export SINEX_STATE_DIR="''${XDG_STATE_HOME:-$HOME/.local/state}/sinex"
                export SINEX_CACHE_DIR="''${XDG_CACHE_HOME:-$HOME/.cache}/sinex"
                export SINEX_TEST_RESULTS_DIR="$SINEX_CACHE_DIR/test-results"
                export SINEX_DEV_STATE_DIR="$PWD/${stateDir}"
                export SINEX_DEV_PG_PORT="${toString pgPort}"
                export SINEX_DEV_NATS_PORT="${toString natsPort}"
                export SINEX_DEV_GATEWAY_PORT="9999"
                export DATABASE_URL="postgresql:///sinex_dev?host=$SINEX_DEV_STATE_DIR/run&port=${toString pgPort}"
                export PGHOST="$SINEX_DEV_STATE_DIR/run"
                export PGPORT="${toString pgPort}"
                export SINEX_NATS_URL="nats://localhost:${toString natsPort}"
                export SINEX_RPC_URL="https://127.0.0.1:9999"

                sx() { cargo xtask "$@"; }
                xt() { cargo xtask "$@"; }

                if [ -n "''${PS1:-}" ] && [ -t 1 ] && [ -z "''${SINEX_DEVENV_MOTD_ONCE:-}" ]; then
                  export SINEX_DEVENV_MOTD_ONCE=1
                  cargo xtask status --summary || true
                fi
              '';
            };
        }
      );
    in
    systemOutputs
    // {
      # NixOS module
      nixosModules = {
        default =
          { ... }:
          {
            imports = [
              agenix.nixosModules.default
              (import ./nixos)
            ];
          };
        sinex = args: (self.nixosModules.default args);
      };

      nixosConfigurations = {
        example = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                nixpkgs.config.allowUnfree = true;
                # Disable services that require secrets/real infrastructure for evaluation
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.sinex.core.gateway.enable = lib.mkForce false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleMonitoring = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-monitoring.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleDevSandbox = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-dev-sandbox.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleHeadless = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-headless.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleRemoteSatellite = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-remote-satellite.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };

        exampleCoordination = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            (
              { ... }:
              {
                nixpkgs.overlays = [ self.overlays.default ];
              }
            )
            ./nixos/example-coordination.nix
            (
              { lib, ... }:
              {
                boot.isContainer = true;
                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
                services.nats.enable = lib.mkForce false;
                services.postgresql.enable = lib.mkForce false;
                system.stateVersion = "24.05";
              }
            )
          ];
        };
      };

      # Unified overlay: pg_jsonschema + all sinex packages
      overlays.default = nixpkgs.lib.composeExtensions pgJsonschemaOverlay (final: prev: {
        inherit (self.packages.${final.system})
          sinex
          sinexctl
          sinex-ingestd
          sinex-gateway
          sinex-fs-ingestor
          sinex-terminal-ingestor
          sinex-desktop-ingestor
          sinex-system-ingestor
          sinex-document-ingestor
          sinex-terminal-command-canonicalizer
          sinex-health-automaton
          sinex-analytics-automaton
          sinex-search-automaton
          sinex-content-automaton
          sinex-pkm-automaton
          sinex-schema;
      });
    };
}
