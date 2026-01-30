{
  description = "Sinex - Universal data capture and query system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
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
      agenix,
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

          fenixPkgs = fenix.packages.${system}.complete;
          rustToolchain = fenixPkgs.toolchain;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = fenixPkgs.cargo;
            rustc = fenixPkgs.rustc;
          };

          # Extract git information for version tracking
          gitRev = self.rev or self.dirtyRev or "unknown";
          gitShortRev = builtins.substring 0 8 gitRev;
          version = "0.1.0"; # TODO: Extract from workspace
          buildTime = "unknown"; # builtins.currentTime not available in pure mode

          pg_jsonschema = pkgs.callPackage ./nix/pkgs/pg_jsonschema { };

          # Postgres with required extensions for SQLx online builds
          postgresForSqlx =
            pkgs.postgresql_16.withPackages (ps: [
              ps.timescaledb
              ps.pgvector
              ps.pgx_ulid
              pg_jsonschema
            ]);

          # Common postPatch that generates build_info.rs
          commonPostPatch = ''
            # Ensure helper scripts (e.g., rustc wrapper) use in-sandbox interpreters
            patchShebangs scripts

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

          # Helper to build Rust packages using online SQLx against an ephemeral Postgres
          buildRustPackage =
            { name, manifestPath }:
            let
              manifestDir = builtins.dirOf manifestPath;
            in
            rustPlatform.buildRustPackage {
              pname = name + "-online";
              version = version;
              src = ./.;
              cargoLock.lockFile = ./Cargo.lock;
              buildInputs = with pkgs; [
                openssl
                dbus
                systemd
                postgresForSqlx
              ];
              nativeBuildInputs = with pkgs; [
                pkg-config
                protobuf
                mold
              ];
              cargoBuildFlags = [
                "--manifest-path"
                manifestPath
              ];
              cargoInstallFlags = [
                "--path"
                manifestDir
              ];
              auditable = false;
              doCheck = false;
              # SQLx queries are validated against an ephemeral Postgres instance
              postPatch = commonPostPatch;
              preBuild = ''
                # Ephemeral Postgres for SQLx online query checking
                export PGDATA="$TMPDIR/pgdata"
                mkdir -p "$PGDATA"
                ${postgresForSqlx}/bin/initdb -D "$PGDATA" --locale=C --encoding=UTF8 --auth=trust

                # Use a local UNIX socket; avoid exposing TCP
                export PGHOST="$TMPDIR"
                export PGPORT=55433
                echo "unix_socket_directories = '$TMPDIR'" >> "$PGDATA/postgresql.conf"
                echo "port = $PGPORT" >> "$PGDATA/postgresql.conf"

                ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -w start

                # Create application database
                ${postgresForSqlx}/bin/createdb -h "$PGHOST" -p "$PGPORT" sinex_dev || true

                # Create application role expected by migrations and runtime,
                # and grant it privileges on the public schema used by the
                # SeaORM migration tracking table. The superuser for this
                # ephemeral cluster is the default 'postgres' role.
                ${postgresForSqlx}/bin/psql -h "$PGHOST" -p "$PGPORT" -d postgres -U postgres -v ON_ERROR_STOP=1 -c \"DO \$\$
                BEGIN
                  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sinity') THEN
                    CREATE ROLE sinity LOGIN CREATEDB;
                  END IF;
                END
                \$\$;\"

                ${postgresForSqlx}/bin/psql -h "$PGHOST" -p "$PGPORT" -d sinex_dev -U postgres -v ON_ERROR_STOP=1 -c \"GRANT ALL ON SCHEMA public TO sinity;\"

                export PGUSER=\"sinity\"
                export DATABASE_URL="postgresql:///sinex_dev?host=$PGHOST&port=$PGPORT"

                # Run schema migrations using sinex-schema binary.
                # Build sinex-schema once; this also warms cargo's target dir.
                cargo build --manifest-path crate/lib/sinex-schema/Cargo.toml
                cargo run --manifest-path crate/lib/sinex-schema/Cargo.toml --bin sinex-schema -- up
              '';
              postBuild = ''
                if [ -n "''${PGDATA:-}" ]; then
                  ${postgresForSqlx}/bin/pg_ctl -D "$PGDATA" -m fast stop || true
                fi
              '';
              postInstall = ''
                # Database migrations ship via the sinex-schema crate/CLI
                # Nothing extra to install here.
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
                            runHook preInstall

                            python=${pkgs.python3}/bin/python3
                            site=$($python - <<'PY'
              import sys
              print(f"lib/python{sys.version_info[0]}.{sys.version_info[1]}/site-packages")
              PY
              )
                            pkg_dir=$out/$site/sinex_cli
                            mkdir -p "$pkg_dir"

                            for file in *.py; do
                              cp "$file" "$pkg_dir/$file"
                            done
                            touch "$pkg_dir/__init__.py"
                            cat > "$pkg_dir/__main__.py" <<'PY'
              from .exo import cli
              import sys

              def main():
                  try:
                      cli()
                  except Exception as exc:  # pragma: no cover
                      try:
                          from rich.console import Console
                          Console().print(f"[red]Error: {exc}[/red]")
                      except Exception:
                          print(f"Error: {exc}")
                      sys.exit(1)

              if __name__ == "__main__":
                  main()
              PY

                            mkdir -p $out/bin
                            cat > $out/bin/sinex-cli <<'PY'
              #!${pkgs.python3}/bin/python3
              import runpy
              import sys
              from pathlib import Path

              site = "{site}"
              pkg_base = Path(__file__).resolve().parent.parent / site
              sys.path.insert(0, str(pkg_base))
              runpy.run_module("sinex_cli.__main__", run_name="__main__")
              PY
                            substituteInPlace $out/bin/sinex-cli --replace "{site}" "$site"
                            chmod +x $out/bin/sinex-cli

                            ln -s $out/bin/sinex-cli $out/bin/exo

                            runHook postInstall
            '';

            doCheck = false;

            meta = with pkgs.lib; {
              description = "Sinex CLI - Query your digital memory";
              license = licenses.mit;
              maintainers = [ ];
            };
          };

        in
        let
          # Core satellite services
          sinexIngestd = buildRustPackage {
            name = "sinex-ingestd";
            manifestPath = "crate/core/sinex-ingestd/Cargo.toml";
          };
          sinexGateway = buildRustPackage {
            name = "sinex-gateway";
            manifestPath = "crate/core/sinex-gateway/Cargo.toml";
          };
          sinexSatelliteSdk = buildRustPackage {
            name = "sinex-satellite-sdk";
            manifestPath = "crate/lib/sinex-satellite-sdk/Cargo.toml";
          };

          # Event source satellites
          sinexFsWatcher = buildRustPackage {
            name = "sinex-fs-watcher";
            manifestPath = "crate/satellites/sinex-fs-watcher/Cargo.toml";
          };
          sinexTerminalSatellite = buildRustPackage {
            name = "sinex-terminal-node";
            manifestPath = "crate/satellites/sinex-terminal-node/Cargo.toml";
          };
          sinexDesktopSatellite = buildRustPackage {
            name = "sinex-desktop-node";
            manifestPath = "crate/satellites/sinex-desktop-node/Cargo.toml";
          };
          sinexSystemSatellite = buildRustPackage {
            name = "sinex-system-node";
            manifestPath = "crate/satellites/sinex-system-node/Cargo.toml";
          };
          sinexDocumentIngestor = buildRustPackage {
            name = "sinex-document-ingestor";
            manifestPath = "crate/satellites/sinex-document-ingestor/Cargo.toml";
          };
          sinexSchema = buildRustPackage {
            name = "sinex-schema";
            manifestPath = "crate/lib/sinex-schema/Cargo.toml";
          };

          # Automaton satellites & support
          sinexTerminalCommandCanonicalizer = buildRustPackage {
            name = "sinex-terminal-command-canonicalizer";
            manifestPath = "crate/satellites/sinex-terminal-command-canonicalizer/Cargo.toml";
          };
          healthAggregator = buildRustPackage {
            name = "sinex-health-aggregator";
            manifestPath = "crate/satellites/sinex-health-aggregator/Cargo.toml";
          };
          sinexHealthAggregator = healthAggregator;
          sinexCli = sinex-cli;

          sinexSuite = pkgs.symlinkJoin {
            name = "sinex-suite";
            paths = [
              sinexIngestd
              sinexSatelliteSdk
              sinexFsWatcher
              sinexTerminalSatellite
              sinexDesktopSatellite
              sinexSystemSatellite
              sinexDocumentIngestor
              sinexTerminalCommandCanonicalizer
              healthAggregator
              sinexCli
              sinexSchema
            ];
          };

          sinexPackages = {
            inherit
              sinexIngestd
              sinexGateway
              sinexSatelliteSdk
              sinexFsWatcher
              sinexTerminalSatellite
              sinexDesktopSatellite
              sinexSystemSatellite
              sinexDocumentIngestor
              sinexTerminalCommandCanonicalizer
              healthAggregator
              sinexHealthAggregator
              sinexSchema
              sinexCli
              ;
            sinex = sinexSuite;
            sinexPreflight = sinexSatelliteSdk;

            # Default package is now the ingestion daemon
            default = sinexIngestd;
            inherit pg_jsonschema;
          };

          vmTests = import ./tests/e2e/nixos-vm/default.nix {
            inherit pkgs;
            sinex-ingestd = sinexPackages.sinexIngestd;
            sinex-gateway = sinexPackages.sinexGateway;
            sinex = sinexPackages.sinex;
            sinexCli = sinexPackages.sinexCli;
            inherit pg_jsonschema;
          };
        in
        let
          limitedVmTests = pkgs.lib.filterAttrs (
            name: _:
            pkgs.lib.elem name [
              "basic"
              "preflight"
            ]
          ) vmTests;
        in
        rec {
          vmPackagesAll = pkgs.lib.mapAttrs' (
            name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value
          ) vmTests;
          
          vmPackagesEssential = pkgs.lib.mapAttrs' (
            name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value
          ) limitedVmTests;

          packages = sinexPackages // vmPackagesEssential;
          
          # Expose all VMs via legacyPackages so they can still be built (e.g. by xtask)
          # but don't force evaluation during standard package enumeration.
          legacyPackages = vmPackagesAll;

          formatter = pkgs.nixpkgs-fmt;

          checks = pkgs.lib.mapAttrs' (name: value: pkgs.lib.nameValuePair "sinex-vm-${name}" value) (
            pkgs.lib.filterAttrs (_: value: pkgs.lib.isDerivation value) limitedVmTests
          );

          # Plain devShell - no devenv dependency
          devShells.default = pkgs.mkShell {
            packages = with pkgs; [
              # Rust toolchain from Fenix
              fenixPkgs.toolchain
              fenixPkgs.rust-analyzer
              fenixPkgs.clippy
              fenixPkgs.rustfmt
              fenixPkgs.llvm-tools
              fenixPkgs.rust-src

              # Cargo tools
              cargo-watch cargo-nextest cargo-llvm-cov cargo-tarpaulin
              cargo-modules bacon tokei cargo-audit cargo-machete
              mold binutils

              # Services (managed by xtask stack)
              nats-server postgresForSqlx

              # Development utilities
              mprocs btop jq coreutils protobuf openssl pkg-config
              dbus dbus.dev git-annex fd fzf bat ripgrep nsc qemu qemu_kvm
            ];

            # Static environment variables
            DATABASE_NAME = "sinex_dev";
            PGUSER = "sinity";
            PGDATABASE = "sinex_dev";
            SINEX_TEST_OPTIMIZATIONS = "true";
            SINEX_PG_BIN = "${postgresForSqlx}/bin";
            NATS_SERVER_BIN = "${pkgs.nats-server}/bin/nats-server";
            SINEX_DEVENV_SYSTEM = system;
            SINEX_DEVENV_TOOLCHAIN = "fenix (${system})";

            shellHook = ''
              export PATH="$PWD/scripts:$PWD/target/debug:$PATH"
              export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.dbus ]}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
              export SINEX_STATE_DIR="''${XDG_STATE_HOME:-$HOME/.local/state}/sinex"
              export SINEX_CACHE_DIR="''${XDG_CACHE_HOME:-$HOME/.cache}/sinex"
              export SINEX_TEST_RESULTS_DIR="$SINEX_CACHE_DIR/test-results"

              # Delegate to xtask for dynamic config
              [ -x "$PWD/target/debug/xtask" ] && eval $("$PWD/target/debug/xtask" stack env --export 2>/dev/null || echo "")

              # Shell shortcuts and banner
              [ -f "$PWD/.zshrc.local" ] && source "$PWD/.zshrc.local"
              [ -x "$PWD/scripts/dev-env-banner.sh" ] && [ -z "''${SINEX_DEVENV_MOTD_ONCE:-}" ] && "$PWD/scripts/dev-env-banner.sh" || true && export SINEX_DEVENV_MOTD_ONCE=1
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
                services.sinex.lifecycle.preflight.enable = false;
                services.sinex.lifecycle.updates.enable = false;
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

      overlays.default =
        let
          databaseExtensionsOverlay = import ./nix/overlays/database-extensions.nix;
          packageOverlay =
            final: prev:
            {
              sinex = self.packages.${final.system}.sinex;
              sinexCli = self.packages.${final.system}.sinexCli;
            };
        in
        nixpkgs.lib.composeExtensions databaseExtensionsOverlay packageOverlay;
    };
}
