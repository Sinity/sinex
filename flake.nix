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
    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      fenix,
      agenix,
      flake-utils,
      devenv,
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

          # Helper to build Rust packages with common configuration
          buildRustPackage =
            { name, manifestPath }:
            let
              manifestDir = builtins.dirOf manifestPath;
            in
            rustPlatform.buildRustPackage {
              pname = name;
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
                "--manifest-path"
                manifestPath
              ];
              cargoInstallFlags = [
                "--path"
                manifestDir
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
            name = "sinex-terminal-satellite";
            manifestPath = "crate/satellites/sinex-terminal-satellite/Cargo.toml";
          };
          sinexDesktopSatellite = buildRustPackage {
            name = "sinex-desktop-satellite";
            manifestPath = "crate/satellites/sinex-desktop-satellite/Cargo.toml";
          };
          sinexSystemSatellite = buildRustPackage {
            name = "sinex-system-satellite";
            manifestPath = "crate/satellites/sinex-system-satellite/Cargo.toml";
          };
          sinexDocumentIngestor = buildRustPackage {
            name = "sinex-document-ingestor";
            manifestPath = "crate/satellites/sinex-document-ingestor/Cargo.toml";
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
              sinexGateway
              sinexSatelliteSdk
              sinexFsWatcher
              sinexTerminalSatellite
              sinexDesktopSatellite
              sinexSystemSatellite
              sinexDocumentIngestor
              sinexTerminalCommandCanonicalizer
              healthAggregator
              sinexCli
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
              sinexCli;
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
          limitedVmTests = pkgs.lib.filterAttrs (name: _: pkgs.lib.elem name [ "basic" "preflight" ]) vmTests;
        in
        {
          packages = sinexPackages;

          formatter = pkgs.nixpkgs-fmt;

          checks = pkgs.lib.mapAttrs' (name: value:
            pkgs.lib.nameValuePair "sinex-vm-${name}" value
          ) (pkgs.lib.filterAttrs (_: value: pkgs.lib.isDerivation value) limitedVmTests);

          devShells.default = devenv.lib.mkShell {
            inherit inputs pkgs;
            modules = [ ./devenv.nix ];
          };

        }
      );
    in
    systemOutputs
    // {
      # NixOS module
      nixosModules = {
        default = import ./nixos;
        sinex = args: (self.nixosModules.default args);
      };

      nixosConfigurations = {
        example = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            ({ ... }: {
              nixpkgs.overlays = [ self.overlays.default ];
            })
            ./nixos/example.nix
            ({ lib, ... }: {
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
            })
          ];
        };

        exampleMonitoring = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            ({ ... }: {
              nixpkgs.overlays = [ self.overlays.default ];
            })
            ./nixos/example-monitoring.nix
            ({ lib, ... }: {
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
            })
          ];
        };

        exampleDevSandbox = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            ({ ... }: {
              nixpkgs.overlays = [ self.overlays.default ];
            })
            ./nixos/example-dev-sandbox.nix
            ({ lib, ... }: {
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
            })
          ];
        };

        exampleHeadless = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            ({ ... }: {
              nixpkgs.overlays = [ self.overlays.default ];
            })
            ./nixos/example-headless.nix
            ({ lib, ... }: {
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
            })
          ];
        };

        exampleRemoteSatellite = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            ({ ... }: {
              nixpkgs.overlays = [ self.overlays.default ];
            })
            ./nixos/example-remote-satellite.nix
            ({ lib, ... }: {
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
            })
          ];
        };

        exampleCoordination = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            ({ ... }: {
              nixpkgs.overlays = [ self.overlays.default ];
            })
            ./nixos/example-coordination.nix
            ({ lib, ... }: {
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
            })
          ];
        };
      };

      # Overlay providing pg_jsonschema
      overlays.default = final: prev: {
        postgresql16Packages = prev.postgresql16Packages // {
          pg_jsonschema = self.packages.${final.system}.pg_jsonschema;
        };

        sinex = self.packages.${final.system}.sinex;
        sinexCli = self.packages.${final.system}.sinexCli;
      };
    };
}
