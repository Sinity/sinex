# Minimal base configuration for VM tests
{ config
, pkgs
, lib
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:
let
  sinexPackage = if sinex != null then sinex else sinex-ingestd;
  sinexCliPackage = sinexCli;
  sinexConfigBase = {
    enable = true;
    package = sinexPackage;
    users.target = "test";

    database = {
      autoSetup = lib.mkDefault true;
      name = lib.mkDefault "sinex";
      user = lib.mkDefault "sinex";
    };

    lifecycle.preflight.enable = lib.mkForce false;

    satellites = {
      enable = lib.mkDefault true;
      coordination.enable = lib.mkDefault false;
      defaults.instances = lib.mkDefault 1;

      filesystem = {
        enable = lib.mkDefault true;
        watchPaths = lib.mkDefault [ "/home/test/watched" ];
      };

      terminal.enable = lib.mkDefault false;
      desktop.enable = lib.mkDefault false;
      system.enable = lib.mkDefault false;

      automata = {
        enable = lib.mkDefault false;
        canonicalizer.enable = lib.mkDefault false;
        healthAggregator.enable = lib.mkDefault false;
      };
    };
  };
in
{
  imports = [
    ./test-helpers.nix
    ./health-checks.nix
    ../../../../nixos  # Import Sinex NixOS module
  ];

  # Secrets/agenix integration is not needed for VM smoke tests and can
  # introduce evaluation errors when the age module is absent. Disable it here.
  disabledModules = [ ../../../../nixos/modules/secrets.nix ];

  # Basic Sinex configuration
  services.sinex = sinexConfigBase
    // lib.optionalAttrs (sinexCliPackage != null) {
      cliPackage = sinexCliPackage;
    };

  # Test user
  users.users.test = {
    isNormalUser = true;
    createHome = true;
    shell = pkgs.bash;
    uid = 1000;
  };

  # Minimal system packages
  environment.systemPackages = with pkgs; [
    # Core utilities
    file
    sqlite
    jq
    bc
    time
    
    # Monitoring tools
    htop
    iotop
    procps
    
    # Test-specific query tool
    (writeScriptBin "sinex" ''
      #!${pkgs.python3}/bin/python3
      import subprocess
      import sys
      import re
      
      def query_events(limit=10):
          cmd = f"psql -d sinex -t -c \"SELECT id, source, event_type, ts_ingest FROM core.events ORDER BY ts_ingest DESC LIMIT {limit};\""
          result = subprocess.run([
              "su", "-", "postgres", "-c", cmd
          ], capture_output=True, text=True)
          
          if result.returncode == 0:
              lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
              if lines:
                  print("Recent events:")
                  for line in lines:
                      print(f"  {line}")
              else:
                  print("No events found")
          else:
              print(f"Query failed: {result.stderr}")
      
      def stats():
          cmd = "psql -d sinex -t -c 'SELECT COUNT(*) FROM core.events;'"
          result = subprocess.run([
              "su", "-", "postgres", "-c", cmd
          ], capture_output=True, text=True)
          
          if result.returncode == 0:
              count = result.stdout.strip()
              print(f"Total events captured: {count}")
          else:
              print(f"Stats failed: {result.stderr}")
      
      if len(sys.argv) > 1 and sys.argv[1] == "stats":
          stats()
      else:
          limit = 10
          if len(sys.argv) > 1:
              try:
                  limit = int(sys.argv[1])
              except:
                  pass
          query_events(limit)
    '')
  ];

  # Minimal tmpfiles rules
  systemd.tmpfiles.rules =
    let
      stateDir = config.services.sinex.stateRoot;
    in [
      "d /home/test/watched 0755 test users -"
      "f ${stateDir}/.zsh_history 0644 sinex sinex -"
      "f ${stateDir}/.bash_history 0644 sinex sinex -"
    ];

  # Package overlays
  nixpkgs.overlays = [
    (final: prev:
      ({
        sinex-ingestd = sinex-ingestd;
        sinex-gateway = sinex-gateway;
        sinex = sinexPackage;
        postgresql16Packages = prev.postgresql16Packages // {
          pg_jsonschema = pg_jsonschema;
        };
      }
      // lib.optionalAttrs (sinexCliPackage != null) {
        sinexCli = sinexCliPackage;
      }))
  ];

  # Default VM configuration (standard profile)
  virtualisation = {
    # Base configuration from standard profile
    memorySize = lib.mkDefault 2048;
    diskSize = lib.mkDefault 4096;
    cores = lib.mkDefault 2;
    graphics = false;
    writableStoreUseTmpfs = false;
    
    # Enable qcow2 disk format for snapshot support
    qemu.options = [
      "-enable-kvm"
      "-cpu host"
    ];
    
    # Use qcow2 format by default (enables snapshots)
    diskImage = lib.mkDefault "./sinex-test-vm.qcow2";
  };

  # Faster boot
  boot.loader.timeout = lib.mkDefault 0;
  
  # Disable unnecessary services for tests
  services.udisks2.enable = lib.mkDefault false;
  services.smartd.enable = lib.mkDefault false;
  documentation.enable = lib.mkDefault false;
  documentation.nixos.enable = lib.mkDefault false;
  
  # Set hostname for easier identification
  networking.hostName = "sinex-test-vm";
}
