# Minimal base configuration for VM tests
{ config, pkgs, lib, sinex-collector, sinex-promo-worker, pg_jsonschema, ... }:

let
  vmConfigs = import ./vm-configs.nix { inherit lib; };
in
{
  imports = [
    ./test-helpers.nix
    ./health-checks.nix
    ../../../nixos  # Import Sinex NixOS module
  ] ++ lib.optional (config.virtualisation ? vmProfile) 
    (lib.mkMerge [ vmConfigs.${config.virtualisation.vmProfile} ]);

  # Basic Sinex configuration
  services.sinex = {
    enable = true;
    package = sinex-collector;
    targetUser = "test";
    
    # Disable promo worker by default (tests can enable if needed)
    promoWorker.enable = lib.mkDefault false;
    
    unifiedCollector = {
      enable = true;
      # Minimal default sources
      sources.filesystem = {
        enable = true;
        watchPaths = [ "/home/test/watched" ];
      };
    };
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
          cmd = f"psql -d sinex -t -c \"SELECT id, source, event_type, ts_ingest FROM raw.events ORDER BY ts_ingest DESC LIMIT {limit};\""
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
          cmd = "psql -d sinex -t -c 'SELECT COUNT(*) FROM raw.events;'"
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
  systemd.tmpfiles.rules = [
    "d /home/test/watched 0755 test users -"
    "f /var/lib/sinex/.zsh_history 0644 sinex sinex -"
    "f /var/lib/sinex/.bash_history 0644 sinex sinex -"
  ];

  # Package overlays
  nixpkgs.overlays = [(final: prev: {
    sinex-unified-collector = sinex-collector;
    sinex-promo-worker = sinex-promo-worker;
    postgresql16Packages = prev.postgresql16Packages // {
      pg_jsonschema = pg_jsonschema;
    };
  })];

  # Default to standard VM profile (can be overridden)
  virtualisation = lib.mkMerge [
    vmConfigs.standard
    {
      # Additional test-specific settings
      vmProfile = lib.mkDefault "standard";
    }
  ];

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
