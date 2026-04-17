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
  stateDir = config.services.sinex.stateRoot;
  workDir = "${stateDir}/.cache/sinex/ingestd-dev";
  databaseName = "sinex_dev";
  sinexConfigBase = {
    enable = true;
    package = sinexPackage;
    users.target = "test";

    database = {
      autoSetup = lib.mkDefault true;
      name = lib.mkDefault databaseName;
      extraDatabases = lib.mkDefault [ "sinex" ];
      user = lib.mkDefault "sinex";
    };

    lifecycle.preflight.enable = lib.mkDefault false;

    core.gateway.autoGenerateTls = lib.mkDefault true;

    nodes = {
      enable = lib.mkDefault true;
      coordination.enable = lib.mkDefault false;
      defaults.instances = lib.mkDefault 1;
      defaults.env.SINEX_COORDINATION_DISABLED = lib.mkDefault "1";

      filesystem = {
        enable = lib.mkDefault true;
        watchPaths = lib.mkDefault [ "/var/lib/sinex/watched" ];
      };

      terminal.enable = lib.mkDefault false;
      desktop.enable = lib.mkDefault false;
      system.enable = lib.mkDefault false;

      automata = {
        enable = lib.mkDefault false;
        canonicalizer.enable = lib.mkDefault false;
        healthAggregator.enable = lib.mkDefault false;
        analyticsAutomaton.enable = lib.mkDefault false;
        sessionDetector.enable = lib.mkDefault false;
      };
    };

    nats.bootstrapStreams.enable = lib.mkForce false;
  };
  databaseUrl = "postgresql://${sinexConfigBase.database.user}@${sinexConfigBase.database.host}:${toString sinexConfigBase.database.port}/${sinexConfigBase.database.name}";
in
{
  imports = [
    ./test-helpers.nix
    ./health-checks.nix
    ../../../../nixos  # Import Sinex NixOS module
  ];

  # Basic Sinex configuration
  services.sinex = sinexConfigBase
    // lib.optionalAttrs (sinexCliPackage != null) {
      cliPackage = sinexCliPackage;
    };

  # Provide dummy secrets expected by the gateway.
  environment.etc."sinex/gateway-admin-token".text = "test-admin-token:admin";
  environment.variables = {
    SINEX_TEST_DB_NAME = databaseName;
    SINEX_RPC_TOKEN_FILE = "/etc/sinex/gateway-admin-token";
  };

  # Use the real NixOS schema-apply unit rather than shadowing it with a second
  # VM-local migration service. Double-applying the declarative schema races on
  # type creation and breaks boot.
  systemd.services.sinex-gateway.after = [ "sinex-schema-apply.service" "sinex-blob-init.service" ];
  systemd.services.sinex-gateway.requires = [ "sinex-schema-apply.service" "sinex-blob-init.service" ];
  systemd.services.sinex-ingestd.path = [ pkgs.git pkgs.git-annex ];
  systemd.services.sinex-gateway.path = [ pkgs.git pkgs.git-annex ];
  systemd.services.sinex-blob-init.path = [ pkgs.git pkgs.git-annex ];
  systemd.services.sinex-filesystem-1.serviceConfig.Type = lib.mkForce "simple";
  systemd.services.sinex-filesystem-1.serviceConfig.TimeoutStartSec = lib.mkForce "infinity";
  systemd.services.sinex-terminal-1.serviceConfig.Type = lib.mkForce "simple";
  systemd.services.sinex-terminal-1.serviceConfig.TimeoutStartSec = lib.mkForce "infinity";

  # Relax Postgres authentication for disposable VM tests.
  services.postgresql.authentication = lib.mkForce ''
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
'';

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
    git-annex
    
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

      DB_NAME = "${databaseName}"
      
      def query_events(limit=10):
          cmd = f"psql -d {DB_NAME} -t -c \"SELECT id, source, event_type, ts_coided FROM core.events ORDER BY ts_coided DESC LIMIT {limit};\""
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
          cmd = f"psql -d {DB_NAME} -t -c 'SELECT COUNT(*) FROM core.events;'"
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

  # Minimal tmpfiles rules.
  # All intermediate directories must be listed explicitly with sinex ownership;
  # systemd-tmpfiles creates unlisted parents as root:root, which prevents the
  # sinex user from writing into subdirectories.
  systemd.tmpfiles.rules = [
    "d ${stateDir} 0755 sinex sinex -"
    "d /var/lib/sinex/watched 0777 sinex sinex -"
    "D ${stateDir}/nats 0755 sinex sinex -"
    "f ${stateDir}/.zsh_history 0644 sinex sinex -"
    "f ${stateDir}/.bash_history 0644 sinex sinex -"
    # Intermediate dirs for workDir = stateDir/.cache/sinex/ingestd-dev
    "d ${stateDir}/.cache 0755 sinex sinex -"
    "d ${stateDir}/.cache/sinex 0755 sinex sinex -"
    "d ${workDir} 0755 sinex sinex -"
    "d ${workDir}/annex 0755 sinex sinex -"
    "d ${workDir}/assembler_state 0755 sinex sinex -"
  ];

  # Prepare ingestd annex before service startup so the blob manager is usable.
  systemd.services.sinex-ingestd-annex-setup = {
    description = "Prepare Sinex ingestd annex repository";
    wantedBy = [ "multi-user.target" ];
    before = [ "sinex-ingestd.service" ];
    serviceConfig = {
      Type = "oneshot";
      User = "sinex";
      Group = "sinex";
      ExecStart = pkgs.writeShellScript "prepare-ingestd-annex" ''
        set -euo pipefail
        install -d -m0755 -o sinex -g sinex ${workDir}/annex
        install -d -m0755 -o sinex -g sinex ${workDir}/assembler_state
        cd ${workDir}/annex
        if [ ! -d .git ]; then ${pkgs.git}/bin/git init; fi
        ${pkgs.git-annex}/bin/git-annex init ingestd || true
      '';
    };
  };

  # Ensure ingestd has its expected working directories before startup.
  # NB: do NOT self-reference config.systemd.services here — it causes infinite
  # recursion because node-services.nix defines systemd.services based on
  # config.services.sinex.core.  The module system auto-merges definitions.
  systemd.services.sinex-ingestd.serviceConfig = {
    PermissionsStartOnly = true;
    ExecStartPre = lib.mkForce [
      "${pkgs.coreutils}/bin/install -d -o sinex -g sinex ${workDir}/annex"
      "${pkgs.coreutils}/bin/install -d -o sinex -g sinex ${workDir}/assembler_state"
      "-${pkgs.git}/bin/git -C ${workDir}/annex init"
      "-${pkgs.git-annex}/bin/git-annex -C ${workDir}/annex init ingestd"
    ];
    Environment = [
      "XDG_CACHE_HOME=${stateDir}/.cache"
      "SINEX_ANNEX_PATH=${workDir}/annex"
    ];
  };

  systemd.services.sinex-ingestd.after = lib.mkAfter [
    "sinex-schema-apply.service"
    "sinex-blob-init.service"
    "sinex-ingestd-annex-setup.service"
  ];
  systemd.services.sinex-ingestd.requires = lib.mkAfter [
    "sinex-schema-apply.service"
    "sinex-blob-init.service"
    "sinex-ingestd-annex-setup.service"
  ];

  # NATS: clear JetStream state on boot to avoid overlap errors and ensure clean bootstrap.
  # The rm -rf uses the `-` soft-fail prefix so that "Device or resource busy" on the
  # JetStream directory (which can be busy on first boot before tmpfs unmounts) doesn't
  # prevent NATS from starting.
  systemd.services.nats.serviceConfig = {
    PermissionsStartOnly = true;
    ExecStartPre = [
      "-${pkgs.coreutils}/bin/rm -rf ${config.services.sinex.stateRoot}/nats"
      "${pkgs.coreutils}/bin/install -d -o nats -g nats ${config.services.sinex.stateRoot}/nats/jetstream"
    ];
  };

  # Package overlays
  nixpkgs.overlays = [
    (final: prev:
      ({
        sinex-ingestd = sinex-ingestd;
        sinex-gateway = sinex-gateway;
        sinex = sinexPackage;
        postgresql18Packages = prev.postgresql18Packages // {
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

  # Enable native test suites inside the VM so cargo tests don't skip them.
  environment.variables = {
    SINEX_NATIVE_SYSTEM_TESTS = "1";
    SINEX_NATIVE_DESKTOP_TESTS = "1";
    SINEX_ENVIRONMENT = "dev";
  };
  environment.sessionVariables = {
    SINEX_NATIVE_SYSTEM_TESTS = "1";
    SINEX_NATIVE_DESKTOP_TESTS = "1";
    SINEX_ENVIRONMENT = "dev";
  };
  
  # Disable unnecessary services for tests
  services.udisks2.enable = lib.mkDefault false;
  services.smartd.enable = lib.mkDefault false;
  documentation.enable = lib.mkDefault false;
  documentation.nixos.enable = lib.mkDefault false;
  
  # Set hostname for easier identification
  networking.hostName = "sinex-test-vm";
}
