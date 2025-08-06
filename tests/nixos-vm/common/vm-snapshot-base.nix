# Base configuration for VM snapshot testing
# Agent Alpha - VM Infrastructure
{ config, lib, pkgs, ... }:

{
  imports = [
    ./test-base.nix
    ./vm-snapshot-config.nix
  ];

  # Enable snapshot mode by default for snapshot-based tests
  virtualisation.snapshotMode = lib.mkDefault false;

  # Optimize for faster snapshot creation and restoration
  boot = {
    # Faster boot sequence
    initrd.systemd.enable = lib.mkDefault true;
    
    # Minimal services during snapshot creation
    initrd.availableKernelModules = [
      "virtio_pci" "virtio_blk" "virtio_net" "virtio_balloon"
      "ahci" "sd_mod" "sr_mod"
    ];
  };

  # Systemd optimizations for snapshot environments
  systemd = {
    services = {
      # Disable time-consuming services during snapshot creation
      "systemd-random-seed".enable = lib.mkDefault false;
      "systemd-machine-id-commit".enable = lib.mkDefault false;
    };
    
    # Faster service startup/shutdown
    extraConfig = ''
      DefaultTimeoutStartSec=30s
      DefaultTimeoutStopSec=15s
    '';
  };

  # PostgreSQL optimizations for test snapshots
  services.postgresql = {
    settings = {
      # Faster checkpoint for snapshots
      checkpoint_timeout = "1min";
      checkpoint_completion_target = 0.9;
      
      # Minimal WAL for test scenarios
      wal_level = "minimal";
      max_wal_senders = 0;
      
      # Memory optimizations for test VMs
      shared_buffers = "64MB";
      effective_cache_size = "256MB";
      work_mem = "4MB";
      maintenance_work_mem = "32MB";
    };
  };

  # Test-specific environment setup
  environment.systemPackages = with pkgs; [
    # Snapshot verification tools
    (writeScriptBin "vm-snapshot-verify" ''
      #!/usr/bin/env bash
      set -euo pipefail
      
      echo "=== VM Snapshot Verification ==="
      echo "Hostname: $(hostname)"
      echo "Uptime: $(uptime)"
      echo "PostgreSQL status: $(systemctl is-active postgresql || echo 'inactive')"
      echo "Sinex collector status: $(systemctl is-active sinex-unified-collector || echo 'inactive')"
      
      if systemctl is-active postgresql > /dev/null; then
        echo "Database connection: $(sudo -u postgres psql -d sinex -t -c 'SELECT 1' 2>/dev/null || echo 'failed')"
        echo "Event count: $(sudo -u postgres psql -d sinex -t -c 'SELECT COUNT(*) FROM core.events' 2>/dev/null || echo 'unknown')"
      fi
      
      echo "=== Verification Complete ==="
    '')
    
    # Snapshot management helper
    (writeScriptBin "vm-snapshot-ready" ''
      #!/usr/bin/env bash
      set -euo pipefail
      
      # Wait for key services to be ready
      echo "Waiting for PostgreSQL..."
      while ! systemctl is-active postgresql > /dev/null; do
        sleep 1
      done
      
      echo "Waiting for database connection..."
      while ! sudo -u postgres psql -d sinex -c 'SELECT 1' > /dev/null 2>&1; do
        sleep 1
      done
      
      echo "System ready for snapshot creation"
      touch /tmp/snapshot-ready
    '')
  ];

  # Snapshot readiness indicator
  systemd.services.snapshot-readiness = {
    description = "Indicate when system is ready for snapshot";
    wantedBy = [ "multi-user.target" ];
    after = [ "postgresql.service" "sinex-unified-collector.service" ];
    serviceConfig = {
      Type = "oneshot";
      RemainAfterExit = true;
      ExecStart = "${pkgs.coreutils}/bin/touch /tmp/snapshot-ready";
    };
  };

  # VM identification for parallel testing
  environment.variables = {
    VM_INSTANCE_ID = lib.mkDefault "snapshot-base";
    SINEX_TEST_MODE = "snapshot";
  };
}