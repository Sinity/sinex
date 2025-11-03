# Sinex Maintenance Services - Timers and Cleanup Tasks
# This module provides consolidated maintenance and monitoring services
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Service enhancement flags
  enableMonitoring = cfg.monitoring.enable or false;
  
  # Common timer configuration
  commonTimerConfig = {
    Unit.Description = "Timer for Sinex maintenance task";
    Install.WantedBy = [ "timers.target" ];
  };
  
  # Common maintenance service configuration
  maintenanceServiceConfig = {
    Type = "oneshot";
    User = cfg.satelliteUser;
    Group = cfg.satelliteUser;
    
    Environment = [
      "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
      "RUST_LOG=${cfg.logLevel}"
      "SINEX_CLI=${cfg.cliPackage}/bin/sinex-cli"
    ];
    
    # Security hardening for maintenance tasks
    NoNewPrivileges = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
  };

in
{
  config = mkIf (cfg.enable && cfg.serviceManagement.serviceGroups.maintenance && cfg.cliPackage != null) {
    systemd = {
      # ============================================================================
      # Maintenance Services
      # ============================================================================
      services = {
        # Dead Letter Queue Cleanup
        sinex-dlq-cleanup = {
          description = "Sinex Dead Letter Queue Cleanup";
          serviceConfig = maintenanceServiceConfig // {
            ExecStart = pkgs.writeShellScript "sinex-dlq-cleanup" ''
              set -euo pipefail
              echo "Starting DLQ cleanup..."

              SINEX_CLI_BIN="${cfg.cliPackage}/bin/sinex-cli"
              if [ ! -x "$SINEX_CLI_BIN" ]; then
                echo "sinex-cli not available at $SINEX_CLI_BIN" >&2
                exit 1
              fi
              
              # Clean up old DLQ entries (older than 30 days)
              "$SINEX_CLI_BIN" dlq cleanup --older-than 30d --confirm
              
              # Generate cleanup metrics
              ${optionalString enableMonitoring ''
                dlq_count=$("$SINEX_CLI_BIN" dlq count)
                "$SINEX_CLI_BIN" metrics gauge sinex.dlq.entries_remaining "$dlq_count"
              ''}
              
              echo "DLQ cleanup completed"
            '';
          };
        };
        
        # Git-annex Maintenance (from blob-storage.nix)
        sinex-git-annex-gc = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enableAutoGc) {
          description = "Git-annex Garbage Collection";
          serviceConfig = maintenanceServiceConfig // {
            WorkingDirectory = cfg.blobStorage.repositoryPath;
            ExecStart = pkgs.writeShellScript "sinex-git-annex-gc" ''
              set -euo pipefail
              echo "Starting git-annex garbage collection..."

              SINEX_CLI_BIN="${cfg.cliPackage}/bin/sinex-cli"
              if [ ! -x "$SINEX_CLI_BIN" ]; then
                echo "sinex-cli not available at $SINEX_CLI_BIN" >&2
                exit 1
              fi
              
              # Run git-annex unused to find unreferenced files
              ${pkgs.git-annex}/bin/git-annex unused
              
              # Drop unused files (if any)
              ${pkgs.git-annex}/bin/git-annex dropunused --force 1-100 || echo "No unused files to drop"
              
              # Run git garbage collection
              ${pkgs.git}/bin/git gc --aggressive
              
              # Emit storage metrics
              ${optionalString enableMonitoring ''
                repo_size=$(${pkgs.coreutils}/bin/du -sb ${cfg.blobStorage.repositoryPath} | cut -f1)
                "$SINEX_CLI_BIN" metrics gauge sinex.storage.repository_bytes "$repo_size"
              ''}
              
              echo "Git-annex garbage collection completed"
            '';
          };
        };
        
        sinex-git-annex-fsck = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enablePeriodicFsck) {
          description = "Git-annex Filesystem Check";
          serviceConfig = maintenanceServiceConfig // {
            WorkingDirectory = cfg.blobStorage.repositoryPath;
            TimeoutStartSec = "3600s";  # Allow up to 1 hour for fsck
            ExecStart = pkgs.writeShellScript "sinex-git-annex-fsck" ''
              set -euo pipefail
              echo "Starting git-annex filesystem check..."

              SINEX_CLI_BIN="${cfg.cliPackage}/bin/sinex-cli"
              if [ ! -x "$SINEX_CLI_BIN" ]; then
                echo "sinex-cli not available at $SINEX_CLI_BIN" >&2
                exit 1
              fi
              
              # Run incremental fsck (checks a portion each time)
              if ${pkgs.git-annex}/bin/git-annex fsck --incremental --time-limit=30m; then
                echo "✓ Git-annex fsck completed successfully"
                ${optionalString enableMonitoring ''
                  "$SINEX_CLI_BIN" metrics gauge sinex.storage.fsck_status 1
                ''}
              else
                echo "✗ Git-annex fsck found issues"
                ${optionalString enableMonitoring ''
                  "$SINEX_CLI_BIN" metrics gauge sinex.storage.fsck_status 0
                  "$SINEX_CLI_BIN" metrics increment sinex.alerts.storage_fsck_failed
                ''}
              fi
            '';
          };
        };
      };
      
      # ============================================================================
      # Maintenance Timers
      # ============================================================================
      timers = {
        # Daily DLQ cleanup
        sinex-dlq-cleanup = {
          description = "Daily Sinex DLQ Cleanup";
          timerConfig = {
            OnCalendar = "daily";
            RandomizedDelaySec = "1h";  # Spread load
            Persistent = true;
          };
          wantedBy = [ "timers.target" ];
        };
        
        # Git-annex maintenance timers
        sinex-git-annex-gc = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enableAutoGc) {
          description = "Weekly Git-annex Garbage Collection";
          timerConfig = {
            OnCalendar = cfg.blobStorage.maintenance.gcSchedule;
            RandomizedDelaySec = "6h";
            Persistent = true;
          };
          wantedBy = [ "timers.target" ];
        };
        
        sinex-git-annex-fsck = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enablePeriodicFsck) {
          description = "Monthly Git-annex Filesystem Check";
          timerConfig = {
            OnCalendar = cfg.blobStorage.maintenance.fsckSchedule;
            RandomizedDelaySec = "1d";
            Persistent = true;
          };
          wantedBy = [ "timers.target" ];
        };
      };
    };
  };
}
