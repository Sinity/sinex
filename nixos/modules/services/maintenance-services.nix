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
    User = cfg.database.user;
    Group = cfg.database.user;
    
    Environment = [
      "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
      "RUST_LOG=${cfg.logLevel}"
    ];
    
    # Security hardening for maintenance tasks
    NoNewPrivileges = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
  };

in
{
  config = mkIf cfg.enable {
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
              
              # Clean up old DLQ entries (older than 30 days)
              ${cfg.package}/bin/sinex-cli dlq cleanup --older-than 30d --confirm
              
              # Generate cleanup metrics
              ${optionalString enableMonitoring ''
                dlq_count=$(${cfg.package}/bin/sinex-cli dlq count)
                ${cfg.package}/bin/sinex-cli metrics gauge sinex.dlq.entries_remaining "$dlq_count"
              ''}
              
              echo "DLQ cleanup completed"
            '';
          };
        };
        
        # Resource Monitoring
        sinex-resource-monitor = mkIf enableMonitoring {
          description = "Sinex Resource Monitoring";
          serviceConfig = maintenanceServiceConfig // {
            ExecStart = pkgs.writeShellScript "sinex-resource-monitor" ''
              set -euo pipefail
              
              # Collect system resource metrics
              cpu_usage=$(${pkgs.procps}/bin/top -bn1 | grep "Cpu(s)" | awk '{print $2}' | sed 's/%us,//')
              memory_usage=$(${pkgs.procps}/bin/free | grep Mem | awk '{printf "%.1f", $3/$2 * 100.0}')
              disk_usage=$(${pkgs.coreutils}/bin/df /var/lib/sinex | tail -1 | awk '{print $5}' | sed 's/%//')
              
              # Report metrics
              ${cfg.package}/bin/sinex-cli metrics gauge sinex.system.cpu_percent "$cpu_usage"
              ${cfg.package}/bin/sinex-cli metrics gauge sinex.system.memory_percent "$memory_usage"
              ${cfg.package}/bin/sinex-cli metrics gauge sinex.system.disk_percent "$disk_usage"
              
              # Check for resource alerts
              if (( $(echo "$memory_usage > 90" | ${pkgs.bc}/bin/bc -l) )); then
                echo "WARNING: High memory usage: $memory_usage%"
                ${cfg.package}/bin/sinex-cli metrics increment sinex.alerts.memory_high
              fi
              
              if (( disk_usage > 85 )); then
                echo "WARNING: High disk usage: $disk_usage%"
                ${cfg.package}/bin/sinex-cli metrics increment sinex.alerts.disk_high
              fi
            '';
          };
        };
        
        # System Health Check
        sinex-system-health = mkIf enableMonitoring {
          description = "Sinex System Health Check";
          serviceConfig = maintenanceServiceConfig // {
            ExecStart = pkgs.writeShellScript "sinex-system-health" ''
              set -euo pipefail
              echo "Running system health check..."
              
              # Check core services
              services_healthy=true
              
              for service in sinex-unified-collector sinex-promo-worker; do
                if systemctl is-active "$service" >/dev/null 2>&1; then
                  echo "✓ $service is active"
                  ${cfg.package}/bin/sinex-cli metrics gauge "sinex.service.$service.active" 1
                else
                  echo "✗ $service is not active"
                  ${cfg.package}/bin/sinex-cli metrics gauge "sinex.service.$service.active" 0
                  services_healthy=false
                fi
              done
              
              # Check database connectivity
              if ${cfg.package}/bin/sinex-cli db ping --timeout 5; then
                echo "✓ Database connectivity OK"
                ${cfg.package}/bin/sinex-cli metrics gauge sinex.database.reachable 1
              else
                echo "✗ Database connectivity failed"
                ${cfg.package}/bin/sinex-cli metrics gauge sinex.database.reachable 0
                services_healthy=false
              fi
              
              # Check work queue health
              queue_depth=$(${cfg.package}/bin/sinex-cli worker queue-depth)
              echo "Work queue depth: $queue_depth"
              ${cfg.package}/bin/sinex-cli metrics gauge sinex.worker.queue_depth "$queue_depth"
              
              # Alert on high queue depth
              if (( queue_depth > 1000 )); then
                echo "WARNING: High work queue depth: $queue_depth"
                ${cfg.package}/bin/sinex-cli metrics increment sinex.alerts.queue_high
              fi
              
              # Overall health status
              if $services_healthy; then
                echo "✓ System health check passed"
                ${cfg.package}/bin/sinex-cli metrics gauge sinex.system.healthy 1
              else
                echo "✗ System health check failed"
                ${cfg.package}/bin/sinex-cli metrics gauge sinex.system.healthy 0
              fi
            '';
          };
        };
        
        # Git-annex Maintenance (from blob-storage.nix)
        sinex-git-annex-gc = mkIf cfg.blobStorage.enable {
          description = "Git-annex Garbage Collection";
          serviceConfig = maintenanceServiceConfig // {
            WorkingDirectory = cfg.blobStorage.repositoryPath;
            ExecStart = pkgs.writeShellScript "sinex-git-annex-gc" ''
              set -euo pipefail
              echo "Starting git-annex garbage collection..."
              
              # Run git-annex unused to find unreferenced files
              ${pkgs.git-annex}/bin/git-annex unused
              
              # Drop unused files (if any)
              ${pkgs.git-annex}/bin/git-annex dropunused --force 1-100 || echo "No unused files to drop"
              
              # Run git garbage collection
              ${pkgs.git}/bin/git gc --aggressive
              
              # Emit storage metrics
              ${optionalString enableMonitoring ''
                repo_size=$(${pkgs.coreutils}/bin/du -sb ${cfg.blobStorage.repositoryPath} | cut -f1)
                ${cfg.package}/bin/sinex-cli metrics gauge sinex.storage.repository_bytes "$repo_size"
              ''}
              
              echo "Git-annex garbage collection completed"
            '';
          };
        };
        
        sinex-git-annex-fsck = mkIf cfg.blobStorage.enable {
          description = "Git-annex Filesystem Check";
          serviceConfig = maintenanceServiceConfig // {
            WorkingDirectory = cfg.blobStorage.repositoryPath;
            TimeoutStartSec = "3600s";  # Allow up to 1 hour for fsck
            ExecStart = pkgs.writeShellScript "sinex-git-annex-fsck" ''
              set -euo pipefail
              echo "Starting git-annex filesystem check..."
              
              # Run incremental fsck (checks a portion each time)
              if ${pkgs.git-annex}/bin/git-annex fsck --incremental --time-limit=30m; then
                echo "✓ Git-annex fsck completed successfully"
                ${optionalString enableMonitoring ''
                  ${cfg.package}/bin/sinex-cli metrics gauge sinex.storage.fsck_status 1
                ''}
              else
                echo "✗ Git-annex fsck found issues"
                ${optionalString enableMonitoring ''
                  ${cfg.package}/bin/sinex-cli metrics gauge sinex.storage.fsck_status 0
                  ${cfg.package}/bin/sinex-cli metrics increment sinex.alerts.storage_fsck_failed
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
        
        # Resource monitoring (every minute if monitoring enabled)
        sinex-resource-monitor = mkIf enableMonitoring {
          description = "Sinex Resource Monitoring";
          timerConfig = {
            OnCalendar = "*:*:00";  # Every minute
            AccuracySec = "10s";
          };
          wantedBy = [ "timers.target" ];
        };
        
        # System health check (every 5 minutes if monitoring enabled)
        sinex-system-health = mkIf enableMonitoring {
          description = "Sinex System Health Check";
          timerConfig = {
            OnCalendar = "*:0/5:00";  # Every 5 minutes
            AccuracySec = "30s";
          };
          wantedBy = [ "timers.target" ];
        };
        
        # Git-annex maintenance timers
        sinex-git-annex-gc = mkIf cfg.blobStorage.enable {
          description = "Weekly Git-annex Garbage Collection";
          timerConfig = {
            OnCalendar = "weekly";
            RandomizedDelaySec = "6h";
            Persistent = true;
          };
          wantedBy = [ "timers.target" ];
        };
        
        sinex-git-annex-fsck = mkIf cfg.blobStorage.enable {
          description = "Monthly Git-annex Filesystem Check";
          timerConfig = {
            OnCalendar = "monthly";
            RandomizedDelaySec = "1d";
            Persistent = true;
          };
          wantedBy = [ "timers.target" ];
        };
      };
    };
  };
}