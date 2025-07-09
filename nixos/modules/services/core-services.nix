# Sinex Core Services - Consolidated Service Definitions
# This module provides a single source of truth for core Sinex services
# with conditional enhancement support (preflight verification, monitoring, etc.)
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Import utility modules
  healthChecks = import ../health-checks.nix { inherit lib; };
  
  # Service enhancement flags
  enablePreflight = cfg.preflightVerification.enable or false;
  enableMonitoring = cfg.monitoring.enable or false;
  
  # Common service configuration
  commonServiceConfig = {
    Type = "notify";
    User = cfg.database.user;
    Group = cfg.database.user;
    Restart = "always";
    RestartSec = "10s";
    
    # Resource limits
    MemoryMax = "2G";
    CPUQuota = "150%";
    
    # Security hardening
    NoNewPrivileges = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
    
    # Environment
    Environment = [
      "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
      "RUST_LOG=${cfg.logLevel}"
      "SINEX_CONFIG_PATH=/etc/sinex"
    ];
  };
  
  # Enhanced dependencies for services with preflight verification
  preflightDeps = optionals enablePreflight [
    "sinex-preflight.service"
  ];
  
  # Standard service dependencies
  standardDeps = [
    "postgresql.service"
    "network-online.target"
  ] ++ preflightDeps;
  
  # Pre-start script factory
  mkPreStartScript = name: additionalChecks: pkgs.writeShellScript "${name}-pre-start" ''
    set -euo pipefail
    echo "Preparing ${name} startup..."
    
    # Database connectivity check
    export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
    
    ${optionalString (cfg.database.autoSetup && cfg.database.migration.enable) ''
      echo "Running database migrations..."
      ${cfg.package}/bin/sinex-preflight verify --phase migrations --timeout 30
    ''}
    
    ${optionalString enablePreflight ''
      echo "Verifying pre-flight status..."
      if ! systemctl is-active sinex-preflight.service >/dev/null 2>&1; then
        echo "ERROR: Pre-flight verification service is not active"
        exit 1
      fi
      
      # Check verification status
      if ! ${cfg.package}/bin/sinex-preflight status --require-success; then
        echo "ERROR: Pre-flight verification has not passed"
        exit 1
      fi
    ''}
    
    ${additionalChecks}
    
    echo "${name} pre-start checks completed successfully"
  '';
  
  # Post-start health check factory  
  mkPostStartScript = name: healthCheck: pkgs.writeShellScript "${name}-post-start" ''
    set -euo pipefail
    echo "Running post-start health check for ${name}..."
    
    # Wait for service to become ready
    sleep 5
    
    ${healthCheck}
    
    echo "${name} health check passed"
  '';

in
{
  options.services.sinex.services = {
    # Core service configuration options
    enhancementMode = mkOption {
      type = types.enum [ "standard" "preflight" "monitoring" "full" ];
      default = if enablePreflight then "preflight" else "standard";
      description = ''
        Service enhancement mode:
        - standard: Basic service definitions
        - preflight: Enhanced with pre-flight verification
        - monitoring: Enhanced with monitoring hooks
        - full: All enhancements enabled
      '';
    };
    
    resourceLimits = mkOption {
      type = types.submodule {
        options = {
          memory = mkOption {
            type = types.str;
            default = "2G";
            description = "Memory limit for services";
          };
          cpu = mkOption {
            type = types.str;
            default = "150%";
            description = "CPU quota for services";
          };
        };
      };
      default = {};
      description = "Resource limits for core services";
    };
  };

  config = mkIf cfg.enable {
    systemd.services = {
      # ============================================================================
      # Sinex Unified Collector - Single Source of Truth
      # ============================================================================
      sinex-unified-collector = {
        description = "Sinex Unified Event Collector" + optionalString enablePreflight " (with Pre-Flight Verification)";
        wantedBy = [ "multi-user.target" ];
        after = standardDeps;
        wants = [ "network-online.target" ];
        requires = [ "postgresql.service" ] ++ preflightDeps;
        
        serviceConfig = commonServiceConfig // {
          ExecStartPre = mkPreStartScript "sinex-unified-collector" ''
            # Collector-specific pre-start checks
            echo "Validating event source configurations..."
            ${optionalString cfg.eventSources.filesystem ''
              echo "✓ Filesystem monitoring enabled"
            ''}
            ${optionalString cfg.eventSources.terminal ''
              echo "✓ Terminal monitoring enabled"
            ''}
            ${optionalString cfg.eventSources.windowManager ''
              echo "✓ Window manager monitoring enabled"  
            ''}
            ${optionalString cfg.eventSources.clipboard ''
              echo "✓ Clipboard monitoring enabled"
            ''}
          '';
          
          ExecStart = "${cfg.package}/bin/sinex-collector --config /etc/sinex/collector.toml";
          
          ExecStartPost = mkIf enableMonitoring (mkPostStartScript "sinex-unified-collector" ''
            # Verify collector is receiving events
            ${cfg.package}/bin/sinex-cli collector status --timeout 10
          '');
          
          # Graceful shutdown
          ExecStop = "${pkgs.coreutils}/bin/kill -TERM $MAINPID";
          TimeoutStopSec = "30s";
          KillSignal = "SIGTERM";
        };
        
        # Service-specific environment variables
        environment = {
          SINEX_SERVICE_NAME = "unified-collector";
          SINEX_COMPONENT_TYPE = "collector";
        } // optionalAttrs enableMonitoring {
          SINEX_METRICS_PORT = "9090";
          SINEX_HEALTH_CHECK_PORT = "8080";
        };
      };
      
      # ============================================================================
      # Sinex Promotion Worker - Single Source of Truth  
      # ============================================================================
      sinex-promo-worker = {
        description = "Sinex Event Promotion Worker" + optionalString enablePreflight " (with Pre-Flight Verification)";
        wantedBy = [ "multi-user.target" ];
        after = standardDeps ++ [ "sinex-unified-collector.service" ];
        wants = [ "network-online.target" ];
        requires = [ "postgresql.service" "sinex-unified-collector.service" ] ++ preflightDeps;
        
        serviceConfig = commonServiceConfig // {
          ExecStartPre = mkPreStartScript "sinex-promo-worker" ''
            # Worker-specific pre-start checks
            echo "Validating worker configuration..."
            
            # Verify collector is running
            if ! systemctl is-active sinex-unified-collector >/dev/null 2>&1; then
              echo "ERROR: Unified collector must be running before starting worker"
              exit 1
            fi
            
            # Check work queue accessibility
            ${cfg.package}/bin/sinex-cli worker check-queue --timeout 10
          '';
          
          ExecStart = "${cfg.package}/bin/sinex-worker --config /etc/sinex/worker.toml";
          
          ExecStartPost = mkIf enableMonitoring (mkPostStartScript "sinex-promo-worker" ''
            # Verify worker is processing events
            ${cfg.package}/bin/sinex-cli worker status --timeout 10
          '');
          
          # Graceful shutdown with work completion
          ExecStop = pkgs.writeShellScript "sinex-worker-stop" ''
            echo "Initiating graceful shutdown of promotion worker..."
            ${pkgs.coreutils}/bin/kill -TERM $MAINPID
            
            # Wait for current work to complete (up to 30 seconds)
            timeout 30 ${cfg.package}/bin/sinex-cli worker wait-idle || echo "Warning: Worker may have stopped with pending work"
          '';
          TimeoutStopSec = "45s";
          KillSignal = "SIGTERM";
        };
        
        environment = {
          SINEX_SERVICE_NAME = "promo-worker";
          SINEX_COMPONENT_TYPE = "worker";
          SINEX_WORKER_BATCH_SIZE = toString cfg.worker.batchSize;
          SINEX_WORKER_MAX_RETRIES = toString cfg.worker.maxRetries;
        } // optionalAttrs enableMonitoring {
          SINEX_WORKER_METRICS_PORT = "9091";
        };
      };
      
      # ============================================================================
      # Sinex Update Coordinator - Single Source of Truth
      # ============================================================================  
      sinex-update = {
        description = "Sinex Update Coordinator" + optionalString enablePreflight " (with Pre-Flight Verification)";
        serviceConfig = commonServiceConfig // {
          Type = "oneshot";
          RemainAfterExit = false;
          
          ExecStartPre = mkPreStartScript "sinex-update" ''
            # Update-specific pre-start checks
            echo "Validating update prerequisites..."
            
            # Check if services are ready for update
            for service in sinex-unified-collector sinex-promo-worker; do
              if systemctl is-active "$service" >/dev/null 2>&1; then
                echo "✓ $service is active and ready for coordinated update"
              else
                echo "Warning: $service is not active"
              fi
            done
          '';
          
          ExecStart = pkgs.writeShellScript "sinex-coordinated-update" ''
            set -euo pipefail
            echo "Starting coordinated Sinex update..."
            
            ${optionalString enablePreflight ''
              # Run pre-flight verification first
              echo "Running pre-flight verification..."
              if ! ${cfg.package}/bin/sinex-preflight verify --timeout ${toString cfg.preflightVerification.timeout}; then
                echo "ERROR: Pre-flight verification failed"
                ${optionalString (cfg.preflightVerification.failureAction == "abort") "exit 1"}
                ${optionalString (cfg.preflightVerification.failureAction == "warn") "echo 'WARNING: Continuing despite pre-flight failures'"}
              fi
            ''}
            
            # Graceful service restart sequence
            echo "Stopping services in reverse dependency order..."
            
            # Stop worker first (depends on collector)
            if systemctl is-active sinex-promo-worker >/dev/null 2>&1; then
              echo "Stopping promotion worker..."
              systemctl stop sinex-promo-worker
              sleep ${toString cfg.update.gracePeriod}
            fi
            
            # Stop collector
            if systemctl is-active sinex-unified-collector >/dev/null 2>&1; then
              echo "Stopping unified collector..."
              systemctl stop sinex-unified-collector
              sleep ${toString cfg.update.gracePeriod}
            fi
            
            # Start services in dependency order
            echo "Starting services in dependency order..."
            
            if ! systemctl start sinex-unified-collector; then
              echo "ERROR: Failed to start unified collector"
              ${optionalString cfg.update.rollbackOnFailure ''
                echo "Attempting service recovery..."
                systemctl restart sinex-unified-collector || echo "Recovery failed"
              ''}
              exit 1
            fi
            
            sleep 10  # Allow collector to stabilize
            
            if ! systemctl start sinex-promo-worker; then
              echo "ERROR: Failed to start promotion worker"
              ${optionalString cfg.update.rollbackOnFailure ''
                echo "Attempting service recovery..."
                systemctl restart sinex-promo-worker || echo "Recovery failed"
              ''}
              exit 1
            fi
            
            echo "Coordinated update completed successfully"
            
            ${optionalString enableMonitoring ''
              # Emit update completion metrics
              ${cfg.package}/bin/sinex-cli metrics increment sinex.update.completed
            ''}
          '';
        };
        
        environment = {
          SINEX_SERVICE_NAME = "update-coordinator";
          SINEX_COMPONENT_TYPE = "coordinator";
        };
      };
    };
  };
}