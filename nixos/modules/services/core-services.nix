# Sinex Core Services - Consolidated Service Definitions
# This module provides a single source of truth for core Sinex services
# with conditional enhancement support (preflight verification, monitoring, etc.)
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
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

  config = mkIf cfg.enable {};
}
