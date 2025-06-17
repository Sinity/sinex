# Sinex NixOS Module - Modularized Structure
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Import utility modules
  healthChecks = import ./health-checks.nix { inherit lib; };
  
in
{
  imports = [
    ./database.nix
    ./event-sources.nix
    ./blob-storage.nix
    ./monitoring.nix
  ];

  options.services.sinex = {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex or (import ../. { }).packages.${pkgs.system}.default;
      defaultText = literalExpression "pkgs.sinex";
      description = "Sinex package to use";
    };

    # Simplified target user configuration
    targetUser = mkOption {
      type = types.str;
      default = "sinity";
      description = "Username whose files to monitor for events";
    };

    # Directory structure configuration
    directories = {
      base = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Base directory for all Sinex data";
      };

      state = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Directory for persistent state data (StateDirectory)";
      };

      runtime = mkOption {
        type = types.path;
        default = "/run/sinex";
        description = "Directory for runtime data (RuntimeDirectory)";
      };

      cache = mkOption {
        type = types.path;
        default = "/var/cache/sinex";
        description = "Directory for cache data (CacheDirectory)";
      };

      logs = mkOption {
        type = types.path;
        default = "/var/log/sinex";
        description = "Directory for log files (LogsDirectory)";
      };

      dlq = mkOption {
        type = types.path;
        default = "/var/lib/sinex/dlq";
        description = "Directory for dead letter queue files";
      };

      monitoring = mkOption {
        type = types.path;
        default = "/var/lib/sinex/monitoring";
        description = "Directory for monitoring data";
      };

      config = mkOption {
        type = types.path;
        default = "/etc/sinex";
        description = "Directory for configuration files";
      };

      sockets = mkOption {
        type = types.path;
        default = "/run/sinex/sockets";
        description = "Directory for Unix domain sockets";
      };

      # Permission settings for directories
      permissions = {
        state = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for state directories";
        };

        runtime = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for runtime directories";
        };

        cache = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for cache directories";
        };

        logs = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for log directories";
        };

        monitoring = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for monitoring directories";
        };

        sockets = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for socket directories";
        };
      };

      # Cleanup configuration for logs and temporary files
      cleanup = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic cleanup of old logs and temporary files";
        };

        maxLogAge = mkOption {
          type = types.str;
          default = "30d";
          description = "Maximum age of log files before cleanup";
        };

        maxLogSize = mkOption {
          type = types.str;
          default = "1G";
          description = "Maximum total size of log files before cleanup";
        };

        cleanupSchedule = mkOption {
          type = types.str;
          default = "daily";
          description = "Schedule for cleanup operations";
        };
      };
    };

    # Unified collector configuration
    unifiedCollector = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the unified event collector";
      };

      metricsPort = mkOption {
        type = types.port;
        default = 2112;
        description = "Port for Prometheus metrics endpoint";
      };

      logLevel = mkOption {
        type = types.enum [ "trace" "debug" "info" "warn" "error" ];
        default = "info";
        description = "Log level for the collector";
      };

      # Use modularized health checks
      healthCheck = healthChecks.mkHealthCheckOptions {
        defaultPort = 8080;
        serviceName = "unified collector";
        enableProbes = true;
      };

      # Use modularized restart options
      restart = healthChecks.mkRestartOptions;

      # DLQ configuration
      dlq = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable Dead Letter Queue for failed events";
        };

        failureStoragePath = mkOption {
          type = types.str;
          default = "/var/lib/sinex/failures";
          description = ''
            Directory for DLQ files and critical failure logs when database is down.
            Contains both failed event files and critical meta-failure logs.
          '';
        };

        maxRetries = mkOption {
          type = types.int;
          default = 3;
          description = "Maximum retry attempts for failed events";
        };

        retryDelaySecs = mkOption {
          type = types.int;
          default = 60;
          description = "Delay between retry attempts in seconds";
        };

        cleanup = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable automatic DLQ file cleanup";
          };

          maxAge = mkOption {
            type = types.str;
            default = "7d";
            description = "Maximum age of DLQ files before cleanup";
          };

          maxFiles = mkOption {
            type = types.int;
            default = 10000;
            description = "Maximum number of DLQ files before cleanup";
          };
        };
      };
    };

    # Promotion worker configuration
    promoWorker = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the promotion worker";
      };

      metricsPort = mkOption {
        type = types.port;
        default = 2113;
        description = "Port for Prometheus metrics endpoint";
      };

      pollInterval = mkOption {
        type = types.int;
        default = 5;
        description = "Queue polling interval in seconds";
      };

      batchSize = mkOption {
        type = types.int;
        default = 100;
        description = "Number of events to process per batch";
      };

      # Use modularized health checks
      healthCheck = healthChecks.mkHealthCheckOptions {
        defaultPort = 8081;
        serviceName = "promotion worker";
        enableProbes = true;
      };
    };

    # Presets for real use cases
    preset = mkOption {
      type = types.enum [ "lite" "normal" "max" ];
      default = "normal";
      description = ''
        Preset configuration for different capture levels:
        - lite: Lightweight capture (core events, minimal resources)
        - normal: Standard comprehensive capture (good default)
        - max: Maximum data capture (everything, high frequency)
      '';
    };

  };

  config = mkIf cfg.enable {
    # Apply preset configurations based on capture intensity
    services.sinex = mkMerge [
      # Lite preset - lightweight capture with minimal resources
      (mkIf (cfg.preset == "lite") {
        unifiedCollector = {
          logLevel = "warn";  # Minimal logging
          sources = {
            # Core events only
            atuin.pollInterval = 10;  # Slower polling
            clipboard.pollInterval = 2000;  # Very slow clipboard
            kittyScrollback.enable = false;  # Skip terminal capture
            filesystem.watchPaths = [ "~/Documents" ];  # Limited scope
            # Minimal D-Bus monitoring
            dbus = {
              extractNotifications = true;
              extractMedia = false;
              extractPower = true;
              extractScreensaver = false;
              extractPolicykit = false;
              extractNetwork = false;
              extractMounts = false;
              extractBluetooth = false;
              extractHardware = false;
              extractSession = false;
            };
            asciinema.enable = false;  # Skip recording
          };
        };
        database.connectionPool.maxConnections = 10;
        promoWorker.pollInterval = 10;
        blobStorage = {
          healthCheck = {
            interval = 7200;  # 2 hours
            wantedSize = "20G";
          };
          maintenance = {
            gcSchedule = "monthly";
            fsckSchedule = "quarterly";
          };
        };
        unifiedCollector.dlq.cleanup = {
          maxAge = "3d";
          maxFiles = 1000;
        };
      })

      # Normal preset - standard comprehensive capture
      (mkIf (cfg.preset == "normal") {
        unifiedCollector = {
          logLevel = "info";
          sources = {
            atuin.pollInterval = 5;
            clipboard.pollInterval = 1000;
            kittyScrollback.captureInterval = 30;
            filesystem.watchPaths = [ "~/Documents" "~/Projects" "~/Downloads" ];
            # Comprehensive D-Bus monitoring
            dbus = {
              extractNotifications = true;
              extractMedia = true;
              extractPower = true;
              extractScreensaver = true;
              extractPolicykit = true;
              extractNetwork = true;
              extractMounts = true;
              extractBluetooth = true;
              extractHardware = true;
              extractSession = true;
            };
          };
        };
        database.connectionPool.maxConnections = 30;
        promoWorker = {
          pollInterval = 3;
          batchSize = 300;
        };
        blobStorage = {
          healthCheck = {
            interval = 1800;  # 30 minutes
            wantedSize = "100G";
          };
          maintenance = {
            gcSchedule = "weekly";
            fsckSchedule = "monthly";
          };
        };
        unifiedCollector.dlq.cleanup = {
          maxAge = "14d";
          maxFiles = 25000;
        };
        monitoring = {
          alerting.enable = true;
          observabilityStack.enable = true;
          dashboards.grafana.enable = true;
        };
      })

      # Max preset - maximum data capture at high frequency
      (mkIf (cfg.preset == "max") {
        unifiedCollector = {
          logLevel = "debug";  # Detailed logging
          sources = {
            # Everything at maximum frequency
            atuin.pollInterval = 1;
            clipboard.pollInterval = 100;
            kittyScrollback.captureInterval = 5;
            filesystem.watchPaths = [ "~/" ];  # Monitor entire home directory
            dbus.extractAll = true;  # All D-Bus events
            asciinema.autoRecord = true;  # Auto-record all sessions
          };
          healthCheck = {
            interval = 5;
            timeout = 2;
          };
          dlq.cleanup = {
            maxAge = "90d";  # Very long retention
            maxFiles = 100000;
          };
        };
        database = {
          connectionPool.maxConnections = 100;
          healthCheck.interval = 10;
        };
        promoWorker = {
          pollInterval = 1;
          batchSize = 1000;
        };
        blobStorage = {
          healthCheck = {
            interval = 900;  # 15 minutes
            wantedSize = "1T";  # Large storage
          };
          maintenance = {
            gcSchedule = "daily";
            fsckSchedule = "weekly";
          };
        };
        monitoring = {
          logging.level = "debug";
          prometheus.enable = true;
          alerting.enable = true;
          observabilityStack.enable = true;
          dashboards.grafana.enable = true;
        };
      })
    ];

    # System integration (simplified from original)
    systemd.services = {
      sinex-unified-collector = {
        description = "Sinex Unified Event Collector";
        wantedBy = [ "multi-user.target" ];
        after = [ "postgresql.service" ];
        
        serviceConfig = {
          Type = "simple";
          User = cfg.database.user;
          Group = cfg.database.user;
          Restart = cfg.unifiedCollector.restart.policy;
          RestartSec = cfg.unifiedCollector.restart.baseDelay;
          
          # Resource limits
          MemoryMax = "1G";
          CPUQuota = "200%";
          
          ExecStart = "${cfg.package}/bin/sinex-collector";
          
          # Secure credential handling via environment file
          EnvironmentFile = "/etc/sinex/credentials.env";
          
          # Non-sensitive environment variables
          Environment = [
            "RUST_LOG=${cfg.unifiedCollector.logLevel}"
            "SINEX_METRICS_PORT=${toString cfg.unifiedCollector.metricsPort}"
            "SINEX_HEALTH_PORT=${toString cfg.unifiedCollector.healthCheck.port}"
          ] ++ lib.optionals cfg.unifiedCollector.dlq.enable [
            "SINEX_DLQ_BASE=${cfg.unifiedCollector.dlq.failureStoragePath}"
            "SINEX_LOG_BASE=${cfg.unifiedCollector.dlq.failureStoragePath}"
          ];
        };
      };

      sinex-promo-worker = mkIf cfg.promoWorker.enable {
        description = "Sinex Promotion Worker";
        wantedBy = [ "multi-user.target" ];
        after = [ "postgresql.service" "sinex-unified-collector.service" ];
        
        serviceConfig = {
          Type = "simple";
          User = cfg.database.user;
          Group = cfg.database.user;
          Restart = "always";
          RestartSec = "5s";
          
          # Resource limits
          MemoryMax = "512M";
          CPUQuota = "100%";
          
          ExecStart = "${cfg.package}/bin/sinex-promo-worker --agent-name=default-worker";
          
          # Secure credential handling via environment file
          EnvironmentFile = "/etc/sinex/credentials.env";
          
          # Non-sensitive environment variables
          Environment = [
            "RUST_LOG=${cfg.unifiedCollector.logLevel}"
            "POLL_INTERVAL=${toString cfg.promoWorker.pollInterval}"
            "BATCH_SIZE=${toString cfg.promoWorker.batchSize}"
          ];
        };
      };

      # DLQ cleanup service
      sinex-dlq-cleanup = mkIf (cfg.unifiedCollector.dlq.enable && cfg.unifiedCollector.dlq.cleanup.enable) {
        description = "Sinex DLQ Cleanup";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;
          
          ExecStart = pkgs.writeShellScript "sinex-dlq-cleanup" ''
            set -euo pipefail
            
            DLQ_PATH="${cfg.unifiedCollector.dlq.failureStoragePath}"
            MAX_AGE="${cfg.unifiedCollector.dlq.cleanup.maxAge}"
            MAX_FILES="${toString cfg.unifiedCollector.dlq.cleanup.maxFiles}"
            
            echo "$(date): Starting DLQ cleanup..."
            
            # Clean by age
            if [ -d "$DLQ_PATH" ]; then
              find "$DLQ_PATH" -name "*.json" -type f -mtime +''${MAX_AGE%d} -delete 2>/dev/null || true
              echo "Cleaned DLQ files older than $MAX_AGE"
              
              # Clean by count (keep newest files)
              FILE_COUNT=$(find "$DLQ_PATH" -name "*.json" -type f | wc -l)
              if [ "$FILE_COUNT" -gt "$MAX_FILES" ]; then
                EXCESS=$((FILE_COUNT - MAX_FILES))
                find "$DLQ_PATH" -name "*.json" -type f -printf '%T@ %p\n' | sort -n | head -n "$EXCESS" | cut -d' ' -f2- | xargs -r rm
                echo "Cleaned $EXCESS excess DLQ files (kept $MAX_FILES newest)"
              fi
            fi
            
            echo "$(date): DLQ cleanup completed"
          '';
        };
      };
    };

    # DLQ cleanup timer
    systemd.timers.sinex-dlq-cleanup = mkIf (cfg.unifiedCollector.dlq.enable && cfg.unifiedCollector.dlq.cleanup.enable) {
      description = "Sinex DLQ Cleanup Timer";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
        RandomizedDelaySec = "1h";
      };
    };

    # General cleanup service for logs and temporary files
    systemd.services.sinex-cleanup = mkIf cfg.directories.cleanup.enable {
      description = "Sinex Log and Cache Cleanup";
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        
        ExecStart = pkgs.writeShellScript "sinex-cleanup" ''
          set -euo pipefail
          
          LOG_DIR="${cfg.directories.logs}"
          CACHE_DIR="${cfg.directories.cache}"
          MAX_AGE="${cfg.directories.cleanup.maxLogAge}"
          MAX_SIZE="${cfg.directories.cleanup.maxLogSize}"
          
          echo "$(date): Starting Sinex cleanup..."
          
          # Clean old log files
          if [ -d "$LOG_DIR" ]; then
            find "$LOG_DIR" -name "*.log" -type f -mtime +''${MAX_AGE%d} -delete 2>/dev/null || true
            echo "Cleaned log files older than $MAX_AGE"
          fi
          
          # Clean cache directory
          if [ -d "$CACHE_DIR" ]; then
            find "$CACHE_DIR" -type f -mtime +7 -delete 2>/dev/null || true
            echo "Cleaned cache files older than 7 days"
          fi
          
          echo "$(date): Cleanup completed"
        '';
      };
    };

    # General cleanup timer
    systemd.timers.sinex-cleanup = mkIf cfg.directories.cleanup.enable {
      description = "Sinex Cleanup Timer";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.directories.cleanup.cleanupSchedule;
        Persistent = true;
        RandomizedDelaySec = "2h";
      };
    };

    # User and group creation
    users.users.${cfg.database.user} = {
      isSystemUser = true;
      group = cfg.database.user;
      description = "Sinex system user";
      home = "/var/lib/${cfg.database.user}";
      createHome = true;
    };

    users.groups.${cfg.database.user} = {};

    # Directory setup - comprehensive directory management
    systemd.tmpfiles.rules = [
      # Core directories
      "d ${cfg.directories.state} ${cfg.directories.permissions.state} ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.runtime} ${cfg.directories.permissions.runtime} ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.cache} ${cfg.directories.permissions.cache} ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.logs} ${cfg.directories.permissions.logs} ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.monitoring} ${cfg.directories.permissions.monitoring} ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.sockets} ${cfg.directories.permissions.sockets} ${cfg.database.user} ${cfg.database.user} -"
      
      # Security: credentials directory and file (secure permissions)
      "d /etc/sinex 0755 root root -"
      "f /etc/sinex/credentials.env 0640 root ${cfg.database.user} -"
    ] ++ lib.optionals cfg.unifiedCollector.dlq.enable [
      # DLQ specific directory
      "d ${cfg.unifiedCollector.dlq.failureStoragePath} 0755 ${cfg.database.user} ${cfg.database.user} -"
    ];

    # Secure credentials file generation
    systemd.services.sinex-credentials-setup = {
      description = "Generate Sinex Credentials File";
      wantedBy = [ "multi-user.target" ];
      before = [ "sinex-unified-collector.service" "sinex-promo-worker.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = "root";  # Needs root to write to /etc/sinex
        
        ExecStart = pkgs.writeShellScript "sinex-credentials-setup" ''
          set -euo pipefail
          
          CREDS_FILE="/etc/sinex/credentials.env"
          
          # Generate secure credentials file
          cat > "$CREDS_FILE" << EOF
          # Sinex Database Credentials - Generated automatically
          # This file contains sensitive information - do not edit manually
          DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}
          EOF
          
          # Ensure secure permissions
          chmod 640 "$CREDS_FILE"
          chown root:${cfg.database.user} "$CREDS_FILE"
          
          echo "Credentials file generated successfully"
        '';
      };
    };

    # Database setup (if enabled)
    services.postgresql = mkIf cfg.database.autoSetup {
      enable = true;
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.database.user;
          ensureDBOwnership = true;
        }
      ];
    };
  };
}