# Sinex NixOS Module - Modularized Structure
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Import utility modules
  healthChecks = import ./health-checks.nix { inherit lib; };
  
  # Import configuration generation utilities
  configGen = import ../config-gen.nix { inherit lib pkgs; };
  
  # Generate TOML configuration file (simplified approach)
  collectorConfigFile = pkgs.writeText "collector.toml" ''
    # Generated Sinex Collector Configuration
    enabled_events = [
      ${lib.concatMapStringsSep "\n  " (event: ''"${event}",''') (
        lib.flatten [
          (lib.optional (cfg.unifiedCollector.sources.filesystem.enable or false) [
            "file.created" "file.modified" "file.deleted"
          ])
          (lib.optional (cfg.unifiedCollector.sources.atuin.enable or false) [
            "shell.command.executed_atuin"
          ])
          (lib.optional (cfg.unifiedCollector.sources.dbus.enable or false) [
            "dbus.signal" "system.notification" "media.playback.changed"
          ])
          (lib.optional (cfg.unifiedCollector.sources.clipboard.enable or false) [
            "clipboard.changed"
          ])
          (lib.optional (cfg.unifiedCollector.sources.kittyScrollback.enable or false) [
            "terminal.scrollback.captured"
          ])
        ]
      )}
    ]
    
    # Global git-annex repository for large content storage
    ${lib.optionalString (cfg.blobStorage.enable or false) ''
    annex_repo_path = "${cfg.blobStorage.repositoryPath}"
    ''}
    
    [output]
    database = true
    logging = ${if cfg.unifiedCollector.logLevel == "debug" then "true" else "false"}
    
    [logging]
    level = "${cfg.unifiedCollector.logLevel}"
    format = "pretty"
    
    ${lib.optionalString (cfg.unifiedCollector.sources.filesystem.enable or false) ''
    [event.files]
    watch_patterns = [${lib.concatMapStringsSep ", " (path: ''"${path}"'') (cfg.unifiedCollector.sources.filesystem.watchPaths or ["~/Documents"])}]
    ignore_patterns = [${lib.concatMapStringsSep ", " (pattern: ''"${pattern}"'') (cfg.unifiedCollector.sources.filesystem.excludePatterns._allExcludePatterns or [])}]
    debounce_ms = 100
    ''}
    
    ${lib.optionalString (cfg.unifiedCollector.sources.atuin.enable or false) ''
    [event.shell_command_executed_atuin]
    db_path = "${cfg.unifiedCollector.sources.atuin.databasePath or "~/.local/share/atuin/history.db"}"
    polling_interval_secs = ${toString (cfg.unifiedCollector.sources.atuin.pollInterval or 5)}
    batch_size = 100
    ''}
    
    ${lib.optionalString (cfg.unifiedCollector.sources.dbus.enable or false) ''
    [event.dbus]
    monitor_session = true
    monitor_system = ${lib.boolToString (cfg.unifiedCollector.sources.dbus.extractAll or false)}
    extract_notifications = ${lib.boolToString (cfg.unifiedCollector.sources.dbus.extractNotifications or true)}
    extract_media = ${lib.boolToString (cfg.unifiedCollector.sources.dbus.extractMedia or true)}
    ''}
  '';
  
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

    # Simplified directories - monitoring.nix compatibility
    directories = {
      state = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Directory for persistent state data";
      };

      logs = mkOption {
        type = types.path;
        default = "/var/log/sinex";
        description = "Directory for log files";
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
        after = [ "postgresql.service" "network-online.target" ];
        wants = [ "network-online.target" ];
        requires = [ "postgresql.service" ];
        
        serviceConfig = {
          Type = "simple";
          User = cfg.database.user;
          Group = cfg.database.user;
          
          # Restart policy with rate limiting
          Restart = cfg.unifiedCollector.restart.policy;
          RestartSec = cfg.unifiedCollector.restart.baseDelay;
          StartLimitIntervalSec = "60s";
          StartLimitBurst = 3;
          
          # Resource limits
          MemoryMax = "1G";
          CPUQuota = "200%";
          TasksMax = 1000;
          IOWeight = 100;
          
          # Security hardening
          PrivateTmp = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          SystemCallFilter = [ "@system-service" "~@privileged" ];
          
          # Allow writes to DLQ and logs
          ReadWritePaths = lib.optionals cfg.unifiedCollector.dlq.enable [
            cfg.unifiedCollector.dlq.failureStoragePath
          ] ++ [
            cfg.directories.state
            cfg.directories.logs
          ];
          
          ExecStart = "${cfg.package}/bin/sinex-collector --config ${collectorConfigFile}";
          
          # Environment variables (use agenix for DATABASE_URL if needed)
          Environment = [
            "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            "RUST_LOG=${cfg.unifiedCollector.logLevel}"
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
          
          # Restart policy with rate limiting
          Restart = "on-failure";
          RestartSec = "5s";
          StartLimitIntervalSec = "60s";
          StartLimitBurst = 3;
          
          # Resource limits
          MemoryMax = "512M";
          CPUQuota = "100%";
          TasksMax = 500;
          IOWeight = 100;
          
          # Security hardening
          PrivateTmp = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          SystemCallFilter = [ "@system-service" "~@privileged" ];
          
          # Database access only, no file writes needed for promo worker
          ReadWritePaths = [ ];
          
          ExecStart = "${cfg.package}/bin/sinex-promo-worker --agent-name=default-worker";
          
          # Environment variables (use agenix for DATABASE_URL if needed)  
          Environment = [
            "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
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
          
          # Resource limits for oneshot service
          MemoryMax = "256M";
          TasksMax = 50;
          IOWeight = 50;
          
          # Security hardening
          PrivateTmp = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          SystemCallFilter = [ "@system-service" "~@privileged" ];
          
          # Only allow writes to DLQ directory
          ReadWritePaths = [ cfg.unifiedCollector.dlq.failureStoragePath ];
          
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


    # User and group creation
    users.users.${cfg.database.user} = {
      isSystemUser = true;
      group = cfg.database.user;
      description = "Sinex system user";
      home = "/var/lib/${cfg.database.user}";
      createHome = true;
    };

    users.groups.${cfg.database.user} = {};

    # Directory setup and configuration
    systemd.tmpfiles.rules = [
      # Basic directories for monitoring.nix compatibility  
      "d ${cfg.directories.state} 0755 ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.logs} 0755 ${cfg.database.user} ${cfg.database.user} -"
      # Configuration directory
      "d /etc/sinex 0755 root root -"
    ] ++ lib.optionals cfg.unifiedCollector.dlq.enable [
      # DLQ failure storage directory
      "d ${cfg.unifiedCollector.dlq.failureStoragePath} 0755 ${cfg.database.user} ${cfg.database.user} -"
    ];
    
    # Place generated configuration file in standard location
    environment.etc."sinex/collector.toml".source = collectorConfigFile;

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