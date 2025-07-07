# Sinex NixOS Module - Modularized Structure
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Import utility modules
  healthChecks = import ./health-checks.nix { inherit lib; };
  
  # Import configuration generation utilities
  configGen = import ../config-gen.nix { inherit lib pkgs; };
  
  # Generate TOML configuration file using config-gen utilities with validation
  collectorConfigFile = configGen.mkCollectorConfigFile cfg.unifiedCollector cfg;
  
in
{
  imports = [
    ./database.nix
    ./event-sources.nix
    ./blob-storage.nix
    ./monitoring.nix
    ./preflight-verification.nix
    ./kitty-shell-integration.nix
  ];

  options.services.sinex = {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex or (import ../. { }).packages.${pkgs.system}.default;
      defaultText = literalExpression "pkgs.sinex";
      description = "Sinex package to use";
    };

    cliPackage = mkOption {
      type = types.package;
      default = pkgs.python3;  # Temporary default to fix VM tests
      defaultText = literalExpression "pkgs.sinex-cli";
      description = "Sinex CLI package to use";
    };

    # Simplified target user configuration
    targetUser = mkOption {
      type = types.str;
      description = "Username whose files to monitor for events";
      example = "myuser";
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

      # Metrics are now emitted as events, not via HTTP endpoint

      logLevel = mkOption {
        type = types.enum [ "trace" "debug" "info" "warn" "error" ];
        default = "info";
        description = "Log level for the collector";
      };

      dryRun = mkOption {
        type = types.bool;
        default = false;
        description = "Enable dry-run mode (events logged but not stored in database)";
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

      # Metrics are now emitted as events, not via HTTP endpoint

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


    # Resource limits configuration
    resources = {
      unifiedCollector = {
        memoryMax = mkOption {
          type = types.str;
          description = "Maximum memory for unified collector service";
        };

        cpuQuota = mkOption {
          type = types.str;
          description = "CPU quota for unified collector service";
        };

        tasksMax = mkOption {
          type = types.int;
          description = "Maximum number of tasks for unified collector service";
        };

        ioWeight = mkOption {
          type = types.int;
          default = 100;
          description = "IO weight for unified collector service (10-1000)";
        };
      };

      promoWorker = {
        memoryMax = mkOption {
          type = types.str;
          description = "Maximum memory for promotion worker service";
        };

        cpuQuota = mkOption {
          type = types.str;
          description = "CPU quota for promotion worker service";
        };

        tasksMax = mkOption {
          type = types.int;
          description = "Maximum number of tasks for promotion worker service";
        };

        ioWeight = mkOption {
          type = types.int;
          default = 100;
          description = "IO weight for promotion worker service (10-1000)";
        };
      };
    };

    # Security configuration
    security = {
      level = mkOption {
        type = types.enum [ "minimal" "balanced" "strict" ];
        default = "balanced";
        description = ''
          Security level for SystemD hardening:
          - minimal: Basic security, maximum functionality
          - balanced: Reasonable security with event monitoring capabilities
          - strict: Maximum security, may restrict some monitoring features
        '';
      };

      allowFileSystemAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow filesystem monitoring access (disabling may break file events)";
      };

      allowSocketAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow access to Unix sockets for terminal and window manager monitoring";
      };

      allowDeviceAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow device access for hardware event monitoring";
      };
    };

    # Update configuration
    update = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable coordinated update process";
      };

      gracePeriod = mkOption {
        type = types.int;
        default = 30;
        description = "Grace period in seconds for services to complete work before update";
      };

      healthCheckTimeout = mkOption {
        type = types.int;
        default = 60;
        description = "Maximum time to wait for health checks after update";
      };

      rollbackOnFailure = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically rollback if health checks fail";
      };

      preserveData = mkOption {
        type = types.bool;
        default = true;
        description = "Preserve DLQ and failure data during updates";
      };
    };

  };

  config = mkIf cfg.enable {
    # Environment packages
    environment.systemPackages = with pkgs; [ 
      asciinema 
      cfg.cliPackage
    ];

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
        # Resource limits for lite preset
        resources = {
          unifiedCollector = {
            memoryMax = mkDefault "512M";
            cpuQuota = mkDefault "100%";
            tasksMax = mkDefault 500;
          };
          promoWorker = {
            memoryMax = mkDefault "256M";
            cpuQuota = mkDefault "50%";
            tasksMax = mkDefault 250;
          };
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
        # Resource limits for normal preset
        resources = {
          unifiedCollector = {
            memoryMax = mkDefault "1G";
            cpuQuota = mkDefault "200%";
            tasksMax = mkDefault 1000;
          };
          promoWorker = {
            memoryMax = mkDefault "512M";
            cpuQuota = mkDefault "100%";
            tasksMax = mkDefault 500;
          };
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
        # Resource limits for max preset
        resources = {
          unifiedCollector = {
            memoryMax = mkDefault "2G";
            cpuQuota = mkDefault "400%";
            tasksMax = mkDefault 2000;
          };
          promoWorker = {
            memoryMax = mkDefault "1G";
            cpuQuota = mkDefault "200%";
            tasksMax = mkDefault 1000;
          };
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
          Type = "notify";  # Changed to notify for proper startup coordination
          User = cfg.database.user;
          Group = cfg.database.user;
          
          # Pre-start validation and migration
          ExecStartPre = mkIf (cfg.database.autoSetup && cfg.database.migration.enable) (
            pkgs.writeShellScript "sinex-collector-pre-start" ''
              set -euo pipefail
              
              echo "Preparing Sinex collector startup..."
              
              # Setup database URL
              export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
              
              # Wait for PostgreSQL with adaptive polling
              echo "Waiting for PostgreSQL..."
              max_attempts=60
              for attempt in $(seq 1 $max_attempts); do
                if ${pkgs.postgresql}/bin/pg_isready -h ${cfg.database.host} -p ${toString cfg.database.port} -U ${cfg.database.user} -d ${cfg.database.name}; then
                  echo "✓ PostgreSQL ready (attempt $attempt)"
                  break
                fi
                
                # Adaptive delay based on connection attempts
                if [ $attempt -lt 10 ]; then
                  ${pkgs.coreutils}/bin/sleep 0.2  # Quick polling initially
                elif [ $attempt -lt 30 ]; then
                  ${pkgs.coreutils}/bin/sleep 1.0  # Moderate polling
                else
                  ${pkgs.coreutils}/bin/sleep 2.0  # Slower polling for persistent issues
                fi
                
                # Progress feedback every 15 attempts
                if [ $((attempt % 15)) -eq 0 ]; then
                  echo "  PostgreSQL not ready, still trying... (attempt $attempt/$max_attempts)"
                fi
              done
              
              # Ensure extensions exist
              echo "Ensuring database extensions..."
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";" || true
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" || true
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" || true
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pgx_ulid;" || true
              
              # Check current schema version
              CURRENT_VERSION=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1;" 2>/dev/null || echo "0")
              echo "Current schema version: $CURRENT_VERSION"
              
              # Run migrations
              if [ -d "${cfg.database.migration.directory}" ]; then
                echo "Running database migrations..."
                if ! ${cfg.database.migration.package}/bin/sqlx migrate run --source "${cfg.database.migration.directory}"; then
                  echo "ERROR: Database migration failed!" >&2
                  exit 1
                fi
                
                # Verify new version
                NEW_VERSION=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1;" 2>/dev/null || echo "0")
                echo "New schema version: $NEW_VERSION"
              fi
              
              # Test database connectivity with actual query
              echo "Testing database connectivity..."
              if ! ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "SELECT 1 FROM pg_tables WHERE schemaname = 'raw' LIMIT 1;" >/dev/null 2>&1; then
                echo "ERROR: Database schema validation failed!" >&2
                exit 1
              fi
              
              echo "Pre-start validation completed successfully"
            ''
          );
          
          # Post-start health check
          ExecStartPost = pkgs.writeShellScript "sinex-collector-post-start" ''
            set -euo pipefail
            
            echo "Validating Sinex collector startup..."
            
            # Wait for service to be ready with process-based monitoring
            echo "Waiting for service to reach running state..."
            max_attempts=60
            for attempt in $(seq 1 $max_attempts); do
              if systemctl show -p SubState --value sinex-unified-collector | grep -q "running"; then
                echo "✓ Service is running (attempt $attempt)"
                break
              fi
              if [ $attempt -eq $max_attempts ]; then
                echo "ERROR: Service failed to reach running state after $max_attempts attempts" >&2
                exit 1
              fi
              
              # Adaptive polling based on service startup phase
              if [ $attempt -lt 15 ]; then
                ${pkgs.coreutils}/bin/sleep 0.2  # Quick polling during startup
              else
                ${pkgs.coreutils}/bin/sleep 1.0  # Slower polling if service is struggling
              fi
              
              # Progress feedback every 20 attempts
              if [ $((attempt % 20)) -eq 0 ]; then
                echo "  Service not ready, still waiting... (attempt $attempt/$max_attempts)"
              fi
            done
            
            # Check database connectivity from the service
            export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            
            # Verify we can insert a test event
            TEST_ID=$(${pkgs.util-linux}/bin/uuidgen)
            if ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "
              INSERT INTO raw.events (id, source, event_type, ts_ingest, ts_orig, host, payload) 
              VALUES ('$TEST_ID', 'sinex.health', 'startup.test', NOW(), NOW(), '$(hostname)', '{\"test\": true}'::jsonb);
              DELETE FROM raw.events WHERE id = '$TEST_ID';
            " >/dev/null 2>&1; then
              echo "✓ Database write test passed"
            else
              echo "ERROR: Database write test failed!" >&2
              exit 1
            fi
            
            # Wait for heartbeat to appear with adaptive timing
            echo "Waiting for heartbeat..."
            check_interval=1
            max_interval=3
            for i in {1..10}; do
              if ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "
                SELECT COUNT(*) FROM component_heartbeats 
                WHERE component_name = 'unified-collector' 
                AND timestamp > NOW() - INTERVAL '1 minute'
              " | grep -q "[1-9]"; then
                echo "✓ Heartbeat detected (attempt $i)"
                break
              fi
              if [ $i -eq 10 ]; then
                echo "WARNING: No heartbeat detected (non-fatal)" >&2
              fi
              echo "  Waiting for heartbeat, checking again in ${check_interval}s... (attempt $i/10)"
              sleep $check_interval
              # Exponential backoff capped at 3s
              if [ $check_interval -lt $max_interval ]; then
                check_interval=$((check_interval * 2))
              fi
            done
            
            echo "Collector startup validation completed successfully"
          '';
          
          # Restart policy with rate limiting
          Restart = cfg.unifiedCollector.restart.policy;
          RestartSec = cfg.unifiedCollector.restart.baseDelay;
          StartLimitIntervalSec = 300;  # 5 minutes
          StartLimitBurst = 3;
          
          # Graceful shutdown
          KillMode = "mixed";
          KillSignal = "SIGTERM";
          TimeoutStopSec = 30;  # Give time for graceful shutdown
          
          # Resource limits
          MemoryMax = cfg.resources.unifiedCollector.memoryMax;
          CPUQuota = cfg.resources.unifiedCollector.cpuQuota;
          TasksMax = cfg.resources.unifiedCollector.tasksMax;
          IOWeight = cfg.resources.unifiedCollector.ioWeight;
          
          # Security hardening (configurable based on security level)
          PrivateTmp = !(cfg.security.allowSocketAccess && cfg.security.level != "strict");
          ProtectSystem = if cfg.security.allowFileSystemAccess && cfg.security.level == "minimal" then false else "strict";
          ProtectHome = !(cfg.security.allowFileSystemAccess && cfg.security.level != "strict");
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          
          # System call filters based on security level
          SystemCallFilter = if cfg.security.level == "minimal" then
            [ "@system-service" "@network-io" "@file-system" "@process" "~@privileged" ]
          else if cfg.security.level == "balanced" then
            [ "@system-service" "@network-io" "@file-system" "~@privileged" "~@mount" ]
          else  # strict
            [ "@system-service" "@network-io" "~@privileged" "~@mount" "~@raw-io" ];
          
          # Network and device access
          PrivateNetwork = false;  # Always need network for PostgreSQL and D-Bus
          PrivateDevices = !cfg.security.allowDeviceAccess;
          ProtectProc = if cfg.security.level == "strict" then "noaccess" else "invisible";
          ProcSubset = "pid";
          
          # Socket and runtime access (conditional)
          BindReadOnlyPaths = lib.optionals cfg.security.allowSocketAccess [
            "/run/user"      # For user sockets (Hyprland, etc.)
          ] ++ lib.optionals (!cfg.security.allowSocketAccess || cfg.security.level != "strict") [
            "/tmp"           # For temporary sockets and files
          ];
          
          # Allow writes to DLQ, logs, and monitoring paths
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
        requires = [ "postgresql.service" ];
        
        serviceConfig = {
          Type = "notify";
          User = cfg.database.user;
          Group = cfg.database.user;
          
          # Pre-start validation
          ExecStartPre = pkgs.writeShellScript "sinex-worker-pre-start" ''
            set -euo pipefail
            
            echo "Preparing Sinex worker startup..."
            
            # Setup database URL
            export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            
            # Wait for PostgreSQL and verify schema
            echo "Verifying database schema..."
            if ! ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "SELECT 1 FROM pg_tables WHERE schemaname = 'sinex_schemas' AND tablename = 'promotion_queue' LIMIT 1;" >/dev/null 2>&1; then
              echo "ERROR: Promotion queue table not found!" >&2
              exit 1
            fi
            
            echo "Pre-start validation completed successfully"
          '';
          
          # Restart policy with rate limiting
          Restart = "on-failure";
          RestartSec = "5s";
          StartLimitIntervalSec = "60s";
          StartLimitBurst = 3;
          
          # Resource limits
          MemoryMax = cfg.resources.promoWorker.memoryMax;
          CPUQuota = cfg.resources.promoWorker.cpuQuota;
          TasksMax = cfg.resources.promoWorker.tasksMax;
          IOWeight = cfg.resources.promoWorker.ioWeight;
          
          # Security hardening (more restrictive for worker - database-only)
          PrivateTmp = cfg.security.level != "minimal";
          ProtectSystem = "strict";  # Worker doesn't monitor filesystem
          ProtectHome = true;        # Worker doesn't need home directory access
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          
          # Worker only needs basic system calls and network
          SystemCallFilter = if cfg.security.level == "minimal" then
            [ "@system-service" "@network-io" "~@privileged" ]
          else
            [ "@system-service" "@network-io" "~@privileged" "~@mount" "~@raw-io" ];
          
          # Worker only needs network for PostgreSQL
          PrivateNetwork = false;  # Need network for PostgreSQL connection
          PrivateDevices = true;   # Worker doesn't need device access
          ProtectProc = if cfg.security.level == "strict" then "noaccess" else "invisible";
          ProcSubset = "pid";      # Minimal process information
          
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

    # Coordinated update service
    systemd.services.sinex-update = mkIf cfg.update.enable {
      description = "Sinex Coordinated Update";
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        
        ExecStart = pkgs.writeShellScript "sinex-update" ''
          set -euo pipefail
          
          echo "$(date): Starting Sinex coordinated update..."
          
          # Function to check service health
          check_health() {
            local service=$1
            
            # Check if service is active using systemd state
            if ! systemctl is-active "$service" >/dev/null 2>&1; then
              return 1
            fi
            
            # Process-based health indicators (not clock-dependent)
            local service_pid=$(systemctl show -p MainPID --value "$service")
            if [ -z "$service_pid" ] || [ "$service_pid" = "0" ]; then
              return 1
            fi
            
            # Check if process is actually running and responsive
            if ! kill -0 "$service_pid" 2>/dev/null; then
              return 1
            fi
            
            # Check process state (not zombie/defunct)
            local proc_state=$(ps -o state= -p "$service_pid" 2>/dev/null | tr -d ' ')
            case "$proc_state" in
              Z|X|D) return 1 ;;  # Zombie, dead, or uninterruptible sleep
              *) ;;               # Running, sleeping, or other normal states
            esac
            
            # Check for database connectivity for data services (optional)
            if systemctl is-active postgresql >/dev/null 2>&1; then
              export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
              
              case "$service" in
                sinex-unified-collector|sinex-promo-worker)
                  # Quick connectivity test (not time-dependent)
                  if ! ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "SELECT 1;" >/dev/null 2>&1; then
                    return 1
                  fi
                  ;;
              esac
            fi
            
            return 0
          }
          
          # Wait for service to reach ready state with exponential backoff
          wait_for_service_ready() {
            local service_name=$1
            local max_attempts=''${2:-120}  # Maximum number of attempts instead of time
            local attempt=0
            local backoff_interval=0.1  # Start with very short interval
            local max_interval=2.0      # Cap backoff at 2s
            
            echo "Waiting for $service_name to become ready..."
            
            while [ $attempt -lt $max_attempts ]; do
              if check_health "$service_name"; then
                echo "✓ $service_name is ready (attempt $attempt)"
                return 0
              fi
              
              attempt=$((attempt + 1))
              
              # Adaptive backoff based on service behavior, not time
              if [ $attempt -lt 10 ]; then
                # Quick polling initially (process startup phase)
                ${pkgs.coreutils}/bin/sleep 0.1
              elif [ $attempt -lt 30 ]; then
                # Moderate polling (service initialization phase)
                ${pkgs.coreutils}/bin/sleep 0.5
              else
                # Slower polling (waiting for dependencies)
                ${pkgs.coreutils}/bin/sleep 1.0
              fi
              
              # Progress feedback every 20 attempts
              if [ $((attempt % 20)) -eq 0 ]; then
                echo "  $service_name not ready yet (attempt $attempt/$max_attempts)..."
              fi
            done
            
            echo "ERROR: $service_name failed to become ready within $max_attempts attempts"
            return 1
          }
          
          # Wait for worker to finish processing with active monitoring
          wait_for_worker_idle() {
            local max_checks=''${1:-60}  # Maximum number of checks instead of time
            local check_count=0
            local consecutive_idle=0      # Count consecutive idle checks
            local required_idle=3         # Require 3 consecutive idle checks
            
            echo "Waiting for worker to finish processing..."
            
            if ! systemctl is-active sinex-promo-worker >/dev/null 2>&1; then
              echo "Worker is not running, no need to wait"
              return 0
            fi
            
            export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            
            while [ $check_count -lt $max_checks ]; do
              # Check if worker is processing items
              local processing_count=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "
                SELECT COUNT(*) FROM work_queue 
                WHERE status = 'processing' 
                AND claimed_by IS NOT NULL
              " 2>/dev/null || echo "0")
              
              if [ "$processing_count" -eq 0 ]; then
                consecutive_idle=$((consecutive_idle + 1))
                if [ $consecutive_idle -ge $required_idle ]; then
                  echo "✓ Worker is idle (confirmed with $required_idle consecutive checks)"
                  return 0
                fi
                echo "  Worker idle ($consecutive_idle/$required_idle consecutive checks)"
              else
                consecutive_idle=0
                echo "  Worker processing $processing_count items (check $check_count/$max_checks)"
              fi
              
              check_count=$((check_count + 1))
              ${pkgs.coreutils}/bin/sleep 0.5  # Fixed short interval for work queue monitoring
            done
            
            echo "WARNING: Worker still processing after $max_checks checks, proceeding anyway"
            return 0
          }
          
          # Save current state
          echo "Saving current state..."
          COLLECTOR_WAS_ACTIVE=false
          WORKER_WAS_ACTIVE=false
          
          if systemctl is-active sinex-unified-collector >/dev/null 2>&1; then
            COLLECTOR_WAS_ACTIVE=true
          fi
          
          if systemctl is-active sinex-promo-worker >/dev/null 2>&1; then
            WORKER_WAS_ACTIVE=true
          fi
          
          # Preserve data if requested
          if [ "${toString cfg.update.preserveData}" = "1" ] && [ -d "${cfg.unifiedCollector.dlq.failureStoragePath}" ]; then
            echo "Preserving DLQ data..."
            BACKUP_DIR="${cfg.unifiedCollector.dlq.failureStoragePath}.backup-$(date +%Y%m%d-%H%M%S)"
            cp -a "${cfg.unifiedCollector.dlq.failureStoragePath}" "$BACKUP_DIR" || true
          fi
          
          # Graceful shutdown
          echo "Initiating graceful shutdown..."
          
          # Stop worker first (processes events)
          if [ "$WORKER_WAS_ACTIVE" = "true" ]; then
            echo "Stopping promotion worker..."
            systemctl stop sinex-promo-worker
          fi
          
          # Wait for worker to finish processing
          wait_for_worker_idle 30
          
          # Stop collector
          if [ "$COLLECTOR_WAS_ACTIVE" = "true" ]; then
            echo "Stopping collector with ${toString cfg.update.gracePeriod}s grace period..."
            systemctl stop sinex-unified-collector
            
            # Wait for graceful shutdown
            sleep ${toString cfg.update.gracePeriod}
          fi
          
          # Perform updates (migrations were already run in ExecStartPre)
          echo "Updates applied via service ExecStartPre hooks"
          
          # Restart services in order
          echo "Starting services..."
          
          if [ "$COLLECTOR_WAS_ACTIVE" = "true" ]; then
            echo "Starting collector..."
            if ! systemctl start sinex-unified-collector; then
              echo "ERROR: Failed to start collector!" >&2
              exit 1
            fi
            
            # Wait for collector to be ready
            wait_for_service_ready sinex-unified-collector 60
          fi
          
          if [ "$WORKER_WAS_ACTIVE" = "true" ]; then
            echo "Starting promotion worker..."
            if ! systemctl start sinex-promo-worker; then
              echo "ERROR: Failed to start worker!" >&2
              # Stop collector if worker fails
              [ "$COLLECTOR_WAS_ACTIVE" = "true" ] && systemctl stop sinex-unified-collector
              exit 1
            fi
          fi
          
          # Health check with attempt-based limits (not time-based)
          echo "Performing health checks (max attempts: ${toString cfg.update.healthCheckTimeout})..."
          
          HEALTH_CHECK_PASSED=true
          check_attempt=0
          max_attempts=${toString cfg.update.healthCheckTimeout}  # Reuse config value as max attempts
          consecutive_healthy=0
          required_consecutive=3  # Require multiple consecutive healthy checks
          
          while [ $check_attempt -lt $max_attempts ]; do
            check_attempt=$((check_attempt + 1))
            
            ALL_HEALTHY=true
            
            if [ "$COLLECTOR_WAS_ACTIVE" = "true" ] && ! check_health sinex-unified-collector; then
              ALL_HEALTHY=false
            fi
            
            if [ "$WORKER_WAS_ACTIVE" = "true" ] && ! check_health sinex-promo-worker; then
              ALL_HEALTHY=false
            fi
            
            if [ "$ALL_HEALTHY" = "true" ]; then
              consecutive_healthy=$((consecutive_healthy + 1))
              if [ $consecutive_healthy -ge $required_consecutive ]; then
                echo "✓ All services healthy (confirmed with $required_consecutive consecutive checks)"
                break
              fi
              echo "  Services healthy ($consecutive_healthy/$required_consecutive consecutive checks)"
            else
              consecutive_healthy=0
              echo "  Waiting for services to become healthy (attempt $check_attempt/$max_attempts)..."
            fi
            
            # Adaptive delay based on check progress
            if [ $check_attempt -lt 10 ]; then
              ${pkgs.coreutils}/bin/sleep 1    # Quick checks initially
            else
              ${pkgs.coreutils}/bin/sleep 2    # Longer interval if services are struggling
            fi
          done
          
          # Final validation
          if [ $consecutive_healthy -lt $required_consecutive ]; then
            echo "ERROR: Health check failed after $max_attempts attempts!" >&2
            HEALTH_CHECK_PASSED=false
          fi
          
          # Handle rollback if needed
          if [ "$HEALTH_CHECK_PASSED" = "false" ] && [ "${toString cfg.update.rollbackOnFailure}" = "true" ]; then
            echo "Initiating rollback due to health check failure..."
            
            # Stop failed services
            [ "$WORKER_WAS_ACTIVE" = "true" ] && systemctl stop sinex-promo-worker || true
            [ "$COLLECTOR_WAS_ACTIVE" = "true" ] && systemctl stop sinex-unified-collector || true
            
            # Restore data if preserved
            if [ -n "''${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
              echo "Restoring DLQ data..."
              rm -rf "${cfg.unifiedCollector.dlq.failureStoragePath}"
              mv "$BACKUP_DIR" "${cfg.unifiedCollector.dlq.failureStoragePath}"
            fi
            
            echo "ERROR: Update failed and was rolled back" >&2
            exit 1
          fi
          
          # Cleanup backup if successful
          if [ -n "''${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
            echo "Cleaning up backup..."
            rm -rf "$BACKUP_DIR"
          fi
          
          echo "$(date): Sinex update completed successfully"
        '';
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

    
    # Terminal auto-recording for all users
    programs.bash.promptInit = mkIf cfg.unifiedCollector.sources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="$HOME/.local/share/asciinema"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    programs.zsh.promptInit = mkIf cfg.unifiedCollector.sources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="$HOME/.local/share/asciinema"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    # Assertions for configuration validation
    assertions = [
      {
        assertion = cfg.enable -> cfg.targetUser != "";
        message = "services.sinex.targetUser must be set when Sinex is enabled";
      }
      {
        assertion = cfg.monitoring.observabilityStack.enable -> cfg.database.autoSetup || config.services.postgresql.enable;
        message = "PostgreSQL must be enabled for Sinex observability stack";
      }
      {
        assertion = cfg.monitoring.dashboards.grafana.enable -> cfg.monitoring.observabilityStack.enable;
        message = "Grafana dashboards require the observability stack to be enabled";
      }
    ];
  };
}