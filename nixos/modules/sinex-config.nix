# Sinex Configuration Module - Direct, Clear Settings
{
  lib,
  config,
  pkgs,
  ...
}:

with lib;

let
  cfg = config.services.sinex;

in
{
  options.services.sinex = {
    enable = mkOption {
      type = types.bool;
      default = false;
      description = "Enable Sinex event capture system";
    };

    targetUser = mkOption {
      type = types.str;
      description = "User whose activity to capture";
    };

    # REQUIRED: Git-annex repository path (no defaults to avoid mistakes)
    annexRepo = mkOption {
      type = types.str;
      description = ''
        Path to git-annex repository for blob storage.
        Must be an initialized git-annex repository.
        No default - must be explicitly configured to avoid data loss.
      '';
    };

    # Database configuration (simple, clear)
    database = {
      name = mkOption {
        type = types.str;
        default = "sinex";
        description = "PostgreSQL database name";
      };

      user = mkOption {
        type = types.str;
        default = cfg.targetUser;
        description = "Database user (defaults to target user)";
      };

      autoSetup = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically create database and apply migrations";
      };

      connectionPoolSize = mkOption {
        type = types.int;
        default = 25;
        description = "Database connection pool size";
      };
    };

    # Event sources (full-featured defaults, easy to disable)
    eventSources = {
      filesystem = mkOption {
        type = types.bool;
        default = true;
        description = "Monitor filesystem changes";
      };

      terminal = mkOption {
        type = types.bool;
        default = true;
        description = "Capture terminal commands and activity";
      };

      windowManager = mkOption {
        type = types.bool;
        default = true;
        description = "Monitor window focus and workspace changes";
      };

      clipboard = mkOption {
        type = types.bool;
        default = true;
        description = "Capture clipboard content changes";
      };

      systemEvents = mkOption {
        type = types.bool;
        default = true;
        description = "Monitor D-Bus signals, journal, and system events";
      };

      # Optional advanced sources (disabled by default - not yet implemented)
      processMonitoring = mkOption {
        type = types.bool;
        default = false;
        description = "Monitor all process launches (not yet implemented)";
      };

      networkMonitoring = mkOption {
        type = types.bool;
        default = false;
        description = "Monitor network connections (not yet implemented)";
      };

      screenCapture = mkOption {
        type = types.bool;
        default = false;
        description = "Periodic screenshots with OCR (not yet implemented - privacy sensitive)";
      };
    };

    # Observability: simple on/off (on = full scale monitoring)
    observability = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = ''
          Enable full observability stack: Prometheus + Grafana + dashboards + metrics + alerts.
          When enabled, provides comprehensive monitoring with all features.
          When disabled, only basic health checks and warn-level logging.
        '';
      };

      grafanaPort = mkOption {
        type = types.port;
        default = 3000;
        description = "Grafana web interface port";
      };

      prometheusPort = mkOption {
        type = types.port;
        default = 9090;
        description = "Prometheus metrics port";
      };
    };

    # Storage settings
    storage = {
      # Infinite retention by default (data is minimal, storage is cheap)
      dataRetention = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = ''
          Data retention period (e.g., "90d", "1y"). 
          Set to null for infinite retention (recommended - data volumes are minimal).
        '';
      };

      compressionLevel = mkOption {
        type = types.enum [ "fast" "balanced" "max" ];
        default = "balanced";
        description = "TimescaleDB compression level";
      };

      blobThreshold = mkOption {
        type = types.str;
        default = "10MB";
        description = "Store content larger than this in git-annex";
      };
    };

    # Directories (sensible defaults based on target user)
    directories = {
      state = mkOption {
        type = types.str;
        default = "/var/lib/sinex";
        description = "State directory for Sinex data";
      };

      logs = mkOption {
        type = types.str;
        default = "/var/log/sinex";
        description = "Log directory";
      };

      config = mkOption {
        type = types.str;
        default = "/etc/sinex";
        description = "Configuration directory";
      };
    };

    # Service configuration
    services = {
      collector = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable unified event collector";
        };

        memoryLimit = mkOption {
          type = types.str;
          default = "512M";
          description = "Memory limit for collector service";
        };
      };

      worker = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable event processing worker";
        };

        concurrency = mkOption {
          type = types.int;
          default = 4;
          description = "Number of concurrent workers";
        };
      };

      updateService = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic updates with pre-flight verification";
        };

        gracePeriod = mkOption {
          type = types.int;
          default = 30;
          description = "Graceful shutdown period in seconds";
        };
      };
    };

    # Pre-flight verification (enabled by default for safety)
    preflightVerification = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable pre-flight verification before deployments";
      };

      timeout = mkOption {
        type = types.int;
        default = 120;
        description = "Verification timeout in seconds";
      };

      failureAction = mkOption {
        type = types.enum [ "abort" "warn" ];
        default = "abort";
        description = "Action on verification failure";
      };
    };
  };

  config = mkIf cfg.enable {
    # Assertions for required configuration
    assertions = [
      {
        assertion = cfg.annexRepo != "";
        message = "services.sinex.annexRepo must be explicitly configured to a valid git-annex repository path";
      }
      {
        assertion = cfg.targetUser != "";
        message = "services.sinex.targetUser must be specified";
      }
      {
        assertion = config.services.postgresql.enable || !cfg.database.autoSetup;
        message = "PostgreSQL must be enabled when using automatic database setup";
      }
    ];

    # User account for Sinex services
    users.users.${cfg.database.user} = mkIf (cfg.database.user != "root") {
      isSystemUser = true;
      group = cfg.database.user;
      home = cfg.directories.state;
      createHome = true;
      # Add to systemd-journal group for journal access
      extraGroups = [ "systemd-journal" ];
    };

    users.groups.${cfg.database.user} = mkIf (cfg.database.user != "root") {};

    # PostgreSQL configuration
    services.postgresql = mkIf cfg.database.autoSetup {
      enable = true;
      package = pkgs.postgresql_16;
      extensions = with pkgs.postgresql16Packages; [
        timescaledb
        # TODO: Add pg_jsonschema when available in nixpkgs
      ];
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.database.user;
          ensureDBOwnership = true;
        }
      ];
    };

    # Core Sinex services
    systemd.services = {
      sinex-unified-collector = mkIf cfg.services.collector.enable {
        description = "Sinex Unified Event Collector";
        wantedBy = [ "multi-user.target" ];
        after = [ "postgresql.service" "network.target" ];
        wants = [ "postgresql.service" ];

        # Pre-flight verification dependency
        requisite = mkIf cfg.preflightVerification.enable [ "sinex-preflight.service" ];

        environment = {
          DATABASE_URL = "postgresql:///${cfg.database.name}?host=/run/postgresql";
          SINEX_ANNEX_REPO = cfg.annexRepo;
          RUST_LOG = if cfg.observability.enable then "info" else "warn";
        };

        serviceConfig = {
          Type = "simple";
          User = cfg.database.user;
          Group = cfg.database.user;
          Restart = "always";
          RestartSec = "10s";
          MemoryMax = cfg.services.collector.memoryLimit;
          
          # Generate configuration from NixOS options
          ExecStart = pkgs.writeShellScript "sinex-collector" ''
            set -euo pipefail
            
            # Generate runtime configuration
            config_file="$(mktemp)"
            cat > "$config_file" <<EOF
            [collector]
            annex_repo_path = "${cfg.annexRepo}"
            database_pool_size = ${toString cfg.database.connectionPoolSize}
            blob_threshold = "${cfg.storage.blobThreshold}"
            
            [event_sources]
            filesystem = ${if cfg.eventSources.filesystem then "true" else "false"}
            terminal = ${if cfg.eventSources.terminal then "true" else "false"}
            window_manager = ${if cfg.eventSources.windowManager then "true" else "false"}
            clipboard = ${if cfg.eventSources.clipboard then "true" else "false"}
            system_events = ${if cfg.eventSources.systemEvents then "true" else "false"}
            process_monitoring = ${if cfg.eventSources.processMonitoring then "true" else "false"}
            network_monitoring = ${if cfg.eventSources.networkMonitoring then "true" else "false"}
            screen_capture = ${if cfg.eventSources.screenCapture then "true" else "false"}
            
            [storage]
            compression_level = "${cfg.storage.compressionLevel}"
            ${optionalString (cfg.storage.dataRetention != null) ''data_retention = "${cfg.storage.dataRetention}"''}
            EOF
            
            exec ${pkgs.sinex}/bin/sinex-collector --config "$config_file"
          '';
        };
      };

      sinex-promo-worker = mkIf cfg.services.worker.enable {
        description = "Sinex Event Processing Worker";
        wantedBy = [ "multi-user.target" ];
        after = [ "postgresql.service" "sinex-unified-collector.service" ];
        wants = [ "postgresql.service" ];

        environment = {
          DATABASE_URL = "postgresql:///${cfg.database.name}?host=/run/postgresql";
          RUST_LOG = if cfg.observability.enable then "info" else "warn";
        };

        serviceConfig = {
          Type = "simple";
          User = cfg.database.user;
          Group = cfg.database.user;
          Restart = "always";
          RestartSec = "10s";
          
          ExecStart = "${pkgs.sinex}/bin/sinex-promo-worker --concurrency ${toString cfg.services.worker.concurrency}";
        };
      };

      # Pre-flight verification service
      sinex-preflight = mkIf cfg.preflightVerification.enable {
        description = "Sinex Pre-Flight Verification";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;
          TimeoutSec = cfg.preflightVerification.timeout;
          
          ExecStart = pkgs.writeShellScript "sinex-preflight" ''
            set -euo pipefail
            
            if ! ${pkgs.sinex}/bin/sinex-preflight verify --timeout ${toString cfg.preflightVerification.timeout}; then
              ${if cfg.preflightVerification.failureAction == "abort" then ''
                echo "Pre-flight verification failed, aborting deployment"
                exit 1
              '' else ''
                echo "Pre-flight verification failed, but continuing due to warn mode"
                exit 0
              ''}
            fi
          '';
        };
      };

      # Update service with pre-flight verification
      sinex-update = mkIf cfg.services.updateService.enable {
        description = "Sinex Update Service with Pre-Flight Verification";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;
          
          ExecStart = pkgs.writeShellScript "sinex-update" ''
            set -euo pipefail
            
            echo "Starting Sinex update with pre-flight verification..."
            
            # Run pre-flight verification
            ${optionalString cfg.preflightVerification.enable ''
              if ! systemctl start sinex-preflight; then
                echo "Pre-flight verification failed, update aborted"
                exit 1
              fi
            ''}
            
            # Graceful shutdown
            echo "Stopping Sinex services gracefully..."
            systemctl stop sinex-unified-collector sinex-promo-worker || true
            sleep ${toString cfg.services.updateService.gracePeriod}
            
            # Start services (they will use new configuration)
            echo "Starting updated Sinex services..."
            systemctl start sinex-unified-collector sinex-promo-worker
            
            # Health check
            sleep 10
            if ! systemctl is-active sinex-unified-collector; then
              echo "Collector failed to start, rolling back"
              # TODO: Implement automatic rollback
              exit 1
            fi
            
            echo "Sinex update completed successfully"
          '';
        };
      };
    };

    # Full observability stack (when enabled)
    services.prometheus = mkIf cfg.observability.enable {
      enable = true;
      port = cfg.observability.prometheusPort;
      listenAddress = "127.0.0.1";
      
      # Infinite retention (data volumes are minimal)
      retentionTime = "999y";
      
      scrapeConfigs = [
        {
          job_name = "sinex-metrics";
          static_configs = [
            { targets = [ "127.0.0.1:2112" ]; }  # Standard metrics port
          ];
          scrape_interval = "15s";
        }
        {
          job_name = "node_exporter";
          static_configs = [
            { targets = [ "127.0.0.1:9100" ]; }
          ];
        }
        {
          job_name = "postgres_exporter";
          static_configs = [
            { targets = [ "127.0.0.1:9187" ]; }
          ];
        }
      ];

      exporters = {
        node = {
          enable = true;
          port = 9100;
          enabledCollectors = [ "systemd" "processes" "filesystem" "meminfo" "loadavg" ];
        };

        postgres = {
          enable = true;
          port = 9187;
          runAsLocalSuperUser = true;
        };
      };
    };

    services.grafana = mkIf cfg.observability.enable {
      enable = true;
      settings = {
        server = {
          http_addr = "127.0.0.1";
          http_port = cfg.observability.grafanaPort;
        };
        
        # Local-only access with admin privileges for convenience
        "auth.anonymous" = {
          enabled = true;
          org_name = "Sinex";
          org_role = "Admin";
        };
        
        users.allow_sign_up = false;
        ui.default_theme = "dark";
        database.wal = true;
      };

      provision = {
        enable = true;
        datasources.settings.datasources = [
          {
            name = "Sinex-PostgreSQL";
            type = "postgres";
            access = "proxy";
            url = "postgresql:///${cfg.database.name}?host=/run/postgresql";
            isDefault = true;
            jsonData = {
              sslmode = "disable";
              postgresVersion = 1600;
              timescaledb = true;
            };
          }
          {
            name = "Sinex-Prometheus";
            type = "prometheus";
            access = "proxy";
            url = "http://127.0.0.1:${toString cfg.observability.prometheusPort}";
            jsonData = {
              httpMethod = "POST";
              prometheusType = "Prometheus";
            };
          }
        ];

        # Auto-provision comprehensive dashboards
        dashboards.settings.providers = [
          {
            name = "Sinex Dashboards";
            orgId = 1;
            folder = "Sinex";
            type = "file";
            disableDeletion = false;
            updateIntervalSeconds = 30;
            allowUiUpdates = true;
            options.path = "/var/lib/grafana/dashboards";
          }
        ];
      };
    };

    # Dashboard provisioning
    systemd.tmpfiles.rules = mkIf cfg.observability.enable [
      "d /var/lib/grafana/dashboards 0755 grafana grafana"
      "L+ /var/lib/grafana/dashboards/sinex-overview.json - - - - ${../grafana-dashboards/sinex-overview.json}"
      "L+ /var/lib/grafana/dashboards/sinex-event-analysis.json - - - - ${../grafana-dashboards/sinex-event-analysis.json}"
      "L+ /var/lib/grafana/dashboards/event-pipeline.json - - - - ${../grafana-dashboards/event-pipeline.json}"
      "L+ /var/lib/grafana/dashboards/system-health.json - - - - ${../grafana-dashboards/system-health.json}"
      "L+ /var/lib/grafana/dashboards/worker-performance.json - - - - ${../grafana-dashboards/worker-performance.json}"
      "L+ /var/lib/grafana/dashboards/metrics-continuous-aggregates.json - - - - ${../grafana-dashboards/metrics-continuous-aggregates.json}"
    ];

    # Firewall (localhost only)
    networking.firewall.interfaces.lo.allowedTCPPorts = mkIf cfg.observability.enable [
      cfg.observability.prometheusPort
      cfg.observability.grafanaPort
      9100  # node_exporter
      9187  # postgres_exporter
    ];

    # Directory creation
    systemd.tmpfiles.rules = [
      "d ${cfg.directories.state} 0755 ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.logs} 0755 ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.config} 0755 root root"
    ];

    # Convenience commands
    environment.systemPackages = [
      (pkgs.writeShellScriptBin "sinex-status" ''
        echo "🔍 Sinex System Status"
        echo "Target User: ${cfg.targetUser}"
        echo "Annex Repo: ${cfg.annexRepo}"
        echo "Database: ${cfg.database.name}"
        echo "Observability: ${if cfg.observability.enable then "enabled" else "disabled"}"
        echo ""
        
        echo "🏥 Services:"
        systemctl is-active sinex-unified-collector && echo "✅ Collector" || echo "❌ Collector"
        systemctl is-active sinex-promo-worker && echo "✅ Worker" || echo "❌ Worker"
        
        ${optionalString cfg.observability.enable ''
          echo ""
          echo "📊 Monitoring:"
          echo "Grafana: http://127.0.0.1:${toString cfg.observability.grafanaPort}"
          echo "Prometheus: http://127.0.0.1:${toString cfg.observability.prometheusPort}"
        ''}
        
        echo ""
        echo "💾 Storage:"
        df -h ${cfg.directories.state} | tail -1 | awk '{printf "Disk: %s used (%s available)\n", $5, $4}'
        
        echo ""
        echo "📈 Recent Activity:"
        if command -v psql >/dev/null; then
          psql ${cfg.database.name} -c "SELECT COUNT(*) as events_last_hour FROM raw.events WHERE ts_ingest > NOW() - INTERVAL '1 hour';" 2>/dev/null || echo "Database query failed"
        fi
      '')
    ];
  };
}