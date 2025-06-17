# Monitoring and observability configuration module
{
  lib,
  config,
  pkgs,
  ...
}:

with lib;

let
  cfg = config.services.sinex;

  # Import health check utilities
  healthChecks = import ./health-checks.nix { inherit lib; };

in
{
  options.services.sinex.monitoring = {
    enable = mkOption {
      type = types.bool;
      default = true;
      description = "Enable monitoring and observability features";
    };

    # Prometheus metrics configuration
    prometheus = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable Prometheus metrics collection";
      };

      metricsPrefix = mkOption {
        type = types.str;
        default = "sinex";
        description = "Prefix for all Sinex metrics";
      };

      scrapeInterval = mkOption {
        type = types.str;
        default = "15s";
        description = "Default scrape interval for metrics";
      };

      # Centralized metrics collection
      centralCollector = {
        enable = mkOption {
          type = types.bool;
          default = false;
          description = "Enable centralized metrics collection service";
        };

        port = mkOption {
          type = types.port;
          default = 2114;
          description = "Port for centralized metrics collector";
        };

        endpoints = mkOption {
          type = types.listOf types.str;
          default = [
            "localhost:${toString cfg.unifiedCollector.metricsPort}/metrics"
            "localhost:${toString cfg.promoWorker.metricsPort}/metrics"
          ];
          description = "List of metrics endpoints to aggregate";
        };
      };
    };

    # Logging configuration
    logging = {
      structured = mkOption {
        type = types.bool;
        default = true;
        description = "Enable structured JSON logging";
      };

      level = mkOption {
        type = types.enum [
          "trace"
          "debug"
          "info"
          "warn"
          "error"
        ];
        default = "info";
        description = "Default log level for all components";
      };

      # Log retention
      retention = {
        maxFiles = mkOption {
          type = types.int;
          default = 10;
          description = "Maximum number of log files to retain";
        };

        maxSize = mkOption {
          type = types.str;
          default = "100M";
          description = "Maximum size per log file";
        };

        maxAge = mkOption {
          type = types.str;
          default = "30d";
          description = "Maximum age of log files";
        };
      };

      # Performance logging
      performance = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable performance-specific logging";
        };

        slowQueryThreshold = mkOption {
          type = types.int;
          default = 1000;
          description = "Log database queries slower than this (milliseconds)";
        };

        traceRequests = mkOption {
          type = types.bool;
          default = false;
          description = "Enable request tracing (verbose)";
        };
      };
    };

    # Alerting configuration
    alerting = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable alerting rules";
      };

      # Health-based alerts
      healthAlerts = {
        serviceDown = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Alert when services go down";
          };

          threshold = mkOption {
            type = types.str;
            default = "2m";
            description = "Time before alerting on service down";
          };
        };

        highErrorRate = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Alert on high error rates";
          };

          threshold = mkOption {
            type = types.float;
            default = 0.05;
            description = "Error rate threshold (0.05 = 5%)";
          };
        };

        databaseConnections = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Alert on database connection issues";
          };

          maxConnectionsPercent = mkOption {
            type = types.float;
            default = 0.8;
            description = "Alert when connections exceed this percentage of max";
          };
        };
      };

      # Resource alerts
      resourceAlerts = {
        highMemoryUsage = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Alert on high memory usage";
          };

          threshold = mkOption {
            type = types.float;
            default = 0.9;
            description = "Memory usage threshold (0.9 = 90%)";
          };
        };

        highCpuUsage = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Alert on high CPU usage";
          };

          threshold = mkOption {
            type = types.float;
            default = 0.8;
            description = "CPU usage threshold (0.8 = 80%)";
          };
        };

        diskSpaceUsage = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Alert on high disk usage";
          };

          threshold = mkOption {
            type = types.float;
            default = 0.85;
            description = "Disk usage threshold (0.85 = 85%)";
          };
        };
      };
    };

    # Dashboards and visualization
    dashboards = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable pre-built monitoring dashboards";
      };

      grafana = {
        enable = mkOption {
          type = types.bool;
          default = false;
          description = "Enable Grafana dashboard provisioning";
        };

        datasourceUrl = mkOption {
          type = types.str;
          default = "http://localhost:9090";
          description = "Prometheus datasource URL for Grafana";
        };
      };
    };

    # Full observability stack (Prometheus + Grafana)
    observabilityStack = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable complete observability stack (Prometheus + Grafana + exporters)";
      };

      prometheusPort = mkOption {
        type = types.port;
        default = 9090;
        description = "Prometheus server port";
      };

      grafanaPort = mkOption {
        type = types.port;
        default = 3000;
        description = "Grafana server port";
      };

      retentionTime = mkOption {
        type = types.str;
        default = "30d";
        description = "Prometheus data retention time";
      };

      listenAddress = mkOption {
        type = types.str;
        default = "127.0.0.1";
        description = "Listen address for monitoring services (localhost only by default)";
      };
    };
  };

  config = mkIf (cfg.enable && cfg.monitoring.enable) {
    # Centralized metrics collector service
    systemd.services.sinex-metrics-collector = mkIf cfg.monitoring.prometheus.centralCollector.enable {
      description = "Sinex Centralized Metrics Collector";
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" ];

      serviceConfig = {
        Type = "simple";
        User = cfg.database.user;
        Group = cfg.database.user;
        Restart = "always";
        RestartSec = "10s";

        # Resource limits
        MemoryMax = "256M";
        CPUQuota = "50%";

        ExecStart = pkgs.writeShellScript "sinex-metrics-collector" ''
          set -euo pipefail

          # Simple metrics aggregation service
          ${pkgs.python3}/bin/python3 -c '
          import http.server
          import socketserver
          import urllib.request
          import json
          from urllib.parse import urlparse

          PORT = ${toString cfg.monitoring.prometheus.centralCollector.port}
          ENDPOINTS = [${
            concatMapStringsSep ", " (ep: "\"${ep}\"") cfg.monitoring.prometheus.centralCollector.endpoints
          }]

          class MetricsHandler(http.server.SimpleHTTPRequestHandler):
              def do_GET(self):
                  if self.path == "/metrics":
                      self.send_response(200)
                      self.send_header("Content-type", "text/plain")
                      self.end_headers()
                      
                      # Aggregate metrics from all endpoints
                      for endpoint in ENDPOINTS:
                          try:
                              with urllib.request.urlopen("http://" + endpoint) as response:
                                  self.wfile.write(response.read())
                                  self.wfile.write(b"\n")
                          except Exception as e:
                              print("Error fetching metrics from " + endpoint + ": " + str(e))
                  else:
                      self.send_response(404)
                      self.end_headers()

          with socketserver.TCPServer(("", PORT), MetricsHandler) as httpd:
              print("Serving metrics aggregation on port " + str(PORT))
              httpd.serve_forever()
          '
        '';

        Environment = [
          "PYTHONUNBUFFERED=1"
        ];
      };
    };

    # Log rotation configuration
    services.logrotate.settings = mkIf cfg.monitoring.logging.structured {
      "${cfg.directories.logs}/*.log" = {
        rotate = cfg.monitoring.logging.retention.maxFiles;
        size = cfg.monitoring.logging.retention.maxSize;
        maxage = cfg.monitoring.logging.retention.maxAge;
        compress = true;
        delaycompress = true;
        missingok = true;
        notifempty = true;
        copytruncate = true;
        su = "${cfg.database.user} ${cfg.database.user}";
      };
    };

    # System monitoring timers
    systemd.timers = {
      sinex-system-health = {
        description = "Sinex System Health Check";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnCalendar = "*:0/5"; # Every 5 minutes
          Persistent = true;
        };
      };

      sinex-resource-monitor = {
        description = "Sinex Resource Usage Monitor";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnCalendar = "minutely";
          Persistent = true;
        };
      };
    };

    systemd.services = {
      sinex-system-health = {
        description = "Sinex System Health Check";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;

          ExecStart = pkgs.writeShellScript "sinex-system-health" ''
            set -euo pipefail

            echo "$(date): Performing system health check..."

            # Check service status
            systemctl is-active sinex-unified-collector || echo "WARNING: Unified collector not running"
            ${optionalString cfg.promoWorker.enable "systemctl is-active sinex-promo-worker || echo \"WARNING: Promo worker not running\""}

            # Check database connectivity
            ${pkgs.postgresql}/bin/psql "${cfg.database.name}" -c "SELECT 1;" > /dev/null || echo "ERROR: Database connectivity failed"

            # Check disk space
            DISK_USAGE=$(df ${cfg.directories.state} | tail -1 | awk '{print $5}' | sed 's/%//')
            if [ "$DISK_USAGE" -gt 85 ]; then
                echo "WARNING: Disk usage at $DISK_USAGE% for ${cfg.directories.state}"
            fi

            # Check memory usage
            MEMORY_USAGE=$(free | grep Mem | awk '{printf "%.0f", $3/$2 * 100}')
            if [ "$MEMORY_USAGE" -gt 90 ]; then
                echo "WARNING: Memory usage at $MEMORY_USAGE%"
            fi

            echo "$(date): System health check completed"
          '';
        };
      };

      sinex-resource-monitor = {
        description = "Sinex Resource Usage Monitor";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;

          ExecStart = pkgs.writeShellScript "sinex-resource-monitor" ''
            set -euo pipefail

            LOG_FILE="${cfg.directories.logs}/resource-usage.log"
            TIMESTAMP=$(date -Iseconds)

            # Memory usage
            MEMORY_TOTAL=$(free -b | grep Mem | awk '{print $2}')
            MEMORY_USED=$(free -b | grep Mem | awk '{print $3}')
            MEMORY_PERCENT=$(echo "scale=2; $MEMORY_USED * 100 / $MEMORY_TOTAL" | bc -l)

            # CPU usage (1-minute load average)
            LOAD_AVG=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | tr -d ',')

            # Disk usage
            DISK_USAGE=$(df ${cfg.directories.state} | tail -1 | awk '{print $5}' | sed 's/%//')

            # Database connections
            DB_CONNECTIONS=$(${pkgs.postgresql}/bin/psql -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname = '${cfg.database.name}';" 2>/dev/null || echo "0")

            # Log metrics in structured format
            ${optionalString cfg.monitoring.logging.structured ''
              echo "{\"timestamp\":\"$TIMESTAMP\",\"memory_percent\":$MEMORY_PERCENT,\"load_avg\":$LOAD_AVG,\"disk_usage\":$DISK_USAGE,\"db_connections\":$DB_CONNECTIONS}" >> "$LOG_FILE"
            ''} 

            ${optionalString (!cfg.monitoring.logging.structured) ''
              echo "$TIMESTAMP memory=$MEMORY_PERCENT% load=$LOAD_AVG disk=$DISK_USAGE% db_connections=$DB_CONNECTIONS" >> "$LOG_FILE"
            ''}
          '';
        };
      };
    };

    # Full observability stack configuration
    services.prometheus = mkIf cfg.monitoring.observabilityStack.enable {
      enable = true;
      listenAddress = cfg.monitoring.observabilityStack.listenAddress;
      port = cfg.monitoring.observabilityStack.prometheusPort;
      retentionTime = cfg.monitoring.observabilityStack.retentionTime;

      scrapeConfigs = [
        {
          job_name = "prometheus";
          static_configs = [
            {
              targets = [
                "${cfg.monitoring.observabilityStack.listenAddress}:${toString cfg.monitoring.observabilityStack.prometheusPort}"
              ];
            }
          ];
        }
        {
          job_name = "node_exporter";
          static_configs = [ { targets = [ "${cfg.monitoring.observabilityStack.listenAddress}:9100" ]; } ];
        }
        {
          job_name = "postgres_exporter";
          static_configs = [ { targets = [ "${cfg.monitoring.observabilityStack.listenAddress}:9187" ]; } ];
        }
        # Sinex services
        {
          job_name = "sinex_unified_collector";
          metrics_path = "/metrics";
          static_configs = [
            {
              targets = [
                "${cfg.monitoring.observabilityStack.listenAddress}:${toString cfg.unifiedCollector.metricsPort}"
              ];
            }
          ];
          scrape_interval = "15s";
        }
        {
          job_name = "sinex_promo_worker";
          metrics_path = "/metrics";
          static_configs = [
            {
              targets = [
                "${cfg.monitoring.observabilityStack.listenAddress}:${toString cfg.promoWorker.metricsPort}"
              ];
            }
          ];
          scrape_interval = "15s";
        }
      ];

      exporters = {
        node = {
          enable = true;
          listenAddress = cfg.monitoring.observabilityStack.listenAddress;
          port = 9100;
          enabledCollectors = [
            "systemd"
            "processes"
            "filesystem"
          ];
        };

        postgres = {
          enable = true;
          listenAddress = cfg.monitoring.observabilityStack.listenAddress;
          port = 9187;
          runAsLocalSuperUser = true;
        };
      };
    };

    # Grafana configuration
    services.grafana =
      mkIf (cfg.monitoring.observabilityStack.enable && cfg.monitoring.dashboards.grafana.enable)
        {
          enable = true;
          settings = {
            server = {
              http_addr = cfg.monitoring.observabilityStack.listenAddress;
              http_port = cfg.monitoring.observabilityStack.grafanaPort;
              domain = "localhost";
            };
            "auth.anonymous" = {
              enabled = true;
              org_name = "Sinex Exocortex";
              org_role = "Admin";
            };
            users = {
              allow_sign_up = false;
              auto_assign_org = true;
              auto_assign_org_role = "Viewer";
            };
          };

          provision = {
            enable = true;
            datasources.settings.datasources = [
              {
                name = "Prometheus-Sinex";
                type = "prometheus";
                access = "proxy";
                url = "http://${cfg.monitoring.observabilityStack.listenAddress}:${toString cfg.monitoring.observabilityStack.prometheusPort}";
                isDefault = true;
                jsonData = {
                  httpMethod = "POST";
                  prometheusType = "Prometheus";
                  prometheusVersion = "2.40.0";
                };
              }
            ];

            dashboards.settings.providers = [
              {
                name = "Sinex Dashboards";
                orgId = 1;
                folder = "Sinex";
                type = "file";
                disableDeletion = false;
                updateIntervalSeconds = 10;
                allowUiUpdates = true;
                options.path = "/var/lib/grafana/dashboards";
              }
            ];
          };
        };

    # Monitoring convenience scripts for user
    environment.systemPackages = mkIf cfg.monitoring.observabilityStack.enable (
      with pkgs;
      [
        bc # for sinex-resource-monitor

        prometheus
        grafana
        (writeShellScriptBin "sinex-metrics" ''
          echo "🔍 Sinex Observability Stack"
          echo "Prometheus: http://${cfg.monitoring.observabilityStack.listenAddress}:${toString cfg.monitoring.observabilityStack.prometheusPort}"
          echo "Grafana: http://${cfg.monitoring.observabilityStack.listenAddress}:${toString cfg.monitoring.observabilityStack.grafanaPort}"
          echo ""
          echo "📊 Current Metrics Status:"
          ${curl}/bin/curl -s http://${cfg.monitoring.observabilityStack.listenAddress}:${toString cfg.monitoring.observabilityStack.prometheusPort}/api/v1/targets | \
            ${jq}/bin/jq -r '.data.activeTargets[] | "\(.scrapePool): \(.health)"' 2>/dev/null || echo "Prometheus not accessible"
        '')
        (writeShellScriptBin "sinex-logs" ''
          echo "📋 Sinex Service Logs"
          echo "Press Ctrl+C to exit, or choose a specific service:"
          echo "1) Unified Collector"
          echo "2) Promo Worker" 
          echo "3) Prometheus"
          echo "4) Grafana"
          echo "5) All services"
          read -p "Choice (1-5): " choice

          case $choice in
            1) ${systemd}/bin/journalctl -u sinex-unified-collector -f ;;
            2) ${systemd}/bin/journalctl -u sinex-promo-worker -f ;;
            3) ${systemd}/bin/journalctl -u prometheus -f ;;
            4) ${systemd}/bin/journalctl -u grafana -f ;;
            5) ${systemd}/bin/journalctl -u prometheus -u grafana -u sinex-unified-collector -u sinex-promo-worker -f ;;
            *) echo "Invalid choice" ;;
          esac
        '')
      ]
    );

    # Grafana dashboard setup
    systemd.tmpfiles.rules =
      mkIf (cfg.monitoring.observabilityStack.enable && cfg.monitoring.dashboards.grafana.enable)
        [
          "d /var/lib/grafana/dashboards 0755 grafana grafana"
          "L+ /var/lib/grafana/dashboards/sinex-dashboard.json - - - - ${./sinex-dashboard.json}"
        ];

    # Firewall for monitoring services (localhost only)
    networking.firewall.interfaces.lo.allowedTCPPorts = mkIf cfg.monitoring.observabilityStack.enable [
      cfg.monitoring.observabilityStack.prometheusPort
      cfg.monitoring.observabilityStack.grafanaPort
      9100 # node_exporter
      9187 # postgres_exporter
    ];

    # Assertions for dependencies
    assertions = mkIf cfg.monitoring.observabilityStack.enable [
      {
        assertion = cfg.database.autoSetup || config.services.postgresql.enable;
        message = "PostgreSQL must be enabled for postgres_exporter to work with Sinex observability stack";
      }
    ];
  };
}

