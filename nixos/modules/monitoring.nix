{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  secretResolution = import ./lib/secret-resolution.nix { inherit lib; };
  inherit (secretResolution) resolveNamedSecretPath;
  obs = cfg.observability;
  monitoringCfg = obs.monitoring;
  loggingCfg = obs.logging;
  alertsCfg = obs.alerts;
  secretPaths = config.sinex.secrets.paths or {};

  logDir = obs.logDir;
  prometheusCfg = monitoringCfg.prometheus;
  grafanaDashboardsPath = ../monitoring/grafana-dashboards;
  grafanaPrometheusUid = "sinex-prometheus";
  grafanaPostgresUid = "sinex-postgres";
  grafanaPostgresHost =
    if cfg.database.host == "0.0.0.0" then
      "127.0.0.1"
    else if cfg.database.host == "::" then
      "::1"
    else
      cfg.database.host;
  # Wrap bare IPv6 addresses in brackets for URL use (e.g. ::1 → [::1]).
  # Grafana datasource URLs must be http://[::1]:port, not http://::1:port.
  grafanaPostgresHostUrl =
    if builtins.match ".*:.*" grafanaPostgresHost != null then
      "[${grafanaPostgresHost}]"
    else
      grafanaPostgresHost;
  grafanaPostgresSslMode =
    if builtins.elem grafanaPostgresHost [
      "127.0.0.1"
      "localhost"
      "::1"
    ] then
      "disable"
    else
      "require";
  effectiveDatabasePasswordFile = resolveNamedSecretPath secretPaths cfg.database.passwordFile [
    "sinex-local-db"
    "sinex-remote-db"
  ];
  grafanaPasswordRef =
    if effectiveDatabasePasswordFile != null then
      "$__file{${toString effectiveDatabasePasswordFile}}"
    else
      null;
  effectiveGrafanaSecretKeyFile = resolveNamedSecretPath secretPaths monitoringCfg.grafana.secretKeyFile [
    "sinex-grafana-secret-key"
    "grafana-secret-key"
  ];
  derivedGrafanaSecretKey =
    let
      fingerprint = builtins.hashString "sha256" (
        concatStringsSep ":" [
          (config.networking.hostName or "localhost")
          cfg.nats.environment
          cfg.database.name
          (toString cfg.stateRoot)
          cfg.users.nodes
        ]
      );
    in
    "sinex-grafana-${builtins.substring 0 48 fingerprint}";
  grafanaSecretKeySetting =
    if effectiveGrafanaSecretKeyFile != null then
      "$__file{${toString effectiveGrafanaSecretKeyFile}}"
    else if monitoringCfg.grafana.secretKey != null then
      monitoringCfg.grafana.secretKey
    else
      derivedGrafanaSecretKey;

  enablePrometheus = cfg.enable && obs.enable && monitoringCfg.enable && prometheusCfg.enable;
  enableGrafana = cfg.enable && obs.enable && monitoringCfg.enable && monitoringCfg.grafana.enable;
  enableLogging = cfg.enable && obs.enable && loggingCfg.structured;

  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;
  natsExporterPkg = pkgs.prometheus-nats-exporter or null;
  # NATS monitoring port serves a JSON API, not Prometheus format.
  # prometheus-nats-exporter bridges the gap: it scrapes the NATS monitoring
  # HTTP API and re-exposes the data in Prometheus format on port 7777.
  enableNatsExporter = natsEnabled && enablePrometheus && monitoringCfg.exporters.nats && natsExporterPkg != null;
  natsExporterPort = 7777;
  natsMonitoringUrl = "http://${cfg.nats.monitoringHost}:${toString cfg.nats.monitoringPort}";

in
{
  config = mkMerge [
    # Sinex services log to stderr; systemd captures that into the journal.
    # Log retention is therefore managed by journald, not logrotate.
    # The logDir path is kept for compatibility (other tooling may write there),
    # but log rotation is wired to journald SystemMaxUse / SystemMaxFiles / MaxRetentionSec.
    (mkIf enableLogging {
      services.journald.extraConfig = lib.mkDefault ''
        SystemMaxUse=${loggingCfg.retention.size}
        SystemMaxFiles=${toString loggingCfg.retention.files}
        MaxRetentionSec=${loggingCfg.retention.age}
      '';
    })

    (mkIf enableNatsExporter {
      systemd.services.sinex-nats-prometheus-exporter = {
        description = "NATS Prometheus exporter for Sinex";
        wantedBy = [ "multi-user.target" ];
        after = [ "nats.service" ];
        wants = [ "nats.service" ];
        serviceConfig = {
          Type = "simple";
          ExecStart = "${natsExporterPkg}/bin/prometheus-nats-exporter"
            + " -port ${toString natsExporterPort}"
            + " -varz"
            + " -jsz all"
            + " -connz"
            + " -routez"
            + " ${natsMonitoringUrl}";
          Restart = "on-failure";
          RestartSec = 5;
          DynamicUser = true;
          NoNewPrivileges = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          PrivateTmp = true;
        };
      };
    })

    (mkIf enablePrometheus {
      services.prometheus =
        let
          builtinScrapeConfigs =
            (optional monitoringCfg.exporters.node {
              job_name = "node";
              static_configs = [{ targets = [ "localhost:9100" ]; }];
            })
            ++ (optional monitoringCfg.exporters.postgres {
              job_name = "postgres";
              static_configs = [{ targets = [ "localhost:9187" ]; }];
            })
            # NATS metrics are served by prometheus-nats-exporter (when enabled),
            # which translates the NATS JSON monitoring API to Prometheus format.
            # Direct scraping of the NATS monitoring port (8222) is intentionally
            # omitted: that port serves JSON, not Prometheus metrics.
            ++ (optional enableNatsExporter {
              job_name = "nats";
              static_configs = [{ targets = [ "localhost:${toString natsExporterPort}" ]; }];
            });
          # Note: sinex-gateway does not expose a Prometheus /metrics endpoint.
          # Gateway metrics are emitted as Sinex events via NATS self-observation
          # and stored in core.events. Query them via sinexctl or the analytics API.
        in
        {
          enable = true;
          listenAddress = prometheusCfg.listen;
          port = prometheusCfg.port;
          retentionTime = prometheusCfg.retention;
          ruleFiles = if alertsCfg.enable then alertsCfg.rulesFiles else [];
          exporters = {
            node.enable = monitoringCfg.exporters.node;
            postgres.enable = monitoringCfg.exporters.postgres;
          };
          scrapeConfigs = builtinScrapeConfigs ++ prometheusCfg.extraScrapeConfigs;
        };
    })

    (mkIf enableGrafana {
      services.grafana.provision = {
        enable = true;
        datasources.settings.datasources =
          (optional enablePrometheus {
            name = "Prometheus";
            type = "prometheus";
            uid = grafanaPrometheusUid;
            url = "http://${prometheusCfg.listen}:${toString prometheusCfg.port}";
            isDefault = false;
            access = "proxy";
            editable = false;
          })
          ++ [
            ({
              name = "Sinex PostgreSQL";
              type = "postgres";
              uid = grafanaPostgresUid;
              url = "${grafanaPostgresHostUrl}:${toString cfg.database.port}";
              database = cfg.database.name;
              user = cfg.database.user;
              isDefault = true;
              access = "proxy";
              editable = false;
              jsonData = {
                sslmode = grafanaPostgresSslMode;
                timescaledb = true;
              };
            }
            // optionalAttrs (grafanaPasswordRef != null) {
              secureJsonData = {
                password = grafanaPasswordRef;
              };
            })
          ];
        dashboards.settings.providers = [
          {
            name = "sinex";
            type = "file";
            folder = "Sinex";
            disableDeletion = false;
            allowUiUpdates = false;
            updateIntervalSeconds = 30;
            options.path = grafanaDashboardsPath;
          }
        ];
      };

      services.grafana = {
        enable = true;
        settings = {
          server = {
            http_addr = mkDefault "127.0.0.1";
            http_port = monitoringCfg.grafana.port;
          };
          security.secret_key = grafanaSecretKeySetting;
        };
      };
    })
  ];
}
