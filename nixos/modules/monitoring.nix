{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  obs = cfg.observability;
  monitoringCfg = obs.monitoring;
  loggingCfg = obs.logging;
  alertsCfg = obs.alerts;

  logDir = obs.logDir;
  prometheusCfg = monitoringCfg.prometheus;

  enablePrometheus = cfg.enable && obs.enable && monitoringCfg.enable && prometheusCfg.enable;
  enableGrafana = cfg.enable && obs.enable && monitoringCfg.enable && monitoringCfg.grafana.enable;
  enableLogging = cfg.enable && obs.enable && loggingCfg.structured;

in
{
  config = mkMerge [
    (mkIf enableLogging {
      services.logrotate.settings."${logDir}/*.log" = {
        rotate = loggingCfg.retention.files;
        size = loggingCfg.retention.size;
        maxage = loggingCfg.retention.age;
        compress = true;
        delaycompress = true;
        missingok = true;
        notifempty = true;
        copytruncate = true;
      };
    })

    (mkIf enablePrometheus {
      services.prometheus =
        let
          # Build scrape configs for enabled exporters
          builtinScrapeConfigs =
            (optional monitoringCfg.exporters.node {
              job_name = "node";
              static_configs = [{ targets = [ "localhost:9100" ]; }];
            })
            ++ (optional monitoringCfg.exporters.postgres {
              job_name = "postgres";
              static_configs = [{ targets = [ "localhost:9187" ]; }];
            });
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
      services.grafana = {
        enable = true;
        settings.server.http_port = monitoringCfg.grafana.port;
      };
    })
  ];
}
