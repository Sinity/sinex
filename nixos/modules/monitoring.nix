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
        }
        // optionalAttrs (prometheusCfg.extraScrapeConfigs != []) {
          scrapeConfigs = prometheusCfg.extraScrapeConfigs;
        };
    })

    (mkIf enableGrafana {
      services.grafana = {
        enable = true;
        inherit (monitoringCfg.grafana) port;
      };
    })
  ];
}
