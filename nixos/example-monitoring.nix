
# Sinex observability example
#
# Enables the monitoring stack (Prometheus/Grafana) and maintenance timers
# alongside the satellite deployment. Suitable for staging environments where
# insight into resource usage and DLQ behaviour is required.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    targetUser = "observer";

    serviceManagement.serviceGroups = {
      core = true;
      maintenance = true;  # keep DLQ cleanup and git-annex maintenance active
      monitoring = true;   # expose observability stack on loopback
    };

    database = {
      autoSetup = true;
      name = "sinex_obs";
      user = "sinex";
    };

    satellite = {
      enable = true;
      coordination.enable = false;
      database.url = "postgresql:///sinex_obs?host=/run/postgresql";

      coreServices.enable = true;

      eventSources = {
        filesystem = {
          enable = true;
          watchPaths = [ "/home/observer" ];
        };
        terminal.enable = true;
        desktop.enable = true;
        system.enable = true;
      };

      automata = {
        canonicalCommandSynthesizer.enable = true;
        healthAggregator.enable = true;
      };
    };

    shell = {
      asciinema.autoRecord = false;
      kitty.enable = true;
    };

    monitoring.observabilityStack = {
      enable = true;
      listenAddress = "127.0.0.1";
      prometheusPort = 9090;
      grafanaPort = 3000;
      retentionTime = "7d";
    };

    monitoring.dashboards.grafana.enable = true;
  };

  users.users.observer = {
    isNormalUser = true;
    extraGroups = [ "wheel" ];
  };

  networking.firewall.interfaces.lo.allowedTCPPorts = [ 9090 3000 ];

  systemd.tmpfiles.rules = [
    "d /var/lib/sinex 0755 sinex sinex -"
    "d /var/log/sinex 0755 sinex sinex -"
  ];
}
