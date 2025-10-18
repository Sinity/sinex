
# Minimal Sinex configuration example
#
# Defines a single-node deployment with the satellite architecture and
# filesystem/terminal capture enabled. Update the REQUIRED fields for your host.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    targetUser = "myuser"; # REQUIRED: replace with the user to observe

    # Optional: select packages explicitly (module defaults work out of the box)
    # package = pkgs.sinex;
    # cliPackage = pkgs.sinexCli;

    serviceManagement.serviceGroups = {
      core = true;
      maintenance = false; # enable when using DLQ/git-annex timers
      monitoring = false;  # enable Prometheus/Grafana stack locally
    };

    database = {
      autoSetup = true;
      name = "sinex";
      user = "sinex";
      listenAddress = "127.0.0.1";
      port = 5432;
    };

    satellite = {
      enable = true;
      coordination.enable = false;
      database.url = "postgresql:///sinex?host=/run/postgresql";
      logLevel = "info";

      coreServices.enable = true;

      eventSources = {
        filesystem = {
          enable = true;
          instances = 1;
        };
        terminal = {
          enable = true;
          instances = 1;
        };
        desktop.enable = false;
        system.enable = false;
      };

      automata = {
        canonicalCommandSynthesizer.enable = true;
        healthAggregator.enable = true;
      };
    };

    monitoring.observabilityStack = {
      enable = false;
      listenAddress = "127.0.0.1";
      prometheusPort = 9002;
      grafanaPort = 9003;
    };
  };

  # Ensure the monitored user exists (adjust to match targetUser above)
  users.users.myuser = {
    isNormalUser = true;
    createHome = true;
    extraGroups = [ "wheel" ];
  };

  systemd.tmpfiles.rules = [
    "d /var/lib/sinex 0755 sinex sinex -"
    "d /var/log/sinex 0755 sinex sinex -"
  ];
}
