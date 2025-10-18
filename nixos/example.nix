
# Minimal Sinex configuration example
#
# This file demonstrates a practical starting point for a single-node deployment
# using the satellite architecture.  Adjust the sections marked REQUIRED for your
# environment and uncomment additional blocks as needed.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    targetUser = "myuser"; # REQUIRED: replace with the workstation user to monitor

    # Package selection (omit to use the module default).
    # package = pkgs.sinex;
    # cliPackage = pkgs.sinexCli;

    serviceManagement.serviceGroups = {
      core = true;        # ingestd + gateway + satellites
      maintenance = false;# enable when you need DLQ/git-annex timers
      monitoring = false; # enable the Prometheus/Grafana stack
    };

    database = {
      autoSetup = true;
      name = "sinex";
      user = "sinex";
      port = 5432;
    };

    satellite = {
      enable = true;
      coordination.enable = false; # enable when running hot-standby clusters
      database.url = "postgresql:///sinex?host=/run/postgresql";

      # Core ingest + API services
      coreServices.enable = true;

      # Event sources collected on this node
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
      enable = false;                # set true to expose Prometheus/Grafana locally
      listenAddress = "127.0.0.1";
      prometheusPort = 9002;
      grafanaPort = 9003;
    };
  };

  # Ensure the monitored user exists (mirrors the example targetUser above)
  users.users.myuser = {
    isNormalUser = true;
    extraGroups = [ "wheel" ];
  };

  # Persistent directories for satellites and logging
  systemd.tmpfiles.rules = [
    "d /var/lib/sinex 0755 sinex sinex -"
    "d /var/log/sinex 0755 sinex sinex -"
  ];
}
