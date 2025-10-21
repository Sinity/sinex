# Sinex headless server example
#
# Intended for machines without desktop/terminal capture requirements.
# Enables filesystem and system satellites along with maintenance timers.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    targetUser = "serveruser";

    serviceManagement.serviceGroups = {
      core = true;
      maintenance = true;
      monitoring = false;
    };

    database = {
      autoSetup = true;
      name = "sinex_server";
      user = "sinex";
      listenAddress = "127.0.0.1";
    };

    satellite = {
      enable = true;
      coordination.enable = false;
      database.url = "postgresql:///sinex_server?host=/run/postgresql";
      logLevel = "info";

      coreServices.enable = true;

      eventSources = {
        filesystem = {
          enable = true;
          instances = 1;
          memoryLimit = "256M";
          watchPaths = [ "/var/lib/sinex/sources" "/srv/data" ];
        };
        system = {
          enable = true;
          instances = 1;
          memoryLimit = "384M";
        };
        terminal.enable = false;
        desktop.enable = false;
      };

      automata = {
        canonicalCommandSynthesizer.enable = true;
        healthAggregator.enable = true;
      };
    };

    shell.kitty.enable = false;
  };

  users.users.serveruser = {
    isNormalUser = true;
    createHome = true;
    extraGroups = [ "wheel" ];
  };

  systemd.tmpfiles.rules = [
    "d /var/lib/sinex 0755 sinex sinex -"
    "d /var/log/sinex 0755 sinex sinex -"
  ];
}
