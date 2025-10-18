# Sinex remote satellite example
#
# Configures a node to run only satellite collectors and forward data to remote
# ingestd/NATS and PostgreSQL endpoints. Suitable for edge devices feeding a
# central Sinex cluster.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    targetUser = "agent";

    serviceManagement.serviceGroups = {
      core = true;
      maintenance = false;
      monitoring = false;
    };

    database = {
      autoSetup = false;
      host = "db.example.net";
      port = 5432;
      name = "sinex";
      user = "sinex_agent";
    };

    satellite = {
      enable = true;
      coordination.enable = false;
      coreServices.enable = false; # ingestd/gateway run on central cluster
      database.url = "postgresql://sinex_agent@db.example.net:5432/sinex";
      nats.servers = "nats://core.example.net:4222";
      logLevel = "info";

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
        canonicalCommandSynthesizer.enable = false;
        healthAggregator.enable = false;
      };
    };
  };

  # Disable local services that would conflict with remote endpoints.
  services.nats.enable = lib.mkForce false;
  services.postgresql.enable = lib.mkForce false;

  users.users.agent = {
    isNormalUser = true;
    createHome = true;
  };

  environment.systemPackages = with pkgs; [ sinexCli jq ];
}
