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
    users.target = "agent";

    database = {
      autoSetup = false;
      host = "db.example.net";
      port = 5432;
      name = "sinex";
      user = "sinex_agent";
    };

    core.enable = false; # ingestd/gateway run on central cluster
    lifecycle.maintenance.enable = false;
    observability.enable = false;

    satellites = {
      enable = true;
      coordination.enable = false;
      nats.servers = [ "tls://core.example.net:4222" ];
      defaults = {
        logLevel = "info";
        env = {
          SINEX_NATS_CA_CERT = "/etc/sinex/nats/ca.pem";
          SINEX_NATS_CLIENT_CERT = "/etc/sinex/nats/client.pem";
          SINEX_NATS_CLIENT_KEY = "/etc/sinex/nats/client.key";
        };
      };

      filesystem = {
        enable = true;
        instances = 1;
        watchPaths = [ "/var/lib/sinex/watch" ];
      };

      terminal = {
        enable = true;
        instances = 1;
      };

      desktop.enable = false;
      system.enable = false;

      automata = {
        enable = false;
        canonicalizer.enable = false;
        healthAggregator.enable = false;
      };
    };

    shell.kitty.enable = false;
  };

  # Disable local services that would conflict with remote endpoints.
  services.nats.enable = lib.mkForce false;
  services.postgresql.enable = lib.mkForce false;

  users.users.agent = {
    isNormalUser = true;
    createHome = true;
  };

  # Placeholder secret material — replace with your own deployment mechanism (e.g. agenix, sops-nix)
  environment.etc = {
    "sinex/nats/ca.pem" = {
      text = "# insert NATS CA certificate\n";
      mode = "0400";
    };
    "sinex/nats/client.pem" = {
      text = "# insert client certificate\n";
      mode = "0400";
    };
    "sinex/nats/client.key" = {
      text = "# insert client key\n";
      mode = "0400";
    };
  };

  environment.systemPackages = with pkgs; [ sinexCli jq ];
}
