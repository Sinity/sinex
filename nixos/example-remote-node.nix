# Sinex remote node example
#
# Configures a node to run only node collectors and forward data to remote
# ingestd/NATS and PostgreSQL endpoints. Suitable for edge devices feeding a
# central Sinex cluster.

{
  config,
  lib,
  pkgs,
  ...
}:

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
      passwordFile = config.sinex.secrets.paths."sinex-remote-db";
    };

    nats.environment = "prod";

    core.enable = false; # ingestd/gateway run on central cluster
    lifecycle.maintenance.enable = false;
    observability.enable = false;

    nodes = {
      enable = true;
      coordination.enable = false;
      nats = {
        servers = [ "tls://core.example.net:4222" ];
        tls = {
          requireTls = true;
          caCertFile = config.sinex.secrets.paths."sinex-remote-nats-ca";
          clientCertFile = config.sinex.secrets.paths."sinex-remote-nats-cert";
          clientKeyFile = config.sinex.secrets.paths."sinex-remote-nats-key";
        };
      };
      defaults = {
        logLevel = "info";
        env = {
          # Enable True Edge Mode (no database dependency)
          # Checkpoints will use NATS KV.
          SINEX_EDGE_MODE = "1";
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

  environment.systemPackages = with pkgs; [
    sinexCli
    jq
  ];
}
