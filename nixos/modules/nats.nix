{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex.nats;
  stateRoot = config.services.sinex.stateRoot;
  natsUser = "nats";
  dataDir = cfg.dataDir or (stateRoot + "/nats");
  storeDir = cfg.storeDir or (dataDir + "/jetstream");
in
{
  options.services.sinex.nats = with types; {
    enable = mkEnableOption "Manage a local NATS server with JetStream for Sinex";

    autoSetup = mkOption {
      type = bool;
      default = false;
      description = "Automatically provision NATS/JetStream alongside Sinex.";
    };

    package = mkOption {
      type = package;
      default = pkgs.nats-server;
      defaultText = literalExpression "pkgs.nats-server";
      description = "NATS server package to deploy.";
    };

    host = mkOption {
      type = str;
      default = "0.0.0.0";
      description = "Listen address for NATS.";
    };

    port = mkOption {
      type = port;
      default = 4222;
      description = "NATS client port.";
    };

    monitoringPort = mkOption {
      type = port;
      default = 8222;
      description = "NATS monitoring/HTTP port.";
    };

    dataDir = mkOption {
      type = path;
      default = stateRoot + "/nats";
      defaultText = literalExpression "config.services.sinex.stateRoot + \"/nats\"";
      description = "Base data directory for NATS (accounts, leafnodes, JetStream).";
    };

    storeDir = mkOption {
      type = path;
      default = stateRoot + "/nats/jetstream";
      defaultText = literalExpression "config.services.sinex.stateRoot + \"/nats/jetstream\"";
      description = "JetStream storage directory.";
    };

    jetstreamMaxMemory = mkOption {
      type = nullOr str;
      default = null;
      description = "Optional JetStream memory cap (e.g., \"1GB\").";
    };

    jetstreamMaxStore = mkOption {
      type = nullOr str;
      default = null;
      description = "Optional JetStream file store cap (e.g., \"20GB\").";
    };

    extraSettings = mkOption {
      type = attrsOf (oneOf [ int str bool ]);
      default = {};
      description = "Additional raw NATS settings merged into the generated config.";
    };
  };

  config = mkIf (cfg.enable || cfg.autoSetup) {
    assertions = [{
      assertion = cfg.package != null;
      message = "services.sinex.nats.package must be set when enabling NATS management.";
    }];

    users.groups.${natsUser} = { };
    users.users.${natsUser} = {
      isSystemUser = true;
      group = natsUser;
      description = "NATS/JetStream service account";
      home = dataDir;
      createHome = true;
    };

    systemd.tmpfiles.rules = mkAfter [
      "d ${dataDir} 0755 ${natsUser} ${natsUser} -"
      "d ${storeDir} 0755 ${natsUser} ${natsUser} -"
    ];

    services.nats = {
      enable = true;
      package = cfg.package;
      user = natsUser;
      group = natsUser;
      settings = {
        server_name = "sinex";
        host = cfg.host;
        port = cfg.port;
        http = cfg.monitoringPort;
        jetstream = {
          store_dir = storeDir;
        } // optionalAttrs (cfg.jetstreamMaxMemory != null) { max_mem = cfg.jetstreamMaxMemory; }
          // optionalAttrs (cfg.jetstreamMaxStore != null) { max_file = cfg.jetstreamMaxStore; };
      } // cfg.extraSettings;
    };
  };
}
