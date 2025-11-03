{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  coreCfg = cfg.core;
  satellitesCfg = cfg.satellites;

  sinexEnabled = cfg.enable;
  coreEnabled = sinexEnabled && coreCfg.enable;
  satellitesEnabled = sinexEnabled && satellitesCfg.enable;

  stateRoot = cfg.stateRoot;
  runtimeDir = "${stateRoot}/run";
  ingestSpool = coreCfg.ingestd.spoolDir;
  satelliteSpool = "${stateRoot}/spool/satellites";
  logDir = cfg.observability.logDir;
  dlqPath = cfg.storage.dlq.path;

  sinexPackage = cfg.package;
  serviceUser = cfg.users.satellites;

  databaseUrl = "postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}";

  natsUrl = concatStringsSep "," satellitesCfg.nats.servers;

  toEnvList = envAttrs: mapAttrsToList (name: value: "${name}=${value}") envAttrs;

  baseEnv = [
    "DATABASE_URL=${databaseUrl}"
    "SINEX_STATE_DIR=${stateRoot}"
    "SINEX_RUNTIME_DIR=${runtimeDir}"
    "SINEX_LOG_DIR=${logDir}"
    "SINEX_SPOOL_INGESTD=${ingestSpool}"
    "SINEX_SPOOL_SATELLITES=${satelliteSpool}"
    "SINEX_DLQ_PATH=${dlqPath}"
    "SINEX_NATS_SERVERS=${natsUrl}"
    "SINEX_NATS_MONITORING_PORT=${toString satellitesCfg.nats.monitoringPort}"
  ] ++ toEnvList satellitesCfg.defaults.env;

  coordinationEnv =
    if satellitesCfg.coordination.enable then [
      "SINEX_COORDINATION_ENABLED=1"
      "SINEX_COORDINATION_HEARTBEAT=${toString satellitesCfg.coordination.heartbeatSec}"
      "SINEX_COORDINATION_TIMEOUT=${toString satellitesCfg.coordination.leadershipTimeoutSec}"
      "SINEX_COORDINATION_HANDOFF=${toString satellitesCfg.coordination.handoffTimeoutSec}"
    ] else [];

  resolveBatch = satelliteBatch:
    if satelliteBatch == null then satellitesCfg.defaults.batch else satelliteBatch;

  resolveResources = satelliteResources:
    if satelliteResources == null then satellitesCfg.defaults.resources else satelliteResources;

  resolveInstances = satelliteInstances:
    if satelliteInstances == null then satellitesCfg.defaults.instances else satelliteInstances;

  renderResources = resources: {
    MemoryMax = resources.memoryMax;
    CPUQuota = resources.cpuQuota;
  };

  mkServiceEnv = additionalEnv: baseEnv ++ coordinationEnv ++ additionalEnv;

  mkBaseServiceConfig = resources: env: extra:
    {
      Type = "notify";
      User = serviceUser;
      Group = serviceUser;
      Restart = "on-failure";
      RestartSec = 10;
      Environment = env;
    }
    // renderResources resources
    // extra;

  mkCoreServices =
    let
      batch = coreCfg.ingestd.batch;
      ingestArgs = concatStringsSep " " ([
        "--nats-url ${natsUrl}"
        "--batch-size ${toString batch.size}"
        "--batch-timeout ${toString batch.timeoutSec}"
        "--log-level ${coreCfg.ingestd.logLevel}"
      ] ++ coreCfg.ingestd.extraArgs);
      gatewayArgs = concatStringsSep " " ([
        "rpc-server"
        "--database-url ${databaseUrl}"
        "--log-level ${coreCfg.gateway.logLevel}"
      ] ++ coreCfg.gateway.extraArgs);
      commonAfter = [ "postgresql.service" ];
    in
    if !coreEnabled then {} else {
      "sinex-ingestd" = {
        description = "Sinex ingestion daemon";
        wantedBy = [ "multi-user.target" ];
        after = commonAfter;
        requires = commonAfter;
        serviceConfig = mkBaseServiceConfig coreCfg.ingestd.resources (
          mkServiceEnv [
            "RUST_LOG=${coreCfg.ingestd.logLevel}"
          ]
        ) {
          ExecStart = "${sinexPackage}/bin/sinex-ingestd ${ingestArgs}";
        };
      };
      "sinex-gateway" = {
        description = "Sinex gateway";
        wantedBy = [ "multi-user.target" ];
        after = [ "postgresql.service" ];
        requires = [ "postgresql.service" ];
        serviceConfig = mkBaseServiceConfig coreCfg.gateway.resources (
          mkServiceEnv [
            "RUST_LOG=${coreCfg.gateway.logLevel}"
          ]
        ) {
          ExecStart = "${sinexPackage}/bin/sinex-gateway ${gatewayArgs}";
        };
      };
    };

  mkFilesystemUnits =
    let
      sat = satellitesCfg.filesystem;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      processorConfig = builtins.toJSON {
        filesystem = {
          watch_paths = sat.watchPaths;
        };
      };
      derivedArgs = [ "--processor-config ${escapeShellArg processorConfig}" ];
      extraArgs = derivedArgs ++ sat.extraArgs;
    in
    mkSatelliteUnits {
      name = "filesystem";
      binary = "fs-watcher";
      description = "Filesystem satellite";
      inherit instances batch resources extraArgs;
      env = [ "RUST_LOG=${satellitesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkTerminalUnits =
    let
      sat = satellitesCfg.terminal;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
    in
    mkSatelliteUnits {
      name = "terminal";
      binary = "terminal-satellite";
      description = "Terminal satellite";
      inherit instances batch resources;
      extraArgs = sat.extraArgs;
      env = [ "RUST_LOG=${satellitesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkDesktopUnits =
    let
      sat = satellitesCfg.desktop;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      clipboardEnv = if sat.clipboard.enable then [ "SINEX_CLIPBOARD=1" ] else [ "SINEX_CLIPBOARD=0" ];
    in
    mkSatelliteUnits {
      name = "desktop";
      binary = "desktop-satellite";
      description = "Desktop satellite";
      inherit instances batch resources;
      extraArgs = sat.extraArgs;
      env = clipboardEnv ++ [ "RUST_LOG=${satellitesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkSystemUnits =
    let
      sat = satellitesCfg.system;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
    in
    mkSatelliteUnits {
      name = "system";
      binary = "system-satellite";
      description = "System satellite";
      inherit instances batch resources;
      extraArgs = sat.extraArgs;
      env = [ "RUST_LOG=${satellitesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkSatelliteUnits = params:
    let
      instances = params.instances;
      batch = params.batch;
      resources = params.resources;
      extraArgs = params.extraArgs or [];
      envExtras = params.env or [];
      afterUnits = [ "network-online.target" ] ++ optionals coreEnabled [ "sinex-ingestd.service" ];
      requiresUnits = optionals coreEnabled [ "sinex-ingestd.service" ];
      execArgs = concatStringsSep " " ([
        "--service-name sinex-${params.name}"
        "--nats-url ${natsUrl}"
        "--batch-size ${toString batch.size}"
        "--batch-timeout ${toString batch.timeoutSec}"
      ] ++ extraArgs);
      env = mkServiceEnv envExtras;
      mkUnit = instance: {
        description = "${params.description} (instance ${toString instance})";
        wantedBy = [ "multi-user.target" ];
        after = afterUnits;
        requires = requiresUnits;
        serviceConfig = mkBaseServiceConfig resources env {
          ExecStart = "${sinexPackage}/bin/sinex-${params.binary} ${execArgs}";
          WorkingDirectory = stateRoot;
        };
      };
    in
    if instances <= 0 then {} else
      listToAttrs (map (idx: nameValuePair "sinex-${params.name}-${toString idx}" (mkUnit idx)) (range 1 instances));

  mkAutomataProfile = profileName:
    let
      profiles = satellitesCfg.automata.profiles;
      defaultProfile = profiles.standard;
    in
    lib.attrByPath [ profileName ] defaultProfile profiles;

  mkAutomataUnit = params:
    let
      profile = mkAutomataProfile params.profile;
      batch = profile.batch;
      resources = profile.resources;
      subjectArgs = map (s: "--subject ${escapeShellArg s}") params.subjects;
      extraArgs = params.extraArgs or [];
      execArgs = concatStringsSep " " ([
        "--service-name sinex-${params.binary}"
        "--nats-url ${natsUrl}"
        "--batch-size ${toString batch.size}"
        "--batch-timeout ${toString batch.timeoutSec}"
      ] ++ extraArgs ++ subjectArgs);
      env = mkServiceEnv ([ "RUST_LOG=${satellitesCfg.defaults.logLevel}" ] ++ toEnvList params.env);
    in
    {
      description = params.description;
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ] ++ optionals coreEnabled [ "sinex-ingestd.service" ];
      requires = [ "postgresql.service" ];
      serviceConfig = mkBaseServiceConfig resources env {
        ExecStart = "${sinexPackage}/bin/sinex-${params.binary} ${execArgs}";
        WorkingDirectory = stateRoot;
      };
    };

  automataServices =
    if !(satellitesEnabled && satellitesCfg.automata.enable) then {} else
      let
        canon = satellitesCfg.automata.canonicalizer;
        health = satellitesCfg.automata.healthAggregator;
        canonicalizerUnit =
          if !canon.enable then {} else {
            "sinex-canonicalizer" = mkAutomataUnit {
              binary = "terminal-command-canonicalizer";
              description = "Sinex canonical command synthesizer";
              profile = canon.profile;
              subjects = canon.subjects;
              env = canon.env;
              extraArgs = [];
            };
          };
        healthUnit =
          if !health.enable then {} else {
            "sinex-health-aggregator" = mkAutomataUnit {
              binary = "health-aggregator";
              description = "Sinex health aggregator";
              profile = health.profile;
              subjects = health.subjects;
              env = health.env;
              extraArgs = [];
            };
          };
      in
      canonicalizerUnit // healthUnit;

  satelliteServices =
    if !satellitesEnabled then {} else
      let
        filesystemUnits = if satellitesCfg.filesystem.enable then mkFilesystemUnits else {};
        terminalUnits = if satellitesCfg.terminal.enable then mkTerminalUnits else {};
        desktopUnits = if satellitesCfg.desktop.enable then mkDesktopUnits else {};
        systemUnits = if satellitesCfg.system.enable then mkSystemUnits else {};
      in
      filesystemUnits // terminalUnits // desktopUnits // systemUnits;

  coreServices = mkCoreServices;

  generatedUnits = attrNames satelliteServices ++ attrNames automataServices;

in
{
  config = mkMerge [
    (mkIf sinexEnabled {
      systemd.services = mkMerge [ coreServices satelliteServices automataServices ];
      services.sinex.satellites.generatedUnits = mkIf satellitesEnabled generatedUnits;
    })
  ];
}
