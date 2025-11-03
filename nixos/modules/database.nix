{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  db = cfg.database;

  sinexEnabled = cfg.enable;
  coreEnabled = sinexEnabled && cfg.core.enable;
  satellitesEnabled = sinexEnabled && cfg.satellites.enable;

  ingestEnabled = coreEnabled && cfg.core.ingestd.enable;
  gatewayEnabled = coreEnabled && cfg.core.gateway.enable;

  defaultInstances = cfg.satellites.defaults.instances;
  resolveInstances = satelliteCfg:
    let
      value = satelliteCfg.instances;
    in
    if value != null then value else defaultInstances;

  satelliteServiceCount =
    if !satellitesEnabled then 0 else
      (if cfg.satellites.filesystem.enable then resolveInstances cfg.satellites.filesystem else 0)
      + (if cfg.satellites.terminal.enable then resolveInstances cfg.satellites.terminal else 0)
      + (if cfg.satellites.desktop.enable then resolveInstances cfg.satellites.desktop else 0)
      + (if cfg.satellites.system.enable then resolveInstances cfg.satellites.system else 0);

  automataEnabled = satellitesEnabled && cfg.satellites.automata.enable;
  automataCount =
    if !automataEnabled then 0 else
      (if cfg.satellites.automata.canonicalizer.enable then 1 else 0)
      + (if cfg.satellites.automata.healthAggregator.enable then 1 else 0);

  coreServiceCount =
    (if ingestEnabled then 1 else 0)
    + (if gatewayEnabled then 1 else 0);

  totalServiceCount = coreServiceCount + satelliteServiceCount + automataCount;

  perServiceConnections = max 1 db.connectionPool.maxConnections;
  baselineConnections = totalServiceCount * perServiceConnections + 50;
  computedMaxConnections = max (perServiceConnections + 10) baselineConnections;

  postgresqlPkg = db.package;
  postgresqlPackages =
    if postgresqlPkg ? pkgs then postgresqlPkg.pkgs else pkgs.postgresql16Packages;

  extensionPackages = unique (
    optionals (postgresqlPackages ? timescaledb) [ postgresqlPackages.timescaledb ]
    ++ optionals (postgresqlPackages ? pgvector) [ postgresqlPackages.pgvector ]
    ++ optionals (db.monotonicUlids && (postgresqlPackages ? pgx_ulid)) [ postgresqlPackages.pgx_ulid ]
  );

  sharedPreloadLibraries =
    let
      base = optionals (postgresqlPackages ? timescaledb) [ "timescaledb" ]
        ++ optionals (db.monotonicUlids && (postgresqlPackages ? pgx_ulid)) [ "pgx_ulid" ];
    in
    concatStringsSep "," (unique base);

  ensuredUsers =
    let
      primaryUser = {
        name = db.user;
        ensureDBOwnership = true;
        ensureClauses.login = true;
      } // optionalAttrs (db.passwordFile != null) { passwordFile = db.passwordFile; };

      satelliteUser =
        optionalAttrs ( cfg.enable && cfg.satellites.enable && cfg.users.satellites != db.user ) {
          name = cfg.users.satellites;
          ensureDBOwnership = false;
          ensureClauses.login = true;
        };
    in
    [ primaryUser ] ++ optionals (satelliteUser != {}) [ satelliteUser ];

  baseSettings = {
    statement_timeout = mkDefault "60s";
    lock_timeout = mkDefault "30s";
    idle_in_transaction_session_timeout = mkDefault "300s";

    shared_buffers = mkDefault "256MB";
    effective_cache_size = mkDefault "1GB";
    maintenance_work_mem = mkDefault "256MB";
    checkpoint_completion_target = mkDefault "0.9";

    max_prepared_transactions = mkDefault 256;
    log_statement = mkDefault "mod";
    log_duration = mkDefault true;
    log_min_duration_statement = mkDefault "1000ms";

    max_connections = mkDefault computedMaxConnections;
    shared_preload_libraries = mkDefault sharedPreloadLibraries;
    listen_addresses = mkDefault db.host;
  };

  authenticationConfig = ''
local   all             all                                     peer
host    all             all             0.0.0.0/0               reject
host    all             all             ::/0                    reject
'';

  migrationPackage =
    if db.migration.package != null then db.migration.package else cfg.package;

in
{
  config = mkMerge [
    (mkIf (db.enable && db.autoSetup) {
      services.postgresql = {
        enable = true;
        package = mkForce postgresqlPkg;
        ensureDatabases = mkDefault [ db.name ];
        ensureUsers = mkDefault ensuredUsers;
        authentication = mkDefault authenticationConfig;
        extensions = extensionPackages;
        settings = mkMerge [ baseSettings { port = mkForce db.port; } ];
      };
    })

    (mkIf (cfg.enable && db.migration.enable) {
      environment.systemPackages = mkAfter [ migrationPackage ];
    })
  ];
}
