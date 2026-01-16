{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  db = cfg.database;
  allDatabases = unique ([ db.name ] ++ db.extraDatabases);

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
  # Ensure reasonable minimum for dev/test workloads (800) even with no services
  # Production with services will compute higher based on service count
  minConnectionsForTests = 800;
  baselineConnections = totalServiceCount * perServiceConnections + 50;
  computedMaxConnections = max minConnectionsForTests (max (perServiceConnections + 10) baselineConnections);

  postgresqlPkgBase = db.package;
  postgresqlPackages = pkgs.postgresql16Packages;

  # pg_jsonschema is packaged in this repository under nix/pkgs/pg_jsonschema.
  # Wire it into the Postgres build explicitly so that any cluster provisioned
  # via this module (including ones used by xtask ci postgres) always has the
  # extension available in pg_available_extensions, instead of relying on a
  # separate overlay to attach it indirectly.
  pgJsonschema = pkgs.callPackage ../../nix/pkgs/pg_jsonschema { };

  extensionPackageBuilder =
    ps:
    unique (
      optionals (ps ? timescaledb) [ ps.timescaledb ]
      ++ optionals (ps ? pgvector) [ ps.pgvector ]
      ++ optionals (ps ? pgx_ulid) [ ps.pgx_ulid ]
      # Always include pg_jsonschema, even if it's not present under
      # postgresql16Packages in this particular pkgs set.
      ++ [ pgJsonschema ]
    );

  postgresqlPkg =
    if lib.hasAttr "withPackages" postgresqlPkgBase then
      postgresqlPkgBase.withPackages extensionPackageBuilder
    else
      postgresqlPkgBase;

  sharedPreloadLibraries =
    let
      base = optionals (postgresqlPackages ? timescaledb) [ "timescaledb" ]
        ++ optionals (postgresqlPackages ? pgx_ulid) [ "pgx_ulid" ];
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
    # Timeouts
    statement_timeout = mkDefault "60s";
    lock_timeout = mkDefault "30s";
    idle_in_transaction_session_timeout = mkDefault "300s";

    # Memory settings - sized for modern systems (16GB+ RAM)
    # Users can override via services.postgresql.settings for different hardware
    shared_buffers = mkDefault "1GB";
    effective_cache_size = mkDefault "8GB";
    work_mem = mkDefault "32MB";
    maintenance_work_mem = mkDefault "512MB";

    # Parallelism - utilize multi-core CPUs
    max_parallel_workers_per_gather = mkDefault 4;
    max_parallel_workers = mkDefault 8;

    # Checkpointing
    checkpoint_completion_target = mkDefault "0.9";
    checkpoint_timeout = mkDefault "15min";

    # WAL settings
    wal_buffers = mkDefault "16MB";
    max_wal_size = mkDefault "2GB";
    min_wal_size = mkDefault "1GB";

    # Prepared transactions for distributed systems
    max_prepared_transactions = mkDefault 256;

    # Logging
    log_statement = mkDefault "mod";
    log_duration = mkDefault true;
    log_min_duration_statement = mkDefault "1000ms";

    # Computed/required settings
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

  databaseExtensionsOverlay = import ../../nix/overlays/database-extensions.nix;
  sinexAllowUnfreePredicate =
    pkg:
    let
      name = lib.getName pkg;
    in
    elem name [ "timescaledb" "pg_jsonschema" ];

in
{
  config = mkMerge [
    (mkIf (db.enable && db.autoSetup) {
      nixpkgs.overlays = mkAfter [ databaseExtensionsOverlay ];
      nixpkgs.config.allowUnfree = mkDefault true;
      nixpkgs.config.allowUnfreePredicate = mkDefault sinexAllowUnfreePredicate;
    })

    (mkIf (db.enable && db.autoSetup) {
      services.postgresql = {
        enable = true;
        package = mkForce postgresqlPkg;
        ensureDatabases = mkDefault allDatabases;
        ensureUsers = mkDefault ensuredUsers;
        authentication = mkDefault authenticationConfig;
        settings = mkMerge [ baseSettings { port = mkForce db.port; } ];
      };
    })

    (mkIf (db.enable && db.autoSetup) {
      systemd.services.postgresql-setup.script = lib.mkAfter ''
        ensure_extension() {
          local dbName="$1"
          local extName="$2"
          echo "[sinex] ensuring extension ''${extName} for ''${dbName}"
          psql -v ON_ERROR_STOP=1 -d "$dbName" -c "CREATE EXTENSION IF NOT EXISTS \"$extName\"" >/dev/null
        }

        for dbName in ${concatStringsSep " " (map escapeShellArg allDatabases)}; do
          ensure_extension "$dbName" "timescaledb"
          ensure_extension "$dbName" "pg_jsonschema"
          ensure_extension "$dbName" "vector"
          ensure_extension "$dbName" "ulid"
        done
      '';
    })

    (mkIf (cfg.enable && db.migration.enable) {
      environment.systemPackages = mkAfter [ migrationPackage ];
    })
  ];
}
