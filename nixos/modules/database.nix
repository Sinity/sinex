{
  config,
  options,
  lib,
  pkgs,
  ...
}:

with lib;

let
  cfg = config.services.sinex;
  db = cfg.database;
  allDatabases = unique ([ db.name ] ++ db.extraDatabases);

  sinexEnabled = cfg.enable;
  coreEnabled = sinexEnabled && cfg.core.enable;
  nodesEnabled = sinexEnabled && cfg.nodes.enable;

  ingestEnabled = coreEnabled && cfg.core.ingestd.enable;
  gatewayEnabled = coreEnabled && cfg.core.gateway.enable;

  defaultInstances = cfg.nodes.defaults.instances;
  resolveInstances =
    nodeCfg:
    let
      value = nodeCfg.instances;
    in
    if value != null then value else defaultInstances;

  nodeServiceCount =
    if !nodesEnabled then
      0
    else
      (if cfg.nodes.filesystem.enable then resolveInstances cfg.nodes.filesystem else 0)
      + (if cfg.nodes.terminal.enable then resolveInstances cfg.nodes.terminal else 0)
      + (if cfg.nodes.desktop.enable then resolveInstances cfg.nodes.desktop else 0)
      + (if cfg.nodes.system.enable then resolveInstances cfg.nodes.system else 0);

  automataEnabled = nodesEnabled && cfg.nodes.automata.enable;
  automataCount =
    if !automataEnabled then
      0
    else
      (if cfg.nodes.automata.canonicalizer.enable then 1 else 0)
      + (if cfg.nodes.automata.healthAggregator.enable then 1 else 0);

  coreServiceCount = (if ingestEnabled then 1 else 0) + (if gatewayEnabled then 1 else 0);

  totalServiceCount = coreServiceCount + nodeServiceCount + automataCount;

  perServiceConnections = max 1 db.connectionPool.maxConnections;
  # Add a 50-connection buffer above the per-service pool totals for migrations,
  # admin tools, and background tasks. At least perServiceConnections + 10 even
  # when no counted services are enabled.
  baselineConnections = totalServiceCount * perServiceConnections + 50;
  computedMaxConnections = max (perServiceConnections + 10) baselineConnections;

  postgresqlPkgBase = db.package;
  # Derive the extension package set from the actual postgres package being used.
  # db.package.pkgs gives the matching postgresql*Packages set (e.g. postgresql_18.pkgs
  # == postgresql18Packages), so extension availability is always checked against the
  # correct version. Falls back to postgresql18Packages if .pkgs is absent (custom package).
  postgresqlPackages = postgresqlPkgBase.pkgs or pkgs.postgresql18Packages;

  # pg_jsonschema must be provided via the flake overlay.
  # Prefer pkgs.postgresql18Packages.pg_jsonschema (overlay-aware top-level) because
  # postgresql_18.pkgs may reference the pre-overlay postgresql18Packages — nixpkgs
  # evaluates postgresql_18.pkgs eagerly, before overlays patch postgresql18Packages.
  pgJsonschema =
    pkgs.postgresql18Packages.pg_jsonschema or
    postgresqlPackages.pg_jsonschema or
    (throw ''
      pg_jsonschema is not available for the configured PostgreSQL package.
      You must apply the sinex flake overlay to your pkgs:

        nixpkgs.overlays = [ inputs.sinex.overlays.default ];

      Or provide services.sinex.package directly from flake outputs.
    '');

  extensionPackageBuilder =
    ps:
    unique (
      optionals (ps ? timescaledb) [ ps.timescaledb ]
      ++ optionals (ps ? pgvector) [ ps.pgvector ]
      # Always include pg_jsonschema, even if it's not present under
      # postgresql18Packages in this particular pkgs set.
      ++ [ pgJsonschema ]
    );

  postgresqlPkg =
    if lib.hasAttr "withPackages" postgresqlPkgBase then
      postgresqlPkgBase.withPackages extensionPackageBuilder
    else
      postgresqlPkgBase;

  sharedPreloadLibraries =
    let
      base = optionals (postgresqlPackages ? timescaledb) [ "timescaledb" ];
    in
    concatStringsSep "," (unique base);

  ensuredUsers =
    let
      primaryUser = {
        name = db.user;
        ensureDBOwnership = true;
        ensureClauses.login = true;
      }
      // optionalAttrs (db.passwordFile != null) { passwordFile = db.passwordFile; };

      nodeUser =
        optionalAttrs (cfg.enable && cfg.nodes.enable && cfg.users.nodes != db.user)
          {
            name = cfg.users.nodes;
            ensureDBOwnership = false;
            ensureClauses.login = true;
          };
    in
    [ primaryUser ] ++ optionals (nodeUser != { }) [ nodeUser ];

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

    # 2PC is not used by sinex (SQLx does not issue PREPARE TRANSACTION).
    # Setting this to 0 disables the feature and reclaims the shared-memory
    # overhead PostgreSQL would otherwise reserve for prepared transactions.
    max_prepared_transactions = mkDefault 0;

    # Logging: log DDL + DML statements for audit purposes.
    # log_duration is intentionally OFF; use log_min_duration_statement to catch
    # slow queries without flooding the log for every fast INSERT in the ingest path.
    log_statement = mkDefault "mod";
    log_duration = mkDefault false;
    log_min_duration_statement = mkDefault "1000ms";
    log_line_prefix = mkDefault "%m [%p] %q%u@%d ";
    log_connections = mkDefault true;
    log_disconnections = mkDefault true;

    # Computed/required settings
    max_connections = mkDefault computedMaxConnections;
    shared_preload_libraries = mkDefault sharedPreloadLibraries;
    listen_addresses = mkDefault db.host;
  };

  authenticationConfig = ''
    local   all             all                                     peer
    host    all             all             127.0.0.1/32            ${db.localAuth}
    host    all             all             ::1/128                 ${db.localAuth}
    host    all             all             0.0.0.0/0               reject
    host    all             all             ::/0                    reject
  '';

  sinexAllowUnfreePredicate =
    pkg:
    let
      name = lib.getName pkg;
    in
    elem name [
      "timescaledb"
      "pg_jsonschema"
    ];

in
{
  config = mkMerge [
    (mkIf (db.enable && db.autoSetup && !options.nixpkgs.pkgs.isDefined) {
      # Allow only the specific unfree packages Sinex requires (TimescaleDB, pg_jsonschema).
      # Using the predicate form rather than setting allowUnfree = true avoids accidentally
      # unblocking all unfree packages in the user's nixpkgs configuration.
      # Guard: skip when nixpkgs is externally managed (e.g. flake VM tests pass pkgs
      # directly via specialArgs — setting nixpkgs.config there would fail NixOS's
      # "externally created instance" assertion and have no effect anyway).
      nixpkgs.config.allowUnfreePredicate = mkDefault sinexAllowUnfreePredicate;
    })

    (mkIf (db.enable && db.autoSetup) {
      assertions = [
        {
          assertion = db.localAuth != "scram-sha-256" || db.passwordFile != null;
          message = ''
            services.sinex.database.localAuth = "scram-sha-256" requires
            services.sinex.database.passwordFile to be set, otherwise no services can connect.
          '';
        }
      ];
    })

    (mkIf cfg.enable {
      assertions = [
        {
          assertion = lib.hasSuffix "_${cfg.nats.environment}" db.name;
          message = ''
            services.sinex.database.name must end with "_${cfg.nats.environment}" so the
            runtime database stays namespaced to the active Sinex environment.
          '';
        }
      ];
    })

    (mkIf (db.enable && db.autoSetup) {
      services.postgresql = {
        enable = true;
        package = mkForce postgresqlPkg;
        ensureDatabases = mkDefault allDatabases;
        ensureUsers = mkDefault ensuredUsers;
        authentication = mkDefault authenticationConfig;
        settings = mkMerge [
          baseSettings
          { port = mkForce db.port; }
        ];
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
          ensure_extension "$dbName" "pg_trgm"
        done
      '';
    })
  ];
}
