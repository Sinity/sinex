{ config
, options
, lib
, pkgs
, ...
}:

with lib;

let
  cfg = config.services.sinex;
  secretResolution = import ./lib/secret-resolution.nix { inherit lib; };
  automataLib = import ./lib/automata.nix { inherit lib; };
  inherit (secretResolution) resolveNamedSecretPath;
  db = cfg.database;
  secretPaths = config.sinex.secrets.paths or { };
  effectiveDatabasePasswordFile = resolveNamedSecretPath secretPaths db.passwordFile [
    "sinex-local-db"
    "sinex-remote-db"
  ];
  allDatabases = unique ([ db.name ] ++ db.extraDatabases);

  sinexEnabled = cfg.enable;
  coreEnabled = sinexEnabled && cfg.core.enable;
  runtimeEnabled = sinexEnabled && cfg.runtime.enable;
  sourcesEnabled = runtimeEnabled && cfg.sources.enable;

  ingestEnabled = coreEnabled && cfg.core.event_engine.enable;
  apiEnabled = coreEnabled && cfg.core.api.enable;

  defaultInstances = cfg.runtime.defaults.instances;
  resolveInstances =
    runtimeModuleCfg:
    let
      value = runtimeModuleCfg.instances;
    in
    if value != null then value else defaultInstances;

  runtimeServiceCount =
    if !sourcesEnabled then
      0
    else
      (if cfg.sources.filesystem.enable then resolveInstances cfg.sources.filesystem else 0)
      + (if cfg.sources.terminal.enable then resolveInstances cfg.sources.terminal else 0)
      + (if cfg.sources.browser.enable then resolveInstances cfg.sources.browser else 0)
      + (if cfg.sources.desktop.enable then resolveInstances cfg.sources.desktop else 0)
      + (if cfg.sources.system.enable then resolveInstances cfg.sources.system else 0);

  automataEnabled = runtimeEnabled && cfg.automata.enable;
  automataCount =
    if !automataEnabled then
      0
    else
      automataLib.countEnabled cfg.automata;

  coreServiceCount = (if ingestEnabled then 1 else 0) + (if apiEnabled then 1 else 0);

  totalServiceCount = coreServiceCount + runtimeServiceCount + automataCount;

  perServiceConnections = max 1 db.connectionPool.maxConnections;

  # sinex-d4qg: the API can carry its own pool size distinct from the
  # uniform per-service default (see default.nix's core.api.poolMaxConnections
  # and sources.nix's apiPoolMaxConnections -- keep this in sync with that
  # computation). uniformServiceCount excludes the API when it has an
  # override, so the API's actual (possibly larger) pool size is added once
  # via apiConnections instead of being double-counted at the uniform rate.
  # No override: uniformServiceCount == totalServiceCount and apiConnections
  # == 0, i.e. exactly the pre-existing formula (no behavior change).
  apiHasOverride = apiEnabled && cfg.core.api.poolMaxConnections != null;
  uniformServiceCount = totalServiceCount - (if apiHasOverride then 1 else 0);
  apiConnections = if apiHasOverride then cfg.core.api.poolMaxConnections else 0;

  # Add a small buffer above per-service pool totals for migrations, admin tools,
  # one-shot sinexctl calls, and exporter/preflight probes. The default service
  # surface is intentionally sized near 100 slots instead of the historical
  # several-hundred-slot overestimate.
  baselineConnections = uniformServiceCount * perServiceConnections + apiConnections + 25;
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
      ++ db.extensionCompatibilityPackages
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
        ensureDBOwnership = false;
        ensureClauses.login = true;
      };

      nodeUser =
        optionalAttrs (cfg.enable && cfg.runtime.enable && cfg.users.runtime != db.user)
          {
            name = cfg.users.runtime;
            ensureDBOwnership = false;
            ensureClauses.login = true;
          };

      sharedAccessRoles = map
        (name: {
          inherit name;
          ensureDBOwnership = false;
          ensureClauses.login = false;
        }) [
        "sinex_event_engine"
        "sinex_api"
        "sinex_readonly"
      ];
    in
    [ primaryUser ] ++ optionals (nodeUser != { }) [ nodeUser ] ++ sharedAccessRoles;

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
    # Cap each autovacuum worker's memory. Default (-1) inherits the 512MB
    # maintenance_work_mem, so autovacuum_max_workers (3) can hold up to 1.5GB
    # concurrently — a real spike when autovacuum touches a multi-GB hypertable
    # chunk (e.g. the journald mega-chunk, #2182). 256MB halves that worst case.
    autovacuum_work_mem = mkDefault "256MB";

    # Parallelism - utilize multi-core CPUs
    max_parallel_workers_per_gather = mkDefault 4;
    max_parallel_workers = mkDefault 8;
    # Hard ceiling on ALL background workers (parallel query + TimescaleDB
    # background workers + continuous-aggregate refresh). Left unset PostgreSQL
    # defaults to 8, which silently under-caps the TimescaleDB worker count
    # below; set explicitly so the configured values are honored.
    max_worker_processes = mkDefault 12;

    # Checkpointing
    checkpoint_completion_target = mkDefault "0.9";
    checkpoint_timeout = mkDefault "15min";

    # WAL settings
    wal_buffers = mkDefault "16MB";
    max_wal_size = mkDefault "2GB";
    min_wal_size = mkDefault "1GB";
    archive_mode = if db.walArchiveCommand != null then "on" else mkDefault "off";
    archive_command =
      if db.walArchiveCommand != null
      then db.walArchiveCommand
      else mkDefault "";

    # 2PC is not used by sinex (SQLx does not issue PREPARE TRANSACTION).
    # Setting this to 0 disables the feature and reclaims the shared-memory
    # overhead PostgreSQL would otherwise reserve for prepared transactions.
    max_prepared_transactions = mkDefault 0;

    # Logging: rely on log_min_duration_statement for slow-query visibility and
    # keep DDL out of the journal. log_statement="ddl" flooded journald with the
    # full schema-apply CREATE TABLE bodies (every column on its own line, ~1M
    # lines/day observed 2026-06-07) because the declarative schema converges on
    # startup; that noise vacuumed journal history within a day. DDL changes are
    # already tracked in the schema source + apply-engine logs.
    log_statement = mkDefault "none";
    log_duration = mkDefault false;
    log_min_duration_statement = mkDefault "1000ms";
    log_line_prefix = mkDefault "%m [%p] %q%u@%d ";
    log_connections = mkDefault true;
    log_disconnections = mkDefault true;

    # TimescaleDB upgrade safety: allow the loaded library version to be
    # newer than the catalog version. During a NixOS package upgrade the
    # postmaster loads the new .so via shared_preload_libraries before
    # postgresql-setup runs ALTER EXTENSION ... UPDATE. Without this flag
    # the background worker refuses to register (version mismatch) and the
    # upgrade script's ts_bgw_db_workers_restart call fails with
    # "extension must be preloaded". This is safe in a declarative
    # deployment because mismatches are always intentional upgrade windows,
    # never unexpected drift.
    "timescaledb.allow_elevated_versions" = mkDefault "on";

    # Bound TimescaleDB's background-worker fan-out. The library default is 16,
    # which is absurd for a single-user 32GB host: each compression/retention/
    # continuous-aggregate worker can grab up to maintenance_work_mem, so a
    # mega-chunk maintenance pass could fan out to many simultaneous 512MB ops
    # (the kind of spike that filled swap and thrashed the host, #2182). Four is
    # plenty of concurrency for one user and keeps concurrent maintenance memory
    # bounded. Pairs with max_worker_processes above as the hard ceiling.
    "timescaledb.max_background_workers" = mkDefault 4;

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
          assertion = db.localAuth != "scram-sha-256" || effectiveDatabasePasswordFile != null;
          message = ''
            services.sinex.database.localAuth = "scram-sha-256" requires
            a database password source (services.sinex.database.passwordFile or
            the conventional sinex-local-db / sinex-remote-db sources, including
            declarative /etc/sinex/db-password / /etc/sinex/remote-db-password),
            otherwise no services can connect.
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
                # Tracks compat dirs already merged into dynamic_library_path this run, so
                # repeated failures for the same stale extension version don't re-issue
                # redundant ALTER SYSTEM/reload calls.
                SINEX_COMPAT_LIBRARY_PATHS_APPLIED=""

                # On minor-version package upgrades (e.g. TimescaleDB 2.27.2 -> 2.28.2) an
                # already-installed extension's versioned .so (''${extName}-''${version}.so)
                # can vanish from the new package derivation while the database still has
                # the OLD version recorded in pg_extension. TimescaleDB's per-database loader
                # dynamically dlopens that versioned .so on EVERY session touching such a
                # database (even a plain catalog SELECT), so this can fail before
                # ALTER EXTENSION UPDATE is ever reached. Parse the missing-file name
                # directly out of the failing psql output (no separate lookup query
                # needed -- that query would fail the same way) and extend
                # dynamic_library_path with the old package's lib dir, found by searching
                # the nix store for the exact missing filename.
                #
                # Ordering matters: the compat dir MUST come AFTER $libdir, never before.
                # TimescaleDB's own upgrade machinery also resolves the UNVERSIONED
                # $libdir/timescaledb path (its already-preloaded loader shim) via this
                # same search path; if the compat dir were searched first, an unversioned
                # lookup would resolve to the OLD package's shim instead of the currently
                # preloaded one, and TimescaleDB's preload-consistency check then fails with
                # "extension \"timescaledb\" must be preloaded" even though it plainly is.
                apply_extension_compat_library_path_from_error() {
                  local errorText="$1"
                  local missingName
                  missingName=$(printf '%s' "$errorText" \
                    | sed -n 's/.*could not access file "\([^"]*\)".*/\1/p' | head -1)
                  if [ -z "$missingName" ]; then
                    return 1
                  fi
                  case " ''${SINEX_COMPAT_LIBRARY_PATHS_APPLIED} " in
                    *" ''${missingName} "*) return 0 ;;
                  esac
                  local compat_dir
                  compat_dir=$(find /nix/store -maxdepth 3 -name "''${missingName}.so" \
                    -printf '%h\n' 2>/dev/null | head -1)
                  if [ -z "$compat_dir" ]; then
                    return 1
                  fi
                  echo "[sinex] $missingName: found compat library at $compat_dir, extending dynamic_library_path (after \$libdir)"
                  psql -v ON_ERROR_STOP=1 -d postgres \
                    -c "ALTER SYSTEM SET dynamic_library_path TO '\$libdir:$compat_dir';" >/dev/null
                  psql -v ON_ERROR_STOP=1 -d postgres \
                    -c "SELECT pg_reload_conf();" >/dev/null
                  SINEX_COMPAT_LIBRARY_PATHS_APPLIED="''${SINEX_COMPAT_LIBRARY_PATHS_APPLIED} ''${missingName}"
                  return 0
                }

                reset_extension_compat_library_path() {
                  if [ -n "$SINEX_COMPAT_LIBRARY_PATHS_APPLIED" ]; then
                    psql -v ON_ERROR_STOP=1 -d postgres \
                      -c "ALTER SYSTEM RESET dynamic_library_path;" >/dev/null
                    psql -v ON_ERROR_STOP=1 -d postgres \
                      -c "SELECT pg_reload_conf();" >/dev/null
                    SINEX_COMPAT_LIBRARY_PATHS_APPLIED=""
                  fi
                }

                ensure_database_owner() {
                  local dbName="$1"
                  local roleName="$2"
                  echo "[sinex] ensuring database owner ''${roleName} for ''${dbName}"
                  psql -v ON_ERROR_STOP=1 \
                    --set=sinex_db_name="$dbName" \
                    --set=sinex_role_name="$roleName" \
                    postgres <<'SQL' >/dev/null
ALTER DATABASE :"sinex_db_name" OWNER TO :"sinex_role_name";
SQL
                }

                extension_exists() {
                  local dbName="$1"
                  local extName="$2"
                  local output rc
                  output=$(psql -X -v ON_ERROR_STOP=1 \
                    --set=sinex_ext_name="$extName" \
                    -d "$dbName" -At 2>&1 <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM pg_extension
  WHERE extname = :'sinex_ext_name'
)::int;
SQL
                  )
                  rc=$?
                  if [ $rc -ne 0 ]; then
                    if printf '%s' "$output" | grep -qF 'could not access file' \
                      && apply_extension_compat_library_path_from_error "$output"; then
                      output=$(psql -X -v ON_ERROR_STOP=1 \
                        --set=sinex_ext_name="$extName" \
                        -d "$dbName" -At 2>&1 <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM pg_extension
  WHERE extname = :'sinex_ext_name'
)::int;
SQL
                      )
                      rc=$?
                    fi
                  fi
                  if [ $rc -ne 0 ]; then
                    printf '%s\n' "$output" >&2
                    return $rc
                  fi
                  printf '%s\n' "$output"
                }

                create_extension() {
                  local dbName="$1"
                  local extName="$2"
                  psql -X -v ON_ERROR_STOP=1 \
                    --set=sinex_ext_name="$extName" \
                    -d "$dbName" <<'SQL' >/dev/null
CREATE EXTENSION IF NOT EXISTS :"sinex_ext_name";
SQL
                }

                update_extension() {
                  local dbName="$1"
                  local extName="$2"
                  # TimescaleDB requires ALTER EXTENSION to be the first command in a
                  # fresh psql session after a package update. Keep every extension on
                  # this stricter path so setup behavior stays uniform.
                  #
                  # On minor-version upgrades (e.g. TimescaleDB 2.27.2 -> 2.28.2) the
                  # installed .so can be missing from the new package derivation.
                  # apply_extension_compat_library_path_from_error (shared with
                  # extension_exists above) detects this via "could not access file" and
                  # extends dynamic_library_path with a compat dir found in the nix store.
                  local output rc
                  output=$(psql -X -v ON_ERROR_STOP=1 \
                    --set=sinex_ext_name="$extName" \
                    -d "$dbName" 2>&1 <<'SQL'
ALTER EXTENSION :"sinex_ext_name" UPDATE;
SQL
                  )
                  rc=$?
                  [ $rc -eq 0 ] && return 0

                  if printf '%s' "$output" | grep -qF 'could not access file' \
                    && apply_extension_compat_library_path_from_error "$output"; then
                    output=$(psql -X -v ON_ERROR_STOP=1 \
                      --set=sinex_ext_name="$extName" \
                      -d "$dbName" 2>&1 <<'SQL'
ALTER EXTENSION :"sinex_ext_name" UPDATE;
SQL
                    )
                    rc=$?
                    if [ $rc -eq 0 ]; then
                      echo "[sinex] $extName: upgraded via compat library path"
                      return 0
                    fi
                  fi

                  printf '[sinex] ERROR: %s update failed: %s\n' "$extName" "$output" >&2
                  echo "[sinex] Hint: add the old package to services.sinex.database.extensionCompatibilityPackages" >&2
                  return $rc
                }

                extension_needs_update() {
                  local dbName="$1"
                  local extName="$2"
                  psql -X -v ON_ERROR_STOP=1 \
                    --set=sinex_ext_name="$extName" \
                    -d "$dbName" -At 2>/dev/null <<'SQL'
SELECT EXISTS (
  SELECT 1
  FROM pg_extension e
  JOIN pg_available_extensions ae ON ae.name = e.extname
  WHERE e.extname = :'sinex_ext_name'
    AND e.extversion != ae.default_version
);
SQL
                }

                ensure_extension() {
                  local dbName="$1"
                  local extName="$2"
                  local exists needs_update
                  echo "[sinex] ensuring extension ''${extName} for ''${dbName}"
                  exists="$(extension_exists "$dbName" "$extName")"
                  if [ "$exists" != "1" ]; then
                    create_extension "$dbName" "$extName"
                    # Freshly created extension is already at latest version.
                    return
                  fi
                  # Skip ALTER EXTENSION UPDATE when the installed version already
                  # matches the default available version — avoids a NOTICE flood
                  # ("version X already installed") on every postgresql restart.
                  needs_update=$(extension_needs_update "$dbName" "$extName")
                  if [ "''${needs_update}" = "t" ]; then
                    update_extension "$dbName" "$extName"
                  fi
                }

                for dbName in ${concatStringsSep " " (map escapeShellArg allDatabases)}; do
                  ensure_database_owner "$dbName" ${escapeShellArg db.user}
                  ensure_extension "$dbName" "timescaledb"
                  ensure_extension "$dbName" "pg_jsonschema"
                  ensure_extension "$dbName" "vector"
                  ensure_extension "$dbName" "pg_trgm"
                done

                reset_extension_compat_library_path
      '';
    })

    (mkIf (db.enable && db.autoSetup && effectiveDatabasePasswordFile != null) {
      systemd.services.postgresql-setup.script = lib.mkAfter ''
                sync_role_password() {
                  local roleName="$1"
                  local passwordFile="$2"
                  local password

                  if [ ! -r "$passwordFile" ]; then
                    echo "[sinex] password file $passwordFile is not readable" >&2
                    return 1
                  fi

                  password="$(tr -d '\n' < "$passwordFile")"
                  PGPASSWORD= psql \
                    -v ON_ERROR_STOP=1 \
                    --set=sinex_role="$roleName" \
                    --set=sinex_password="$password" \
                    postgres <<'SQL' >/dev/null
ALTER ROLE :"sinex_role" WITH PASSWORD :'sinex_password';
SQL
                }

                sync_role_password ${escapeShellArg db.user} ${escapeShellArg effectiveDatabasePasswordFile}
      '';
    })
  ];
}
