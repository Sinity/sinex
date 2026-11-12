{
  pkgs,
  crane,
  toolchain,
  postgres,
  source,
  revision,
}:
let
  craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
  common = {
    src = source;
    pname = "sinex";
    version = "0.1.0";
    CARGO_PROFILE = "release";
    strictDeps = true;
    doCheck = false;
  };
  # Resolve the same features and codegen units the runtime consumes. Build
  # identity belongs to the real daemon, not this reusable dependency cache.
  packageArgs = "--locked -p sinexd -p sinexctl -p sinex-session -p sinex-capture";
  dependencies = craneLib.buildDepsOnly (
    common
    // {
      src = craneLib.cleanCargoSource source;
      cargoExtraArgs = packageArgs;
      buildPhaseCargoCommand = "cargoWithProfile build ${packageArgs}";
    }
  );
  # Dependency builds use dummy sources. The real schema tool has no checked
  # queries and must provision the database before compiling the real store.
  schema = craneLib.buildPackage (
    common
    // {
      pname = "sinex-schema";
      cargoArtifacts = dependencies;
      cargoExtraArgs = "--locked -p sinex-schema";
      doInstallCargoArtifacts = false;
    }
  );
  package = craneLib.buildPackage (
    common
    // {
      cargoArtifacts = dependencies;
      cargoExtraArgs = packageArgs;
      doInstallCargoArtifacts = false;
      SINEX_BUILD = revision;
      nativeBuildInputs = [ postgres ];
      preBuild = ''
        export PGDATA=$(mktemp -d)
        export PGHOST=$(mktemp -d)
        export PGPORT=5432
        initdb --no-locale --auth=trust --data-checksums > /dev/null
        pg_ctl -D "$PGDATA" -o "-k $PGHOST -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on -p $PGPORT" -w start > /dev/null
        trap 'pg_ctl -D "$PGDATA" -m immediate -w stop >/dev/null || true' EXIT
        createdb foundation
        export DATABASE_URL="postgresql:///foundation?host=$PGHOST&port=$PGPORT"
        ${schema}/bin/sinex-schema apply "$DATABASE_URL"
        psql -d postgres -v ON_ERROR_STOP=1 \
          -c "ALTER DATABASE foundation SET default_transaction_read_only = on" \
          -c "ALTER DATABASE foundation SET timescaledb.disable_load = on"
      '';
    }
  );
in
{
  inherit package dependencies;
}
