# The plain table is the independent result oracle for native UUID pruning.
{ pkgs, postgres }:
pkgs.runCommand "postgresql-uuid-bounds" { nativeBuildInputs = [ postgres ]; } ''
  export PGDATA="$TMPDIR/data" PGHOST="$TMPDIR/socket" PGPORT=5432
  mkdir "$PGHOST"
  initdb --no-locale --auth=trust > /dev/null
  pg_ctl -o "-k $PGHOST -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off" -w start > /dev/null
  trap 'pg_ctl -m immediate -w stop > /dev/null' EXIT
  createdb regression
  psql -X -v ON_ERROR_STOP=1 -d regression -c 'CREATE EXTENSION timescaledb'
  psql -X -v ON_ERROR_STOP=1 -d regression -f ${./postgresql-uuid-bounds.sql}
  touch "$out"
''
