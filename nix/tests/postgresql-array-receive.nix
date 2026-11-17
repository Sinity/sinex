# The unmodified receiver is a test control, never a deployment provider.
{ pkgs, postgres }:
let
  before = pkgs.postgresql_18.withPackages (p: [
    (p.timescaledb.overrideAttrs (old: {
      patches = (old.patches or [ ]) ++ [ ../timescaledb-uuid-bounds.patch ];
    }))
    p.pgvector
  ]);
in
pkgs.runCommand "postgresql-array-receive"
  {
    nativeBuildInputs = [
      postgres
      pkgs.python3
      pkgs.stdenv.cc
    ];
  }
  ''
    cc -fPIC -shared -I${pkgs.postgresql_18.dev}/include/server \
        ${./postgresql-array-receive.c} -o "$TMPDIR/receive-memory.so"
      cd "$TMPDIR"
      python ${./postgresql-array-receive.py} ${before}/bin ${postgres}/bin \
        "$TMPDIR/receive-memory.so" "$out"
  ''
