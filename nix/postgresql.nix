# TimescaleDB's UUIDv7 chunk bounds are converted to milliseconds for chunk
# pruning. Keep that conversion conservative and keep the exact UUID row
# predicate. recheck: remove the patch once the pinned TimescaleDB source
# implements conservative UUID bounds and does not treat their lossy
# conversion as exact.

{
  postgresql,
  extensions ? (_: [ ]),
}:
postgresql.withPackages (
  p:
  [
    (p.timescaledb.overrideAttrs (old: {
      patches = (old.patches or [ ]) ++ [ ./timescaledb-uuid-bounds.patch ];
    }))
  ]
  ++ extensions p
)
