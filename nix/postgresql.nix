# Native UUIDv7 bounds are projected to milliseconds for chunk pruning.
# Keep that projection conservative and retain the exact UUID row predicate.
# recheck: remove the patch once the pinned TimescaleDB source implements
# conservative UUID bounds and does not classify their lossy projection as exact.
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
