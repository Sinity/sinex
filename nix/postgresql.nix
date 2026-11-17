# TimescaleDB's UUIDv7 chunk bounds are converted to milliseconds for chunk
# pruning. Keep that conversion conservative and keep the exact UUID row
# predicate. recheck: remove the patch once the pinned TimescaleDB source
# implements conservative UUID bounds and does not treat their lossy
# conversion as exact.
# Array receive temporaries belong to one element; the compressor owns copied
# bytes. Remove that patch once the pinned receiver bounds this memory itself.

{
  postgresql,
  extensions ? (_: [ ]),
}:
postgresql.withPackages (
  p:
  [
    (p.timescaledb.overrideAttrs (old: {
      patches = (old.patches or [ ]) ++ [
        ./timescaledb-uuid-bounds.patch
        ./timescaledb-array-receive-memory.patch
      ];
    }))
  ]
  ++ extensions p
)
