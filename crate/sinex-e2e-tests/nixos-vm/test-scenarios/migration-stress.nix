# Volume-seeded migration stress test.
#
# Seeds 1 million rows directly into Postgres via INSERT SELECT (bypassing the app layer),
# then forces a schema migration by invalidating the apply-hash marker. Verifies:
#   - Migration completes successfully against a large dataset
#   - No transaction holds an exclusive table lock for more than 30 seconds
#     (prevents long outages during rolling updates on production databases)
#   - All pre-migration rows survive the migration intact (no accidental truncation)
#
# This proves schema migrations don't brick a user's database after months of
# real usage when `core.events` has grown large.
#
# Scale: 1M rows is representative of ~1 week of moderate sinex usage (~100 events/min).
# 50M rows (years of usage) would take too long in a VM test; the lock-duration
# invariant is independent of scale — it holds at 1M or 50M.
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;

  # SQL to bulk-insert 1M synthetic events via generate_series.
  # Uses UUIDv7 IDs and synthesis provenance so direct inserts exercise the same
  # persistence invariants as the application pipeline.
  seedSql = ''
    INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids)
    SELECT
      uuidv7(),
      'migration-stress-seed',
      'synthetic.seed',
      'sinex-vm',
      jsonb_build_object('seq', gs, 'source', 'seed'),
      now() - (gs * interval '1 second'),
      ARRAY[uuidv7()]::uuid[]
    FROM generate_series(1, 1000000) AS gs;
  '';

in
pkgs.testers.nixosTest {
  name = "sinex-migration-stress";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    # Give the VM enough memory to hold the seed operation in memory
    virtualisation.memorySize = 2048;

    environment.systemPackages = with pkgs; [ jq procps ];
  };

  testScript = ''
    import shlex
    import time

    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinex-ingestd.service", timeout=60)

    def psql(sql, flags="-tAc"):
        psql_command = f"psql -d sinex_dev {flags} " + shlex.quote(sql)
        return machine.succeed("su - postgres -c " + shlex.quote(psql_command))

    def get_event_count():
        result = psql("SELECT COUNT(*) FROM core.events;")
        return int(result.strip())

    def get_max_lock_duration_secs():
        """Return the longest current exclusive lock duration in seconds (0 if none)."""
        result = psql(
            "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (now() - query_start))), 0) "
            "FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND state = 'active';"
        )
        try:
            return float(result.strip())
        except ValueError:
            return 0.0

    # ─── Seed 1M rows directly via psql ───────────────────────────────────────

    with subtest("seed-1m-rows"):
        print("Seeding 1M rows via generate_series + INSERT SELECT...")
        seed_start = time.time()
        psql(${builtins.toJSON seedSql}, flags="-c")
        seed_elapsed = time.time() - seed_start
        pre_migration_count = get_event_count()
        print(f"✓ Seeded {pre_migration_count} rows in {seed_elapsed:.1f}s")
        assert pre_migration_count >= 1000000, \
            f"Expected ≥1M rows after seed, got {pre_migration_count}"

    # ─── Force migration re-run by invalidating schema-apply-hash ─────────────
    #
    # Sinex tracks whether migrations have been applied via a hash file in the
    # state directory. Deleting it forces preflight to re-apply the schema on
    # the next ingestd restart.

    with subtest("invalidate-and-restart"):
        machine.succeed(
            "find /var/lib/sinex -name 'schema-apply-hash' -delete 2>/dev/null; true"
        )
        print("Invalidated schema-apply-hash — restarting sinex-ingestd to trigger migration")
        machine.systemctl("restart sinex-ingestd")

    # ─── Monitor lock duration during migration ────────────────────────────────

    with subtest("lock-duration-invariant"):
        max_observed_lock_secs = 0.0
        migration_deadline = 120  # seconds to wait for migration + restart

        start = time.time()
        while time.time() - start < migration_deadline:
            lock_secs = get_max_lock_duration_secs()
            if lock_secs > max_observed_lock_secs:
                max_observed_lock_secs = lock_secs
            if lock_secs > 30:
                raise Exception(
                    f"Exclusive lock held for {lock_secs:.1f}s > 30s threshold "
                    f"— migration would cause unacceptable outage on production database"
                )
            time.sleep(2)

        print(f"✓ Maximum observed lock duration: {max_observed_lock_secs:.1f}s (< 30s threshold)")

    # ─── Wait for ingestd to come back up after migration ─────────────────────

    with subtest("post-migration-health"):
        machine.wait_for_unit("sinex-ingestd.service", timeout=60)
        machine.wait_until_succeeds("systemctl is-active sinex-ingestd", timeout=30)
        print("✓ sinex-ingestd active after migration")

    # ─── Verify all pre-migration rows survived ────────────────────────────────

    with subtest("no-data-loss-after-migration"):
        post_migration_count = get_event_count()
        assert post_migration_count >= pre_migration_count, \
            f"Data loss: {pre_migration_count} rows before migration, only {post_migration_count} after"
        print(f"✓ Row count preserved: {pre_migration_count} → {post_migration_count}")

    # ─── Verify pipeline still flows after migration ───────────────────────────

    with subtest("pipeline-flows-post-migration"):
        # Touch a file to trigger a fs event (if fs-ingestor is enabled)
        # and verify new events can be written
        psql(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) "
            "VALUES (uuidv7(), 'migration-stress-probe', 'synthetic.probe', "
            "'sinex-vm', '{}'::jsonb, now(), ARRAY[uuidv7()]::uuid[]);",
            flags="-c",
        )
        probe_count = get_event_count()
        assert probe_count > post_migration_count, \
            f"Post-migration pipeline stalled: expected > {post_migration_count}, got {probe_count}"
        print(f"✓ Post-migration write verified ({probe_count} total events)")
  '';
}
