# Chaos test: real VM power-cut between recovery-spool rename and parent-dir
# fsync (sinex-r6d.9, sinex-r6d.5).
#
# A process-level SIGKILL (as chaos-process-restart.nix exercises) cannot
# test this: killing a process does not drop the kernel page cache, so a
# parent directory's dirty metadata survives regardless of whether fsync
# ever ran. This scenario instead:
#   - puts the spool file on a persistent (non-tmpfs) virtual disk
#   - arms the spool_rename_crash_harness binary (testing-feature build) to
#     pause forever right after the rename, before the parent-dir fsync
#   - waits for that pause, then calls machine.crash() — a real QEMU
#     power-cut (`quit` via the monitor) that discards the guest's entire
#     RAM state, including any dirty page-cache metadata for the disk
#   - restarts the same machine (reusing the same persistent disk image)
#     and checks whether the renamed spool file actually survived
{ pkgs
, sinex ? null
, ...
}:

pkgs.testers.nixosTest {
  name = "sinex-chaos-spool-rename-durability";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    virtualisation.emptyDiskImages = [ 256 ];
    environment.systemPackages = [ sinex pkgs.e2fsprogs pkgs.util-linux ];
  };

  testScript = ''
    start_all()
    machine.wait_for_unit("multi-user.target")

    # Format and mount the persistent disk that will hold the spool file.
    machine.succeed("mkfs.ext4 -F /dev/vdb")
    machine.succeed("mkdir -p /mnt/spool-disk")
    machine.succeed("mount /dev/vdb /mnt/spool-disk")

    # Launch the harness in the background, armed via the marker env var.
    # It builds one synthetic event, writes the spool temp file, fsyncs it,
    # renames it into place, then (because the marker env var is set)
    # writes the marker file and blocks forever — never reaching the
    # parent-directory fsync below that point in the real code.
    machine.succeed(
      "SINEX_TEST_SPOOL_RENAME_MARKER=/mnt/spool-disk/marker "
      "${sinex}/bin/spool_rename_crash_harness /mnt/spool-disk/spool.jsonl "
      "> /tmp/harness.log 2>&1 & disown"
    )

    # Wait for the marker: proves the rename completed and the harness is
    # now paused exactly at the fsync-not-yet-run point.
    machine.wait_for_file("/mnt/spool-disk/marker", timeout=30)

    # Real power-cut. Anything dirty in the guest's page cache — including
    # ext4 metadata for /dev/vdb that the (never-reached) fsync_dir would
    # have flushed — is discarded here, not just the harness process.
    machine.crash()
    machine.start()

    machine.wait_for_unit("multi-user.target")

    # The root filesystem is ephemeral (tmpfs) and resets completely on
    # every boot; only /dev/vdb itself (an emptyDiskImages-backed qcow2)
    # persisted across the crash. Recreate the mount point before mounting.
    print("blkid before fsck:", machine.succeed("blkid /dev/vdb || true"))
    machine.succeed("fsck -f -y /dev/vdb || true")
    print("blkid after fsck:", machine.succeed("blkid /dev/vdb || true"))
    machine.succeed("mkdir -p /mnt/spool-disk")
    machine.succeed("mount -t ext4 /dev/vdb /mnt/spool-disk")

    result = machine.succeed(
      "test -f /mnt/spool-disk/spool.jsonl && echo PRESENT || echo ABSENT"
    ).strip()
    print(f"sinex-r6d.9 spool rename-before-fsync durability result: {result}")

    with subtest("record the durability result for sinex-r6d.5/r6d.9 evidence"):
      machine.succeed(f"echo '{result}' > /tmp/spool-durability-result.txt")
  '';
}
