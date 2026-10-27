# Moving session state

Session binding names include the input spelling. Preserve `session.stateDir`
when moving its storage directory: use a bind mount at that same path, rather
than changing the argument the producer receives. A new `--state` spelling
creates new binding names and positions even when its contents were copied.

For a host moving `~/.local/state/sinex-session` onto `/realm`, the activation
owner follows this drain-then-switch procedure during the coordinated producer
replacement:

1. Record the installed unit's exact `--state`, host and producer arguments.
   Preserve a private inventory and backup of that state directory, including
   `spool`, `positions` and any pending frames in the content store. Record the
   producer's registered materials and its saved cursors.
2. Stop the session unit and verify that its capture children have exited.
   Stopping capture leaves its finished spool segments available for reading.
3. With capture stopped, drain every included spool through its existing binding
   and producer identity: run the replacement `sinex-session run` binary with
   the installed unit's exact arguments plus `--drain` (omit `--seconds` if set).
   This command opens only existing spools, starts no captures, retries pending
   frames and exits after admission acknowledges the finished segments. Keep
   the same exclusions and credentials. Private mode refuses this operation.
   Exclusions are not permission to delete or transmit their queued bytes.
   If an included spool cannot drain, preserve it and hold the relocation.
4. Require the drain to finish successfully: it saves each position only after
   its frames are acknowledged. Verify that no unacknowledged-frame files remain
   for included bindings. Save the resulting positions and registered-material
   inventory. Producer cursors are kept by the producer; the server stores frame
   acknowledgments, not matching producer cursors. Keep the original state and
   private inventory until verification after restart is complete.
5. Copy the stopped state directory to its declared `/realm` destination,
   preserving ownership, modes and all files. Compare file hashes and position
   contents. Mount the destination at the original `--state` path before the
   replacement unit starts; keep the original contents outside the mount.
   Declare the mount and its ordering through the host configuration owner.
   Keep `session.stateDir` at its original spelling.
6. Run `--drain` again through the mounted path before starting captures. Check
   that the copied positions and binding names are unchanged and no finished
   records are sent again. Then start the replacement unit and verify its
   installed arguments and new admitted events. Existing names must continue
   from their saved positions. Confirm that writes reach the `/realm` mount.
   Keep the source copy until the activation owner accepts these observations;
   cleanup needs its own authorization.

A failed drain or a missing acknowledgment holds relocation. Keeping the current
state path is the deployable default.
