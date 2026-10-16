# Task routing

This checkout routes current work to the external Beads owner at
`/realm/state/tasks/sinex-next`, using the `sxnext` prefix. Task records live
outside Git; the tracked metadata records the database selection.

Use the installed host `bd` with an explicit actor, for example:

```sh
bd --actor <name> -C /realm/state/tasks/sinex-next ready
```

Preserved `sinex-*` tasks remain owned by `/realm/state/tasks/sinex`. Use that
entrance when reading or updating those legacy IDs. Do not initialize another
database or export task records into this repository. Git commits do not sync
task state.
