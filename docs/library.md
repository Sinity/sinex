# Library records

Library intake scans configured roots through the ordinary source and
admission path. Copies become items and renditions; subsequent edits are
`library.request` records, each stored as a material before admission.
[The library crate](../crates/sinex-library/src/lib.rs) owns item identity,
curation, ranking and rendition rules; [the daemon adapter](../crates/sinexd/src/library.rs)
submits requests and reads the resulting events.

A copy's identity and its current path are separate: moving or revising a
file need not create another item. Library reads use the admitted records,
not an independently maintained library database. Hide and excision follow
the ordinary [removal rules](architecture.md#sealing-and-excision).

[Shown values](../crates/sinex-library/src/shown.rs) are selected by level:
an operator override wins over an import, which wins over a model proposal.
A live explicit empty override means empty; absence means unknown. Different
values at the deciding level are a conflict rather than a timestamp contest.
Withdrawn assignments do not contribute. Tags combine their supporting
assignments; an override can remove one lower-level tag or explicitly clear
all lower-level tags.

[Ranking](../crates/sinex-library/src/rank.rs) uses recorded pairwise
comparisons. [Rendition and opening code](../crates/sinex-library/src/render.rs)
owns how stored media is prepared and opened. These operations are separate
from the property-selection rule, so a proposed value never silently becomes
an operator override.
