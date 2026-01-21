# Loop 022 - Next Analysis Ideas

- Audit `EventBuilder` call sites for hard-coded sources/event types not covered by schemas.
- Extend schema drift scripts to parse `define_event_payload!` macros and re-evaluate registry diffs.
- Check external tooling or deployment notes for legacy journald/system historical events.
