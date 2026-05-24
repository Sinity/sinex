# States, components, and little details

## Global state matrix

Every view should render:

- initial loading
- refresh loading with previous data
- empty because no data exists
- empty because filter excludes all data
- partial because source coverage is incomplete
- stale because generated_at is old
- disconnected because gateway/RPC failed
- permission denied
- private/redacted
- source gap
- continuity gap
- parser drift
- material missing
- replay overlay
- late evidence arrived
- operation pending
- operation failed
- target-only feature
- unsupported projection

## Core components

- View header
- Runtime target chip
- Freshness chip
- Privacy chip
- Caveat chip stack
- Object ref pill
- Source family badge + raw source label
- Event card
- Payload renderer
- Raw JSON panel
- Trace tree
- Source material anchor
- Action menu
- Copy menu
- Disabled reason tooltip/panel
- Selection basket
- Timeline lane
- Gap overlay
- Operation run card
- Context pack item
- Agent projection resource

## UX copy rules

Use specific cause-oriented copy:

- “No events matched this query” instead of “No data”
- “Browser source stale since 14:05” instead of “Warning”
- “Private mode suppressed content; metadata retained” instead of “Hidden”
- “Target-only: context packs are tracked by #1095” instead of a dead button
- “Dry-run required before execute” instead of just disabling execute

## Microinteractions

- Copy actions show short success toast and preserve focus.
- Disabled actions open an explanation panel on Enter/Space.
- Refresh preserves selected object when still present.
- A stale view shows “refreshing…” without blanking existing data.
- Dangerous actions require typing a short confirmation token or selecting an explicit confirm step.
- Payload panels remember fold state per event while the TUI session is open.
- Raw JSON can be copied even when pretty renderer fails.
