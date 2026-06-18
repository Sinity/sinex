# Operator Surfaces

Sinex exposes several operator-facing surfaces other than the read-only MCP
server. They are not interchangeable: each has a distinct authority class, a
distinct latency budget, and a distinct relationship to durable runtime state.
Confusing the *contract* of these surfaces with the *UX* of any particular
build is how aspirational sketches turn into shipped surfaces that quietly
violate the runtime boundary they were meant to respect.

This record fixes the long-lived contract. Concrete TUI screens, panel
layouts, key maps, and visual polish belong to the UX-MK3 program
(#1438–#1443) and to per-issue design notes; they are intentionally not
duplicated here.

## Scope And Non-Scope

Owned by this record:

- which operator surfaces exist beyond MCP read access;
- what authority class each surface holds;
- how each surface relates to the runtime planes defined in
  `runtime-target-boundaries.md`;
- which surfaces may issue writes and through which authority spine they must
  flow.

Not owned by this record:

- TUI panel layouts, screen compositions, key chords, color schemes — those
  are UX-MK3 territory.
- MCP tool contracts and the MCP read role — owned by
  `mcp-readonly-server.md`.
- `xtask` vs `sinexctl` vs Rust tests vs VM tests command ownership — owned
  by `runtime-target-boundaries.md`.
- Proposal/judgment authority for write-like operations — owned by
  `proposal-judgment-finalizer.md`.

## Surfaces

| Surface | Authority class | Plane | Notes |
| --- | --- | --- | --- |
| `sinexctl` (CLI) | Live runtime operation through the gateway | Deployed host runtime | Authoritative operator surface for events, runtime, operations, replay, lifecycle, DLQ, privacy, and source materials. |
| TUI workbench (`sinexctl tui`) | View layer over `sinexctl` and the MCP read role; write actions tunnel through gateway authority | Deployed host runtime | UX-MK3 program owns the design surface (#1438–#1443). |
| Shell integration (prompt, aliases, completions) | Read-only adornment of the operator's shell session | Operator workstation, outside Sinex runtime | Pulls cached status; must not become a write path. |
| Hyprland keybinds | Launchers for the surfaces above | Operator workstation, outside Sinex runtime | Bind to existing `sinexctl`/TUI commands; do not encode bespoke logic. |
| MCP read-only server | Agent-facing read role | Deployed host runtime | Owned by `mcp-readonly-server.md`. |

`sinexctl` is the only one that holds authority to act on live runtime. Every
other surface either renders data it does not own, or fires commands that
ultimately flow through `sinexctl` (or its underlying gateway RPCs) and the
proposal/judgment spine when the action would mutate canonical state.

## Authority Rules

1. **Write actions terminate at the gateway.** TUI buttons, key chords, and
   shell aliases that look like they mutate state must dispatch through the
   same RPCs that `sinexctl` uses. There is no second mutation path.
2. **Write-like agent actions become proposals.** Any operator surface that
   surfaces an agent-driven action must record it as a proposal per
   `proposal-judgment-finalizer.md`; it does not promote to canonical state
   on its own.
3. **Shell and Hyprland surfaces are read-shaped by default.** Adding a
   write-capable shortcut to either surface is a design change that needs the
   same review as a new gateway RPC. Prompt adornments and tab completion are
   not exempt because they happen "in the terminal".
4. **Privacy mode is opaque to view surfaces.** TUI/shell surfaces render
   redacted content the same way `sinexctl` does; they do not bypass redaction
   by reading from local caches. Privacy enforcement remains owned by
   `runtime-private-mode.md`.
5. **Runtime target attribution is preserved.** Per
   `runtime-target-boundaries.md`, every surface that renders runtime signals
   must show which target it probed. The TUI must not silently merge
   checkout-local and deployed-host views.

## Relation To The UX-MK3 Program

UX-MK3 (#1438–#1443) is the active design surface for the workbench TUI:

- #1438 fixes the shared view DTO spine (`ViewEnvelope`, `SinexObjectRef`,
  `ActionAvailability`, `EventCardView`) so CLI, TUI, and MCP read the same
  projections.
- #1439 builds the TUI workbench shell over those DTOs.
- #1440 covers the event inspector and copy/action system.
- #1441 covers the source-readiness cockpit.
- #1442 covers the operations room: replay, DLQ, snapshot, lifecycle, and
  privacy authority grammar.
- #1443 covers fixture and visual smoke coverage.

This record's job is to ensure that whatever UX-MK3 ships respects the
authority contract above. If a UX-MK3 panel wants to issue a write, it does
so through the same gateway authority `sinexctl` uses; if it wants to render
runtime signals, it must do so with target attribution; if it wants to embed
agent-proposed actions, those actions are proposals first.

When UX-MK3 closes, the per-panel ergonomics belong in issue threads and
crate-level docs, not in this record.

## Shell And Hyprland Surfaces

These surfaces are best understood as ergonomic shortcuts that launch the
authoritative surfaces — not as separate substrates.

- Prompt segments should read cached status emitted by the bare `sinexctl`
  command center, `sinexctl runtime health`, or the MCP read role. They must degrade silently when no runtime is reachable;
  they must not synthesize state.
- Tab completions should derive their option lists from the runtime
  (registered sources, schema-known event types, known material ids,
  registered source units, cached graph entities) rather than maintaining
  parallel registries.
- Hyprland keybinds bind to `sinexctl` and the TUI; they should not embed
  pipelines that bypass redaction, hold credentials, or write directly to
  CAS, gateway, or NATS.
- Aspirational keybind sets in target-vision references are illustrative.
  Adopting one requires an active issue and a written contract for what it
  invokes.

## Verification Expectations

An operator-surface change is complete only when:

- the surface's authority class is explicit (read-only view, launcher,
  write-through-gateway);
- write-capable interactions are exercised through the same RPCs as
  `sinexctl`, with tests that cover gateway authority enforcement;
- target attribution is preserved in any rendered status;
- privacy classification is honored without bypassing the redactor;
- agent-driven actions enter as proposals and are not promoted by the
  surface itself;
- shell/Hyprland sketches are gated on an active issue when they introduce a
  new shape rather than alias an existing command.

## Boundaries

- Do not redefine command ownership here; that belongs to
  `runtime-target-boundaries.md`.
- Do not redefine MCP semantics here; that belongs to
  `mcp-readonly-server.md`.
- Do not encode TUI panel layouts or visual grammar here; UX-MK3 owns the
  living design.
- Do not let shell adornments or Hyprland keybinds become a parallel
  mutation path or a parallel redaction policy.
