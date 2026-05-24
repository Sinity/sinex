# Source material and Explore Bridge

The source material system is one of Sinex's most important UX opportunities. It is where raw ground truth becomes explainable, replayable events.

## Source material detail

A material card should show:

- material id
- source identifier/path
- format and shape
- size/hash
- first/last observed range
- parser/source-unit binding
- timing quality
- privacy tier and sampling policy
- natural key/dedup status
- readiness/caveats
- current emitted event families/types
- source anchors and example records
- replay/promote/archive actions with authority labels

## Staged material states

- staged but not inspected
- inspected shape ready
- parser missing
- parser candidate proposed
- simulation running
- simulation failed
- simulation complete with event previews
- parity check failed
- privacy review required
- ready to promote
- promoted/admitted
- archived

## Explore Bridge target

The target `explore inspect/propose/simulate/promote` workbench should not auto-install knowledge. It should make candidate interpretation auditable:

1. Inspect material shape and timestamps.
2. Propose parser/source-unit mapping.
3. Simulate events into a sandbox result.
4. Review event previews, anchors, privacy, dedup, timing quality, and drift risk.
5. Promote through normal authority/issue/PR/Nix flow.
6. Record audit evidence.

This is the UX version of the staged-source parser substrate. It should respect the architecture's distinction between source material, input-shape adapter, parser, source unit, runtime topology, and replay.
