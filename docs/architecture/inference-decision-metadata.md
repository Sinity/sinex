# Inference Decision Metadata

**Status:** dissolved into issue tracking. The substantive contract
that lived here — when to write inference metadata (non-obvious
decisions only, not deterministic 1:1 transforms), the
`InferenceDecisionMetadata` + `InferenceReplayPolicy` Rust shapes, the
`core.inference_decisions` side table, the model-effects boundary
table, the external-state-must-be-captured invariant, deterministic
seed derivation, the "confidence is not authority — weak inference
becomes a proposal" rule, the three fixture scenarios (entity match
confidence / deterministic selection / threshold change in shadow
lane), and the boundaries list — now lives in [issue #1118
(design(automata): record inference confidence seeds and decisions)](https://github.com/Sinity/sinex/issues/1118)
as a design comment.

`#1118` is the live tracking issue. The model-effect substrate
(separate concern) is `#1063`. The proposal/judgment/finalizer
substrate that authority-promotes weak inference is owned by
`docs/architecture/proposal-judgment-finalizer.md`. Shadow-lane
threshold changes route through `docs/architecture/semantic-epochs-shadow-lanes.md`.

**Related:** `docs/architecture/proposal-judgment-finalizer.md`,
`docs/architecture/semantic-epochs-shadow-lanes.md`,
`docs/architecture/prompt-router-budget.md`.
