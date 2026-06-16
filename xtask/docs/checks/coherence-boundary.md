# Coherence Boundary Check

`xtask check --forbidden` includes a bounded #1738/#1791 coherence check.

The check blocks first-party runtime/library `src` code from embedding the
operator's deployment-local `/realm` topology or Lynchpin product semantics.
Those belong in deployment examples, docs, NixOS wiring, source-specific
descriptors, or operator configuration, not in generic Sinex runtime behavior.

This slice intentionally excludes source-contract descriptors and
`ViewEnvelope` fixtures because those files are being handled in adjacent audit
lanes. Do not add new allowlist entries for ordinary runtime code; move concrete
paths and product-specific examples to scoped docs/config instead.

The same lint also keeps dependency-hygiene docs tied to the live duplicate
dependency vocabulary:

- use `direct_workspace` and `transitive_upstream` from
  `xtask deps duplicates --json`;
- do not keep claiming direct workspace duplicate debt is zero after the live
  duplicate report shows direct-workspace duplicates;
- do not reintroduce the removed direct-workspace/transitive duplicate boolean
  vocabulary that preceded the current classification field.
