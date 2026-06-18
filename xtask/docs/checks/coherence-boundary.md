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

Dependency-hygiene docs are checked beside the `xtask deps` behavior they rely
on, not through this forbidden-pattern scan. Use
`xtask test -p xtask -E 'test(test_dependency_hygiene_doc_matches_duplicate_classifier)'`
when changing duplicate-classification docs or output.
