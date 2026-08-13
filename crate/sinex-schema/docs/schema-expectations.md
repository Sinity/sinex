# Schema expectation interpreters

`sinex-schema` has one typed expectation substrate for the catalog surfaces
that must be drift-proof:

- table definitions provide columns and inline generated/default contracts;
- the convergence registry provides named CHECK and foreign-key declarations;
- schema definitions provide exact index definitions;
- managed trigger metadata provides timing, event mask, row/statement mode,
  `WHEN`, enabled state, and target function;
- trigger-function source provides normalized body hashes.

`expectation::check_catalog` is currently a standalone verify-interpreter
vertical slice. Its focused tests exercise the catalog-comparison contract;
the production `apply::diff` and `strict_diff::check_strict` surfaces still use
their established checks while the remaining PostgreSQL normalization work is
completed. Convergence continues to use the existing registry for forward DDL.
Wiring this interpreter into the live drift gates is a follow-up gate, not an
implicit claim made by this slice.

The first migrated exact surfaces are columns, named CHECKs, foreign keys,
indexes, managed triggers, and trigger functions. Index comparisons use
`pg_get_indexdef`, CHECK/FK comparisons use `pg_get_constraintdef`, columns
use `format_type` plus generated/default/collation catalog data, and function
comparisons use normalized `pg_proc.prosrc` hashes. Same-name objects are not
considered clean merely because their names survive.

## Ratified low-blast decisions (`sinex-cv2`)

- Excess grants: report-only; never auto-revoke.
- Extension versions: declare/report minimum-version drift; upgrades remain a
  deployment action.
- PostgreSQL enums: explicit non-goal; domain enums remain Rust-side text
  contracts. The `CREATE TYPE ... AS ENUM` pattern is a forbidden schema-def
  tripwire when the forbidden-pattern catalog is extended for this surface.
- Collations: explicit non-goal for the single-host deployment. Reopen when a
  satellite host or a user-text ordering contract exists.
- Extra schemas/tables inside managed schemas: report-only; never auto-drop.

These choices are visibility and safety decisions, not a claim that those
surfaces can never become contracts.
