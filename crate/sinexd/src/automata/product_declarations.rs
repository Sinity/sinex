//! Startup reconciler: populate `derivation.product_declarations` from every
//! registered automaton's static `AutomatonSpec::outputs` (sinex-x79t, split
//! out of sinex-0vx.5).
//!
//! `derivation.enforce_event_product_declaration()` (sinex-0vx.4) rejects any
//! `core.events`/`reflection.events` row whose `product_class` isn't matched
//! by a `derivation.product_declarations` row keyed on
//! `derivation_declaration_id`. Every automaton already declares its outputs
//! in code via `AutomatonSpec.outputs` (forwarded from
//! `DeclaresOutputs::OUTPUT_DECLARATIONS`), but nothing wrote that static
//! source of truth into the DB table the trigger actually checks — meaning
//! every automaton in a freshly-applied schema is structurally unable to
//! persist a single derived event. This module closes that gap by running
//! once at `sinexd` startup (`Supervisor::run`, after the event engine is
//! confirmed ready and before any automaton spawns) and upserting each
//! declaration.
//!
//! Fail-closed by design: an existing DB row that disagrees with the static
//! declaration for the same `declaration_id` is a startup error, not a
//! silent overwrite or silent skip — code and DB must never quietly drift
//! for a security-relevant enforcement table like this one. Curation/
//! non-automaton declarations (sinex-q46n) are explicitly out of scope; this
//! reconciler only ever writes rows for `AutomatonSpec` entries.

use sinex_db::DbPool;
use sinex_db::repositories::{DbPoolExt, ExistingProductDeclaration};
use sinex_primitives::derivation::DerivationOutputDeclaration;
use sinex_primitives::error::{Result, SinexError};
use tracing::info;

use super::registry::AutomatonSpec;

/// Reconcile `derivation.product_declarations` against every declaration
/// reachable from `automata`. Returns the number of declarations newly
/// inserted (rows that already matched are left untouched).
///
/// Fails closed on the first `declaration_id` whose static shape disagrees
/// with an existing DB row — see module docs.
pub async fn reconcile_product_declarations(
    pool: &DbPool,
    automata: &[AutomatonSpec],
) -> Result<usize> {
    let mut inserted = 0usize;

    for spec in automata {
        for declaration in spec.outputs {
            declaration.validate().map_err(|error| {
                SinexError::configuration(format!(
                    "automaton '{}' declares an invalid product declaration '{}': {error}",
                    spec.name, declaration.declaration_id
                ))
            })?;

            if reconcile_one(pool, spec.name, declaration).await? {
                inserted += 1;
            }
        }
    }

    info!(
        automata = automata.len(),
        inserted, "reconciled derivation.product_declarations from static automaton registry"
    );

    Ok(inserted)
}

/// Reconcile a single declaration. Returns `Ok(true)` if a new row was
/// inserted, `Ok(false)` if an existing row already matched, and `Err` if an
/// existing row disagrees with the static declaration.
async fn reconcile_one(
    pool: &DbPool,
    automaton_name: &'static str,
    declaration: &DerivationOutputDeclaration,
) -> Result<bool> {
    let repo = pool.product_declarations();

    let Some(existing) = repo
        .find_by_declaration_id(declaration.declaration_id)
        .await?
    else {
        repo.insert(declaration).await?;
        return Ok(true);
    };

    match declaration_matches(declaration, &existing) {
        Ok(()) => Ok(false),
        Err(mismatch) => Err(SinexError::configuration(format!(
            "automaton '{automaton_name}' static declaration '{}' conflicts with the \
             existing derivation.product_declarations row: {mismatch}. Refusing to start \
             rather than silently overwrite or silently accept drift — reconcile the code \
             declaration or the DB row by hand.",
            declaration.declaration_id
        ))),
    }
}

/// Compare a static declaration against the DB row already registered for
/// its `declaration_id`. `Err(String)` names the first field that disagrees.
fn declaration_matches(
    declaration: &DerivationOutputDeclaration,
    existing: &ExistingProductDeclaration,
) -> std::result::Result<(), String> {
    let default_claim_support = serde_json::to_value(declaration.default_support)
        .map_err(|error| format!("default_support could not be serialized: {error}"))?;

    macro_rules! check {
        ($field:expr, $expected:expr, $actual:expr) => {
            if $expected != $actual {
                return Err(format!(
                    "{} differs (code declares {:?}, DB has {:?})",
                    $field, $expected, $actual
                ));
            }
        };
    }

    check!("owner", declaration.owner, existing.owner);
    check!(
        "product_class",
        declaration.product_class.as_str(),
        existing.product_class
    );
    check!(
        "write_surface",
        declaration.write_surface.as_str(),
        existing.write_surface
    );
    check!(
        "output_source",
        declaration.output_source,
        existing.output_source.as_deref()
    );
    check!(
        "output_event_type",
        declaration.output_event_type,
        existing.output_event_type.as_deref()
    );
    check!(
        "projection_kind",
        declaration.projection_kind,
        existing.projection_kind.as_deref()
    );
    check!(
        "artifact_kind",
        declaration.artifact_kind,
        existing.artifact_kind.as_deref()
    );
    check!(
        "proposal_kind",
        declaration.proposal_kind,
        existing.proposal_kind.as_deref()
    );
    check!(
        "semantics_version",
        declaration.semantics_version,
        existing.semantics_version
    );
    check!(
        "input_eligibility",
        declaration.input_eligibility.as_str(),
        existing.input_eligibility
    );
    check!(
        "default_claim_support",
        default_claim_support,
        existing.default_claim_support
    );
    check!(
        "verification_command",
        declaration.verification_command,
        existing.verification_command
    );

    Ok(())
}

#[cfg(test)]
#[path = "product_declarations_test.rs"]
mod product_declarations_test;
