//! Forward upgrades: numbered, data-preserving steps, run in order by the
//! explicit [`crate::apply`] in one transaction. Step `n` of [`STEPS`] moves
//! a database from version `LANDED_VERSION + n` to the next; the schema
//! description states the shape after the last one, and a fresh database is
//! created directly at [`crate::VERSION`].

/// One named forward step and the statements that make it.
#[derive(Clone, Copy)]
pub(crate) struct Step {
    pub name: &'static str,
    pub statements: fn() -> Vec<String>,
}

/// The oldest version this build upgrades from: the version of every
/// deployed database when the steps before it were retired.
pub(crate) const LANDED_VERSION: i32 = 40;

/// The steps after [`LANDED_VERSION`], in landing order.
pub(crate) const STEPS: &[Step] = &[];
