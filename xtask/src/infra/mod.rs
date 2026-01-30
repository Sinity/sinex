//! Infrastructure management (no database dependencies).

pub mod stack;
pub mod state;

pub use stack::{StackConfig, StackStatus};
pub use state::CheckoutState;
