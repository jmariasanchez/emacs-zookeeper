//! `zk-recipes`: distributed coordination recipes over `zk-core`.
//!
//! Every recipe that holds something on behalf of a process (locks,
//! leadership, permits, registrations, leases) is bound to a pinned
//! [`zk_core::Session`] and exposes an [`Ownership`] whose state follows the
//! session: see [`ownership`]. Losing the session always means losing
//! ownership.

pub mod common;
pub mod ownership;

pub use ownership::{Ownership, OwnershipState, SuspendPolicy};
