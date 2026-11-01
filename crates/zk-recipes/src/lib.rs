//! `zk-recipes`: distributed coordination recipes over `zk-core`.
//!
//! Every recipe that holds something on behalf of a process (locks,
//! leadership, permits, registrations, leases) is bound to a pinned
//! [`zk_core::Session`] and exposes an [`Ownership`] whose state follows the
//! session: see [`ownership`]. Losing the session always means losing
//! ownership.
//!
//! Recipes:
//!
//! * [`locks`]: [`Mutex`], [`ReentrantMutex`], [`ReadWriteLock`] (Curator node layout).
//! * [`election`]: [`LeaderLatch`], [`LeaderElection`].
//! * [`semaphore`]: [`Semaphore`] (Curator `InterProcessSemaphoreV2` layout).
//! * [`barrier`]: [`Barrier`], [`DoubleBarrier`].
//! * [`counter`]: [`SharedCounter`] with Curator-compatible encodings.
//! * [`queue`]: [`DistributedQueue`], [`PriorityQueue`], [`LockingQueue`], delayed items.
//! * [`id`]: unique id generation.
//! * [`persistent_node`]: [`PersistentNode`] (survives session expiry).
//! * [`membership`] provides [`GroupMember`].
//! * [`discovery`]: Curator x-discovery compatible [`ServiceDiscovery`] and [`ServiceCache`].
//! * [`lease`]: [`PartitionAssigner`] (exclusive partition ownership).
//! * [`inspect`]: read-only structure detection for UIs.

pub mod barrier;
pub mod common;
pub mod counter;
pub mod discovery;
pub mod election;
pub mod id;
pub mod inspect;
pub mod lease;
pub mod locks;
pub mod membership;
pub mod ownership;
pub mod persistent_node;
pub mod pinned;
pub mod queue;
pub mod semaphore;

pub use barrier::{Barrier, DoubleBarrier};
pub use counter::{CounterEncoding, SharedCounter, VersionedValue};
pub use discovery::{ServiceCache, ServiceDiscovery, ServiceInstance, ServiceType, UriSpec};
pub use election::{LeaderElection, LeaderLatch};
pub use id::{CounterIdGenerator, SequentialIdGenerator};
pub use inspect::{inspect, Inspection};
pub use lease::{Assignment, PartitionAssigner};
pub use locks::{LockGuard, Mutex, Participant, ReadWriteLock, ReentrantGuard, ReentrantMutex};
pub use membership::GroupMember;
pub use ownership::{Ownership, OwnershipState, SuspendPolicy};
pub use persistent_node::{PersistentNode, PersistentNodeMode};
pub use pinned::Pinned;
pub use queue::{DistributedQueue, LockedItem, LockingQueue, PriorityQueue, QueueItem};
pub use semaphore::{Lease, Semaphore};

/// Alias: Curator calls the CAS counter `SharedCount` / `DistributedAtomicLong`.
pub type AtomicCounter = SharedCounter;
