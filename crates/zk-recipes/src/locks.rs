//! Distributed locks: [`Mutex`], [`ReentrantMutex`] and [`ReadWriteLock`].
//!
//! # Node layout (Curator compatible)
//!
//! Contenders create *protected* ephemeral-sequential nodes
//! `{base}/_c_{uuid}-lock-{seq}` exactly like Curator's
//! `InterProcessMutex`, so Rust and Java processes contend for the same lock.
//! The read/write lock uses `_c_{uuid}-__READ__{seq}` and
//! `_c_{uuid}-__WRIT__{seq}` like `InterProcessReadWriteLock`.
//!
//! The contender with the lowest sequence holds the lock; every other
//! contender watches only its predecessor (no herd effect).
//!
//! # Session semantics
//!
//! Every acquisition is pinned to one ZooKeeper session (see
//! [`crate::pinned`]). The returned [`LockGuard`] exposes an [`Ownership`]
//! which becomes `Lost` when that session expires: from that moment another
//! contender may acquire the lock, so protected work must stop (`select!` on
//! [`Ownership::lost`]). With [`SuspendPolicy::LoseOnSuspend`] ownership is
//! already considered lost on disconnection, which is the conservative
//! choice when the protected resource cannot tolerate overlap.
//!
//! If the create request's outcome is unknown (connection lost after it was
//! sent) the contender node is located by its uuid prefix instead of creating
//! a second one (Curator's "protection" mode).
//!
//! # Cancellation
//!
//! `acquire` is cancellation safe: dropping the future (or a timeout) deletes
//! the contender node in the background. Dropping a [`LockGuard`] releases the
//! lock in the background; call [`LockGuard::release`] to wait for it.
//!
//! # Differences vs Curator
//!
//! * Reentrancy is per [`ReentrantMutex`] *instance* (Curator: per thread).
//!   Tokio tasks migrate between threads, so thread identity is meaningless.
//! * Lock revocation (`Revocable`) and custom lock drivers are not provided.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::time::Instant;
use zk_core::{raw as zk, Acl, ErrorKind, Result, ZkClient, ZkError};

use crate::common::{sort_by_sequence, validate_base};
use crate::ownership::{Ownership, SuspendPolicy};
use crate::pinned::{ContenderCleanup, Pinned};

/// Lock name used by Curator's `InterProcessMutex`.
pub const LOCK_NAME: &str = "lock-";
/// Read lock name used by Curator's `InterProcessReadWriteLock`.
pub const READ_LOCK_NAME: &str = "__READ__";
/// Write lock name used by Curator's `InterProcessReadWriteLock`.
pub const WRITE_LOCK_NAME: &str = "__WRIT__";

/// One contender of a lock-like recipe, for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Participant {
    /// Node name (`_c_{uuid}-lock-0000000003`).
    pub node: String,
    pub sequence: i64,
    /// Node data (Curator stores the participant id / host here).
    #[serde(with = "zk_core::b64")]
    pub data: Vec<u8>,
    /// Session that owns the ephemeral node (`0x...`).
    pub session_id: String,
    /// Whether this contender currently holds the lock / leadership.
    pub is_owner: bool,
}

/// What the contender at `ours` must do given the ordered contenders.
pub(crate) enum Turn {
    Acquired,
    /// Watch the contender at this index and re-check when it changes.
    Wait(usize),
}

pub(crate) struct Acquired {
    pub pinned: Pinned,
    pub path: String,
    pub ownership: Ownership,
}

/// Generic "sequential ephemeral contender" acquisition used by locks, the
/// read/write lock and leader election.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn acquire_sequential(
    client: &ZkClient,
    dir: &str,
    lock_name: &str,
    data: &[u8],
    policy: SuspendPolicy,
    deadline: Option<Instant>,
    filter: impl Fn(&str) -> bool,
    turn: impl Fn(&[(i64, String)], usize) -> Turn,
) -> Result<Acquired> {
    let pinned = Pinned::current(client).await?;
    client.ensure_path(dir, &Acl::anyone_all()).await?;
    let uuid = uuid::Uuid::new_v4().to_string();
    let mut cleanup = ContenderCleanup { pinned: pinned.clone(), dir: dir.to_string(), uuid: uuid.clone(), path: None, armed: true };
    let (path, _) = pinned.create_protected(dir, lock_name, data, zk::CreateMode::EphemeralSequential, &uuid).await?;
    cleanup.path = Some(path.clone());
    let name = path.rsplit('/').next().unwrap_or_default().to_string();
    loop {
        let children = pinned.children(dir).await?;
        let sorted = sort_by_sequence(&children, &filter);
        let Some(ours) = sorted.iter().position(|(_, n)| *n == name) else {
            return Err(ZkError::new(ErrorKind::Inconsistent, format!("contender node {path} vanished")).with_path(&path));
        };
        match turn(&sorted, ours) {
            Turn::Acquired => break,
            Turn::Wait(i) => {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    return Err(ZkError::timeout("lock acquisition"));
                }
                let watch_path = format!("{dir}/{}", sorted[i].1);
                let (stat, w) = pinned.watch_exists(&watch_path).await?;
                if stat.is_none() {
                    continue;
                }
                if !pinned.wait_watch(w, deadline).await? {
                    return Err(ZkError::timeout("lock acquisition"));
                }
            }
        }
    }
    cleanup.armed = false;
    let ownership = Ownership::track(client, pinned.session().clone(), policy);
    Ok(Acquired { pinned, path, ownership })
}

/// Lists contenders under `dir` matching `filter`, ordered by sequence.
/// `owner` decides, given the ordered list and an index, whether that
/// contender currently owns the recipe.
pub(crate) async fn list_participants(
    client: &ZkClient,
    dir: &str,
    filter: impl Fn(&str) -> bool,
    owner: impl Fn(&[(i64, String)], usize) -> bool,
) -> Result<Vec<Participant>> {
    let children = match client.children(dir).await {
        Ok(c) => c,
        Err(e) if e.is(ErrorKind::NoNode) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let sorted = sort_by_sequence(&children, filter);
    let mut out = Vec::with_capacity(sorted.len());
    for (i, (seq, name)) in sorted.iter().enumerate() {
        match client.get_data(&format!("{dir}/{name}")).await {
            Ok(d) => out.push(Participant {
                node: name.clone(),
                sequence: *seq,
                data: d.data,
                session_id: format!("{:#x}", d.stat.ephemeral_owner),
                is_owner: owner(&sorted, i),
            }),
            Err(e) if e.is(ErrorKind::NoNode) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

fn deadline_of(timeout: Option<Duration>) -> Option<Instant> {
    timeout.map(|t| Instant::now() + t)
}

/// Guard for a held lock. Dropping it releases the lock in the background.
#[derive(Debug)]
pub struct LockGuard {
    pinned: Pinned,
    path: String,
    ownership: Ownership,
    released: bool,
}

impl LockGuard {
    pub(crate) fn new(a: Acquired) -> LockGuard {
        LockGuard { pinned: a.pinned, path: a.path, ownership: a.ownership, released: false }
    }

    /// Ownership of the lock. Stop protected work when it is no longer held.
    pub fn ownership(&self) -> &Ownership {
        &self.ownership
    }

    /// Full path of our contender node.
    pub fn node_path(&self) -> &str {
        &self.path
    }

    /// Marks released and hands back the session + node path without deleting.
    pub(crate) fn into_parts(mut self) -> (Pinned, String) {
        self.released = true;
        self.ownership.mark_released();
        (self.pinned.clone(), std::mem::take(&mut self.path))
    }

    /// Releases the lock and waits for the node deletion.
    ///
    /// If the session already expired the node is gone and this succeeds.
    pub async fn release(mut self) -> Result<()> {
        self.released = true;
        self.ownership.mark_released();
        match self.pinned.delete_if_exists(&self.path).await {
            Err(e) if e.kind.is_session_terminal() || e.is(ErrorKind::SessionExpired) => Ok(()),
            r => r,
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if !self.released {
            self.ownership.mark_released();
            if self.pinned.is_alive() {
                self.pinned.spawn_delete(self.path.clone());
            }
        }
    }
}

/// Exclusive inter-process lock (Curator `InterProcessMutex` layout).
#[derive(Clone, Debug)]
pub struct Mutex {
    client: ZkClient,
    base: String,
    data: Vec<u8>,
    policy: SuspendPolicy,
}

impl Mutex {
    pub fn new(client: &ZkClient, base: &str) -> Result<Mutex> {
        validate_base(base)?;
        Ok(Mutex { client: client.clone(), base: base.to_string(), data: Vec::new(), policy: SuspendPolicy::default() })
    }

    /// Data stored in the contender node (shown by [`Mutex::participants`]).
    pub fn with_data(mut self, data: &[u8]) -> Self {
        self.data = data.to_vec();
        self
    }

    pub fn with_policy(mut self, policy: SuspendPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// Acquires the lock, waiting at most `timeout` (`None`: forever).
    /// Fails with [`ErrorKind::Timeout`] when the time runs out.
    pub async fn acquire(&self, timeout: Option<Duration>) -> Result<LockGuard> {
        let a = acquire_sequential(
            &self.client,
            &self.base,
            LOCK_NAME,
            &self.data,
            self.policy,
            deadline_of(timeout),
            |n| n.contains(LOCK_NAME),
            |_, ours| if ours == 0 { Turn::Acquired } else { Turn::Wait(ours - 1) },
        )
        .await?;
        Ok(LockGuard::new(a))
    }

    /// Tries to acquire without waiting.
    pub async fn try_acquire(&self) -> Result<Option<LockGuard>> {
        match self.acquire(Some(Duration::ZERO)).await {
            Ok(g) => Ok(Some(g)),
            Err(e) if e.is(ErrorKind::Timeout) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Contenders in acquisition order; the first is the owner.
    pub async fn participants(&self) -> Result<Vec<Participant>> {
        list_participants(&self.client, &self.base, |n| n.contains(LOCK_NAME), |_, i| i == 0).await
    }
}

struct ReentrantState {
    guard: Option<LockGuard>,
    count: usize,
}

/// Reentrant wrapper over [`Mutex`]: acquisitions through the same instance
/// (or its clones) while held only increment a counter. The ZooKeeper lock
/// is released when the last [`ReentrantGuard`] is dropped or released.
#[derive(Clone)]
pub struct ReentrantMutex {
    mutex: Mutex,
    state: Arc<std::sync::Mutex<ReentrantState>>,
    acquiring: Arc<tokio::sync::Mutex<()>>,
}

impl ReentrantMutex {
    pub fn new(mutex: Mutex) -> ReentrantMutex {
        ReentrantMutex {
            mutex,
            state: Arc::new(std::sync::Mutex::new(ReentrantState { guard: None, count: 0 })),
            acquiring: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Acquires (or re-enters) the lock.
    pub async fn acquire(&self, timeout: Option<Duration>) -> Result<ReentrantGuard> {
        let wait = async {
            let _serial = self.acquiring.lock().await;
            if let Some(o) = self.reenter() {
                return Ok(o);
            }
            let guard = self.mutex.acquire(timeout).await?;
            let ownership = guard.ownership().clone();
            let mut st = self.state.lock().unwrap();
            st.guard = Some(guard);
            st.count = 1;
            Ok::<_, ZkError>(ownership)
        };
        // Fast path for re-entry without queueing behind a waiting acquirer.
        let ownership = match self.reenter() {
            Some(o) => o,
            None => match timeout {
                Some(t) => tokio::time::timeout(t, wait).await.map_err(|_| ZkError::timeout("lock acquisition"))??,
                None => wait.await?,
            },
        };
        Ok(ReentrantGuard { state: self.state.clone(), ownership, released: false })
    }

    fn reenter(&self) -> Option<Ownership> {
        let mut st = self.state.lock().unwrap();
        let g = st.guard.as_ref()?;
        if g.ownership().state().is_terminal() {
            // Lost: drop the stale guard and acquire afresh.
            st.guard = None;
            st.count = 0;
            return None;
        }
        let o = g.ownership().clone();
        st.count += 1;
        Some(o)
    }

    /// Current hold count of this instance.
    pub fn hold_count(&self) -> usize {
        self.state.lock().unwrap().count
    }

    pub fn inner(&self) -> &Mutex {
        &self.mutex
    }
}

/// One acquisition of a [`ReentrantMutex`].
pub struct ReentrantGuard {
    state: Arc<std::sync::Mutex<ReentrantState>>,
    ownership: Ownership,
    released: bool,
}

impl ReentrantGuard {
    pub fn ownership(&self) -> &Ownership {
        &self.ownership
    }

    fn take_last(&mut self) -> Option<LockGuard> {
        if self.released {
            return None;
        }
        self.released = true;
        let mut st = self.state.lock().unwrap();
        st.count = st.count.saturating_sub(1);
        if st.count == 0 {
            st.guard.take()
        } else {
            None
        }
    }

    /// Releases this hold; the last one releases the ZooKeeper lock.
    pub async fn release(mut self) -> Result<()> {
        match self.take_last() {
            Some(g) => g.release().await,
            None => Ok(()),
        }
    }
}

impl Drop for ReentrantGuard {
    fn drop(&mut self) {
        drop(self.take_last());
    }
}

/// Curator `InterProcessReadWriteLock`-compatible read/write lock.
///
/// Readers acquire when no *earlier* writer is queued; writers acquire when
/// they are the earliest node overall. This gives writer preference in
/// arrival order (no writer starvation).
///
/// Difference vs Curator: a write-lock holder cannot additionally take the
/// read lock through this API (Curator allows read-after-write reentrancy).
#[derive(Clone, Debug)]
pub struct ReadWriteLock {
    client: ZkClient,
    base: String,
    data: Vec<u8>,
    policy: SuspendPolicy,
}

fn is_rw(n: &str) -> bool {
    n.contains(READ_LOCK_NAME) || n.contains(WRITE_LOCK_NAME)
}

fn rw_owner(sorted: &[(i64, String)], i: usize) -> bool {
    if sorted[i].1.contains(WRITE_LOCK_NAME) {
        i == 0
    } else {
        !sorted[..i].iter().any(|(_, n)| n.contains(WRITE_LOCK_NAME))
    }
}

impl ReadWriteLock {
    pub fn new(client: &ZkClient, base: &str) -> Result<ReadWriteLock> {
        validate_base(base)?;
        Ok(ReadWriteLock { client: client.clone(), base: base.to_string(), data: Vec::new(), policy: SuspendPolicy::default() })
    }

    pub fn with_data(mut self, data: &[u8]) -> Self {
        self.data = data.to_vec();
        self
    }

    pub fn with_policy(mut self, policy: SuspendPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Acquires a shared read lock.
    pub async fn read(&self, timeout: Option<Duration>) -> Result<LockGuard> {
        let a = acquire_sequential(&self.client, &self.base, READ_LOCK_NAME, &self.data, self.policy, deadline_of(timeout), is_rw, |s, ours| {
            match s[..ours].iter().rposition(|(_, n)| n.contains(WRITE_LOCK_NAME)) {
                None => Turn::Acquired,
                Some(w) => Turn::Wait(w),
            }
        })
        .await?;
        Ok(LockGuard::new(a))
    }

    /// Acquires the exclusive write lock.
    pub async fn write(&self, timeout: Option<Duration>) -> Result<LockGuard> {
        let a = acquire_sequential(&self.client, &self.base, WRITE_LOCK_NAME, &self.data, self.policy, deadline_of(timeout), is_rw, |_, ours| {
            if ours == 0 {
                Turn::Acquired
            } else {
                Turn::Wait(ours - 1)
            }
        })
        .await?;
        Ok(LockGuard::new(a))
    }

    /// Readers and writers in queue order with their current ownership.
    pub async fn participants(&self) -> Result<Vec<Participant>> {
        list_participants(&self.client, &self.base, is_rw, rw_owner).await
    }
}

pub(crate) fn rw_is_owner(sorted: &[(i64, String)], i: usize) -> bool {
    rw_owner(sorted, i)
}
