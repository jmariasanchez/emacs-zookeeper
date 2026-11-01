//! Counting semaphore with N leases (Curator `InterProcessSemaphoreV2` layout).
//!
//! Layout under `{base}`:
//!
//! * `{base}/locks/`: an internal [`Mutex`] serialising acquirers.
//! * `{base}/leases/_c_{uuid}-lease-{seq}`: one protected ephemeral-sequential
//!   node per held lease.
//!
//! An acquirer takes the internal lock, creates its lease node, then waits
//! (still holding the lock) until the number of lease nodes is `<= max_leases`;
//! then it releases the lock. Holding the lock while waiting is what makes the
//! count check race-free, exactly as in Curator.
//!
//! Each [`Lease`] is bound to the session that created it and carries an
//! [`Ownership`]. When the session expires the lease node vanishes and
//! another process may take the permit.
//!
//! Differences vs Curator: `max_leases` is fixed per instance (no
//! `SharedCountReader` for dynamic limits); all participants must agree on it.

use std::time::Duration;

use tokio::time::Instant;
use zk_core::{raw as zk, Acl, ErrorKind, Result, ZkClient, ZkError};

use crate::common::validate_base;
use crate::locks::Mutex;
use crate::ownership::{Ownership, SuspendPolicy};
use crate::pinned::{ContenderCleanup, Pinned};

/// Lease node name used by Curator.
pub const LEASE_NAME: &str = "lease-";

/// A counting semaphore.
#[derive(Clone, Debug)]
pub struct Semaphore {
    client: ZkClient,
    base: String,
    max_leases: usize,
    data: Vec<u8>,
    policy: SuspendPolicy,
}

/// A held permit. Dropping it returns the permit in the background.
#[derive(Debug)]
pub struct Lease {
    pinned: Pinned,
    path: String,
    ownership: Ownership,
    released: bool,
}

impl Lease {
    pub fn ownership(&self) -> &Ownership {
        &self.ownership
    }

    pub fn node_path(&self) -> &str {
        &self.path
    }

    /// Returns the permit.
    pub async fn release(mut self) -> Result<()> {
        self.released = true;
        self.ownership.mark_released();
        match self.pinned.delete_if_exists(&self.path).await {
            Err(e) if e.kind.is_session_terminal() => Ok(()),
            r => r,
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if !self.released {
            self.ownership.mark_released();
            if self.pinned.is_alive() {
                self.pinned.spawn_delete(self.path.clone());
            }
        }
    }
}

impl Semaphore {
    pub fn new(client: &ZkClient, base: &str, max_leases: usize) -> Result<Semaphore> {
        validate_base(base)?;
        if max_leases == 0 {
            return Err(ZkError::bad_arguments("max_leases must be positive"));
        }
        Ok(Semaphore { client: client.clone(), base: base.to_string(), max_leases, data: Vec::new(), policy: SuspendPolicy::default() })
    }

    pub fn with_data(mut self, data: &[u8]) -> Self {
        self.data = data.to_vec();
        self
    }

    pub fn with_policy(mut self, policy: SuspendPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn max_leases(&self) -> usize {
        self.max_leases
    }

    fn leases_dir(&self) -> String {
        format!("{}/leases", self.base)
    }

    /// Acquires one lease.
    pub async fn acquire(&self, timeout: Option<Duration>) -> Result<Lease> {
        let mut v = self.acquire_many(1, timeout).await?;
        Ok(v.pop().expect("one lease"))
    }

    /// Acquires `n` leases (all or nothing: on failure acquired ones are returned).
    pub async fn acquire_many(&self, n: usize, timeout: Option<Duration>) -> Result<Vec<Lease>> {
        if n == 0 || n > self.max_leases {
            return Err(ZkError::bad_arguments(format!("cannot acquire {n} of {} leases", self.max_leases)));
        }
        let deadline = timeout.map(|t| Instant::now() + t);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let remaining = deadline.map(|d| d.saturating_duration_since(Instant::now()));
            out.push(self.acquire_one(remaining).await?);
        }
        Ok(out)
    }

    async fn acquire_one(&self, timeout: Option<Duration>) -> Result<Lease> {
        let deadline = timeout.map(|t| Instant::now() + t);
        let lock = Mutex::new(&self.client, &format!("{}/locks", self.base))?;
        let guard = lock.acquire(timeout).await?;
        let pinned = Pinned::current(&self.client).await?;
        let dir = self.leases_dir();
        self.client.ensure_path(&dir, &Acl::anyone_all()).await?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let mut cleanup = ContenderCleanup { pinned: pinned.clone(), dir: dir.clone(), uuid: uuid.clone(), path: None, armed: true };
        let (path, _) = pinned.create_protected(&dir, LEASE_NAME, &self.data, zk::CreateMode::EphemeralSequential, &uuid).await?;
        cleanup.path = Some(path.clone());
        let name = path.rsplit('/').next().unwrap_or_default().to_string();
        loop {
            let (children, w) = pinned.watch_children(&dir).await?;
            let leases: Vec<&String> = children.iter().filter(|c| c.contains(LEASE_NAME)).collect();
            if !leases.contains(&&name) {
                return Err(ZkError::new(ErrorKind::Inconsistent, format!("lease node {path} vanished")));
            }
            if leases.len() <= self.max_leases {
                break;
            }
            if !pinned.wait_watch(w, deadline).await? {
                return Err(ZkError::timeout("semaphore acquisition"));
            }
        }
        cleanup.armed = false;
        let ownership = Ownership::track(&self.client, pinned.session().clone(), self.policy);
        // Lease node must share the guard's session for the count to be safe;
        // both are pinned from the client's current session. If they differ the
        // lock session already expired, which only frees others, never over-admits.
        drop(guard);
        Ok(Lease { pinned, path, ownership, released: false })
    }

    /// Node names (and data) of currently held leases.
    pub async fn leases(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let dir = self.leases_dir();
        let children = match self.client.children(&dir).await {
            Ok(c) => c,
            Err(e) if e.is(ErrorKind::NoNode) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for c in children.into_iter().filter(|c| c.contains(LEASE_NAME)) {
            match self.client.get_data(&format!("{dir}/{c}")).await {
                Ok(d) => out.push((c, d.data)),
                Err(e) if e.is(ErrorKind::NoNode) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }
}
