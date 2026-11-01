//! Leader election: [`LeaderLatch`] and [`LeaderElection`] (selector style).
//!
//! Both use the lock layout of [`crate::locks`]: contenders create protected
//! ephemeral-sequential nodes under the election path and the lowest
//! sequence is the leader. [`LeaderLatch`] uses Curator's `latch-` node
//! name; [`LeaderElection`] uses `lock-` like Curator's `LeaderSelector`
//! (which is built on `InterProcessMutex`), so they interoperate with Java
//! participants on the same paths.
//!
//! # Session semantics
//!
//! Leadership is an [`Ownership`]: it is lost when the session that created
//! the contender node expires (or on suspension with
//! [`SuspendPolicy::LoseOnSuspend`]). A latch that loses leadership
//! automatically re-queues on the next session, like Curator's
//! `LeaderLatch` after `LOST`.
//!
//! # Differences vs Curator/Kazoo
//!
//! * [`LeaderElection::run`] cannot interrupt the leadership closure (Curator
//!   interrupts the thread). The closure receives the [`Ownership`] and must
//!   `select!` on [`Ownership::lost`] itself. The next leader may be elected
//!   as soon as the session is lost, before the closure returns.
//! * Latch participant ids are stored as node data (as in Curator).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zk_core::{ErrorKind, Result, ZkClient, ZkError};

use crate::common::validate_base;
use crate::locks::{acquire_sequential, list_participants, LockGuard, Participant, Turn, LOCK_NAME};
use crate::ownership::{Ownership, SuspendPolicy};

/// Node name used by Curator's `LeaderLatch`.
pub const LATCH_NAME: &str = "latch-";

fn first_wins(_: &[(i64, String)], ours: usize) -> Turn {
    if ours == 0 {
        Turn::Acquired
    } else {
        Turn::Wait(ours - 1)
    }
}

/// Releases a guard, retrying while the session is alive so that a stale
/// contender node never blocks the re-queued one.
pub(crate) async fn release_firmly(guard: LockGuard) {
    let (pinned, path) = guard.into_parts();
    for _ in 0..100 {
        match pinned.delete_if_exists(&path).await {
            Ok(()) => return,
            Err(e) if e.kind.is_session_terminal() => return,
            Err(_) if !pinned.is_alive() => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

struct LatchInner {
    leader: watch::Sender<Option<Ownership>>,
    cancel: CancellationToken,
    task: ::std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Curator-style leader latch: once started, the participant stays in the
/// election until closed, regaining leadership after session loss when its
/// turn comes again.
#[derive(Clone)]
pub struct LeaderLatch {
    client: ZkClient,
    path: String,
    id: String,
    policy: SuspendPolicy,
    inner: Arc<LatchInner>,
}

impl LeaderLatch {
    /// `id` is stored in the contender node (Curator's participant id).
    pub fn new(client: &ZkClient, path: &str, id: &str) -> Result<LeaderLatch> {
        validate_base(path)?;
        let (tx, _) = watch::channel(None);
        Ok(LeaderLatch {
            client: client.clone(),
            path: path.to_string(),
            id: id.to_string(),
            policy: SuspendPolicy::default(),
            inner: Arc::new(LatchInner { leader: tx, cancel: CancellationToken::new(), task: ::std::sync::Mutex::new(None) }),
        })
    }

    pub fn with_policy(mut self, policy: SuspendPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Joins the election (idempotent).
    pub fn start(&self) -> Result<()> {
        let mut task = self.inner.task.lock().unwrap();
        if self.inner.cancel.is_cancelled() {
            return Err(ZkError::new(ErrorKind::Closed, "leader latch closed"));
        }
        if task.is_some() {
            return Ok(());
        }
        let me = self.clone();
        *task = Some(tokio::spawn(async move { me.run_loop().await }));
        Ok(())
    }

    async fn run_loop(self) {
        let cancel = self.inner.cancel.clone();
        while !cancel.is_cancelled() {
            let acquire = acquire_sequential(
                &self.client,
                &self.path,
                LATCH_NAME,
                self.id.as_bytes(),
                self.policy,
                None,
                |n| n.contains(LATCH_NAME),
                first_wins,
            );
            let r = tokio::select! {
                _ = cancel.cancelled() => return,
                r = acquire => r,
            };
            match r {
                Ok(a) => {
                    let guard = LockGuard::new(a);
                    let own = guard.ownership().clone();
                    self.inner.leader.send_replace(Some(own.clone()));
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = own.lost() => {}
                    }
                    self.inner.leader.send_replace(None);
                    release_firmly(guard).await;
                }
                Err(e) => {
                    tracing::debug!(path = %self.path, error = %e, "leader latch: retrying");
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                }
            }
        }
    }

    /// Whether we are the leader right now (ownership `Held`).
    pub fn has_leadership(&self) -> bool {
        self.inner.leader.borrow().as_ref().is_some_and(|o| o.is_held())
    }

    /// Current leadership ownership, if leader.
    pub fn leadership(&self) -> Option<Ownership> {
        self.inner.leader.borrow().clone().filter(|o| !o.state().is_terminal())
    }

    /// Waits until we are leader. Fails with `Timeout` or `Closed`.
    pub async fn await_leadership(&self, timeout: Option<Duration>) -> Result<Ownership> {
        let mut rx = self.inner.leader.subscribe();
        let cancel = self.inner.cancel.clone();
        let fut = async {
            loop {
                if let Some(o) = rx.borrow_and_update().clone() {
                    if o.is_held() {
                        return Ok(o);
                    }
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Err(ZkError::new(ErrorKind::Closed, "leader latch closed")),
                    r = rx.changed() => if r.is_err() { return Err(ZkError::new(ErrorKind::Closed, "leader latch closed")) },
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
        };
        match timeout {
            Some(t) => tokio::time::timeout(t, fut).await.map_err(|_| ZkError::timeout("await_leadership"))?,
            None => fut.await,
        }
    }

    /// Subscribes to leadership changes (`Some` while leader).
    pub fn subscribe(&self) -> watch::Receiver<Option<Ownership>> {
        self.inner.leader.subscribe()
    }

    /// Leaves the election, deleting our node and relinquishing leadership.
    pub async fn close(&self) {
        self.inner.cancel.cancel();
        let task = self.inner.task.lock().unwrap().take();
        if let Some(t) = task {
            let _ = t.await;
        }
    }

    /// All participants in queue order (first is leader).
    pub async fn participants(&self) -> Result<Vec<Participant>> {
        list_participants(&self.client, &self.path, |n| n.contains(LATCH_NAME), |_, i| i == 0).await
    }

    /// The current leader, if any.
    pub async fn leader(&self) -> Result<Option<Participant>> {
        Ok(self.participants().await?.into_iter().next())
    }
}

/// Selector-style election: [`LeaderElection::run`] repeatedly waits for
/// leadership, runs the closure while leader, then relinquishes leadership
/// and re-queues (Curator `LeaderSelector` with `autoRequeue`).
#[derive(Clone)]
pub struct LeaderElection {
    client: ZkClient,
    path: String,
    id: String,
    policy: SuspendPolicy,
    cancel: CancellationToken,
}

impl LeaderElection {
    pub fn new(client: &ZkClient, path: &str, id: &str) -> Result<LeaderElection> {
        validate_base(path)?;
        Ok(LeaderElection {
            client: client.clone(),
            path: path.to_string(),
            id: id.to_string(),
            policy: SuspendPolicy::default(),
            cancel: CancellationToken::new(),
        })
    }

    pub fn with_policy(mut self, policy: SuspendPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Runs leadership terms until [`LeaderElection::close`] is called.
    ///
    /// For each term `f` receives the term's [`Ownership`]; when `f` returns,
    /// leadership is released and the participant re-queues. Returns `Ok(())`
    /// once closed (a running term is allowed to finish).
    pub async fn run<F, Fut>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Ownership) -> Fut,
        Fut: ::std::future::Future<Output = ()>,
    {
        while !self.cancel.is_cancelled() {
            let acquire = acquire_sequential(
                &self.client,
                &self.path,
                LOCK_NAME,
                self.id.as_bytes(),
                self.policy,
                None,
                |n| n.contains(LOCK_NAME),
                first_wins,
            );
            let r = tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                r = acquire => r,
            };
            match r {
                Ok(a) => {
                    let guard = LockGuard::new(a);
                    f(guard.ownership().clone()).await;
                    release_firmly(guard).await;
                }
                Err(e) if e.is(ErrorKind::Closed) || e.is(ErrorKind::AuthFailed) => return Err(e),
                Err(_) => {
                    tokio::select! {
                        _ = self.cancel.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Stops re-queueing; a waiting `run` returns promptly.
    pub fn close(&self) {
        self.cancel.cancel();
    }

    pub async fn participants(&self) -> Result<Vec<Participant>> {
        list_participants(&self.client, &self.path, |n| n.contains(LOCK_NAME), |_, i| i == 0).await
    }

    pub async fn leader(&self) -> Result<Option<Participant>> {
        Ok(self.participants().await?.into_iter().next())
    }
}
