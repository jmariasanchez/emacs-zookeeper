//! Barriers: [`Barrier`] (Curator `DistributedBarrier`) and
//! [`DoubleBarrier`] (Curator `DistributedDoubleBarrier`).
//!
//! # Barrier
//!
//! The barrier is "up" while its node exists. [`Barrier::set`] creates it,
//! [`Barrier::remove`] deletes it, [`Barrier::wait_on_barrier`] blocks until
//! it is gone. The node is persistent (as in Curator), so a barrier survives
//! its creator.
//!
//! # Double barrier
//!
//! `member_count` participants call [`DoubleBarrier::enter`]; all are released
//! together once `member_count` ephemeral member nodes exist (signalled via a
//! `ready` node as in Curator). Each later calls [`DoubleBarrier::leave`],
//! which returns once all members have left. Member nodes are ephemeral and
//! pinned to the entering session: a member whose session expires is treated
//! as having left.
//!
//! Difference vs Curator: `enter` uses a protected node name
//! (`_c_{uuid}-{id}`); the member id is stored in node data. Curator uses
//! the raw UUID as the node name.

use std::time::Duration;

use tokio::time::Instant;
use zk_core::{raw as zk, Acl, CreateOptions, ErrorKind, Result, ZkClient, ZkError};

use crate::common::validate_base;
use crate::pinned::{ContenderCleanup, Pinned};

/// A single-node barrier.
#[derive(Clone, Debug)]
pub struct Barrier {
    client: ZkClient,
    path: String,
}

impl Barrier {
    pub fn new(client: &ZkClient, path: &str) -> Result<Barrier> {
        validate_base(path)?;
        Ok(Barrier { client: client.clone(), path: path.to_string() })
    }

    /// Raises the barrier (idempotent).
    pub async fn set(&self) -> Result<()> {
        match self.client.create(&self.path, b"", &CreateOptions::persistent().with_parents()).await {
            Ok(_) => Ok(()),
            Err(e) if e.is(ErrorKind::NodeExists) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Lowers the barrier (idempotent), releasing waiters.
    pub async fn remove(&self) -> Result<()> {
        match self.client.delete(&self.path, None).await {
            Ok(()) => Ok(()),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn is_set(&self) -> Result<bool> {
        Ok(self.client.exists(&self.path).await?.is_some())
    }

    /// Waits until the barrier is removed. Returns `false` on timeout.
    pub async fn wait_on_barrier(&self, timeout: Option<Duration>) -> Result<bool> {
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            let (stat, w) = self.client.watch_exists(&self.path).await?;
            if stat.is_none() {
                return Ok(true);
            }
            let ev = match deadline {
                Some(d) => match tokio::time::timeout_at(d, w.changed()).await {
                    Ok(ev) => ev,
                    Err(_) => return Ok(false),
                },
                None => w.changed().await,
            };
            if ev.kind == zk_core::WatchEventKind::Closed {
                return Err(ZkError::new(ErrorKind::Closed, "client closed"));
            }
        }
    }
}

/// Curator-style double barrier.
#[derive(Debug)]
pub struct DoubleBarrier {
    client: ZkClient,
    path: String,
    member_count: usize,
    id: String,
    uuid: String,
    entered: Option<(Pinned, String)>,
}

const READY: &str = "ready";

impl DoubleBarrier {
    pub fn new(client: &ZkClient, path: &str, member_count: usize, id: &str) -> Result<DoubleBarrier> {
        validate_base(path)?;
        if member_count == 0 {
            return Err(ZkError::bad_arguments("member_count must be positive"));
        }
        Ok(DoubleBarrier {
            client: client.clone(),
            path: path.to_string(),
            member_count,
            id: id.to_string(),
            uuid: uuid::Uuid::new_v4().to_string(),
            entered: None,
        })
    }

    fn members(children: &[String]) -> usize {
        children.iter().filter(|c| c.as_str() != READY).count()
    }

    /// Enters and waits until `member_count` members are present. Returns
    /// `false` on timeout (our member node is then removed).
    pub async fn enter(&mut self, timeout: Option<Duration>) -> Result<bool> {
        if self.entered.is_some() {
            return Ok(true);
        }
        let deadline = timeout.map(|t| Instant::now() + t);
        self.client.ensure_path(&self.path, &Acl::anyone_all()).await?;
        let pinned = Pinned::current(&self.client).await?;
        let ready = format!("{}/{READY}", self.path);
        // Watch `ready` before joining so we cannot miss its creation.
        let (ready_stat, ready_w) = pinned.watch_exists(&ready).await?;
        let mut cleanup = ContenderCleanup { pinned: pinned.clone(), dir: self.path.clone(), uuid: self.uuid.clone(), path: None, armed: true };
        let (node, _) = pinned.create_protected(&self.path, &self.id, self.id.as_bytes(), zk::CreateMode::Ephemeral, &self.uuid).await?;
        cleanup.path = Some(node.clone());
        if ready_stat.is_none() {
            let children = pinned.children(&self.path).await?;
            if Self::members(&children) >= self.member_count {
                match pinned.create(&ready, b"", zk::CreateMode::Persistent).await {
                    Ok(_) => {}
                    Err(e) if e.is(ErrorKind::NodeExists) => {}
                    Err(e) => return Err(e),
                }
            } else if !pinned.wait_watch(ready_w, deadline).await? {
                return Ok(false);
            }
        }
        cleanup.armed = false;
        self.entered = Some((pinned, node));
        Ok(true)
    }

    /// Leaves and waits until every member has left. Returns `false` on timeout.
    pub async fn leave(&mut self, timeout: Option<Duration>) -> Result<bool> {
        let Some((pinned, node)) = self.entered.take() else {
            return Err(ZkError::bad_arguments("double barrier: leave without enter"));
        };
        let deadline = timeout.map(|t| Instant::now() + t);
        let ours = node.rsplit('/').next().unwrap_or_default().to_string();
        let ready = format!("{}/{READY}", self.path);
        let mut left = false;
        loop {
            // Use the client (not the pinned session) for reads: our session may be gone.
            let children = match self.client.children(&self.path).await {
                Ok(c) => c,
                Err(e) if e.is(ErrorKind::NoNode) => return Ok(true),
                Err(e) => return Err(e),
            };
            let mut members: Vec<String> = children.into_iter().filter(|c| c != READY).collect();
            members.sort();
            if members.is_empty() || (members.len() == 1 && members[0] == ours) {
                if !left {
                    let _ = pinned.delete_if_exists(&node).await;
                }
                match self.client.delete(&ready, None).await {
                    Ok(()) => {}
                    Err(e) if e.is(ErrorKind::NoNode) => {}
                    Err(e) => return Err(e),
                }
                return Ok(true);
            }
            // Curator algorithm: the lowest member waits for the highest; the
            // others delete themselves and wait for the lowest.
            let (watch_target, delete_self) = if members[0] == ours { (members.last().unwrap().clone(), false) } else { (members[0].clone(), true) };
            if delete_self && !left {
                pinned.delete_if_exists(&node).await.or_else(|e| if e.kind.is_session_terminal() { Ok(()) } else { Err(e) })?;
                left = true;
            }
            let (stat, w) = self.client.watch_exists(&format!("{}/{watch_target}", self.path)).await?;
            if stat.is_none() {
                continue;
            }
            let ev = match deadline {
                Some(d) => match tokio::time::timeout_at(d, w.changed()).await {
                    Ok(ev) => ev,
                    Err(_) => {
                        if !left {
                            let _ = pinned.delete_if_exists(&node).await;
                        }
                        return Ok(false);
                    }
                },
                None => w.changed().await,
            };
            if ev.kind == zk_core::WatchEventKind::Closed {
                return Err(ZkError::new(ErrorKind::Closed, "client closed"));
            }
        }
    }
}
