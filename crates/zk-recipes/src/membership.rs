//! [`GroupMember`]: group membership (Curator `GroupMember`).
//!
//! Each member owns an ephemeral node `{base}/{member_id}` with a payload,
//! supervised by a [`PersistentNode`] so it is re-created after session
//! re-establishment. [`GroupMember::members`] lists the group and
//! [`GroupMember::watch`] streams membership snapshots.
//!
//! Differences vs Curator: membership snapshots are pushed through a
//! `tokio::sync::watch` channel instead of a `CuratorCache`.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zk_core::{path as zpath, Acl, ErrorKind, Result, ZkClient, ZkError};

use crate::common::validate_base;
use crate::ownership::{Ownership, SuspendPolicy};
use crate::persistent_node::{PersistentNode, PersistentNodeMode};

/// Snapshot of members: id -> payload.
pub type Members = BTreeMap<String, Vec<u8>>;

/// A member of a group.
#[derive(Debug, Clone)]
pub struct GroupMember {
    client: ZkClient,
    base: String,
    id: String,
    node: PersistentNode,
}

impl GroupMember {
    /// Joins the group immediately (in the background; see
    /// [`GroupMember::wait_joined`]).
    pub fn join(client: &ZkClient, base: &str, id: &str, payload: &[u8]) -> Result<GroupMember> {
        Self::join_with_policy(client, base, id, payload, SuspendPolicy::default())
    }

    pub fn join_with_policy(client: &ZkClient, base: &str, id: &str, payload: &[u8], policy: SuspendPolicy) -> Result<GroupMember> {
        validate_base(base)?;
        if id.is_empty() || id.contains('/') {
            return Err(ZkError::bad_arguments(format!("invalid member id {id:?}")));
        }
        let node = PersistentNode::start_with_policy(client, &zpath::join(base, id), PersistentNodeMode::Ephemeral, payload, policy)?;
        Ok(GroupMember { client: client.clone(), base: base.to_string(), id: id.to_string(), node })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Waits until our member node exists.
    pub async fn wait_joined(&self, timeout: Duration) -> Result<Ownership> {
        self.node.wait_for_created(timeout).await
    }

    /// Ownership of the current registration.
    pub fn ownership(&self) -> Option<Ownership> {
        self.node.ownership()
    }

    /// How many times our node was (re-)created.
    pub fn incarnations(&self) -> u64 {
        self.node.incarnations()
    }

    pub async fn set_payload(&self, payload: &[u8]) -> Result<()> {
        self.node.set_data(payload).await
    }

    /// Lists current members.
    pub async fn members(&self) -> Result<Members> {
        list_members(&self.client, &self.base).await
    }

    /// Streams membership snapshots. The task stops when the receiver is dropped.
    pub fn watch(&self) -> watch::Receiver<Members> {
        watch_members(&self.client, &self.base)
    }

    /// Leaves the group.
    pub async fn leave(&self) -> Result<()> {
        self.node.close().await
    }
}

/// Lists members of the group at `base`.
pub async fn list_members(client: &ZkClient, base: &str) -> Result<Members> {
    let children = match client.children(base).await {
        Ok(c) => c,
        Err(e) if e.is(ErrorKind::NoNode) => return Ok(Members::new()),
        Err(e) => return Err(e),
    };
    let mut out = Members::new();
    for c in children {
        match client.get_data(&zpath::join(base, &c)).await {
            Ok(d) => {
                out.insert(c, d.data);
            }
            Err(e) if e.is(ErrorKind::NoNode) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Watches the children (and payloads) of `base`, publishing snapshots.
pub fn watch_members(client: &ZkClient, base: &str) -> watch::Receiver<Members> {
    let (tx, rx) = watch::channel(Members::new());
    let client = client.clone();
    let base = base.to_string();
    let stop = CancellationToken::new();
    tokio::spawn(async move {
        let _ = client.ensure_path(&base, &Acl::anyone_all()).await;
        loop {
            if tx.is_closed() || stop.is_cancelled() {
                return;
            }
            let w = match client.watch_children(&base).await {
                Ok((_, _, w)) => Some(w),
                Err(e) if e.is(ErrorKind::Closed) => return,
                Err(_) => None,
            };
            if let Ok(m) = list_members(&client, &base).await {
                tx.send_if_modified(|cur| {
                    if *cur != m {
                        *cur = m;
                        true
                    } else {
                        false
                    }
                });
            }
            // Payload changes are not child events: also poll periodically.
            let tick = tokio::time::sleep(Duration::from_millis(500));
            match w {
                Some(w) => tokio::select! {
                    ev = w.changed() => if ev.kind == zk_core::WatchEventKind::Closed { return },
                    _ = tick => {}
                    _ = tx.closed() => return,
                },
                None => tick.await,
            }
        }
    });
    rx
}
