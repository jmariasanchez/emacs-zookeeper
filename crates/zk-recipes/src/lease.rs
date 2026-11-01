//! Partition assignment with exclusive, session-bound ownership.
//!
//! [`PartitionAssigner`] lets a group of members split `n` partitions:
//!
//! * Members register via [`GroupMember`] under `{base}/members/{id}`.
//! * The *desired* assignment is computed deterministically by every member
//!   using rendezvous (highest-random-weight) hashing over the live member
//!   ids, so all members agree without coordination and a membership change
//!   only moves the partitions of the joining/leaving member.
//! * Ownership is *claimed*: a member owns partition `p` only after creating
//!   the ephemeral node `{base}/owners/{p}` through its pinned session. A
//!   newly assigned member waits until the previous owner releases (or its
//!   session expires), so two members never own the same partition at once,
//!   even mid-rebalance.
//! * The [`Assignment`] snapshot carries an `epoch` (the members directory
//!   `cversion`, bumped on every join/leave) and the [`Ownership`] of the
//!   session holding the claims: when it is lost, all owned partitions are
//!   lost with it.
//!
//! Differences vs Kafka-style/Curator-like assignors: no sticky or
//! weighted strategies; data movement is not coordinated, only ownership.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zk_core::{raw as zk, Acl, ErrorKind, Result, ZkClient, ZkError};

use crate::common::validate_base;
use crate::membership::GroupMember;
use crate::ownership::{Ownership, SuspendPolicy};
use crate::pinned::Pinned;

/// Stable 64-bit FNV-1a.
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    // Final avalanche (splitmix64) to spread similar inputs.
    let mut z = h.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Rendezvous assignment: the owner of each partition among `members`.
pub fn rendezvous_owner<'a>(partition: u32, members: &'a [String]) -> Option<&'a str> {
    members.iter().max_by_key(|m| (fnv1a(&format!("{m}/{partition}")), m.as_str())).map(|s| s.as_str())
}

/// Partitions assigned to `member` given the member set.
pub fn assign(partitions: u32, members: &[String], member: &str) -> Vec<u32> {
    (0..partitions).filter(|p| rendezvous_owner(*p, members) == Some(member)).collect()
}

/// A member's view of the assignment.
#[derive(Debug, Clone, Serialize)]
pub struct Assignment {
    pub member_id: String,
    /// Members dir `cversion` the assignment was computed from.
    pub epoch: i64,
    pub members: Vec<String>,
    /// Partitions the hash assigns to us.
    pub assigned: Vec<u32>,
    /// Partitions we exclusively own right now (claimed and session held).
    pub owned: Vec<u32>,
    #[serde(skip)]
    pub ownership: Option<Ownership>,
}

impl Assignment {
    /// Whether the assignment is stable: we own exactly what we are assigned
    /// and the session is held.
    pub fn is_settled(&self) -> bool {
        self.assigned == self.owned && self.ownership.as_ref().is_some_and(|o| o.is_held())
    }
}

/// Participates in partition assignment until closed.
#[derive(Clone)]
pub struct PartitionAssigner {
    member: GroupMember,
    rx: watch::Receiver<Assignment>,
    cancel: CancellationToken,
    task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

struct Ctx {
    client: ZkClient,
    base: String,
    id: String,
    partitions: u32,
    policy: SuspendPolicy,
}

impl PartitionAssigner {
    pub fn start(client: &ZkClient, base: &str, member_id: &str, partitions: u32) -> Result<PartitionAssigner> {
        Self::start_with_policy(client, base, member_id, partitions, SuspendPolicy::default())
    }

    pub fn start_with_policy(client: &ZkClient, base: &str, member_id: &str, partitions: u32, policy: SuspendPolicy) -> Result<PartitionAssigner> {
        validate_base(base)?;
        if partitions == 0 {
            return Err(ZkError::bad_arguments("partitions must be positive"));
        }
        let member = GroupMember::join_with_policy(client, &format!("{base}/members"), member_id, b"", policy)?;
        let (tx, rx) = watch::channel(Assignment {
            member_id: member_id.to_string(),
            epoch: -1,
            members: Vec::new(),
            assigned: Vec::new(),
            owned: Vec::new(),
            ownership: None,
        });
        let cancel = CancellationToken::new();
        let ctx = Ctx { client: client.clone(), base: base.to_string(), id: member_id.to_string(), partitions, policy };
        let c = cancel.clone();
        let task = tokio::spawn(async move { run(ctx, tx, c).await });
        Ok(PartitionAssigner { member, rx, cancel, task: Arc::new(std::sync::Mutex::new(Some(task))) })
    }

    /// Latest assignment snapshot.
    pub fn assignment(&self) -> Assignment {
        self.rx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Assignment> {
        self.rx.clone()
    }

    /// Waits until `pred` holds.
    pub async fn wait_for(&self, timeout: Duration, pred: impl Fn(&Assignment) -> bool) -> Result<Assignment> {
        let mut rx = self.rx.clone();
        let fut = async {
            loop {
                let a = rx.borrow_and_update().clone();
                if pred(&a) {
                    return Ok(a);
                }
                if rx.changed().await.is_err() {
                    return Err(ZkError::new(ErrorKind::Closed, "assigner closed"));
                }
            }
        };
        tokio::time::timeout(timeout, fut).await.map_err(|_| ZkError::timeout("assignment wait"))?
    }

    /// Releases all partitions and leaves the group.
    pub async fn close(&self) {
        self.cancel.cancel();
        let t = self.task.lock().unwrap().take();
        if let Some(t) = t {
            let _ = t.await;
        }
        let _ = self.member.leave().await;
    }
}

async fn run(ctx: Ctx, tx: watch::Sender<Assignment>, cancel: CancellationToken) {
    let owners = format!("{}/owners", ctx.base);
    let members_dir = format!("{}/members", ctx.base);
    let _ = ctx.client.ensure_path(&owners, &Acl::anyone_all()).await;
    let _ = ctx.client.ensure_path(&members_dir, &Acl::anyone_all()).await;
    let mut session: Option<(Pinned, Ownership)> = None;
    let mut owned: BTreeSet<u32> = BTreeSet::new();
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // (Re)pin a session; losing it means losing every claim.
        if session.as_ref().is_some_and(|(p, o)| !p.is_alive() || o.state().is_terminal()) {
            if let Some((_, o)) = session.take() {
                o.mark_lost();
            }
            owned.clear();
        }
        if session.is_none() {
            match Pinned::current(&ctx.client).await {
                Ok(p) => {
                    let o = Ownership::track(&ctx.client, p.session().clone(), ctx.policy);
                    session = Some((p, o));
                }
                Err(e) if e.is(ErrorKind::Closed) => break,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            }
        }
        let (pinned, own) = session.clone().unwrap();
        let step = async {
            let (members, w) = pinned.watch_children(&members_dir).await?;
            let epoch = pinned.exists(&members_dir).await?.map(|s| s.cversion as i64).unwrap_or(-1);
            let assigned: BTreeSet<u32> =
                if members.contains(&ctx.id) { assign(ctx.partitions, &members, &ctx.id).into_iter().collect() } else { BTreeSet::new() };
            // Release partitions no longer ours first (lets others claim them).
            for p in owned.clone().difference(&assigned) {
                pinned.delete_if_exists(&format!("{owners}/{p}")).await?;
                owned.remove(p);
            }
            let mut owner_watches = Vec::new();
            if own.is_held() {
                for p in assigned.difference(&owned.clone()) {
                    let path = format!("{owners}/{p}");
                    match pinned.create(&path, ctx.id.as_bytes(), zk::CreateMode::Ephemeral).await {
                        Ok(_) => {
                            owned.insert(*p);
                        }
                        Err(e) if e.is(ErrorKind::NodeExists) || e.is(ErrorKind::OutcomeUnknown) => {
                            let (st, w) = pinned.watch_exists(&path).await?;
                            match st {
                                Some(st) if st.ephemeral_owner == pinned.session().id() => {
                                    owned.insert(*p);
                                }
                                Some(_) => owner_watches.push(w),
                                None => {}
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            let snapshot = Assignment {
                member_id: ctx.id.clone(),
                epoch,
                members: members.clone(),
                assigned: assigned.iter().copied().collect(),
                owned: if own.is_held() { owned.iter().copied().collect() } else { Vec::new() },
                ownership: Some(own.clone()),
            };
            tx.send_replace(snapshot);
            Ok::<_, ZkError>((w, owner_watches))
        };
        match step.await {
            Ok((w, owner_watches)) => {
                let any_owner = async {
                    if owner_watches.is_empty() {
                        std::future::pending::<()>().await;
                    } else {
                        futures::future::select_all(owner_watches.into_iter().map(|w| Box::pin(w.changed()))).await;
                    }
                };
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = w.changed() => {}
                    _ = any_owner => {}
                    _ = own.interrupted() => { tokio::time::sleep(Duration::from_millis(100)).await; }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "partition assigner step failed");
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                }
            }
        }
    }
    if let Some((pinned, own)) = session {
        for p in &owned {
            let _ = pinned.delete_if_exists(&format!("{owners}/{p}")).await;
        }
        own.mark_released();
    }
    tx.send_modify(|a| {
        a.owned.clear();
        a.assigned.clear();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendezvous_is_total_and_stable() {
        let m: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let mut all: Vec<u32> = m.iter().flat_map(|x| assign(64, &m, x)).collect();
        all.sort();
        assert_eq!(all, (0..64).collect::<Vec<_>>());
        for x in &m {
            assert!(!assign(64, &m, x).is_empty());
        }
        // Removing c only moves c's partitions.
        let m2: Vec<String> = m[..2].to_vec();
        for x in &m2 {
            for p in assign(64, &m, x) {
                assert_eq!(rendezvous_owner(p, &m2), Some(x.as_str()));
            }
        }
    }
}
