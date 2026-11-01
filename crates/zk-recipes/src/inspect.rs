//! Read-only recipe inspectors for UIs (the Emacs front end).
//!
//! [`inspect`] looks at the children of a path and guesses which recipe
//! lives there, returning a serde-serializable [`Inspection`]:
//!
//! * lock / leader-election contenders (`_c_*-lock-*`, `*latch-*`) ordered by
//!   sequence with the owner flagged,
//! * read/write locks (`__READ__` / `__WRIT__`) with every current holder,
//! * semaphores (`leases/` + `locks/`),
//! * queues (`qn-*`, including priority/delayed items) and locking queues
//!   (`entries/` + `taken/`),
//! * x-discovery registries (children holding Curator JSON instances),
//! * counters (4/8-byte values).
//!
//! Inspection never writes and never leaves watches behind.

use serde::Serialize;
use zk_core::{path as zpath, ErrorKind, Result, ZkClient};

use crate::common::sort_by_sequence;
use crate::discovery::{query_instances, ServiceInstance};
use crate::election::LATCH_NAME;
use crate::locks::{list_participants, rw_is_owner, Participant, LOCK_NAME, READ_LOCK_NAME, WRITE_LOCK_NAME};
use crate::queue::{parse_item, sorted_items};
use crate::semaphore::LEASE_NAME;

/// Summary of a queue item.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct QueueEntry {
    pub node: String,
    pub sequence: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub claimed: bool,
}

/// What [`inspect`] found.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Inspection {
    Lock { path: String, contenders: Vec<Participant>, owner: Option<String> },
    ReadWriteLock { path: String, contenders: Vec<Participant>, readers_holding: usize, writer_holding: bool },
    LeaderLatch { path: String, participants: Vec<Participant>, leader: Option<String> },
    Semaphore { path: String, leases: Vec<Participant>, lock_contenders: usize },
    Queue { path: String, size: usize, ready: usize, head: Vec<QueueEntry> },
    LockingQueue { path: String, size: usize, claimed: usize, head: Vec<QueueEntry> },
    ServiceRegistry { path: String, services: Vec<ServiceSummary> },
    Service { path: String, name: String, instances: Vec<ServiceInstance> },
    Counter { path: String, int32: Option<i32>, int64: Option<i64>, decimal: Option<i64> },
    Unknown { path: String, children: usize },
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceSummary {
    pub name: String,
    pub instances: Vec<ServiceInstance>,
}

const HEAD: usize = 50;

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

async fn children(client: &ZkClient, path: &str) -> Result<Vec<String>> {
    match client.children(path).await {
        Ok(c) => Ok(c),
        Err(e) if e.is(ErrorKind::NoNode) => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

fn queue_head(children: &[String], claimed: &[String]) -> (Vec<QueueEntry>, usize) {
    let now = now_ms();
    let items = sorted_items(children);
    let ready = items.iter().filter(|i| i.ready_at_ms.is_none_or(|r| r <= now)).count();
    let head = items
        .into_iter()
        .take(HEAD)
        .map(|i| QueueEntry { claimed: claimed.contains(&i.name), node: i.name, sequence: i.sequence, priority: i.priority, ready_at_ms: i.ready_at_ms })
        .collect();
    (head, ready)
}

/// Detects and describes the recipe stored at `path`.
pub async fn inspect(client: &ZkClient, path: &str) -> Result<Inspection> {
    zpath::validate(path)?;
    let kids = children(client, path).await?;
    let p = path.to_string();
    let has = |n: &str| kids.iter().any(|c| c == n);

    if has("leases") && (has("locks") || kids.len() <= 2) {
        let leases = list_participants(client, &zpath::join(path, "leases"), |n| n.contains(LEASE_NAME), |_, _| true).await?;
        let lock_contenders = children(client, &zpath::join(path, "locks")).await?.len();
        return Ok(Inspection::Semaphore { path: p, leases, lock_contenders });
    }
    if has("entries") && has("taken") {
        let entries = children(client, &zpath::join(path, "entries")).await?;
        let taken = children(client, &zpath::join(path, "taken")).await?;
        let (head, _) = queue_head(&entries, &taken);
        let size = entries.iter().filter(|c| parse_item(c).is_some()).count();
        return Ok(Inspection::LockingQueue { path: p, size, claimed: taken.len(), head });
    }
    if kids.iter().any(|c| c.contains(READ_LOCK_NAME) || c.contains(WRITE_LOCK_NAME)) {
        let filter = |n: &str| n.contains(READ_LOCK_NAME) || n.contains(WRITE_LOCK_NAME);
        let contenders = list_participants(client, path, filter, rw_is_owner).await?;
        let readers_holding = contenders.iter().filter(|c| c.is_owner && c.node.contains(READ_LOCK_NAME)).count();
        let writer_holding = contenders.iter().any(|c| c.is_owner && c.node.contains(WRITE_LOCK_NAME));
        return Ok(Inspection::ReadWriteLock { path: p, contenders, readers_holding, writer_holding });
    }
    if kids.iter().any(|c| c.contains(LATCH_NAME) && sort_by_sequence([c], |_| true).len() == 1) {
        let participants = list_participants(client, path, |n| n.contains(LATCH_NAME), |_, i| i == 0).await?;
        let leader = participants.first().map(|x| String::from_utf8_lossy(&x.data).into_owned());
        return Ok(Inspection::LeaderLatch { path: p, participants, leader });
    }
    if kids.iter().any(|c| c.contains(LOCK_NAME) && sort_by_sequence([c], |_| true).len() == 1) {
        let contenders = list_participants(client, path, |n| n.contains(LOCK_NAME), |_, i| i == 0).await?;
        let owner = contenders.first().map(|c| c.node.clone());
        return Ok(Inspection::Lock { path: p, contenders, owner });
    }
    if kids.iter().any(|c| parse_item(c).is_some()) {
        let (head, ready) = queue_head(&kids, &[]);
        let size = kids.iter().filter(|c| parse_item(c).is_some()).count();
        return Ok(Inspection::Queue { path: p, size, ready, head });
    }
    // Service: children are instance JSON.
    if !kids.is_empty() {
        let instances = query_instances(client, path).await?;
        if !instances.is_empty() {
            let name = instances[0].name.clone();
            return Ok(Inspection::Service { path: p, name, instances });
        }
        // Registry: grandchildren are instance JSON.
        let mut services = Vec::new();
        for k in kids.iter().take(200) {
            let inst = query_instances(client, &zpath::join(path, k)).await?;
            if !inst.is_empty() {
                services.push(ServiceSummary { name: k.clone(), instances: inst });
            }
        }
        if !services.is_empty() {
            return Ok(Inspection::ServiceRegistry { path: p, services });
        }
        return Ok(Inspection::Unknown { path: p, children: kids.len() });
    }
    // Leaf: maybe a counter.
    match client.get_data(path).await {
        Ok(d) => {
            let int32 = <[u8; 4]>::try_from(d.data.as_slice()).ok().map(i32::from_be_bytes);
            let int64 = <[u8; 8]>::try_from(d.data.as_slice()).ok().map(i64::from_be_bytes);
            let decimal = std::str::from_utf8(&d.data).ok().and_then(|s| s.trim().parse().ok());
            if int32.is_some() || int64.is_some() || decimal.is_some() {
                Ok(Inspection::Counter { path: p, int32, int64, decimal })
            } else {
                Ok(Inspection::Unknown { path: p, children: 0 })
            }
        }
        Err(e) => Err(e),
    }
}
