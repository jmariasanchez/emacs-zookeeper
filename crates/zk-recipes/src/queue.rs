//! Distributed queues.
//!
//! * [`DistributedQueue`]: FIFO over persistent-sequential nodes
//!   `{base}/qn-{seq}` (Curator `DistributedQueue` / Kazoo `Queue` naming).
//!   Items may be *delayed* ([`DistributedQueue::put_delayed`]): the ready
//!   time is encoded in the name (`qn-d{ready_at_ms:016x}-{seq}`) and `take`
//!   skips items that are not ready yet, still ordering by sequence.
//! * [`PriorityQueue`]: priority in the name
//!   (`qn-p{priority:08x}-{seq}`, sign-flipped so lexical order is numeric
//!   order); lower priority values are taken first (Curator
//!   `DistributedPriorityQueue` semantics), FIFO within a priority.
//! * [`LockingQueue`]: Kazoo `LockingQueue`: `{base}/entries/qn-...` plus
//!   ephemeral `{base}/taken/{entry}` markers. `get` claims an item without
//!   removing it; `consume` deletes it, `release` (or session loss) makes it
//!   available again. At-least-once processing.
//!
//! # Exactly-once take
//!
//! `take` claims an item by deleting it with its read version: exactly one
//! consumer's delete succeeds. If the delete reply is lost
//! ([`ErrorKind::OutcomeUnknown`]) the item may or may not be consumed; the
//! error is returned rather than guessing (Curator would retry and possibly
//! lose or duplicate the item). Use [`LockingQueue`] for at-least-once
//! processing that tolerates crashes.
//!
//! Differences vs Curator: no serializer abstraction (payloads are bytes)
//! and no background consumer threads; `take` is a pull API.

use std::time::Duration;

use serde::Serialize;
use tokio::time::Instant;
use zk_core::{raw as zk, Acl, CreateMode, CreateOptions, ErrorKind, Result, ZkClient, ZkError};

use crate::common::{sequence_of, validate_base};
use crate::ownership::{Ownership, SuspendPolicy};
use crate::pinned::Pinned;

/// Prefix of queue item nodes.
pub const QUEUE_PREFIX: &str = "qn-";

/// A dequeued (or peeked) item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueItem {
    pub node: String,
    pub sequence: i64,
    #[serde(with = "zk_core::b64")]
    pub data: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_at_ms: Option<i64>,
}

/// Parsed item name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ItemName {
    pub name: String,
    pub sequence: i64,
    pub priority: Option<i32>,
    pub ready_at_ms: Option<i64>,
}

pub(crate) fn parse_item(name: &str) -> Option<ItemName> {
    let rest = name.strip_prefix(QUEUE_PREFIX)?;
    let sequence = sequence_of(name)?;
    let mut priority = None;
    let mut ready_at_ms = None;
    if let Some(r) = rest.strip_prefix('p') {
        let hex = r.split('-').next()?;
        priority = Some((u32::from_str_radix(hex, 16).ok()? ^ 0x8000_0000) as i32);
    } else if let Some(r) = rest.strip_prefix('d') {
        let hex = r.split('-').next()?;
        ready_at_ms = Some(i64::from_str_radix(hex, 16).ok()?);
    }
    Some(ItemName { name: name.to_string(), sequence, priority, ready_at_ms })
}

fn priority_prefix(priority: i32) -> String {
    format!("{QUEUE_PREFIX}p{:08x}-", (priority as u32) ^ 0x8000_0000)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Sorted items: by priority (if any), then sequence.
pub(crate) fn sorted_items(children: &[String]) -> Vec<ItemName> {
    let mut v: Vec<ItemName> = children.iter().filter_map(|c| parse_item(c)).collect();
    v.sort_by_key(|i| (i.priority.unwrap_or(0), i.sequence));
    v
}

#[derive(Clone, Debug)]
struct Core {
    client: ZkClient,
    dir: String,
}

impl Core {
    async fn ensure(&self) -> Result<()> {
        self.client.ensure_path(&self.dir, &Acl::anyone_all()).await
    }

    async fn put_named(&self, prefix: &str, data: &[u8]) -> Result<String> {
        let path = format!("{}/{prefix}", self.dir);
        let opts = CreateOptions::mode(CreateMode::PersistentSequential);
        match self.client.create(&path, data, &opts).await {
            Ok(c) => Ok(c.path),
            Err(e) if e.is(ErrorKind::NoNode) => {
                self.ensure().await?;
                Ok(self.client.create(&path, data, &opts).await?.path)
            }
            Err(e) => Err(e),
        }
    }

    async fn list(&self) -> Result<Vec<String>> {
        match self.client.children(&self.dir).await {
            Ok(c) => Ok(c),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    async fn read(&self, it: &ItemName) -> Result<Option<(QueueItem, i32)>> {
        match self.client.get_data(&format!("{}/{}", self.dir, it.name)).await {
            Ok(d) => Ok(Some((
                QueueItem { node: it.name.clone(), sequence: it.sequence, data: d.data, priority: it.priority, ready_at_ms: it.ready_at_ms },
                d.stat.version,
            ))),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn peek(&self) -> Result<Option<QueueItem>> {
        let now = now_ms();
        for it in sorted_items(&self.list().await?) {
            if it.ready_at_ms.is_some_and(|r| r > now) {
                continue;
            }
            if let Some((item, _)) = self.read(&it).await? {
                return Ok(Some(item));
            }
        }
        Ok(None)
    }

    async fn try_take(&self) -> Result<(Option<QueueItem>, Option<i64>)> {
        let now = now_ms();
        let mut next_ready: Option<i64> = None;
        for it in sorted_items(&self.list().await?) {
            if let Some(r) = it.ready_at_ms.filter(|r| *r > now) {
                next_ready = Some(next_ready.map_or(r, |n| n.min(r)));
                continue;
            }
            let Some((item, version)) = self.read(&it).await? else { continue };
            match self.client.delete(&format!("{}/{}", self.dir, it.name), Some(version)).await {
                Ok(()) => return Ok((Some(item), None)),
                Err(e) if e.is(ErrorKind::NoNode) || e.is(ErrorKind::BadVersion) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok((None, next_ready))
    }

    async fn take(&self, timeout: Option<Duration>) -> Result<Option<QueueItem>> {
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            self.ensure().await?;
            let (_, _, w) = self.client.watch_children(&self.dir).await?;
            let (item, next_ready) = self.try_take().await?;
            if item.is_some() {
                return Ok(item);
            }
            let mut wake = deadline;
            if let Some(r) = next_ready {
                let at = Instant::now() + Duration::from_millis((r - now_ms()).max(0) as u64 + 5);
                wake = Some(wake.map_or(at, |d| d.min(at)));
            }
            match wake {
                Some(d) => {
                    let _ = tokio::time::timeout_at(d, w.changed()).await;
                }
                None => {
                    let ev = w.changed().await;
                    if ev.kind == zk_core::WatchEventKind::Closed {
                        return Err(ZkError::new(ErrorKind::Closed, "client closed"));
                    }
                }
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(None);
            }
        }
    }

    async fn len(&self) -> Result<usize> {
        Ok(self.list().await?.iter().filter(|c| parse_item(c).is_some()).count())
    }
}

/// FIFO queue with optional delayed items.
#[derive(Clone, Debug)]
pub struct DistributedQueue {
    core: Core,
}

impl DistributedQueue {
    pub fn new(client: &ZkClient, base: &str) -> Result<DistributedQueue> {
        validate_base(base)?;
        Ok(DistributedQueue { core: Core { client: client.clone(), dir: base.to_string() } })
    }

    /// Appends an item; returns its node path.
    pub async fn put(&self, data: &[u8]) -> Result<String> {
        self.core.put_named(QUEUE_PREFIX, data).await
    }

    /// Appends an item that `take` will not return before `ready_at_ms`
    /// (milliseconds since the epoch, wall clock of the consumer).
    pub async fn put_delayed(&self, data: &[u8], ready_at_ms: i64) -> Result<String> {
        self.core.put_named(&format!("{QUEUE_PREFIX}d{:016x}-", ready_at_ms.max(0)), data).await
    }

    /// Removes and returns the oldest ready item, waiting up to `timeout`
    /// (`None`: forever). Returns `Ok(None)` on timeout.
    pub async fn take(&self, timeout: Option<Duration>) -> Result<Option<QueueItem>> {
        self.core.take(timeout).await
    }

    /// Non-blocking take.
    pub async fn try_take(&self) -> Result<Option<QueueItem>> {
        Ok(self.core.try_take().await?.0)
    }

    /// The oldest ready item without removing it.
    pub async fn peek(&self) -> Result<Option<QueueItem>> {
        self.core.peek().await
    }

    /// Number of items (including not-yet-ready delayed ones).
    pub async fn len(&self) -> Result<usize> {
        self.core.len().await
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }
}

/// Priority queue: lower `priority` values are taken first.
#[derive(Clone, Debug)]
pub struct PriorityQueue {
    core: Core,
}

impl PriorityQueue {
    pub fn new(client: &ZkClient, base: &str) -> Result<PriorityQueue> {
        validate_base(base)?;
        Ok(PriorityQueue { core: Core { client: client.clone(), dir: base.to_string() } })
    }

    pub async fn put(&self, data: &[u8], priority: i32) -> Result<String> {
        self.core.put_named(&priority_prefix(priority), data).await
    }

    pub async fn take(&self, timeout: Option<Duration>) -> Result<Option<QueueItem>> {
        self.core.take(timeout).await
    }

    pub async fn try_take(&self) -> Result<Option<QueueItem>> {
        Ok(self.core.try_take().await?.0)
    }

    pub async fn peek(&self) -> Result<Option<QueueItem>> {
        self.core.peek().await
    }

    pub async fn len(&self) -> Result<usize> {
        self.core.len().await
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }
}

/// Kazoo-style locking queue (at-least-once).
#[derive(Clone, Debug)]
pub struct LockingQueue {
    client: ZkClient,
    base: String,
    policy: SuspendPolicy,
}

/// An item claimed from a [`LockingQueue`]. It stays in the queue until
/// [`LockedItem::consume`]; dropping it (or losing the session) releases the
/// claim so another consumer can process it.
#[derive(Debug)]
pub struct LockedItem {
    pub item: QueueItem,
    pinned: Pinned,
    entry_path: String,
    marker_path: String,
    ownership: Ownership,
    done: bool,
}

impl LockedItem {
    pub fn ownership(&self) -> &Ownership {
        &self.ownership
    }

    /// Removes the item permanently (entry + marker in one transaction,
    /// through the claiming session so it fails if the claim was lost).
    pub async fn consume(mut self) -> Result<()> {
        let mut w = self.pinned.session().raw().new_multi_writer();
        w.add_delete(&self.marker_path, None).map_err(ZkError::from)?;
        w.add_delete(&self.entry_path, None).map_err(ZkError::from)?;
        let r = tokio::time::timeout(self.pinned.client().config().operation_timeout(), w.commit()).await;
        self.done = true;
        self.ownership.mark_released();
        match r {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(ZkError::from(zk::Error::from(e)).with_path(&self.entry_path).into_write_outcome()),
            Err(_) => Err(ZkError::timeout("consume").into_write_outcome()),
        }
    }

    /// Gives the item back to the queue.
    pub async fn release(mut self) -> Result<()> {
        self.done = true;
        self.ownership.mark_released();
        match self.pinned.delete_if_exists(&self.marker_path).await {
            Err(e) if e.kind.is_session_terminal() => Ok(()),
            r => r,
        }
    }
}

impl Drop for LockedItem {
    fn drop(&mut self) {
        if !self.done {
            self.ownership.mark_released();
            if self.pinned.is_alive() {
                self.pinned.spawn_delete(self.marker_path.clone());
            }
        }
    }
}

impl LockingQueue {
    pub fn new(client: &ZkClient, base: &str) -> Result<LockingQueue> {
        validate_base(base)?;
        Ok(LockingQueue { client: client.clone(), base: base.to_string(), policy: SuspendPolicy::default() })
    }

    pub fn with_policy(mut self, policy: SuspendPolicy) -> Self {
        self.policy = policy;
        self
    }

    fn entries(&self) -> String {
        format!("{}/entries", self.base)
    }

    fn taken(&self) -> String {
        format!("{}/taken", self.base)
    }

    async fn ensure(&self) -> Result<()> {
        self.client.ensure_path(&self.entries(), &Acl::anyone_all()).await?;
        self.client.ensure_path(&self.taken(), &Acl::anyone_all()).await
    }

    pub async fn put(&self, data: &[u8], priority: i32) -> Result<String> {
        self.ensure().await?;
        let path = format!("{}/{}", self.entries(), priority_prefix(priority));
        Ok(self.client.create(&path, data, &CreateOptions::mode(CreateMode::PersistentSequential)).await?.path)
    }

    /// Claims the next unclaimed item, waiting up to `timeout`.
    pub async fn get(&self, timeout: Option<Duration>) -> Result<Option<LockedItem>> {
        let deadline = timeout.map(|t| Instant::now() + t);
        self.ensure().await?;
        loop {
            let pinned = Pinned::current(&self.client).await?;
            let (entries, ew) = pinned.watch_children(&self.entries()).await?;
            let (taken, tw) = pinned.watch_children(&self.taken()).await?;
            for it in sorted_items(&entries) {
                if taken.contains(&it.name) {
                    continue;
                }
                let marker = format!("{}/{}", self.taken(), it.name);
                match pinned.create(&marker, b"", zk::CreateMode::Ephemeral).await {
                    Ok(_) => {}
                    Err(e) if e.is(ErrorKind::NodeExists) => continue,
                    Err(e) if e.is(ErrorKind::OutcomeUnknown) => {
                        // Ours if it exists and is owned by our session.
                        match pinned.exists(&marker).await? {
                            Some(st) if st.ephemeral_owner == pinned.session().id() => {}
                            _ => continue,
                        }
                    }
                    Err(e) => return Err(e),
                }
                let entry_path = format!("{}/{}", self.entries(), it.name);
                match pinned.get_data(&entry_path).await {
                    Ok(d) => {
                        let ownership = Ownership::track(&self.client, pinned.session().clone(), self.policy);
                        return Ok(Some(LockedItem {
                            item: QueueItem { node: it.name.clone(), sequence: it.sequence, data: d.data, priority: it.priority, ready_at_ms: None },
                            pinned,
                            entry_path,
                            marker_path: marker,
                            ownership,
                            done: false,
                        }));
                    }
                    Err(e) if e.is(ErrorKind::NoNode) => {
                        let _ = pinned.delete_if_exists(&marker).await;
                        continue;
                    }
                    Err(e) => {
                        let _ = pinned.delete_if_exists(&marker).await;
                        return Err(e);
                    }
                }
            }
            // Wait for new entries or released claims.
            let wait = async {
                tokio::select! {
                    _ = ew.changed() => {}
                    _ = tw.changed() => {}
                }
            };
            match deadline {
                Some(d) => {
                    if tokio::time::timeout_at(d, wait).await.is_err() {
                        return Ok(None);
                    }
                }
                None => wait.await,
            }
        }
    }

    /// Number of entries (claimed or not).
    pub async fn len(&self) -> Result<usize> {
        match self.client.children(&self.entries()).await {
            Ok(c) => Ok(c.iter().filter(|c| parse_item(c).is_some()).count()),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(0),
            Err(e) => Err(e),
        }
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Number of entries currently claimed.
    pub async fn claimed(&self) -> Result<usize> {
        match self.client.children(&self.taken()).await {
            Ok(c) => Ok(c.len()),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(0),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        let p = parse_item(&format!("{}0000000003", priority_prefix(-5))).unwrap();
        assert_eq!(p.priority, Some(-5));
        assert_eq!(p.sequence, 3);
        assert!(priority_prefix(-1) < priority_prefix(0));
        assert!(priority_prefix(0) < priority_prefix(7));
        let d = parse_item("qn-d00000000000003e8-0000000001").unwrap();
        assert_eq!(d.ready_at_ms, Some(1000));
        assert_eq!(parse_item("qn-0000000009").unwrap().sequence, 9);
        assert!(parse_item("junk").is_none());
    }
}
