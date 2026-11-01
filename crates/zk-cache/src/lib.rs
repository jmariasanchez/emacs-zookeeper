//! `zk-cache`: Curator-class caches over [`zk_core::ZkClient`].
//!
//! * [`NodeCache`]: a single node (which may not exist).
//! * [`ChildrenCache`]: a node and its direct children.
//! * [`TreeCache`]: a whole subtree (optionally depth limited).
//!
//! All three share one engine built on ZooKeeper 3.6+ persistent watches
//! (`Recursive` for tree/children caches, `Node` for the node cache).
//! Node events are applied incrementally; on reconnect (`Resync`, events may
//! have been missed, ZOOKEEPER-4698) and after session loss (`Reregistered`)
//! the cache rebuilds from scratch, diffs against the old snapshot and emits
//! precise `NodeAdded` / `NodeUpdated` / `NodeRemoved` events followed by
//! `Resynced`.
//!
//! # Consistency
//!
//! * Events are processed sequentially and every fetch is guarded by the
//!   node's stat: a stat with a lower `czxid` (older incarnation) or a lower
//!   `mzxid`/`pzxid` of the same incarnation never replaces a newer one.
//! * Deletion events are confirmed with `exists` before removing, so a delete
//!   notification queued behind a rebuild cannot remove a re-created node.
//! * [`CacheStatus::state`] tells consumers how much to trust the contents:
//!   `Live` = connected, watch registered, contents rebuilt since the last
//!   gap; `Suspended` = connection lost, contents may be stale; `Resyncing` =
//!   rebuilding; `Stale` = last rebuild or fetch failed, retrying.
//!
//! # Budgets
//!
//! * `max_nodes`: once reached, new nodes are not added and `truncated` is
//!   set (existing nodes keep being updated/removed). A later rebuild with
//!   fewer nodes clears the flag.
//! * `max_bytes`: total cached data bytes. When exceeded, data of the
//!   least-recently-fetched nodes is dropped (`data = None`, stat kept) and
//!   `evictions` is incremented.
//! * `max_data_per_node`: larger nodes are cached with stat only.
//! * Truncation keeps the cache `Live`; it is reported via `truncated`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use zk_core::{
    ConnectionEventKind, ErrorKind, PersistentMode, Result, Stat, WalkOptions, WatchEvent, WatchEventKind, WatchStream,
    ZkClient, ZkError,
};

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Cache notification.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CacheEvent {
    NodeAdded { path: String, stat: Stat },
    NodeUpdated { path: String, stat: Stat, old_stat: Stat },
    NodeRemoved { path: String },
    /// Initial load finished.
    Initialized { nodes: usize },
    /// A rebuild started (after reconnect or session loss).
    Resyncing { reason: String },
    /// A rebuild finished; `changed` = number of node events it produced.
    Resynced { changed: usize },
    /// Contents may be out of date (connection suspended or rebuild failed).
    Stale { reason: String },
    Error { error: ZkError },
    Closed,
}

/// A cached znode.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CachedNode {
    pub path: String,
    /// `None` when data caching is disabled, the node exceeds
    /// `max_data_per_node`, or the data was evicted by `max_bytes`.
    #[serde(skip)]
    pub data: Option<Arc<Vec<u8>>>,
    pub stat: Stat,
    pub fetched_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CacheState {
    Initializing,
    Live,
    Suspended,
    Resyncing,
    Stale,
    Closed,
}

/// Freshness indicators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheStatus {
    pub state: CacheState,
    /// Session generation the contents were last synchronized on.
    pub generation: u64,
    pub last_sync_ms: i64,
    pub nodes: usize,
    pub bytes: usize,
    pub evictions: u64,
    pub truncated: bool,
}

/// Alias kept for API symmetry.
pub type Freshness = CacheStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(default)]
pub struct CacheOptions {
    pub cache_data: bool,
    pub max_nodes: usize,
    pub max_bytes: usize,
    /// Depth relative to the root (0 = root only). Tree caches only.
    pub max_depth: Option<usize>,
    pub max_data_per_node: usize,
    /// Include `/zookeeper` when the root is `/`.
    pub include_system: bool,
}

impl Default for CacheOptions {
    fn default() -> Self {
        CacheOptions {
            cache_data: true,
            max_nodes: 100_000,
            max_bytes: 64 * 1024 * 1024,
            max_depth: None,
            max_data_per_node: 1024 * 1024,
            include_system: false,
        }
    }
}

// ------------------------------------------------------------------ state

struct State {
    nodes: BTreeMap<String, CachedNode>,
    /// fetch sequence -> path, for nodes holding data (LRU eviction).
    lru: BTreeMap<u64, String>,
    seq_of: BTreeMap<String, u64>,
    seq: u64,
    bytes: usize,
    evictions: u64,
    truncated: bool,
    status: CacheState,
    generation: u64,
    last_sync_ms: i64,
}

impl State {
    fn new() -> Self {
        State {
            nodes: BTreeMap::new(),
            lru: BTreeMap::new(),
            seq_of: BTreeMap::new(),
            seq: 0,
            bytes: 0,
            evictions: 0,
            truncated: false,
            status: CacheState::Initializing,
            generation: 0,
            last_sync_ms: 0,
        }
    }

    fn drop_data(&mut self, path: &str) {
        if let Some(s) = self.seq_of.remove(path) {
            self.lru.remove(&s);
        }
        if let Some(n) = self.nodes.get_mut(path) {
            if let Some(d) = n.data.take() {
                self.bytes -= d.len();
            }
        }
    }

    fn remove(&mut self, path: &str) -> Option<CachedNode> {
        self.drop_data(path);
        self.nodes.remove(path)
    }

    /// Inserts/replaces without guards or events; enforces byte budget.
    fn put(&mut self, node: CachedNode, opts: &CacheOptions) {
        let path = node.path.clone();
        self.drop_data(&path);
        let has = node.data.as_ref().map(|d| d.len());
        self.nodes.insert(path.clone(), node);
        if let Some(len) = has {
            self.bytes += len;
            self.seq += 1;
            self.lru.insert(self.seq, path.clone());
            self.seq_of.insert(path, self.seq);
            while self.bytes > opts.max_bytes {
                let Some((_, victim)) = self.lru.pop_first() else { break };
                self.seq_of.remove(&victim);
                if let Some(n) = self.nodes.get_mut(&victim) {
                    if let Some(d) = n.data.take() {
                        self.bytes -= d.len();
                        self.evictions += 1;
                    }
                }
            }
        }
    }

    fn status(&self) -> CacheStatus {
        CacheStatus {
            state: self.status,
            generation: self.generation,
            last_sync_ms: self.last_sync_ms,
            nodes: self.nodes.len(),
            bytes: self.bytes,
            evictions: self.evictions,
            truncated: self.truncated,
        }
    }
}

struct Shared {
    state: Mutex<State>,
    tx: broadcast::Sender<CacheEvent>,
}

impl Shared {
    fn emit(&self, e: CacheEvent) {
        let _ = self.tx.send(e);
    }
    fn set_status(&self, s: CacheState) {
        let mut st = self.state.lock().unwrap();
        if st.status != CacheState::Closed {
            st.status = s;
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Node,
    Children,
    Tree,
}

// ------------------------------------------------------------------ core

/// Shared cache handle; [`NodeCache`], [`ChildrenCache`] and [`TreeCache`]
/// deref to it. Dropping the owning cache cancels the background task.
pub struct CacheCore {
    shared: Arc<Shared>,
    cancel: CancellationToken,
    root: String,
}

impl CacheCore {
    async fn start(client: ZkClient, root: &str, mut options: CacheOptions, kind: Kind) -> Result<CacheCore> {
        zk_core::path::validate(root)?;
        match kind {
            Kind::Node => options.max_depth = Some(0),
            Kind::Children => options.max_depth = Some(1),
            Kind::Tree => {},
        }
        let mode = if kind == Kind::Node { PersistentMode::Node } else { PersistentMode::Recursive };
        let (tx, _) = broadcast::channel(4096);
        let shared = Arc::new(Shared { state: Mutex::new(State::new()), tx });
        let conn = client.subscribe_events();
        let stream = client.watch_persistent(root, mode, 4096).await?;
        let mut engine = Engine { client, root: root.to_string(), opts: options, shared: shared.clone(), kind };
        let changed = engine.rebuild().await?;
        let _ = changed;
        let n = shared.state.lock().unwrap().nodes.len();
        shared.emit(CacheEvent::Initialized { nodes: n });
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        tokio::spawn(async move { engine.run(stream, conn, c).await });
        Ok(CacheCore { shared, cancel, root: root.to_string() })
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn get(&self, path: &str) -> Option<CachedNode> {
        self.shared.state.lock().unwrap().nodes.get(path).cloned()
    }

    /// Sorted names of cached direct children of `path`.
    pub fn children(&self, path: &str) -> Vec<String> {
        let st = self.shared.state.lock().unwrap();
        let prefix = if path == "/" { "/".to_string() } else { format!("{path}/") };
        st.nodes
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .filter_map(|(k, _)| {
                let rest = &k[prefix.len()..];
                (!rest.is_empty() && !rest.contains('/')).then(|| rest.to_string())
            })
            .collect()
    }

    pub fn snapshot(&self) -> BTreeMap<String, CachedNode> {
        self.shared.state.lock().unwrap().nodes.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CacheEvent> {
        self.shared.tx.subscribe()
    }

    pub fn status(&self) -> CacheStatus {
        self.shared.state.lock().unwrap().status()
    }

    /// Stops the cache. Contents remain readable; state becomes `Closed`.
    pub fn close(&self) {
        if !self.cancel.is_cancelled() {
            self.cancel.cancel();
            self.shared.state.lock().unwrap().status = CacheState::Closed;
            self.shared.emit(CacheEvent::Closed);
        }
    }
}

impl Drop for CacheCore {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

macro_rules! cache_type {
    ($(#[$m:meta])* $name:ident, $kind:expr) => {
        $(#[$m])*
        pub struct $name(CacheCore);

        impl $name {
            /// Starts the cache; returns after the initial load.
            pub async fn start(client: ZkClient, root: &str, options: CacheOptions) -> Result<$name> {
                Ok($name(CacheCore::start(client, root, options, $kind).await?))
            }
        }

        impl std::ops::Deref for $name {
            type Target = CacheCore;
            fn deref(&self) -> &CacheCore {
                &self.0
            }
        }
    };
}

cache_type!(
    /// Caches a whole subtree (bounded by `max_depth`).
    TreeCache,
    Kind::Tree
);
cache_type!(
    /// Caches a node and its direct children (the parent itself is included
    /// in the snapshot when it exists).
    ChildrenCache,
    Kind::Children
);
cache_type!(
    /// Caches a single node, which may not exist.
    NodeCache,
    Kind::Node
);

impl NodeCache {
    /// The cached node, `None` if it does not exist.
    pub fn node(&self) -> Option<CachedNode> {
        self.get(&self.0.root)
    }
}

// ------------------------------------------------------------------ engine

struct Engine {
    client: ZkClient,
    root: String,
    opts: CacheOptions,
    shared: Arc<Shared>,
    kind: Kind,
}

fn newer_or_equal(new: &Stat, old: &Stat) -> bool {
    if new.czxid != old.czxid {
        return new.czxid > old.czxid;
    }
    new.mzxid >= old.mzxid && new.pzxid >= old.pzxid
}

impl Engine {
    fn depth(&self, path: &str) -> Option<usize> {
        if path == self.root {
            return Some(0);
        }
        let rest = if self.root == "/" { path.strip_prefix('/')? } else { path.strip_prefix(&self.root)?.strip_prefix('/')? };
        if rest.is_empty() {
            return None;
        }
        if self.root == "/" && !self.opts.include_system && (rest == "zookeeper" || rest.starts_with("zookeeper/")) {
            return None;
        }
        Some(rest.split('/').count())
    }

    fn in_scope(&self, path: &str) -> bool {
        self.depth(path).map(|d| self.opts.max_depth.map(|m| d <= m).unwrap_or(true)).unwrap_or(false)
    }

    fn make_node(&self, path: String, stat: Stat, data: Option<Vec<u8>>) -> CachedNode {
        let data = data.filter(|d| self.opts.cache_data && d.len() <= self.opts.max_data_per_node).map(Arc::new);
        CachedNode { path, data, stat, fetched_at_ms: now_ms() }
    }

    /// Fetches one node; `Ok(None)` if it does not exist.
    async fn fetch(&self, path: &str) -> Result<Option<CachedNode>> {
        if self.opts.cache_data {
            match self.client.get_data(path).await {
                Ok(d) => Ok(Some(self.make_node(path.to_string(), d.stat, Some(d.data)))),
                Err(e) if e.kind == ErrorKind::NoNode => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            Ok(self.client.exists(path).await?.map(|s| self.make_node(path.to_string(), s, None)))
        }
    }

    /// Full rebuild; returns the number of node events emitted.
    async fn rebuild(&mut self) -> Result<usize> {
        let generation = self.client.current_session().map(|s| s.generation()).unwrap_or(0);
        let mut fresh: Vec<CachedNode> = Vec::new();
        let mut truncated = false;
        let walk_opts = WalkOptions {
            max_depth: self.opts.max_depth,
            max_nodes: self.opts.max_nodes,
            include_system: self.opts.include_system,
            ..Default::default()
        };
        let entries = if self.opts.max_depth == Some(0) {
            self.client.exists(&self.root).await?.map(|s| vec![(self.root.clone(), s)]).unwrap_or_default()
        } else {
            match self.client.walk(&self.root, &walk_opts).await {
                Ok(w) => {
                    truncated = w.truncated;
                    w.nodes.into_iter().map(|e| (e.path, e.stat)).collect()
                },
                Err(e) if e.kind == ErrorKind::NoNode => vec![],
                Err(e) => return Err(e),
            }
        };
        if self.opts.cache_data {
            for chunk in entries.chunks(256) {
                let res = futures::future::join_all(chunk.iter().map(|(p, _)| self.client.get_data(p))).await;
                for ((p, _), r) in chunk.iter().zip(res) {
                    match r {
                        Ok(d) => fresh.push(self.make_node(p.clone(), d.stat, Some(d.data))),
                        Err(e) if e.kind == ErrorKind::NoNode => {},
                        Err(e) => return Err(e),
                    }
                }
            }
        } else {
            fresh = entries.into_iter().map(|(p, s)| self.make_node(p, s, None)).collect();
        }

        let mut events = Vec::new();
        {
            let mut st = self.shared.state.lock().unwrap();
            let old = std::mem::take(&mut st.nodes);
            let evictions = st.evictions;
            let status = st.status;
            *st = State::new();
            st.evictions = evictions;
            st.status = status;
            let mut new_paths = std::collections::BTreeSet::new();
            for n in fresh {
                new_paths.insert(n.path.clone());
                match old.get(&n.path) {
                    None => events.push(CacheEvent::NodeAdded { path: n.path.clone(), stat: n.stat }),
                    Some(o) if o.stat != n.stat => {
                        events.push(CacheEvent::NodeUpdated { path: n.path.clone(), stat: n.stat, old_stat: o.stat })
                    },
                    _ => {},
                }
                st.put(n, &self.opts);
            }
            for p in old.keys().filter(|p| !new_paths.contains(*p)) {
                events.push(CacheEvent::NodeRemoved { path: p.clone() });
            }
            st.truncated = truncated;
            st.generation = generation;
            st.last_sync_ms = now_ms();
            if st.status != CacheState::Closed {
                st.status = if self.client.state().is_connected() { CacheState::Live } else { CacheState::Suspended };
            }
        }
        let n = events.len();
        for e in events {
            self.shared.emit(e);
        }
        Ok(n)
    }

    /// Applies a fetched node (or absence) with ordering guards.
    fn apply(&self, path: &str, fetched: Option<CachedNode>) {
        let ev = {
            let mut st = self.shared.state.lock().unwrap();
            match (fetched, st.nodes.get(path).cloned()) {
                (None, None) => None,
                (None, Some(_)) => {
                    // Remove the node and any cached descendants.
                    let prefix = format!("{}/", path.trim_end_matches('/'));
                    let desc: Vec<String> =
                        st.nodes.range(prefix.clone()..).take_while(|(k, _)| k.starts_with(&prefix)).map(|(k, _)| k.clone()).collect();
                    for d in &desc {
                        st.remove(d);
                    }
                    st.remove(path);
                    let mut evs: Vec<_> = desc.into_iter().map(|path| CacheEvent::NodeRemoved { path }).collect();
                    evs.push(CacheEvent::NodeRemoved { path: path.to_string() });
                    Some(evs)
                },
                (Some(n), None) => {
                    if st.nodes.len() >= self.opts.max_nodes {
                        st.truncated = true;
                        None
                    } else {
                        let stat = n.stat;
                        st.put(n, &self.opts);
                        Some(vec![CacheEvent::NodeAdded { path: path.to_string(), stat }])
                    }
                },
                (Some(n), Some(old)) => {
                    if !newer_or_equal(&n.stat, &old.stat) || (n.stat == old.stat && (old.data.is_some() || n.data.is_none())) {
                        None
                    } else {
                        let stat = n.stat;
                        st.put(n, &self.opts);
                        (stat != old.stat).then(|| vec![CacheEvent::NodeUpdated { path: path.to_string(), stat, old_stat: old.stat }])
                    }
                },
            }
        };
        for e in ev.into_iter().flatten() {
            self.shared.emit(e);
        }
    }

    /// Refreshes a node's stat (e.g. a parent after a child change).
    async fn refresh_stat(&self, path: &str) -> Result<()> {
        let old = self.shared.state.lock().unwrap().nodes.get(path).cloned();
        let Some(old) = old else { return Ok(()) };
        match self.client.exists(path).await? {
            None => self.apply(path, None),
            Some(s) if s.mzxid != old.stat.mzxid || s.czxid != old.stat.czxid => {
                let f = self.fetch(path).await?;
                self.apply(path, f);
            },
            Some(s) => self.apply(path, Some(CachedNode { stat: s, fetched_at_ms: now_ms(), ..old })),
        }
        Ok(())
    }

    async fn handle_node_event(&self, ev: &WatchEvent) -> Result<()> {
        let p = ev.path.as_str();
        if self.in_scope(p) {
            let f = self.fetch(p).await?;
            self.apply(p, f);
        }
        if matches!(ev.kind, WatchEventKind::NodeCreated | WatchEventKind::NodeDeleted) && p != self.root {
            if let Some(parent) = zk_core::path::parent(p) {
                if self.in_scope(parent) {
                    self.refresh_stat(parent).await?;
                }
            }
        }
        Ok(())
    }

    fn fail(&self, e: ZkError) {
        tracing::debug!(root = %self.root, error = %e, "cache fetch failed; rebuild scheduled");
        self.shared.set_status(CacheState::Stale);
        self.shared.emit(CacheEvent::Stale { reason: e.to_string() });
        self.shared.emit(CacheEvent::Error { error: e });
    }

    async fn run(
        mut self,
        mut stream: WatchStream,
        mut conn: broadcast::Receiver<zk_core::ConnectionEvent>,
        cancel: CancellationToken,
    ) {
        let _ = self.kind;
        let mut need_rebuild: Option<String> = None;
        let mut waiting_reregister = false;
        loop {
            if let (Some(reason), false) = (need_rebuild.clone(), waiting_reregister) {
                self.shared.set_status(CacheState::Resyncing);
                self.shared.emit(CacheEvent::Resyncing { reason });
                match self.rebuild().await {
                    Ok(changed) => {
                        need_rebuild = None;
                        self.shared.emit(CacheEvent::Resynced { changed });
                    },
                    Err(e) => {
                        self.fail(e);
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        }
                        continue;
                    },
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                ev = stream.next() => {
                    let Some(ev) = ev else {
                        self.shared.set_status(CacheState::Closed);
                        self.shared.emit(CacheEvent::Closed);
                        return;
                    };
                    match ev.kind {
                        WatchEventKind::Resync => need_rebuild = Some("reconnected; events may have been missed".into()),
                        WatchEventKind::SessionLost => {
                            self.shared.set_status(CacheState::Resyncing);
                            self.shared.emit(CacheEvent::Resyncing { reason: format!("session lost: {}", ev.message.unwrap_or_default()) });
                            waiting_reregister = true;
                            need_rebuild = Some("session re-established".into());
                        },
                        WatchEventKind::Reregistered => {
                            waiting_reregister = false;
                            need_rebuild = Some("session re-established".into());
                        },
                        WatchEventKind::Closed => {
                            self.shared.set_status(CacheState::Closed);
                            self.shared.emit(CacheEvent::Closed);
                            return;
                        },
                        _ => {
                            if let Err(e) = self.handle_node_event(&ev).await {
                                self.fail(e);
                                need_rebuild.get_or_insert_with(|| "fetch failed".into());
                            }
                        },
                    }
                }
                ce = conn.recv() => {
                    match ce {
                        Ok(ce) => match ce.kind {
                            ConnectionEventKind::Suspended | ConnectionEventKind::Lost => {
                                let st = self.shared.state.lock().unwrap().status;
                                if st != CacheState::Resyncing {
                                    self.shared.set_status(CacheState::Suspended);
                                }
                                self.shared.emit(CacheEvent::Stale { reason: format!("connection {:?}", ce.kind).to_lowercase() });
                            },
                            ConnectionEventKind::Closed | ConnectionEventKind::AuthFailed => {
                                self.shared.set_status(CacheState::Closed);
                                self.shared.emit(CacheEvent::Closed);
                                return;
                            },
                            _ => {},
                        },
                        Err(broadcast::error::RecvError::Lagged(_)) => {},
                        Err(broadcast::error::RecvError::Closed) => {
                            conn = self.client.subscribe_events();
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        },
                    }
                }
            }
        }
    }
}
