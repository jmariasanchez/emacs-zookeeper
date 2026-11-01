//! Bridge state and method dispatch.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use zk_core::{
    admin, b64, path as zpath, Acl, AuditRecord, AuditSink, ClusterConfig, CreateMode, CreateOptions, DeleteRecursiveOptions,
    DiffOptions, ErrorKind, NodeData, PersistentMode, ReadOp, ReadResult, SearchQuery, Secret, TreeNode, TxnOp, WalkOptions,
    WatchEvent, WatchEventKind, ZkClient, ZkError,
};

use crate::rpc::{params, Outbox, RpcError, RpcResult};

pub const PROTOCOL_VERSION: &str = "1.0";

pub struct Cluster {
    pub id: String,
    pub client: ZkClient,
    pub forwarder: CancellationToken,
}

pub struct WatchEntry {
    pub id: String,
    pub cluster: String,
    pub path: String,
    pub kind: String,
    pub events: Arc<AtomicU64>,
    pub cancel: CancellationToken,
}

pub struct Limits {
    pub watch_rate: u32,
}

/// Global bridge state.
pub struct Bridge {
    pub outbox: Outbox,
    pub clusters: Mutex<BTreeMap<String, Arc<Cluster>>>,
    pub watches: Mutex<BTreeMap<String, WatchEntry>>,
    pub audit: Arc<AuditLog>,
    pub next_id: AtomicU64,
    pub started: Instant,
    pub limits: Limits,
    pub ext: crate::ext::ExtState,
    pub recipes: crate::recipes::RecipeState,
}

/// Audit ring buffer, notification emitter and optional JSONL file.
pub struct AuditLog {
    ring: Mutex<VecDeque<AuditRecord>>,
    outbox: Outbox,
    file: Mutex<Option<std::fs::File>>,
}

impl AuditLog {
    fn new(outbox: Outbox) -> Self {
        let file = std::env::var_os("ZK_BRIDGE_AUDIT_LOG").and_then(|p| {
            std::fs::OpenOptions::new().create(true).append(true).open(p).ok()
        });
        AuditLog { ring: Mutex::new(VecDeque::new()), outbox, file: Mutex::new(file) }
    }

    pub fn list(&self, limit: usize) -> Vec<AuditRecord> {
        let ring = self.ring.lock().unwrap();
        ring.iter().rev().take(limit).rev().cloned().collect()
    }
}

/// Per-cluster sink that tags records with the bridge cluster id.
struct ClusterAudit {
    log: Arc<AuditLog>,
    cluster: String,
}

impl AuditSink for ClusterAudit {
    fn record(&self, r: AuditRecord) {
        let mut v = serde_json::to_value(&r).unwrap_or(Value::Null);
        if let Value::Object(m) = &mut v {
            m.insert("cluster_id".into(), json!(self.cluster));
        }
        {
            let mut ring = self.log.ring.lock().unwrap();
            ring.push_back(r);
            while ring.len() > 1000 {
                ring.pop_front();
            }
        }
        if let Some(f) = self.log.file.lock().unwrap().as_mut() {
            use std::io::Write;
            let _ = writeln!(f, "{v}");
        }
        self.log.outbox.notify("audit.record", v);
    }
}

impl Bridge {
    pub fn new(outbox: Outbox, limits: Limits) -> Arc<Bridge> {
        Arc::new(Bridge {
            audit: Arc::new(AuditLog::new(outbox.clone())),
            outbox,
            clusters: Mutex::new(BTreeMap::new()),
            watches: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
            started: Instant::now(),
            limits,
            ext: Default::default(),
            recipes: Default::default(),
        })
    }

    pub fn new_id(&self, prefix: &str) -> String {
        format!("{prefix}{}", self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    pub fn cluster(&self, id: &str) -> Result<Arc<Cluster>, RpcError> {
        self.clusters.lock().unwrap().get(id).cloned().ok_or_else(|| RpcError::unknown("cluster", id))
    }

    /// A client handle honoring the request's `force` flag.
    fn client(&self, id: &str, force: bool) -> Result<ZkClient, RpcError> {
        let c = self.cluster(id)?.client.clone();
        Ok(if force { c.forced() } else { c })
    }

    pub async fn shutdown(&self) {
        let clusters: Vec<Arc<Cluster>> = std::mem::take(&mut *self.clusters.lock().unwrap()).into_values().collect();
        for w in std::mem::take(&mut *self.watches.lock().unwrap()).into_values() {
            w.cancel.cancel();
        }
        self.ext.shutdown();
        self.recipes.shutdown().await;
        for c in clusters {
            c.forwarder.cancel();
            c.client.close().await;
        }
    }

    pub fn method_names() -> Vec<&'static str> {
        let mut v = vec![
            "initialize", "shutdown", "$/ping", "$/cancelRequest",
            "cluster.connect", "cluster.disconnect", "cluster.list", "cluster.status", "cluster.health",
            "cluster.four-letter", "cluster.config", "cluster.reconfig",
            "node.get", "node.exists", "node.children", "node.create", "node.set", "node.delete", "node.sync",
            "node.count", "node.acl.get", "node.acl.set", "node.ephemerals", "node.transaction", "node.multi-read",
            "node.walk", "node.export", "node.import", "node.copy", "node.move", "node.search", "node.diff",
            "auth.add", "auth.whoami", "acl.digest",
            "watch.add", "watch.remove", "watch.list",
            "audit.list",
        ];
        v.extend(crate::ext::METHODS);
        v.extend(crate::recipes::METHODS);
        v
    }

    pub async fn dispatch(self: &Arc<Self>, method: &str, p: Value) -> RpcResult {
        match method {
            "$/ping" => Ok(json!({"pong": true, "uptime_ms": self.started.elapsed().as_millis() as u64})),
            "cluster.connect" => self.cluster_connect(p).await,
            "cluster.disconnect" => self.cluster_disconnect(p).await,
            "cluster.list" => self.cluster_list(),
            "cluster.status" => {
                let a: ClusterArg = params(p)?;
                Ok(serde_json::to_value(self.cluster(&a.cluster)?.client.diagnostics()).unwrap())
            },
            "cluster.health" => self.cluster_health(p).await,
            "cluster.four-letter" => self.cluster_four_letter(p).await,
            "cluster.config" => {
                let a: ClusterArg = params(p)?;
                let d = self.cluster(&a.cluster)?.client.get_config().await?;
                Ok(data_json("/zookeeper/config", &d, None, None))
            },
            "cluster.reconfig" => self.cluster_reconfig(p).await,
            "node.get" => self.node_get(p).await,
            "node.exists" => {
                let a: PathArg = params(p)?;
                let st = self.client(&a.cluster, false)?.exists(&a.path).await?;
                Ok(json!({"path": a.path, "stat": st}))
            },
            "node.children" => self.node_children(p).await,
            "node.create" => self.node_create(p).await,
            "node.set" => self.node_set(p).await,
            "node.delete" => self.node_delete(p).await,
            "node.sync" => {
                let a: PathArg = params(p)?;
                self.client(&a.cluster, false)?.sync(&a.path).await?;
                Ok(Value::Null)
            },
            "node.count" => {
                let a: PathArg = params(p)?;
                let n = self.client(&a.cluster, false)?.count_descendants(&a.path).await?;
                Ok(json!({"path": a.path, "descendants": n}))
            },
            "node.acl.get" => {
                let a: PathArg = params(p)?;
                let (acl, stat) = self.client(&a.cluster, false)?.get_acl(&a.path).await?;
                Ok(json!({"path": a.path, "acl": acl, "stat": stat}))
            },
            "node.acl.set" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    path: String,
                    acl: Vec<Acl>,
                    #[serde(default)]
                    version: Option<i32>,
                    #[serde(default)]
                    force: bool,
                }
                let a: A = params(p)?;
                let st = self.client(&a.cluster, a.force)?.set_acl(&a.path, &a.acl, a.version).await?;
                Ok(json!({"path": a.path, "stat": st}))
            },
            "node.ephemerals" => {
                let a: PathArg = params(p)?;
                let paths = self.client(&a.cluster, false)?.session_ephemerals(&a.path).await?;
                Ok(json!({"paths": paths}))
            },
            "node.transaction" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    ops: Vec<TxnOp>,
                    #[serde(default)]
                    force: bool,
                }
                let a: A = params(p)?;
                let r = self.client(&a.cluster, a.force)?.transaction(&a.ops).await?;
                Ok(json!({"results": r}))
            },
            "node.multi-read" => self.node_multi_read(p).await,
            "node.walk" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    path: String,
                    #[serde(flatten)]
                    options: WalkOptions,
                }
                let a: A = params(p)?;
                let r = self.client(&a.cluster, false)?.walk(&a.path, &a.options).await?;
                Ok(serde_json::to_value(r).unwrap())
            },
            "node.export" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    path: String,
                    #[serde(default)]
                    with_acl: bool,
                    #[serde(default)]
                    with_stat: bool,
                    #[serde(default = "d_max_nodes")]
                    max_nodes: usize,
                }
                let a: A = params(p)?;
                let (nodes, truncated) = self.client(&a.cluster, false)?.export(&a.path, a.with_acl, a.with_stat, a.max_nodes).await?;
                Ok(json!({"root": a.path, "nodes": nodes, "truncated": truncated}))
            },
            "node.import" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    path: String,
                    nodes: Vec<TreeNode>,
                    #[serde(default)]
                    overwrite: bool,
                    #[serde(default)]
                    keep_acl: bool,
                    #[serde(default)]
                    force: bool,
                }
                let a: A = params(p)?;
                let (created, updated) = self.client(&a.cluster, a.force)?.import(&a.path, &a.nodes, a.overwrite, a.keep_acl).await?;
                Ok(json!({"created": created, "updated": updated}))
            },
            "node.copy" => self.node_copy(p).await,
            "node.move" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    path: String,
                    to: String,
                    #[serde(default)]
                    allow_non_atomic: bool,
                    #[serde(default = "d_copy_nodes")]
                    max_nodes: usize,
                    #[serde(default)]
                    force: bool,
                }
                let a: A = params(p)?;
                let paths = self.client(&a.cluster, a.force)?.move_tree(&a.path, &a.to, a.allow_non_atomic, a.max_nodes).await?;
                Ok(json!({"paths": paths}))
            },
            "node.search" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    path: String,
                    #[serde(flatten)]
                    query: SearchQuery,
                }
                let mut v = p;
                // `max_nodes`/`max_results` default through serde only when absent.
                if let Value::Object(m) = &mut v {
                    m.entry("max_nodes").or_insert(json!(100_000));
                    m.entry("max_results").or_insert(json!(1_000));
                }
                let a: A = params(v)?;
                let r = self.client(&a.cluster, false)?.search(&a.path, &a.query).await?;
                Ok(serde_json::to_value(r).unwrap())
            },
            "node.diff" => {
                #[derive(Deserialize)]
                struct Side {
                    cluster: String,
                    path: String,
                }
                #[derive(Deserialize)]
                struct A {
                    left: Side,
                    right: Side,
                    #[serde(flatten)]
                    options: DiffOptions,
                }
                let mut v = p;
                if let Value::Object(m) = &mut v {
                    m.entry("data").or_insert(json!(true));
                    m.entry("kind").or_insert(json!(true));
                    m.entry("max_nodes").or_insert(json!(100_000));
                }
                let a: A = params(v)?;
                let l = self.client(&a.left.cluster, false)?;
                let r = self.client(&a.right.cluster, false)?;
                let d = zk_core::diff_trees(&l, &a.left.path, &r, &a.right.path, &a.options).await?;
                Ok(serde_json::to_value(d).unwrap())
            },
            "auth.add" => {
                #[derive(Deserialize)]
                struct A {
                    cluster: String,
                    scheme: String,
                    credential: Secret,
                }
                let a: A = params(p)?;
                let cred = a.credential.resolve()?;
                self.client(&a.cluster, false)?.add_auth(&a.scheme, cred.as_bytes()).await?;
                Ok(Value::Null)
            },
            "auth.whoami" => {
                let a: ClusterArg = params(p)?;
                Ok(serde_json::to_value(self.client(&a.cluster, false)?.whoami().await?).unwrap())
            },
            "acl.digest" => {
                #[derive(Deserialize)]
                struct A {
                    user: String,
                    password: Secret,
                }
                let a: A = params(p)?;
                let pw = a.password.resolve()?;
                Ok(json!({"id": zk_core::digest_id(&a.user, &pw)}))
            },
            "watch.add" => self.watch_add(p).await,
            "watch.remove" => {
                #[derive(Deserialize)]
                struct A {
                    watch: String,
                }
                let a: A = params(p)?;
                let w = self.watches.lock().unwrap().remove(&a.watch).ok_or_else(|| RpcError::unknown("watch", &a.watch))?;
                w.cancel.cancel();
                Ok(Value::Null)
            },
            "watch.list" => {
                #[derive(Deserialize, Default)]
                struct A {
                    #[serde(default)]
                    cluster: Option<String>,
                }
                let a: A = params(p)?;
                let w = self.watches.lock().unwrap();
                let v: Vec<Value> = w
                    .values()
                    .filter(|w| a.cluster.as_ref().map(|c| c == &w.cluster).unwrap_or(true))
                    .map(|w| json!({"watch": w.id, "cluster": w.cluster, "path": w.path, "kind": w.kind, "events": w.events.load(Ordering::Relaxed)}))
                    .collect();
                Ok(json!(v))
            },
            "audit.list" => {
                #[derive(Deserialize, Default)]
                struct A {
                    #[serde(default)]
                    limit: Option<usize>,
                }
                let a: A = params(p)?;
                Ok(serde_json::to_value(self.audit.list(a.limit.unwrap_or(200))).unwrap())
            },
            m if crate::ext::METHODS.contains(&m) => crate::ext::dispatch(self, m, p).await,
            m if crate::recipes::METHODS.contains(&m) => crate::recipes::dispatch(self, m, p).await,
            _ => Err(RpcError::new(crate::rpc::METHOD_NOT_FOUND, format!("method not found: {method}"))),
        }
    }

    // ---------------------------------------------------------------- clusters

    async fn cluster_connect(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            config: ClusterConfig,
            #[serde(default = "yes")]
            wait: bool,
        }
        let a: A = params(p)?;
        let id = self.new_id("c");
        let deadline = Duration::from_millis(a.config.connect_deadline_ms);
        // Start first and subscribe before waiting, so no event (notably `connected`) is missed.
        let client = ZkClient::start(a.config)?;
        client.set_audit_sink(Some(Arc::new(ClusterAudit { log: self.audit.clone(), cluster: id.clone() })));
        let forwarder = CancellationToken::new();
        // Forward connection events.
        {
            let mut events = client.subscribe_events();
            let outbox = self.outbox.clone();
            let cid = id.clone();
            let c = client.clone();
            let stop = forwarder.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        e = events.recv() => match e {
                            Ok(ev) => outbox.notify("connection.event", json!({"cluster": cid, "event": ev, "state": c.state()})),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                outbox.notify("connection.event", json!({"cluster": cid, "event": {"kind": "overflow", "dropped": n}, "state": c.state()}))
                            }
                            Err(_) => return,
                        }
                    }
                }
            });
        }
        if a.wait {
            if let Err(e) = client.wait_connected(deadline).await {
                let e = client.diagnostics().last_error.filter(|_| e.kind == ErrorKind::Timeout).unwrap_or(e);
                forwarder.cancel();
                client.close().await;
                return Err(e.into());
            }
        }
        let state = client.state();
        let cluster = Arc::new(Cluster { id: id.clone(), client, forwarder });
        self.clusters.lock().unwrap().insert(id.clone(), cluster);
        Ok(json!({"cluster": id, "state": state}))
    }

    async fn cluster_disconnect(self: &Arc<Self>, p: Value) -> RpcResult {
        let a: ClusterArg = params(p)?;
        let c = self.clusters.lock().unwrap().remove(&a.cluster).ok_or_else(|| RpcError::unknown("cluster", &a.cluster))?;
        let dead: Vec<WatchEntry> = {
            let mut w = self.watches.lock().unwrap();
            let ids: Vec<String> = w.values().filter(|e| e.cluster == a.cluster).map(|e| e.id.clone()).collect();
            ids.into_iter().filter_map(|i| w.remove(&i)).collect()
        };
        for w in dead {
            w.cancel.cancel();
        }
        self.ext.drop_cluster(&a.cluster);
        self.recipes.drop_cluster(&a.cluster).await;
        c.client.close().await;
        // Emit the final state, the forwarder may already be gone.
        self.outbox.notify(
            "connection.event",
            json!({"cluster": a.cluster, "event": {"kind": "closed", "generation": 0, "at_ms": now_ms(), "message": "disconnected"}, "state": c.client.state()}),
        );
        c.forwarder.cancel();
        Ok(Value::Null)
    }

    fn cluster_list(&self) -> RpcResult {
        let v: Vec<Value> = self
            .clusters
            .lock()
            .unwrap()
            .values()
            .map(|c| json!({"cluster": c.id, "name": c.client.name(), "connect": c.client.config().connect, "state": c.client.state()}))
            .collect();
        Ok(json!(v))
    }

    fn admin_hosts(c: &ZkClient) -> Vec<String> {
        if !c.config().admin_hosts.is_empty() {
            c.config().admin_hosts.clone()
        } else {
            c.config().hosts_and_chroot().0
        }
    }

    async fn cluster_health(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            #[serde(default)]
            timeout_ms: Option<u64>,
        }
        let a: A = params(p)?;
        let c = self.cluster(&a.cluster)?;
        let hosts = Self::admin_hosts(&c.client);
        let r = admin::ensemble_health(&hosts, Duration::from_millis(a.timeout_ms.unwrap_or(3000))).await;
        Ok(serde_json::to_value(r).unwrap())
    }

    async fn cluster_four_letter(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            command: String,
            #[serde(default)]
            host: Option<String>,
            #[serde(default)]
            timeout_ms: Option<u64>,
        }
        let a: A = params(p)?;
        let c = self.cluster(&a.cluster)?;
        let hosts = Self::admin_hosts(&c.client);
        let host = match a.host {
            Some(h) if hosts.contains(&h) => h,
            Some(h) => return Err(ZkError::policy(format!("host {h} is not part of cluster {}", a.cluster)).into()),
            None => hosts.first().cloned().ok_or_else(|| RpcError::invalid_params("cluster has no hosts"))?,
        };
        let out = admin::four_letter(&host, &a.command, Duration::from_millis(a.timeout_ms.unwrap_or(3000))).await?;
        Ok(json!({"host": host, "output": out}))
    }

    async fn cluster_reconfig(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            #[serde(default)]
            joining: Vec<String>,
            #[serde(default)]
            leaving: Vec<String>,
            #[serde(default)]
            members: Option<Vec<String>>,
            #[serde(default)]
            from_config_version: Option<i64>,
        }
        let a: A = params(p)?;
        let c = self.cluster(&a.cluster)?;
        let d = c.client.reconfig(&a.joining, &a.leaving, a.members.as_deref(), a.from_config_version).await?;
        Ok(data_json("/zookeeper/config", &d, None, None))
    }

    // ------------------------------------------------------------------ nodes

    async fn node_get(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            path: String,
            #[serde(default)]
            sync: bool,
            #[serde(default)]
            max_bytes: Option<usize>,
            #[serde(default)]
            max_text_bytes: Option<usize>,
        }
        let a: A = params(p)?;
        let c = self.client(&a.cluster, false)?;
        let d = if a.sync { c.get_data_synced(&a.path).await? } else { c.get_data(&a.path).await? };
        Ok(data_json(&a.path, &d, a.max_bytes, a.max_text_bytes))
    }

    async fn node_children(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            path: String,
            #[serde(default)]
            stats: bool,
            #[serde(default)]
            limit: Option<usize>,
            #[serde(default)]
            offset: Option<usize>,
        }
        let a: A = params(p)?;
        let c = self.client(&a.cluster, false)?;
        let (mut names, stat) = c.children_with_stat(&a.path).await?;
        names.sort();
        let total = names.len();
        let offset = a.offset.unwrap_or(0).min(total);
        let end = a.limit.map(|l| (offset + l).min(total)).unwrap_or(total);
        let page = &names[offset..end];
        let truncated = end < total;
        let children: Vec<Value> = if a.stats {
            let session = c.session(c.config().operation_timeout()).await?;
            let futs = page.iter().map(|n| {
                let p = zpath::join(&a.path, n);
                let raw = session.raw().clone();
                async move {
                    let st = raw.check_stat(&p).await;
                    (n.clone(), p, st)
                }
            });
            let results = tokio::time::timeout(c.config().operation_timeout(), futures::future::join_all(futs))
                .await
                .map_err(|_| ZkError::timeout("node.children"))?;
            let mut out = Vec::with_capacity(results.len());
            for (n, p, st) in results {
                match st {
                    Ok(Some(st)) => out.push(json!({"name": n, "path": p, "stat": zk_core::Stat::from(st)})),
                    Ok(None) | Err(zk_core::raw::Error::NoNode) => {},
                    Err(e) => return Err(ZkError::from(e).with_path(p).into()),
                }
            }
            out
        } else {
            page.iter().map(|n| json!({"name": n, "path": zpath::join(&a.path, n)})).collect()
        };
        Ok(json!({"path": a.path, "stat": stat, "total": total, "offset": offset, "children": children, "truncated": truncated}))
    }

    async fn node_create(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            path: String,
            #[serde(flatten)]
            payload: Payload,
            #[serde(default)]
            mode: CreateMode,
            #[serde(default)]
            acl: Option<Vec<Acl>>,
            #[serde(default)]
            ttl_ms: Option<u64>,
            #[serde(default)]
            create_parents: bool,
            #[serde(default)]
            force: bool,
        }
        let a: A = params(p)?;
        let data = a.payload.bytes()?;
        let opts = CreateOptions { mode: a.mode, acl: a.acl.unwrap_or_else(Acl::anyone_all), ttl_ms: a.ttl_ms, create_parents: a.create_parents };
        let r = self.client(&a.cluster, a.force)?.create(&a.path, &data, &opts).await?;
        Ok(serde_json::to_value(r).unwrap())
    }

    async fn node_set(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            path: String,
            #[serde(flatten)]
            payload: Payload,
            #[serde(default)]
            version: Option<i32>,
            #[serde(default)]
            force: bool,
        }
        let a: A = params(p)?;
        let data = a.payload.bytes()?;
        let st = self.client(&a.cluster, a.force)?.set_data(&a.path, &data, a.version).await?;
        Ok(json!({"path": a.path, "stat": st}))
    }

    async fn node_delete(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            path: String,
            #[serde(default)]
            version: Option<i32>,
            #[serde(default)]
            recursive: bool,
            #[serde(default)]
            dry_run: bool,
            #[serde(default)]
            limit: Option<usize>,
            #[serde(default)]
            force: bool,
        }
        let a: A = params(p)?;
        let c = self.client(&a.cluster, a.force)?;
        if a.recursive || a.dry_run {
            let r = c.delete_recursive(&a.path, &DeleteRecursiveOptions { dry_run: a.dry_run, limit: a.limit, version: a.version }).await?;
            Ok(serde_json::to_value(r).unwrap())
        } else {
            c.delete(&a.path, a.version).await?;
            Ok(json!({"paths": [a.path], "dry_run": false, "rescans": 0}))
        }
    }

    async fn node_multi_read(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            ops: Vec<ReadOp>,
        }
        let a: A = params(p)?;
        let r = self.client(&a.cluster, false)?.multi_read(&a.ops).await?;
        let v: Vec<Value> = r
            .into_iter()
            .zip(a.ops.iter())
            .map(|(r, op)| match r {
                ReadResult::Data(d) => {
                    let p = match op {
                        ReadOp::GetData { path } | ReadOp::GetChildren { path } => path,
                    };
                    data_json(p, &d, None, None)
                },
                ReadResult::Children(c) => json!({"children": c}),
                ReadResult::Error(e) => json!({"error": e}),
            })
            .collect();
        Ok(json!({"results": v}))
    }

    async fn node_copy(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            path: String,
            to: String,
            #[serde(default)]
            to_cluster: Option<String>,
            #[serde(default)]
            with_acl: bool,
            #[serde(default = "d_copy_nodes")]
            max_nodes: usize,
            #[serde(default)]
            force: bool,
        }
        let a: A = params(p)?;
        let src = self.client(&a.cluster, false)?;
        match a.to_cluster.as_deref() {
            None => {
                let created = self.client(&a.cluster, a.force)?.copy_tree(&a.path, &a.to, a.with_acl, a.max_nodes).await?;
                Ok(json!({"created": created}))
            },
            Some(dst_id) => {
                let dst = self.client(dst_id, a.force)?;
                zpath::validate(&a.to)?;
                if dst.exists(&a.to).await?.is_some() {
                    return Err(ZkError::new(ErrorKind::NodeExists, "destination exists").with_path(&a.to).into());
                }
                let (nodes, truncated) = src.export(&a.path, a.with_acl, false, a.max_nodes.saturating_add(1)).await?;
                if truncated || nodes.len() > a.max_nodes {
                    return Err(ZkError::new(ErrorKind::LimitExceeded, format!("source has more than {} nodes", a.max_nodes)).into());
                }
                let (created, _) = dst.import(&a.to, &nodes, false, a.with_acl).await?;
                Ok(json!({"created": created}))
            },
        }
    }

    // ---------------------------------------------------------------- watches

    async fn watch_add(self: &Arc<Self>, p: Value) -> RpcResult {
        #[derive(Deserialize)]
        struct A {
            cluster: String,
            path: String,
            kind: String,
        }
        let a: A = params(p)?;
        let c = self.client(&a.cluster, false)?;
        let id = self.new_id("w");
        let cancel = CancellationToken::new();
        let events = Arc::new(AtomicU64::new(0));
        let entry = WatchEntry {
            id: id.clone(),
            cluster: a.cluster.clone(),
            path: a.path.clone(),
            kind: a.kind.clone(),
            events: events.clone(),
            cancel: cancel.clone(),
        };
        let outbox = self.outbox.clone();
        let bridge = Arc::downgrade(self);
        let (wid, cid) = (id.clone(), a.cluster.clone());
        let emit = move |ev: &WatchEvent| {
            events.fetch_add(1, Ordering::Relaxed);
            outbox.notify("watch.event", json!({"watch": wid, "cluster": cid, "event": ev}));
        };
        let finish = {
            let outbox = self.outbox.clone();
            let wid = id.clone();
            move || {
                if let Some(b) = bridge.upgrade() {
                    b.watches.lock().unwrap().remove(&wid);
                }
                outbox.notify("watch.removed", json!({"watch": wid}));
            }
        };
        let result = match a.kind.as_str() {
            "data" | "exists" | "children" => {
                let (initial, w) = match a.kind.as_str() {
                    "data" => {
                        let (d, w) = c.watch_data(&a.path).await?;
                        (json!({"stat": d.stat}), w)
                    },
                    "exists" => {
                        let (st, w) = c.watch_exists(&a.path).await?;
                        (json!({"stat": st}), w)
                    },
                    _ => {
                        let (ch, st, w) = c.watch_children(&a.path).await?;
                        (json!({"stat": st, "children": ch}), w)
                    },
                };
                let cancel2 = cancel.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = cancel2.cancelled() => {}
                        ev = w.changed() => emit(&ev),
                    }
                    finish();
                });
                initial
            },
            "persistent" | "recursive" => {
                let mode = if a.kind == "persistent" { PersistentMode::Node } else { PersistentMode::Recursive };
                let mut stream = c.watch_persistent(&a.path, mode, 1024).await?;
                let rate = self.limits.watch_rate.max(1);
                let cancel2 = cancel.clone();
                let outbox = self.outbox.clone();
                let (wid, cid) = (id.clone(), a.cluster.clone());
                tokio::spawn(async move {
                    let mut window = Instant::now();
                    let mut in_window = 0u32;
                    let mut dropped = 0u64;
                    loop {
                        let ev = tokio::select! {
                            _ = cancel2.cancelled() => break,
                            ev = stream.next() => ev,
                        };
                        let Some(ev) = ev else { break };
                        if window.elapsed() >= Duration::from_secs(1) {
                            if dropped > 0 {
                                outbox.notify("watch.event", json!({"watch": wid, "cluster": cid, "event": {"kind": "overflow", "dropped": dropped, "path": ev.path, "at_ms": ev.at_ms, "zxid": -1}}));
                                dropped = 0;
                            }
                            window = Instant::now();
                            in_window = 0;
                        }
                        let session_level = !ev.is_node_event();
                        if in_window >= rate && !session_level {
                            dropped += 1;
                            continue;
                        }
                        in_window += 1;
                        emit(&ev);
                        if ev.kind == WatchEventKind::Closed {
                            break;
                        }
                    }
                    finish();
                });
                json!({})
            },
            k => return Err(RpcError::invalid_params(format!("unknown watch kind {k:?}"))),
        };
        self.watches.lock().unwrap().insert(id.clone(), entry);
        let mut r = result;
        r["watch"] = json!(id);
        Ok(r)
    }
}

fn yes() -> bool {
    true
}
fn d_max_nodes() -> usize {
    100_000
}
fn d_copy_nodes() -> usize {
    10_000
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[derive(Deserialize)]
pub struct ClusterArg {
    pub cluster: String,
}

#[derive(Deserialize)]
pub struct PathArg {
    pub cluster: String,
    pub path: String,
}

/// `data` (base64) or `text` payload.
#[derive(Deserialize, Default)]
pub struct Payload {
    #[serde(default)]
    pub data: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

impl Payload {
    pub fn bytes(&self) -> Result<Vec<u8>, RpcError> {
        match (&self.data, &self.text) {
            (Some(_), Some(_)) => Err(RpcError::invalid_params("give either data or text, not both")),
            (Some(d), None) => b64::decode(d).map_err(|e| RpcError::invalid_params(format!("data is not valid base64: {e}"))),
            (None, Some(t)) => Ok(t.as_bytes().to_vec()),
            (None, None) => Ok(Vec::new()),
        }
    }
}

/// Node data response shape.
pub fn data_json(path: &str, d: &NodeData, max_bytes: Option<usize>, max_text: Option<usize>) -> Value {
    let max_text = max_text.unwrap_or(1 << 20);
    let (bytes, truncated) = match max_bytes {
        Some(m) if d.data.len() > m => (&d.data[..m], true),
        _ => (&d.data[..], false),
    };
    let utf8 = std::str::from_utf8(&d.data).is_ok();
    let mut v = json!({
        "path": path,
        "stat": d.stat,
        "data": b64::encode(bytes),
        "utf8": utf8,
        "truncated": truncated,
        "size": d.data.len(),
    });
    if utf8 && bytes.len() <= max_text {
        if let Ok(s) = std::str::from_utf8(bytes) {
            v["text"] = json!(s);
        }
    }
    if d.stat.ephemeral_owner != 0 {
        v["ephemeral_owner_hex"] = json!(format!("{:#x}", d.stat.ephemeral_owner));
    }
    v
}
