//! Cache and recipe methods (backed by zk-cache and zk-recipes).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use zk_cache::{CacheCore, CacheEvent, CacheOptions, CachedNode, ChildrenCache, NodeCache, TreeCache};
use zk_core::b64;

use crate::dispatch::Bridge;
use crate::rpc::{params, RpcError, RpcResult};

pub const METHODS: &[&str] = &[
    "cache.start",
    "cache.get",
    "cache.children",
    "cache.snapshot",
    "cache.status",
    "cache.stop",
    "cache.list",
];

enum AnyCache {
    Node(NodeCache),
    Children(ChildrenCache),
    Tree(TreeCache),
}

impl AnyCache {
    fn core(&self) -> &CacheCore {
        match self {
            AnyCache::Node(c) => c,
            AnyCache::Children(c) => c,
            AnyCache::Tree(c) => c,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            AnyCache::Node(_) => "node",
            AnyCache::Children(_) => "children",
            AnyCache::Tree(_) => "tree",
        }
    }
}

struct CacheEntry {
    cluster: String,
    cache: Arc<AnyCache>,
    forwarder: CancellationToken,
}

#[derive(Default)]
pub struct ExtState {
    caches: Mutex<BTreeMap<String, CacheEntry>>,
}

impl ExtState {
    pub fn shutdown(&self) {
        for (_, e) in std::mem::take(&mut *self.caches.lock().unwrap()) {
            e.forwarder.cancel();
            e.cache.core().close();
        }
    }

    pub fn drop_cluster(&self, cluster: &str) {
        let mut caches = self.caches.lock().unwrap();
        let ids: Vec<String> = caches.iter().filter(|(_, e)| e.cluster == cluster).map(|(k, _)| k.clone()).collect();
        for id in ids {
            if let Some(e) = caches.remove(&id) {
                e.forwarder.cancel();
                e.cache.core().close();
            }
        }
    }

    fn get(&self, id: &str) -> Result<Arc<AnyCache>, RpcError> {
        self.caches.lock().unwrap().get(id).map(|e| e.cache.clone()).ok_or_else(|| RpcError::unknown("cache", id))
    }
}

fn node_json(n: &CachedNode, with_data: bool) -> Value {
    let mut v = json!({"path": n.path, "stat": n.stat, "fetched_at_ms": n.fetched_at_ms, "has_data": n.data.is_some()});
    if with_data {
        if let Some(d) = &n.data {
            v["data"] = json!(b64::encode(d));
            if let Ok(s) = std::str::from_utf8(d) {
                v["text"] = json!(s);
            }
        }
    }
    v
}

pub async fn dispatch(b: &Arc<Bridge>, method: &str, p: Value) -> RpcResult {
    match method {
        "cache.start" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
                kind: String,
                #[serde(default)]
                options: CacheOptions,
            }
            let a: A = params(p)?;
            let client = b.cluster(&a.cluster)?.client.clone();
            let cache = match a.kind.as_str() {
                "node" => AnyCache::Node(NodeCache::start(client, &a.path, a.options).await?),
                "children" => AnyCache::Children(ChildrenCache::start(client, &a.path, a.options).await?),
                "tree" => AnyCache::Tree(TreeCache::start(client, &a.path, a.options).await?),
                k => return Err(RpcError::invalid_params(format!("unknown cache kind {k:?}"))),
            };
            let cache = Arc::new(cache);
            let id = b.new_id("k");
            let forwarder = CancellationToken::new();
            {
                let mut rx = cache.core().subscribe();
                let outbox = b.outbox.clone();
                let stop = forwarder.clone();
                let cid = id.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = stop.cancelled() => return,
                            ev = rx.recv() => match ev {
                                Ok(ev) => {
                                    let closed = matches!(ev, CacheEvent::Closed);
                                    outbox.notify("cache.event", json!({"cache": cid, "event": ev}));
                                    if closed { return; }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                    // Consumers must re-snapshot after a gap.
                                    outbox.notify("cache.event", json!({"cache": cid, "event": {"type": "overflow", "dropped": n}}));
                                }
                                Err(_) => return,
                            }
                        }
                    }
                });
            }
            let status = cache.core().status();
            b.ext.caches.lock().unwrap().insert(id.clone(), CacheEntry { cluster: a.cluster, cache, forwarder });
            Ok(json!({"cache": id, "status": status}))
        },
        "cache.get" => {
            #[derive(Deserialize)]
            struct A {
                cache: String,
                #[serde(default)]
                path: Option<String>,
                #[serde(default = "yes")]
                with_data: bool,
            }
            let a: A = params(p)?;
            let c = b.ext.get(&a.cache)?;
            let path = a.path.unwrap_or_else(|| c.core().root().to_string());
            Ok(c.core().get(&path).map(|n| node_json(&n, a.with_data)).unwrap_or(Value::Null))
        },
        "cache.children" => {
            #[derive(Deserialize)]
            struct A {
                cache: String,
                #[serde(default)]
                path: Option<String>,
            }
            let a: A = params(p)?;
            let c = b.ext.get(&a.cache)?;
            let path = a.path.unwrap_or_else(|| c.core().root().to_string());
            Ok(json!(c.core().children(&path)))
        },
        "cache.snapshot" => {
            #[derive(Deserialize)]
            struct A {
                cache: String,
                #[serde(default)]
                with_data: bool,
            }
            let a: A = params(p)?;
            let c = b.ext.get(&a.cache)?;
            let nodes: Vec<Value> = c.core().snapshot().values().map(|n| node_json(n, a.with_data)).collect();
            Ok(json!({"nodes": nodes, "status": c.core().status()}))
        },
        "cache.status" => {
            #[derive(Deserialize)]
            struct A {
                cache: String,
            }
            let a: A = params(p)?;
            Ok(serde_json::to_value(b.ext.get(&a.cache)?.core().status()).unwrap())
        },
        "cache.stop" => {
            #[derive(Deserialize)]
            struct A {
                cache: String,
            }
            let a: A = params(p)?;
            let e = b.ext.caches.lock().unwrap().remove(&a.cache).ok_or_else(|| RpcError::unknown("cache", &a.cache))?;
            e.cache.core().close();
            // Let the forwarder deliver `closed`, then stop it.
            let fwd = e.forwarder.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                fwd.cancel();
            });
            Ok(Value::Null)
        },
        "cache.list" => {
            let caches = b.ext.caches.lock().unwrap();
            let v: Vec<Value> = caches
                .iter()
                .map(|(id, e)| json!({"cache": id, "cluster": e.cluster, "kind": e.cache.kind(), "path": e.cache.core().root(), "status": e.cache.core().status()}))
                .collect();
            Ok(json!(v))
        },
        _ => Err(RpcError::new(crate::rpc::METHOD_NOT_FOUND, format!("method not found: {method}"))),
    }
}

fn yes() -> bool {
    true
}
