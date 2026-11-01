//! `recipe.*` methods (backed by zk-recipes).
//!
//! Inspection, discovery queries, counters and queue operations are
//! stateless. Session-bound participants (lock, latch, member) are owned by
//! the bridge and referenced by handle. Every ownership change is pushed to the
//! editor as `recipe.ownership`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use zk_recipes::{
    CounterEncoding, DistributedQueue, GroupMember, LeaderLatch, LockGuard, Mutex, Ownership, OwnershipState, PriorityQueue,
    ServiceDiscovery, SharedCounter,
};

use crate::dispatch::{Bridge, Payload};
use crate::rpc::{params, Outbox, RpcError, RpcResult};

pub const METHODS: &[&str] = &[
    "recipe.inspect",
    "recipe.discovery.services",
    "recipe.discovery.instances",
    "recipe.counter.get",
    "recipe.counter.add",
    "recipe.queue.put",
    "recipe.queue.peek",
    "recipe.queue.take",
    "recipe.queue.len",
    "recipe.lock.acquire",
    "recipe.latch.start",
    "recipe.member.join",
    "recipe.release",
    "recipe.list",
];

enum Held {
    Lock(Option<Box<LockGuard>>),
    Latch(LeaderLatch),
    Member(GroupMember),
}

struct RecipeEntry {
    kind: &'static str,
    cluster: String,
    path: String,
    node: Option<String>,
    held: Held,
    ownership: Arc<StdMutex<OwnershipState>>,
    leader: Arc<StdMutex<Option<bool>>>,
    stop: CancellationToken,
}

#[derive(Default)]
pub struct RecipeState {
    entries: tokio::sync::Mutex<BTreeMap<String, RecipeEntry>>,
}

impl RecipeState {
    pub async fn shutdown(&self) {
        let all = std::mem::take(&mut *self.entries.lock().await);
        for (_, e) in all {
            release_entry(e).await;
        }
    }

    pub async fn drop_cluster(&self, cluster: &str) {
        let mut entries = self.entries.lock().await;
        let ids: Vec<String> = entries.iter().filter(|(_, e)| e.cluster == cluster).map(|(k, _)| k.clone()).collect();
        let drained: Vec<RecipeEntry> = ids.into_iter().filter_map(|i| entries.remove(&i)).collect();
        drop(entries);
        for e in drained {
            release_entry(e).await;
        }
    }
}

async fn release_entry(e: RecipeEntry) {
    e.stop.cancel();
    match e.held {
        Held::Lock(Some(g)) => {
            let _ = g.release().await;
        },
        Held::Lock(None) => {},
        Held::Latch(l) => l.close().await,
        Held::Member(m) => {
            let _ = m.leave().await;
        },
    }
}

fn state_name(s: OwnershipState) -> &'static str {
    match s {
        OwnershipState::Held => "held",
        OwnershipState::Suspended => "suspended",
        OwnershipState::Lost => "lost",
        OwnershipState::Released => "released",
    }
}

/// Forwards ownership state changes as notifications until `stop`.
fn forward_ownership(outbox: Outbox, id: String, own: Ownership, cell: Arc<StdMutex<OwnershipState>>, stop: CancellationToken) {
    tokio::spawn(async move {
        let mut rx = own.subscribe();
        loop {
            let s = *rx.borrow_and_update();
            let changed = {
                let mut c = cell.lock().unwrap();
                let changed = *c != s;
                *c = s;
                changed
            };
            if changed {
                outbox.notify("recipe.ownership", json!({"recipe": id, "state": state_name(s)}));
            }
            if s.is_terminal() {
                return;
            }
            tokio::select! {
                _ = stop.cancelled() => return,
                r = rx.changed() => if r.is_err() { return },
            }
        }
    });
}

fn encoding(e: Option<&str>) -> Result<CounterEncoding, RpcError> {
    Ok(match e.unwrap_or("i64") {
        "i64" | "int64" | "int64-be" => CounterEncoding::Int64Be,
        "i32" | "int32" | "int32-be" => CounterEncoding::Int32Be,
        "text" | "decimal" => CounterEncoding::Decimal,
        other => return Err(RpcError::invalid_params(format!("unknown counter encoding {other:?}"))),
    })
}

fn timeout(ms: Option<u64>) -> Option<Duration> {
    ms.map(Duration::from_millis)
}

pub async fn dispatch(b: &Arc<Bridge>, method: &str, p: Value) -> RpcResult {
    match method {
        "recipe.inspect" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
            }
            let a: A = params(p)?;
            let c = b.cluster(&a.cluster)?.client.clone();
            Ok(serde_json::to_value(zk_recipes::inspect(&c, &a.path).await?).unwrap())
        },
        "recipe.discovery.services" | "recipe.discovery.instances" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                base: String,
                #[serde(default)]
                service: Option<String>,
            }
            let a: A = params(p)?;
            let c = b.cluster(&a.cluster)?.client.clone();
            let d = ServiceDiscovery::new(&c, &a.base)?;
            if method == "recipe.discovery.services" {
                Ok(json!(d.query_for_names().await?))
            } else {
                let svc = a.service.ok_or_else(|| RpcError::invalid_params("service is required"))?;
                Ok(serde_json::to_value(d.query_for_instances(&svc).await?).unwrap())
            }
        },
        "recipe.counter.get" | "recipe.counter.add" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
                #[serde(default)]
                encoding: Option<String>,
                #[serde(default)]
                delta: Option<i64>,
                #[serde(default)]
                force: bool,
            }
            let a: A = params(p)?;
            let mut c = b.cluster(&a.cluster)?.client.clone();
            if a.force {
                c = c.forced();
            }
            let counter = SharedCounter::new(&c, &a.path, encoding(a.encoding.as_deref())?)?;
            if method == "recipe.counter.add" {
                let delta = a.delta.ok_or_else(|| RpcError::invalid_params("delta is required"))?;
                counter.add(delta).await?;
            }
            Ok(serde_json::to_value(counter.get_versioned().await?).unwrap())
        },
        "recipe.queue.put" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
                #[serde(flatten)]
                payload: Payload,
                #[serde(default)]
                priority: Option<i32>,
                #[serde(default)]
                ready_at_ms: Option<i64>,
                #[serde(default)]
                force: bool,
            }
            let a: A = params(p)?;
            let mut c = b.cluster(&a.cluster)?.client.clone();
            if a.force {
                c = c.forced();
            }
            let data = a.payload.bytes()?;
            let node = match (a.priority, a.ready_at_ms) {
                (Some(_), Some(_)) => return Err(RpcError::invalid_params("give priority or ready_at_ms, not both")),
                (Some(pr), None) => PriorityQueue::new(&c, &a.path)?.put(&data, pr).await?,
                (None, Some(at)) => DistributedQueue::new(&c, &a.path)?.put_delayed(&data, at).await?,
                (None, None) => DistributedQueue::new(&c, &a.path)?.put(&data).await?,
            };
            Ok(json!({"path": node}))
        },
        "recipe.queue.peek" | "recipe.queue.take" | "recipe.queue.len" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
                #[serde(default)]
                timeout_ms: Option<u64>,
                #[serde(default)]
                force: bool,
            }
            let a: A = params(p)?;
            let mut c = b.cluster(&a.cluster)?.client.clone();
            if a.force {
                c = c.forced();
            }
            let q = DistributedQueue::new(&c, &a.path)?;
            match method {
                "recipe.queue.peek" => Ok(serde_json::to_value(q.peek().await?).unwrap()),
                "recipe.queue.len" => Ok(json!({"len": q.len().await?})),
                _ => {
                    c.check_write_allowed(&a.path)?;
                    Ok(serde_json::to_value(q.take(timeout(a.timeout_ms)).await?).unwrap())
                },
            }
        },
        "recipe.lock.acquire" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
                #[serde(default)]
                timeout_ms: Option<u64>,
                #[serde(flatten)]
                payload: Payload,
                #[serde(default)]
                force: bool,
            }
            let a: A = params(p)?;
            let mut c = b.cluster(&a.cluster)?.client.clone();
            if a.force {
                c = c.forced();
            }
            c.check_write_allowed(&a.path)?;
            let m = Mutex::new(&c, &a.path)?.with_data(&a.payload.bytes()?);
            let guard = m.acquire(Some(timeout(a.timeout_ms).unwrap_or(Duration::from_secs(30)))).await?;
            let id = b.new_id("r");
            let own = guard.ownership().clone();
            let node = guard.node_path().to_string();
            let cell = Arc::new(StdMutex::new(own.state()));
            let stop = CancellationToken::new();
            forward_ownership(b.outbox.clone(), id.clone(), own.clone(), cell.clone(), stop.clone());
            let state = state_name(own.state());
            b.recipes.entries.lock().await.insert(
                id.clone(),
                RecipeEntry {
                    kind: "lock",
                    cluster: a.cluster,
                    path: a.path,
                    node: Some(node.clone()),
                    held: Held::Lock(Some(Box::new(guard))),
                    ownership: cell,
                    leader: Arc::new(StdMutex::new(None)),
                    stop,
                },
            );
            Ok(json!({"recipe": id, "node": node, "ownership": state}))
        },
        "recipe.latch.start" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
                #[serde(default)]
                id: Option<String>,
                #[serde(default)]
                force: bool,
            }
            let a: A = params(p)?;
            let mut c = b.cluster(&a.cluster)?.client.clone();
            if a.force {
                c = c.forced();
            }
            c.check_write_allowed(&a.path)?;
            let participant = a.id.unwrap_or_else(|| format!("zk-bridge-{}", std::process::id()));
            let latch = LeaderLatch::new(&c, &a.path, &participant)?;
            latch.start()?;
            let id = b.new_id("r");
            let cell = Arc::new(StdMutex::new(OwnershipState::Released));
            let leader = Arc::new(StdMutex::new(Some(false)));
            let stop = CancellationToken::new();
            // Leadership changes come as Option<Ownership>; follow each ownership.
            {
                let mut rx = latch.subscribe();
                let outbox = b.outbox.clone();
                let rid = id.clone();
                let (cell, leader, stop) = (cell.clone(), leader.clone(), stop.clone());
                tokio::spawn(async move {
                    loop {
                        let cur = rx.borrow_and_update().clone();
                        match cur {
                            Some(own) => {
                                *leader.lock().unwrap() = Some(true);
                                *cell.lock().unwrap() = own.state();
                                outbox.notify("recipe.ownership", json!({"recipe": rid, "state": state_name(own.state()), "leader": true}));
                                let mut orx = own.subscribe();
                                loop {
                                    tokio::select! {
                                        _ = stop.cancelled() => return,
                                        r = orx.changed() => {
                                            if r.is_err() { break; }
                                            let s = *orx.borrow_and_update();
                                            *cell.lock().unwrap() = s;
                                            outbox.notify("recipe.ownership", json!({"recipe": rid, "state": state_name(s), "leader": s == OwnershipState::Held}));
                                            if s.is_terminal() { break; }
                                        }
                                        r = rx.changed() => { if r.is_err() { return; } break; }
                                    }
                                }
                            },
                            None => {
                                let was = leader.lock().unwrap().replace(false);
                                if was == Some(true) {
                                    outbox.notify("recipe.ownership", json!({"recipe": rid, "state": "lost", "leader": false}));
                                }
                                tokio::select! {
                                    _ = stop.cancelled() => return,
                                    r = rx.changed() => if r.is_err() { return },
                                }
                            },
                        }
                    }
                });
            }
            b.recipes.entries.lock().await.insert(
                id.clone(),
                RecipeEntry {
                    kind: "latch",
                    cluster: a.cluster,
                    path: a.path,
                    node: None,
                    held: Held::Latch(latch),
                    ownership: cell,
                    leader,
                    stop,
                },
            );
            Ok(json!({"recipe": id, "participant": participant}))
        },
        "recipe.member.join" => {
            #[derive(Deserialize)]
            struct A {
                cluster: String,
                path: String,
                member_id: String,
                #[serde(flatten)]
                payload: Payload,
                #[serde(default)]
                timeout_ms: Option<u64>,
                #[serde(default)]
                force: bool,
            }
            let a: A = params(p)?;
            let mut c = b.cluster(&a.cluster)?.client.clone();
            if a.force {
                c = c.forced();
            }
            c.check_write_allowed(&a.path)?;
            let member = GroupMember::join(&c, &a.path, &a.member_id, &a.payload.bytes()?)?;
            let own = member.wait_joined(timeout(a.timeout_ms).unwrap_or(Duration::from_secs(30))).await?;
            let id = b.new_id("r");
            let cell = Arc::new(StdMutex::new(own.state()));
            let stop = CancellationToken::new();
            forward_ownership(b.outbox.clone(), id.clone(), own.clone(), cell.clone(), stop.clone());
            let state = state_name(own.state());
            b.recipes.entries.lock().await.insert(
                id.clone(),
                RecipeEntry {
                    kind: "member",
                    cluster: a.cluster,
                    path: a.path,
                    node: None,
                    held: Held::Member(member),
                    ownership: cell,
                    leader: Arc::new(StdMutex::new(None)),
                    stop,
                },
            );
            Ok(json!({"recipe": id, "ownership": state}))
        },
        "recipe.release" => {
            #[derive(Deserialize)]
            struct A {
                recipe: String,
            }
            let a: A = params(p)?;
            let e = b.recipes.entries.lock().await.remove(&a.recipe).ok_or_else(|| RpcError::unknown("recipe", &a.recipe))?;
            release_entry(e).await;
            b.outbox.notify("recipe.ownership", json!({"recipe": a.recipe, "state": "released"}));
            Ok(Value::Null)
        },
        "recipe.list" => {
            let entries = b.recipes.entries.lock().await;
            let v: Vec<Value> = entries
                .iter()
                .map(|(id, e)| {
                    let mut v = json!({
                        "recipe": id, "kind": e.kind, "cluster": e.cluster, "path": e.path,
                        "ownership": state_name(*e.ownership.lock().unwrap()),
                    });
                    if let Some(n) = &e.node {
                        v["node"] = json!(n);
                    }
                    if let Some(l) = *e.leader.lock().unwrap() {
                        v["leader"] = json!(l);
                    }
                    v
                })
                .collect();
            Ok(json!(v))
        },
        _ => Err(RpcError::new(crate::rpc::METHOD_NOT_FOUND, format!("method not found: {method}"))),
    }
}
