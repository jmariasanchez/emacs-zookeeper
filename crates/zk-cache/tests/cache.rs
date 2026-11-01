//! zk-cache integration tests against a real ZooKeeper server.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rand::Rng;
use zk_cache::*;
use zk_core::*;
use zk_testkit::{require_server, unique_path, FaultProxy, ProxyMode};

fn cfg(addr: &str) -> ClusterConfig {
    let mut c = ClusterConfig::new(addr).with_session_timeout(Duration::from_secs(4));
    c.operation_timeout_ms = 5_000;
    c.connect_deadline_ms = 10_000;
    c
}

async fn connect(addr: &str) -> ZkClient {
    ZkClient::connect(cfg(addr)).await.expect("connect")
}

async fn mk(zk: &ZkClient, p: &str, d: &[u8]) {
    zk.create(p, d, &CreateOptions::persistent().with_parents()).await.unwrap();
}

/// Server truth: path -> (stat, data).
async fn truth(zk: &ZkClient, root: &str) -> BTreeMap<String, (Stat, Vec<u8>)> {
    let mut out = BTreeMap::new();
    let w = match zk.walk(root, &WalkOptions::default()).await {
        Ok(w) => w,
        Err(e) if e.kind == ErrorKind::NoNode => return out,
        Err(e) => panic!("{e}"),
    };
    for n in w.nodes {
        if let Ok(d) = zk.get_data(&n.path).await {
            out.insert(n.path, (d.stat, d.data));
        }
    }
    out
}

fn view(c: &CacheCore) -> BTreeMap<String, (Stat, Vec<u8>)> {
    c.snapshot().into_iter().map(|(k, v)| (k, (v.stat, v.data.map(|d| (*d).clone()).unwrap_or_default()))).collect()
}

async fn converges(zk: &ZkClient, c: &CacheCore, root: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let t = truth(zk, root).await;
        let v = view(c);
        if t == v && c.status().state == CacheState::Live {
            return;
        }
        if Instant::now() > deadline {
            let tk: Vec<_> = t.keys().collect();
            let vk: Vec<_> = v.keys().collect();
            panic!("cache did not converge; status {:?}\ntruth {tk:?}\ncache {vk:?}", c.status());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn next_matching<F: Fn(&CacheEvent) -> bool>(
    rx: &mut tokio::sync::broadcast::Receiver<CacheEvent>,
    timeout: Duration,
    f: F,
) -> Vec<CacheEvent> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut seen = vec![];
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Ok(e)) => {
                let done = f(&e);
                seen.push(e);
                if done {
                    return seen;
                }
            },
            Ok(Err(_)) => panic!("channel error; seen {seen:?}"),
            Err(_) => panic!("timeout; seen {seen:?}"),
        }
    }
}

#[tokio::test]
async fn initial_load_and_incremental() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let zk = connect(&server.address()).await;
    let root = unique_path("tree");
    mk(&zk, &format!("{root}/a/x"), b"ax").await;
    mk(&zk, &format!("{root}/b"), b"b").await;
    let cache = TreeCache::start(zk.clone(), &root, CacheOptions::default()).await.unwrap();
    assert_eq!(view(&cache), truth(&zk, &root).await);
    assert_eq!(cache.children(&root), vec!["a", "b"]);
    assert_eq!(cache.status().state, CacheState::Live);
    assert_eq!(cache.status().nodes, 4);
    let mut rx = cache.subscribe();

    // add
    mk(&zk, &format!("{root}/a/y"), b"ay").await;
    let ev = next_matching(&mut rx, Duration::from_secs(5), |e| matches!(e, CacheEvent::NodeAdded { .. })).await;
    let CacheEvent::NodeAdded { path, stat } = ev.last().unwrap() else { unreachable!() };
    assert_eq!(path, &format!("{root}/a/y"));
    assert_eq!(stat.data_length, 2);
    // update
    zk.set_data(&format!("{root}/b"), b"bb", None).await.unwrap();
    let ev = next_matching(&mut rx, Duration::from_secs(5), |e| matches!(e, CacheEvent::NodeUpdated { path, .. } if path.ends_with("/b"))).await;
    let CacheEvent::NodeUpdated { stat, old_stat, .. } = ev.last().unwrap() else { unreachable!() };
    assert_eq!(stat.version, old_stat.version + 1);
    assert_eq!(cache.get(&format!("{root}/b")).unwrap().data.unwrap().as_slice(), b"bb");
    // nested delete
    zk.delete_recursive(&format!("{root}/a"), &Default::default()).await.unwrap();
    converges(&zk, &cache, &root, Duration::from_secs(5)).await;
    assert_eq!(cache.children(&root), vec!["b"]);
    let mut removed = vec![];
    while let Ok(e) = rx.try_recv() {
        if let CacheEvent::NodeRemoved { path } = e {
            removed.push(path);
        }
    }
    removed.sort();
    removed.dedup();
    assert_eq!(removed, vec![format!("{root}/a"), format!("{root}/a/x"), format!("{root}/a/y")]);
    // root parent stat (num_children) is maintained
    assert_eq!(cache.get(&root).unwrap().stat.num_children, 1);

    // json shape
    let j = serde_json::to_value(CacheEvent::NodeRemoved { path: "/x".into() }).unwrap();
    assert_eq!(j["type"], "node-removed");
    cache.close();
    assert_eq!(cache.status().state, CacheState::Closed);
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
}

#[tokio::test]
async fn budgets() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let zk = connect(&server.address()).await;
    let root = unique_path("budget");
    mk(&zk, &root, b"").await;
    for i in 0..10 {
        mk(&zk, &format!("{root}/n{i}"), &[b'x'; 100]).await;
    }
    mk(&zk, &format!("{root}/big"), &[b'y'; 5000]).await;

    let opts = CacheOptions { max_bytes: 450, max_data_per_node: 1000, ..Default::default() };
    let cache = TreeCache::start(zk.clone(), &root, opts).await.unwrap();
    let s = cache.status();
    assert_eq!(s.nodes, 12);
    assert!(s.bytes <= 450, "{s:?}");
    assert!(s.evictions >= 6, "{s:?}");
    assert!(!s.truncated);
    let big = cache.get(&format!("{root}/big")).unwrap();
    assert!(big.data.is_none());
    assert_eq!(big.stat.data_length, 5000);
    let with_data = cache.snapshot().values().filter(|n| n.data.is_some()).count();
    assert!((1..=5).contains(&with_data), "{with_data}");

    let opts = CacheOptions { max_nodes: 5, ..Default::default() };
    let small = TreeCache::start(zk.clone(), &root, opts).await.unwrap();
    assert!(small.status().truncated);
    assert_eq!(small.status().nodes, 5);
    mk(&zk, &format!("{root}/later"), b"").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(small.status().nodes, 5);
    assert!(small.get(&format!("{root}/later")).is_none());

    let nodata = TreeCache::start(zk.clone(), &root, CacheOptions { cache_data: false, ..Default::default() }).await.unwrap();
    assert!(nodata.snapshot().values().all(|n| n.data.is_none()));
    assert_eq!(nodata.status().bytes, 0);
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
}

#[tokio::test]
async fn node_cache_nonexistent_then_created() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let zk = connect(&server.address()).await;
    let root = unique_path("nodecache");
    let cache = NodeCache::start(zk.clone(), &root, CacheOptions::default()).await.unwrap();
    assert!(cache.node().is_none());
    assert_eq!(cache.status().state, CacheState::Live);
    let mut rx = cache.subscribe();
    mk(&zk, &root, b"v1").await;
    next_matching(&mut rx, Duration::from_secs(5), |e| matches!(e, CacheEvent::NodeAdded { .. })).await;
    assert_eq!(cache.node().unwrap().data.unwrap().as_slice(), b"v1");
    // children do not appear in a node cache
    mk(&zk, &format!("{root}/c"), b"").await;
    zk.set_data(&root, b"v2", None).await.unwrap();
    next_matching(&mut rx, Duration::from_secs(5), |e| matches!(e, CacheEvent::NodeUpdated { stat, .. } if stat.version == 1)).await;
    assert_eq!(cache.snapshot().len(), 1);
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
    next_matching(&mut rx, Duration::from_secs(5), |e| matches!(e, CacheEvent::NodeRemoved { .. })).await;
    assert!(cache.node().is_none());
}

#[tokio::test]
async fn children_cache() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let zk = connect(&server.address()).await;
    let root = unique_path("children");
    mk(&zk, &format!("{root}/a/deep"), b"").await;
    let cache = ChildrenCache::start(zk.clone(), &root, CacheOptions::default()).await.unwrap();
    assert_eq!(cache.snapshot().len(), 2);
    mk(&zk, &format!("{root}/b"), b"").await;
    mk(&zk, &format!("{root}/a/deep2"), b"").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(zk_testkit::eventually(Duration::from_secs(5), || cache.children(&root) == vec!["a", "b"]).await);
    assert!(cache.get(&format!("{root}/a/deep2")).is_none());
    // child stat num_children refreshed by grandchild creation
    assert!(zk_testkit::eventually(Duration::from_secs(5), || cache.get(&format!("{root}/a")).unwrap().stat.num_children == 2).await);
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
}

#[tokio::test]
async fn resync_after_disconnect() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let proxy = FaultProxy::start(server.address()).await;
    let mut c = cfg(&proxy.address());
    c.session_timeout_ms = 8_000;
    c.operation_timeout_ms = 1_000;
    let zk = ZkClient::connect(c).await.unwrap();
    let other = connect(&server.address()).await;
    let root = unique_path("resync");
    mk(&other, &format!("{root}/keep"), b"k").await;
    mk(&other, &format!("{root}/gone/sub"), b"g").await;
    mk(&other, &format!("{root}/mod"), b"m").await;
    let cache = TreeCache::start(zk.clone(), &root, CacheOptions::default()).await.unwrap();
    let gen = cache.status().generation;
    let mut rx = cache.subscribe();

    proxy.set_mode(ProxyMode::Refuse);
    proxy.drop_connections();
    assert!(zk_testkit::eventually(Duration::from_secs(5), || cache.status().state != CacheState::Live).await);
    other.delete_recursive(&format!("{root}/gone"), &Default::default()).await.unwrap();
    other.set_data(&format!("{root}/mod"), b"m2", None).await.unwrap();
    mk(&other, &format!("{root}/new"), b"n").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    proxy.set_mode(ProxyMode::Pass);

    let evs = next_matching(&mut rx, Duration::from_secs(15), |e| matches!(e, CacheEvent::Resynced { .. })).await;
    let ri = evs.iter().position(|e| matches!(e, CacheEvent::Resyncing { .. })).expect("Resyncing");
    let tail = &evs[ri..];
    assert!(tail.iter().any(|e| matches!(e, CacheEvent::NodeAdded { path, .. } if path.ends_with("/new"))), "{evs:?}");
    assert!(tail.iter().any(|e| matches!(e, CacheEvent::NodeRemoved { path } if path.ends_with("/gone/sub"))), "{evs:?}");
    assert!(tail.iter().any(|e| matches!(e, CacheEvent::NodeUpdated { path, .. } if path.ends_with("/mod"))), "{evs:?}");
    converges(&other, &cache, &root, Duration::from_secs(10)).await;
    assert_eq!(cache.status().generation, gen);
    other.delete_recursive(&root, &Default::default()).await.unwrap();
}

#[tokio::test]
async fn rebuild_after_session_expiry() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let proxy = FaultProxy::start(server.address()).await;
    let mut c = cfg(&proxy.address());
    c.session_timeout_ms = 2_000;
    c.operation_timeout_ms = 3_000;
    let zk = ZkClient::connect(c).await.unwrap();
    let other = connect(&server.address()).await;
    let root = unique_path("expiry");
    mk(&other, &format!("{root}/a"), b"a").await;
    zk.create(&format!("{root}/eph"), b"", &CreateOptions::ephemeral()).await.unwrap();
    let cache = TreeCache::start(zk.clone(), &root, CacheOptions::default()).await.unwrap();
    assert!(cache.get(&format!("{root}/eph")).is_some());
    let gen = cache.status().generation;

    let p = &proxy;
    let o = other.clone();
    let r = root.clone();
    let partition = async move {
        p.set_mode(ProxyMode::Refuse);
        p.drop_connections();
        tokio::time::sleep(Duration::from_secs(1)).await;
        o.set_data(&format!("{r}/a"), b"a2", None).await.unwrap();
        mk(&o, &format!("{r}/b"), b"b").await;
        tokio::time::sleep(Duration::from_millis(4500)).await;
        p.set_mode(ProxyMode::Pass);
    };
    let mut states = vec![];
    let observe = async {
        let deadline = Instant::now() + Duration::from_secs(6);
        while Instant::now() < deadline {
            let s = cache.status().state;
            if states.last() != Some(&s) {
                states.push(s);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::join!(partition, observe);
    assert!(states.iter().any(|s| matches!(s, CacheState::Suspended | CacheState::Resyncing | CacheState::Stale)), "{states:?}");
    converges(&other, &cache, &root, Duration::from_secs(20)).await;
    assert!(cache.status().generation > gen, "{:?}", cache.status());
    assert!(cache.get(&format!("{root}/eph")).is_none());
    assert_eq!(cache.get(&format!("{root}/a")).unwrap().data.unwrap().as_slice(), b"a2");
    other.delete_recursive(&root, &Default::default()).await.unwrap();
}

#[tokio::test]
async fn concurrent_writers_converge() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let zk = connect(&server.address()).await;
    let root = unique_path("random");
    mk(&zk, &root, b"").await;
    let cache = TreeCache::start(zk.clone(), &root, CacheOptions::default()).await.unwrap();
    let mut writers = vec![];
    for w in 0..3u64 {
        let c = connect(&server.address()).await;
        let root = root.clone();
        writers.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64_compat(w);
            let end = Instant::now() + Duration::from_secs(4);
            while Instant::now() < end {
                let a = rng.gen_range(0..6);
                let b = rng.gen_range(0..4);
                let p = if rng.gen_bool(0.5) { format!("{root}/n{a}") } else { format!("{root}/n{a}/c{b}") };
                let data = format!("{}", rng.gen::<u32>());
                match rng.gen_range(0..3) {
                    0 => {
                        let _ = c.create(&p, data.as_bytes(), &CreateOptions::persistent()).await;
                    },
                    1 => {
                        let _ = c.set_data(&p, data.as_bytes(), None).await;
                    },
                    _ => {
                        let _ = c.delete_recursive(&p, &Default::default()).await;
                    },
                }
            }
            c.close().await;
        }));
    }
    for w in writers {
        w.await.unwrap();
    }
    converges(&zk, &cache, &root, Duration::from_secs(10)).await;
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
}

trait SeedCompat {
    fn seed_from_u64_compat(s: u64) -> Self;
}
impl SeedCompat for rand::rngs::StdRng {
    fn seed_from_u64_compat(s: u64) -> Self {
        use rand::SeedableRng;
        rand::rngs::StdRng::seed_from_u64(s ^ std::process::id() as u64)
    }
}

#[tokio::test]
#[ignore]
async fn bench_10k_tree() {
    let server = require_server!();
    // Caches are built on persistent watches (ZooKeeper 3.6+).
    zk_testkit::require_version!(server, 3, 6);
    let zk = connect(&server.address()).await;
    let root = unique_path("bench");
    mk(&zk, &root, b"").await;
    let t = Instant::now();
    for i in 0..100 {
        let p = format!("{root}/d{i}");
        mk(&zk, &p, b"dir").await;
        let futs: Vec<_> = (0..99)
            .map(|j| {
                let zk = zk.clone();
                let p = format!("{p}/n{j}");
                async move { zk.create(&p, b"0123456789", &CreateOptions::persistent()).await.unwrap() }
            })
            .collect();
        futures::future::join_all(futs).await;
    }
    eprintln!("populate 10k: {:?}", t.elapsed());
    let t = Instant::now();
    let cache = TreeCache::start(zk.clone(), &root, CacheOptions::default()).await.unwrap();
    eprintln!("initial load of {} nodes: {:?} ({:?})", cache.status().nodes, t.elapsed(), cache.status());
    drop(cache);
    zk.delete_recursive(&root, &DeleteRecursiveOptions { limit: Some(20_000), ..Default::default() }).await.unwrap();
}

#[tokio::test]
async fn old_servers_report_unimplemented() {
    let server = require_server!();
    if server.at_least(3, 6) {
        return;
    }
    let zk = connect(&server.address()).await;
    let e = TreeCache::start(zk.clone(), "/", CacheOptions::default()).await.err().expect("must fail on < 3.6");
    assert_eq!(e.kind, ErrorKind::Unimplemented);
    assert!(e.message.contains("3.6"), "{e}");
    zk.close().await;
}
