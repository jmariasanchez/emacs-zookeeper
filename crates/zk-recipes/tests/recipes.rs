//! Multi-client integration tests for zk-recipes against a real ZooKeeper.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zk_core::{ClusterConfig, CreateOptions, ZkClient};
use zk_recipes::*;
use zk_testkit::{require_server, unique_path};

fn cfg(addr: &str) -> ClusterConfig {
    let mut c = ClusterConfig::new(addr).with_session_timeout(Duration::from_secs(4));
    c.operation_timeout_ms = 5_000;
    c.connect_deadline_ms = 10_000;
    c
}

async fn clients(addr: &str, n: usize) -> Vec<ZkClient> {
    let mut v = Vec::new();
    for _ in 0..n {
        v.push(ZkClient::connect(cfg(addr)).await.expect("connect"));
    }
    v
}

/// Tracks concurrent holders and the maximum ever observed.
#[derive(Default)]
struct Concurrency {
    cur: AtomicUsize,
    max: AtomicUsize,
}

impl Concurrency {
    fn enter(&self) {
        let n = self.cur.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(n, Ordering::SeqCst);
    }
    fn leave(&self) {
        self.cur.fetch_sub(1, Ordering::SeqCst);
    }
    fn max(&self) -> usize {
        self.max.load(Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutex_mutual_exclusion() {
    let server = require_server!();
    let base = unique_path("mutex");
    let cs = clients(&server.address(), 5).await;
    let conc = Arc::new(Concurrency::default());
    let done = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for (i, c) in cs.iter().enumerate() {
        let m = Mutex::new(c, &base).unwrap().with_data(format!("client-{i}").as_bytes());
        let conc = conc.clone();
        let done = done.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..4 {
                let g = m.acquire(Some(Duration::from_secs(30))).await.unwrap();
                assert!(g.ownership().is_held());
                conc.enter();
                tokio::time::sleep(Duration::from_millis(10)).await;
                conc.leave();
                done.fetch_add(1, Ordering::SeqCst);
                g.release().await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(done.load(Ordering::SeqCst), 20);
    assert_eq!(conc.max(), 1);
    // Node layout is Curator compatible and nothing is left behind.
    assert!(cs[0].children(&base).await.unwrap().is_empty());
}

#[tokio::test]
async fn mutex_timeout_cancellation_and_participants() {
    let server = require_server!();
    let base = unique_path("mutex-cancel");
    let cs = clients(&server.address(), 2).await;
    let a = Mutex::new(&cs[0], &base).unwrap().with_data(b"A");
    let b = Mutex::new(&cs[1], &base).unwrap().with_data(b"B");
    let g = a.acquire(None).await.unwrap();
    let name = g.node_path().rsplit('/').next().unwrap().to_string();
    assert!(name.starts_with("_c_") && name.contains("-lock-"), "{name}");
    assert!(b.try_acquire().await.unwrap().is_none());
    let e = b.acquire(Some(Duration::from_millis(300))).await.unwrap_err();
    assert_eq!(e.kind, zk_core::ErrorKind::Timeout);

    // Dropping a pending acquire future removes its contender node.
    {
        let fut = b.acquire(None);
        let _ = tokio::time::timeout(Duration::from_millis(300), fut).await;
    }
    let mut ok = false;
    for _ in 0..40 {
        if cs[0].children(&base).await.unwrap().len() == 1 {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ok, "abandoned contender must be cleaned up");

    let pending = tokio::spawn({
        let b = b.clone();
        async move { b.acquire(Some(Duration::from_secs(10))).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let parts = a.participants().await.unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts[0].is_owner && parts[0].data == b"A");
    assert!(!parts[1].is_owner && parts[1].data == b"B");
    let insp = inspect(&cs[0], &base).await.unwrap();
    assert!(matches!(insp, Inspection::Lock { ref contenders, .. } if contenders.len() == 2));
    serde_json::to_string(&insp).unwrap();

    // Dropping the guard releases in the background.
    drop(g);
    let g2 = pending.await.unwrap().unwrap();
    assert!(g2.ownership().is_held());
    g2.release().await.unwrap();
}

#[tokio::test]
async fn reentrant_mutex() {
    let server = require_server!();
    let base = unique_path("reentrant");
    let cs = clients(&server.address(), 2).await;
    let r = ReentrantMutex::new(Mutex::new(&cs[0], &base).unwrap());
    let other = Mutex::new(&cs[1], &base).unwrap();
    let g1 = r.acquire(None).await.unwrap();
    let g2 = r.acquire(Some(Duration::from_secs(1))).await.unwrap();
    assert_eq!(r.hold_count(), 2);
    assert_eq!(cs[0].children(&base).await.unwrap().len(), 1);
    g1.release().await.unwrap();
    assert!(other.try_acquire().await.unwrap().is_none());
    g2.release().await.unwrap();
    assert_eq!(r.hold_count(), 0);
    let g = other.acquire(Some(Duration::from_secs(5))).await.unwrap();
    g.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_write_lock() {
    let server = require_server!();
    let base = unique_path("rwlock");
    let cs = clients(&server.address(), 4).await;
    let locks: Vec<ReadWriteLock> = cs.iter().map(|c| ReadWriteLock::new(c, &base).unwrap()).collect();
    // Readers share.
    let r1 = locks[0].read(Some(Duration::from_secs(5))).await.unwrap();
    let r2 = locks[1].read(Some(Duration::from_secs(5))).await.unwrap();
    // Writer waits.
    let w = tokio::spawn({
        let l = locks[2].clone();
        async move { l.write(Some(Duration::from_secs(10))).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!w.is_finished());
    // A later reader queues behind the writer (no writer starvation).
    let r3 = tokio::spawn({
        let l = locks[3].clone();
        async move { l.read(Some(Duration::from_secs(10))).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!r3.is_finished());
    match inspect(&cs[0], &base).await.unwrap() {
        Inspection::ReadWriteLock { readers_holding, writer_holding, contenders, .. } => {
            assert_eq!(readers_holding, 2);
            assert!(!writer_holding);
            assert_eq!(contenders.len(), 4);
        }
        other => panic!("{other:?}"),
    }
    r1.release().await.unwrap();
    r2.release().await.unwrap();
    let wg = w.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!r3.is_finished());
    wg.release().await.unwrap();
    r3.await.unwrap().unwrap().release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_latch_single_leader_and_failover() {
    let server = require_server!();
    let base = unique_path("latch");
    let cs = clients(&server.address(), 3).await;
    let latches: Vec<LeaderLatch> = cs.iter().enumerate().map(|(i, c)| LeaderLatch::new(c, &base, &format!("p{i}")).unwrap()).collect();
    for l in &latches {
        l.start().unwrap();
        // Stagger starts so queue order is deterministic.
        let n = latches.iter().position(|x| std::ptr::eq(x, l)).unwrap() + 1;
        for _ in 0..50 {
            if l.participants().await.unwrap().len() == n {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let first = latches[0].await_leadership(Some(Duration::from_secs(10))).await;
    assert!(first.is_ok(), "first starter should lead");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(latches.iter().filter(|l| l.has_leadership()).count(), 1);
    assert_eq!(latches[1].participants().await.unwrap().len(), 3);
    assert_eq!(latches[2].leader().await.unwrap().unwrap().data, b"p0");
    assert!(matches!(inspect(&cs[0], &base).await.unwrap(), Inspection::LeaderLatch { leader: Some(ref l), .. } if l == "p0"));

    let own = latches[0].leadership().unwrap();
    latches[0].close().await;
    assert_eq!(own.state(), OwnershipState::Released);
    latches[1].await_leadership(Some(Duration::from_secs(10))).await.unwrap();
    assert!(!latches[2].has_leadership());
    latches[1].close().await;
    latches[2].await_leadership(Some(Duration::from_secs(10))).await.unwrap();
    latches[2].close().await;
    assert!(latches[0].start().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_election_requeues() {
    let server = require_server!();
    let base = unique_path("election");
    let cs = clients(&server.address(), 3).await;
    let conc = Arc::new(Concurrency::default());
    let terms = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    let mut elections = Vec::new();
    for (i, c) in cs.iter().enumerate() {
        let e = LeaderElection::new(c, &base, &format!("e{i}")).unwrap();
        elections.push(e.clone());
        let conc = conc.clone();
        let terms = terms.clone();
        handles.push(tokio::spawn(async move {
            let mine = Arc::new(AtomicUsize::new(0));
            let m = mine.clone();
            e.run(move |own| {
                let conc = conc.clone();
                let terms = terms.clone();
                let m = m.clone();
                async move {
                    assert!(own.is_held());
                    conc.enter();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    conc.leave();
                    terms.fetch_add(1, Ordering::SeqCst);
                    m.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await
            .unwrap();
            mine.load(Ordering::SeqCst)
        }));
    }
    assert!(zk_testkit::eventually(Duration::from_secs(20), || terms.load(Ordering::SeqCst) >= 9).await);
    for e in &elections {
        e.close();
    }
    let mut per = Vec::new();
    for h in handles {
        per.push(h.await.unwrap());
    }
    assert_eq!(conc.max(), 1);
    assert!(per.iter().all(|n| *n >= 1), "every participant re-queued and led: {per:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn semaphore_limits_concurrency() {
    let server = require_server!();
    let base = unique_path("sem");
    let cs = clients(&server.address(), 6).await;
    let conc = Arc::new(Concurrency::default());
    let mut tasks = Vec::new();
    for c in &cs {
        let s = Semaphore::new(c, &base, 2).unwrap();
        let conc = conc.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..3 {
                let l = s.acquire(Some(Duration::from_secs(30))).await.unwrap();
                assert!(l.ownership().is_held());
                conc.enter();
                tokio::time::sleep(Duration::from_millis(30)).await;
                conc.leave();
                l.release().await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert!(conc.max() <= 2, "max {}", conc.max());
    assert_eq!(conc.max(), 2, "both permits should be used concurrently");

    let s = Semaphore::new(&cs[0], &base, 2).unwrap();
    let ls = s.acquire_many(2, Some(Duration::from_secs(5))).await.unwrap();
    let s2 = Semaphore::new(&cs[1], &base, 2).unwrap();
    assert!(s2.acquire(Some(Duration::from_millis(300))).await.is_err());
    assert_eq!(s.leases().await.unwrap().len(), 2);
    assert!(matches!(inspect(&cs[0], &base).await.unwrap(), Inspection::Semaphore { ref leases, .. } if leases.len() == 2));
    drop(ls);
    s2.acquire(Some(Duration::from_secs(5))).await.unwrap().release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barriers() {
    let server = require_server!();
    let base = unique_path("barrier");
    let cs = clients(&server.address(), 3).await;
    let b = Barrier::new(&cs[0], &format!("{base}/b")).unwrap();
    b.set().await.unwrap();
    assert!(!Barrier::new(&cs[1], &format!("{base}/b")).unwrap().wait_on_barrier(Some(Duration::from_millis(200))).await.unwrap());
    let w = tokio::spawn({
        let b = Barrier::new(&cs[1], &format!("{base}/b")).unwrap();
        async move { b.wait_on_barrier(Some(Duration::from_secs(10))).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    b.remove().await.unwrap();
    assert!(w.await.unwrap().unwrap());

    // Double barrier with 3 members.
    let dpath = format!("{base}/double");
    let entered = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for (i, c) in cs.iter().enumerate() {
        let mut d = DoubleBarrier::new(c, &dpath, 3, &format!("m{i}")).unwrap();
        let entered = entered.clone();
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100 * i as u64)).await;
            assert!(d.enter(Some(Duration::from_secs(10))).await.unwrap());
            // Everyone must be present once enter returns.
            entered.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(d.leave(Some(Duration::from_secs(10))).await.unwrap());
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(entered.load(Ordering::SeqCst), 3);
    assert!(cs[0].children(&dpath).await.unwrap().is_empty());
    // Too few members: enter times out.
    let mut lone = DoubleBarrier::new(&cs[0], &format!("{base}/lone"), 2, "x").unwrap();
    assert!(!lone.enter(Some(Duration::from_millis(300))).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counters_are_atomic() {
    let server = require_server!();
    let base = unique_path("counter");
    let cs = clients(&server.address(), 4).await;
    for enc in [CounterEncoding::Int64Be, CounterEncoding::Int32Be, CounterEncoding::Decimal] {
        let path = format!("{base}/{enc:?}");
        let mut tasks = Vec::new();
        for c in &cs {
            let ctr = SharedCounter::new(c, &path, enc).unwrap();
            tasks.push(tokio::spawn(async move {
                for _ in 0..10 {
                    ctr.increment().await.unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let ctr = SharedCounter::new(&cs[0], &path, enc).unwrap();
        assert_eq!(ctr.get().await.unwrap(), 40, "{enc:?}");
        assert!(ctr.compare_and_set(40, 100).await.unwrap());
        assert!(!ctr.compare_and_set(40, 7).await.unwrap());
        let v = ctr.get_versioned().await.unwrap();
        assert!(ctr.try_set(v, 5).await.unwrap());
        assert!(!ctr.try_set(v, 6).await.unwrap(), "stale version must fail");
        assert_eq!(ctr.add(-3).await.unwrap(), 2);
    }
    let raw = cs[0].get_data(&format!("{base}/Int32Be")).await.unwrap().data;
    assert_eq!(raw, 2i32.to_be_bytes());
    let raw = cs[0].get_data(&format!("{base}/Int64Be")).await.unwrap().data;
    assert_eq!(raw, 2i64.to_be_bytes());
    assert!(matches!(inspect(&cs[0], &format!("{base}/Int64Be")).await.unwrap(), Inspection::Counter { int64: Some(2), .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_fifo_and_exactly_once() {
    let server = require_server!();
    let base = unique_path("queue");
    let cs = clients(&server.address(), 6).await;
    let q = DistributedQueue::new(&cs[0], &base).unwrap();
    for i in 0..5 {
        q.put(format!("{i}").as_bytes()).await.unwrap();
    }
    assert_eq!(q.len().await.unwrap(), 5);
    assert_eq!(q.peek().await.unwrap().unwrap().data, b"0");
    for i in 0..5 {
        assert_eq!(q.take(Some(Duration::from_secs(1))).await.unwrap().unwrap().data, format!("{i}").as_bytes());
    }
    assert!(q.take(Some(Duration::from_millis(200))).await.unwrap().is_none());

    // Exactly-once under 5 concurrent consumers.
    let n = 100;
    let consumers: Vec<_> = cs[1..].iter().map(|c| DistributedQueue::new(c, &base).unwrap()).collect();
    let taken = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for cq in consumers {
        let taken = taken.clone();
        tasks.push(tokio::spawn(async move {
            while let Some(item) = cq.take(Some(Duration::from_secs(2))).await.unwrap() {
                taken.lock().unwrap().push(String::from_utf8(item.data).unwrap().parse::<usize>().unwrap());
            }
        }));
    }
    for i in 0..n {
        q.put(format!("{i}").as_bytes()).await.unwrap();
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut v = taken.lock().unwrap().clone();
    v.sort();
    assert_eq!(v, (0..n).collect::<Vec<_>>(), "each item taken exactly once");

    // Blocking take wakes on put.
    let waiter = tokio::spawn({
        let q = DistributedQueue::new(&cs[1], &base).unwrap();
        async move { q.take(Some(Duration::from_secs(10))).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    q.put(b"late").await.unwrap();
    assert_eq!(waiter.await.unwrap().unwrap().unwrap().data, b"late");
}

#[tokio::test]
async fn delayed_and_priority_queue() {
    let server = require_server!();
    let base = unique_path("pq");
    let cs = clients(&server.address(), 1).await;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
    let q = DistributedQueue::new(&cs[0], &format!("{base}/delayed")).unwrap();
    q.put_delayed(b"later", now + 800).await.unwrap();
    q.put(b"now").await.unwrap();
    assert_eq!(q.take(Some(Duration::from_secs(1))).await.unwrap().unwrap().data, b"now");
    assert!(q.try_take().await.unwrap().is_none());
    let start = std::time::Instant::now();
    let item = q.take(Some(Duration::from_secs(5))).await.unwrap().unwrap();
    assert_eq!(item.data, b"later");
    assert!(item.ready_at_ms.is_some());
    assert!(start.elapsed() >= Duration::from_millis(300));

    let pq = PriorityQueue::new(&cs[0], &format!("{base}/prio")).unwrap();
    pq.put(b"low", 10).await.unwrap();
    pq.put(b"high", -5).await.unwrap();
    pq.put(b"mid1", 0).await.unwrap();
    pq.put(b"mid2", 0).await.unwrap();
    match inspect(&cs[0], &format!("{base}/prio")).await.unwrap() {
        Inspection::Queue { size, ready, .. } => assert_eq!((size, ready), (4, 4)),
        other => panic!("{other:?}"),
    }
    let mut order = Vec::new();
    while let Some(i) = pq.try_take().await.unwrap() {
        order.push(String::from_utf8(i.data).unwrap());
    }
    assert_eq!(order, vec!["high", "mid1", "mid2", "low"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn locking_queue() {
    let server = require_server!();
    let base = unique_path("lq");
    let cs = clients(&server.address(), 3).await;
    let q = LockingQueue::new(&cs[0], &base).unwrap();
    for i in 0..3 {
        q.put(format!("{i}").as_bytes(), 0).await.unwrap();
    }
    let a = LockingQueue::new(&cs[1], &base).unwrap();
    let b = LockingQueue::new(&cs[2], &base).unwrap();
    let ia = a.get(Some(Duration::from_secs(2))).await.unwrap().unwrap();
    let ib = b.get(Some(Duration::from_secs(2))).await.unwrap().unwrap();
    assert_ne!(ia.item.node, ib.item.node);
    assert_eq!(q.claimed().await.unwrap(), 2);
    assert!(matches!(inspect(&cs[0], &base).await.unwrap(), Inspection::LockingQueue { size: 3, claimed: 2, .. }));
    // Released item becomes available again.
    let released = ib.item.node.clone();
    ib.release().await.unwrap();
    ia.consume().await.unwrap();
    let mut seen = Vec::new();
    while let Some(it) = b.get(Some(Duration::from_millis(500))).await.unwrap() {
        seen.push(it.item.node.clone());
        it.consume().await.unwrap();
    }
    assert!(seen.contains(&released));
    assert_eq!(seen.len(), 2);
    assert!(q.is_empty().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn id_generators_unique() {
    let server = require_server!();
    let base = unique_path("ids");
    let cs = clients(&server.address(), 4).await;
    let all = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for c in &cs {
        let s = SequentialIdGenerator::new(c, &format!("{base}/seq")).unwrap();
        let k = CounterIdGenerator::new(c, &format!("{base}/ctr"), 5).unwrap();
        let all = all.clone();
        tasks.push(tokio::spawn(async move {
            let mut mine = Vec::new();
            for _ in 0..15 {
                mine.push(("s", s.next_id().await.unwrap()));
                mine.push(("c", k.next_id().await.unwrap()));
            }
            all.lock().unwrap().extend(mine);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let v = all.lock().unwrap().clone();
    let mut s: Vec<i64> = v.iter().filter(|x| x.0 == "s").map(|x| x.1).collect();
    let mut c: Vec<i64> = v.iter().filter(|x| x.0 == "c").map(|x| x.1).collect();
    s.sort();
    s.dedup();
    c.sort();
    c.dedup();
    assert_eq!(s.len(), 60);
    assert_eq!(c.len(), 60);
    let e = SequentialIdGenerator::new(&cs[0], &base).unwrap().with_epoch(3);
    assert_eq!(e.next_id().await.unwrap() >> 32, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_membership() {
    let server = require_server!();
    let base = unique_path("group");
    let cs = clients(&server.address(), 3).await;
    let a = GroupMember::join(&cs[0], &base, "a", b"pa").unwrap();
    a.wait_joined(Duration::from_secs(5)).await.unwrap();
    let mut w = a.watch();
    let b = GroupMember::join(&cs[1], &base, "b", b"pb").unwrap();
    b.wait_joined(Duration::from_secs(5)).await.unwrap();
    let ok = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if w.borrow_and_update().len() == 2 {
                return;
            }
            w.changed().await.unwrap();
        }
    })
    .await;
    assert!(ok.is_ok());
    b.set_payload(b"pb2").await.unwrap();
    assert_eq!(a.members().await.unwrap().get("b").unwrap(), b"pb2");
    // Deleted by an operator: re-created.
    cs[2].delete(&format!("{base}/b"), None).await.unwrap();
    assert!(zk_testkit::eventually(Duration::from_secs(5), || b.incarnations() >= 2).await);
    assert_eq!(a.members().await.unwrap().get("b").unwrap(), b"pb2");
    b.leave().await.unwrap();
    assert_eq!(a.members().await.unwrap().len(), 1);
    a.leave().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_discovery() {
    let server = require_server!();
    let base = unique_path("disco");
    let cs = clients(&server.address(), 2).await;
    let d1 = ServiceDiscovery::new(&cs[0], &base).unwrap();
    let d2 = ServiceDiscovery::new(&cs[1], &base).unwrap();
    let cache = d2.service_cache("api");
    let mut i1 = ServiceInstance::new("api", "10.0.0.1", Some(8080));
    i1.payload = Some(serde_json::json!({"zone": "a"}));
    i1.uri_spec = Some(UriSpec::parse("{scheme}://{address}:{port}"));
    let i2 = ServiceInstance::new("api", "10.0.0.2", Some(8080));
    d1.register(&i1, Duration::from_secs(5)).await.unwrap();
    d2.register(&i2, Duration::from_secs(5)).await.unwrap();
    assert_eq!(d2.query_for_names().await.unwrap(), vec!["api"]);
    assert_eq!(d2.query_for_instances("api").await.unwrap().len(), 2);
    let got = d2.query_for_instance("api", &i1.id).await.unwrap().unwrap();
    assert_eq!(got, i1);
    assert_eq!(got.uri().unwrap(), "http://10.0.0.1:8080");
    // Raw JSON is Curator's field naming.
    let raw: serde_json::Value = serde_json::from_slice(&cs[1].get_data(&format!("{base}/api/{}", i1.id)).await.unwrap().data).unwrap();
    assert!(raw.get("registrationTimeUTC").is_some() && raw.get("sslPort").is_some() && raw["serviceType"] == "DYNAMIC");
    cache.wait_for(Duration::from_secs(5), |v| v.len() == 2).await.unwrap();
    let mut upd = i1.clone();
    upd.port = Some(9090);
    d1.update(&upd).await.unwrap();
    cache.wait_for(Duration::from_secs(5), |v| v.iter().any(|i| i.port == Some(9090))).await.unwrap();
    assert!(matches!(inspect(&cs[0], &base).await.unwrap(), Inspection::ServiceRegistry { ref services, .. } if services[0].instances.len() == 2));
    assert!(matches!(inspect(&cs[0], &format!("{base}/api")).await.unwrap(), Inspection::Service { ref instances, .. } if instances.len() == 2));
    d1.unregister("api", &i1.id).await.unwrap();
    cache.wait_for(Duration::from_secs(5), |v| v.len() == 1).await.unwrap();
    d2.close().await;
    cache.wait_for(Duration::from_secs(5), |v| v.is_empty()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_assignment_rebalances() {
    let server = require_server!();
    let base = unique_path("parts");
    let cs = clients(&server.address(), 3).await;
    let n = 12;
    let a = PartitionAssigner::start(&cs[0], &base, "a", n).unwrap();
    let full = a.wait_for(Duration::from_secs(10), |x| x.owned.len() == n as usize && x.is_settled()).await.unwrap();
    assert_eq!(full.members, vec!["a"]);
    let b = PartitionAssigner::start(&cs[1], &base, "b", n).unwrap();
    let c = PartitionAssigner::start(&cs[2], &base, "c", n).unwrap();
    let all = [&a, &b, &c];
    let settled = |x: &Assignment| x.members.len() == 3 && x.is_settled();
    for p in all {
        p.wait_for(Duration::from_secs(15), settled).await.unwrap();
    }
    let mut owned: Vec<u32> = all.iter().flat_map(|p| p.assignment().owned).collect();
    owned.sort();
    assert_eq!(owned, (0..n).collect::<Vec<_>>(), "disjoint and complete");
    // Owner nodes reflect claims.
    let owners = cs[0].children(&format!("{base}/owners")).await.unwrap();
    assert_eq!(owners.len(), n as usize);
    c.close().await;
    for p in [&a, &b] {
        p.wait_for(Duration::from_secs(15), |x| x.members.len() == 2 && x.is_settled()).await.unwrap();
    }
    let mut owned: Vec<u32> = [&a, &b].iter().flat_map(|p| p.assignment().owned).collect();
    owned.sort();
    assert_eq!(owned, (0..n).collect::<Vec<_>>());
    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn inspect_unknown_and_counter_paths() {
    let server = require_server!();
    let base = unique_path("inspect");
    let cs = clients(&server.address(), 1).await;
    cs[0].create(&base, b"", &CreateOptions::persistent()).await.unwrap();
    cs[0].create(&format!("{base}/x"), b"hello", &CreateOptions::persistent()).await.unwrap();
    assert!(matches!(inspect(&cs[0], &base).await.unwrap(), Inspection::Unknown { children: 1, .. }));
    assert!(matches!(inspect(&cs[0], &format!("{base}/x")).await.unwrap(), Inspection::Unknown { children: 0, .. }));
}
