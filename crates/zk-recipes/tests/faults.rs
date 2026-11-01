//! Fault injection: session expiry through a FaultProxy.
//!
//! Each test expires one participant's session with
//! `partition_until_expired` and checks that (a) its Ownership becomes Lost,
//! (b) another contender takes over, (c) there are never two Held owners,
//! (d) supervised registrations come back on the new session.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zk_core::{ClusterConfig, ZkClient};
use zk_recipes::*;
use zk_testkit::{require_server, unique_path, FaultProxy};

const SESSION: Duration = Duration::from_secs(2);

fn cfg(addr: &str) -> ClusterConfig {
    let mut c = ClusterConfig::new(addr).with_session_timeout(Duration::from_secs(4));
    c.operation_timeout_ms = 5_000;
    c.connect_deadline_ms = 10_000;
    c
}

fn short_session(addr: &str) -> ClusterConfig {
    let mut c = cfg(addr);
    c.session_timeout_ms = 2_000;
    c.operation_timeout_ms = 3_000;
    c
}

async fn wait_state(o: &Ownership, want: OwnershipState, timeout: Duration) -> bool {
    tokio::time::timeout(timeout, async {
        let mut rx = o.subscribe();
        loop {
            if *rx.borrow_and_update() == want {
                return;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    })
    .await
    .is_ok()
}

/// Watches a set of ownerships and records whether two were ever Held at once.
fn overlap_monitor(owns: Arc<std::sync::Mutex<Vec<Ownership>>>) -> (Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let overlap = Arc::new(AtomicBool::new(false));
    let o = overlap.clone();
    let h = tokio::spawn(async move {
        loop {
            let held = owns.lock().unwrap().iter().filter(|x| x.is_held()).count();
            if held > 1 {
                o.store(true, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    (overlap, h)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutex_holder_expiry() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let base = unique_path("fi-mutex");
    let victim = ZkClient::connect(short_session(&proxy.address())).await.unwrap();
    let other = ZkClient::connect(cfg(&server.address())).await.unwrap();
    // LoseOnSuspend: the holder considers itself unlocked as soon as it is
    // disconnected, i.e. strictly before the server can expire it.
    let g = Mutex::new(&victim, &base).unwrap().with_policy(SuspendPolicy::LoseOnSuspend).acquire(None).await.unwrap();
    let owns = Arc::new(std::sync::Mutex::new(vec![g.ownership().clone()]));
    let (overlap, mon) = overlap_monitor(owns.clone());

    let waiter = tokio::spawn({
        let m = Mutex::new(&other, &base).unwrap();
        let owns = owns.clone();
        async move {
            let g2 = m.acquire(Some(Duration::from_secs(30))).await.unwrap();
            owns.lock().unwrap().push(g2.ownership().clone());
            g2
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiter.is_finished());
    proxy.partition_until_expired(SESSION).await;
    assert!(wait_state(g.ownership(), OwnershipState::Lost, Duration::from_secs(10)).await, "{:?}", g.ownership().state());
    let g2 = waiter.await.unwrap();
    assert!(g2.ownership().is_held());
    mon.abort();
    assert!(!overlap.load(Ordering::SeqCst), "two Held owners at once");
    // The stale guard's release is harmless.
    g.release().await.unwrap();
    assert!(g2.ownership().is_held());

    // The victim can lock again on its new session (after g2 releases).
    g2.release().await.unwrap();
    let g3 = Mutex::new(&victim, &base).unwrap().acquire(Some(Duration::from_secs(15))).await.unwrap();
    assert!(g3.ownership().is_held());
    g3.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_expiry_failover_and_requeue() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let base = unique_path("fi-latch");
    let victim = ZkClient::connect(short_session(&proxy.address())).await.unwrap();
    let other = ZkClient::connect(cfg(&server.address())).await.unwrap();
    let lv = LeaderLatch::new(&victim, &base, "victim").unwrap().with_policy(SuspendPolicy::LoseOnSuspend);
    lv.start().unwrap();
    let own_v = lv.await_leadership(Some(Duration::from_secs(10))).await.unwrap();
    let lo = LeaderLatch::new(&other, &base, "other").unwrap();
    lo.start().unwrap();
    for _ in 0..50 {
        if lo.participants().await.unwrap().len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let owns = Arc::new(std::sync::Mutex::new(vec![own_v.clone()]));
    let (overlap, mon) = overlap_monitor(owns.clone());

    let partition = proxy.partition_until_expired(SESSION);
    let failover = async {
        let o = lo.await_leadership(Some(Duration::from_secs(20))).await.unwrap();
        owns.lock().unwrap().push(o.clone());
        o
    };
    let ((), own_o) = tokio::join!(partition, failover);
    assert!(wait_state(&own_v, OwnershipState::Lost, Duration::from_secs(10)).await);
    assert!(own_o.is_held());
    assert!(!lv.has_leadership());
    // The victim re-queues on its new session behind the new leader.
    let mut requeued = false;
    for _ in 0..100 {
        let ps = lo.participants().await.unwrap();
        if ps.len() == 2 && ps[0].data == b"other" && ps[1].data == b"victim" {
            requeued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(requeued, "{:?}", lo.participants().await.unwrap());
    mon.abort();
    assert!(!overlap.load(Ordering::SeqCst));
    // And regains leadership when the other closes.
    lo.close().await;
    lv.await_leadership(Some(Duration::from_secs(10))).await.unwrap();
    lv.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn semaphore_lease_expiry() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let base = unique_path("fi-sem");
    let victim = ZkClient::connect(short_session(&proxy.address())).await.unwrap();
    let other = ZkClient::connect(cfg(&server.address())).await.unwrap();
    let lease = Semaphore::new(&victim, &base, 1).unwrap().acquire(Some(Duration::from_secs(5))).await.unwrap();
    let waiter = tokio::spawn({
        let s = Semaphore::new(&other, &base, 1).unwrap();
        async move { s.acquire(Some(Duration::from_secs(30))).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiter.is_finished());
    proxy.partition_until_expired(SESSION).await;
    assert!(wait_state(lease.ownership(), OwnershipState::Lost, Duration::from_secs(10)).await);
    let l2 = waiter.await.unwrap().unwrap();
    assert!(l2.ownership().is_held());
    assert_eq!(Semaphore::new(&other, &base, 1).unwrap().leases().await.unwrap().len(), 1);
    l2.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_node_member_and_discovery_reregister() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let base = unique_path("fi-reg");
    let victim = ZkClient::connect(short_session(&proxy.address())).await.unwrap();
    let observer = ZkClient::connect(cfg(&server.address())).await.unwrap();

    let pn = PersistentNode::start(&victim, &format!("{base}/pn/node"), PersistentNodeMode::Ephemeral, b"v1").unwrap();
    let seq = PersistentNode::start(&victim, &format!("{base}/seq/item-"), PersistentNodeMode::ProtectedEphemeralSequential, b"s").unwrap();
    let member = GroupMember::join(&victim, &format!("{base}/group"), "m1", b"payload").unwrap();
    let disco = ServiceDiscovery::new(&victim, &format!("{base}/services")).unwrap();
    let inst = ServiceInstance::new("api", "127.0.0.1", Some(1));
    disco.register(&inst, Duration::from_secs(5)).await.unwrap();
    let o_pn = pn.wait_for_created(Duration::from_secs(5)).await.unwrap();
    seq.wait_for_created(Duration::from_secs(5)).await.unwrap();
    let o_m = member.wait_joined(Duration::from_secs(5)).await.unwrap();
    let first_seq_path = seq.actual_path().unwrap();
    let sid1 = observer.exists(&format!("{base}/pn/node")).await.unwrap().unwrap().ephemeral_owner;
    let cache = ServiceDiscovery::new(&observer, &format!("{base}/services")).unwrap().service_cache("api");
    cache.wait_for(Duration::from_secs(5), |v| v.len() == 1).await.unwrap();

    // Data update reaches the node.
    pn.set_data(b"v2").await.unwrap();
    assert_eq!(observer.get_data(&format!("{base}/pn/node")).await.unwrap().data, b"v2");

    proxy.partition_until_expired(SESSION).await;
    assert!(wait_state(&o_pn, OwnershipState::Lost, Duration::from_secs(10)).await);
    assert!(wait_state(&o_m, OwnershipState::Lost, Duration::from_secs(10)).await);

    // Re-created on the new session.
    let o_pn2 = pn.wait_for_created(Duration::from_secs(15)).await.unwrap();
    assert!(o_pn2.is_held());
    assert_ne!(o_pn2.session().id(), o_pn.session().id());
    let st = observer.exists(&format!("{base}/pn/node")).await.unwrap().unwrap();
    assert_ne!(st.ephemeral_owner, sid1);
    assert_eq!(observer.get_data(&format!("{base}/pn/node")).await.unwrap().data, b"v2");
    seq.wait_for_created(Duration::from_secs(15)).await.unwrap();
    assert_ne!(seq.actual_path().unwrap(), first_seq_path);
    assert_eq!(observer.children(&format!("{base}/seq")).await.unwrap().len(), 1);
    member.wait_joined(Duration::from_secs(15)).await.unwrap();
    assert!(member.incarnations() >= 2);
    assert_eq!(observer.get_data(&format!("{base}/group/m1")).await.unwrap().data, b"payload");
    let reg = disco.registration("api", &inst.id).unwrap();
    reg.wait_for_created(Duration::from_secs(15)).await.unwrap();
    cache.wait_for(Duration::from_secs(10), |v| v.len() == 1 && v[0].id == inst.id).await.unwrap();

    pn.close().await.unwrap();
    seq.close().await.unwrap();
    member.leave().await.unwrap();
    disco.close().await;
    assert!(observer.exists(&format!("{base}/pn/node")).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_owner_expiry_moves_partitions() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let base = unique_path("fi-parts");
    let victim = ZkClient::connect(short_session(&proxy.address())).await.unwrap();
    let other = ZkClient::connect(cfg(&server.address())).await.unwrap();
    let n = 8;
    let pv = PartitionAssigner::start_with_policy(&victim, &base, "v", n, SuspendPolicy::LoseOnSuspend).unwrap();
    let po = PartitionAssigner::start(&other, &base, "o", n).unwrap();
    let both = |x: &Assignment| x.members.len() == 2 && x.is_settled();
    let av = pv.wait_for(Duration::from_secs(15), both).await.unwrap();
    po.wait_for(Duration::from_secs(15), both).await.unwrap();
    assert!(!av.owned.is_empty());
    let own_v = av.ownership.clone().unwrap();

    // Never two owners per partition: sample both views during the fault.
    let violations = Arc::new(AtomicUsize::new(0));
    let mon = tokio::spawn({
        let (pv, po, violations) = (pv.clone(), po.clone(), violations.clone());
        async move {
            loop {
                let a = pv.assignment();
                let b = po.assignment();
                let a_owned: Vec<u32> = if a.ownership.as_ref().is_some_and(|o| o.is_held()) { a.owned.clone() } else { vec![] };
                if a_owned.iter().any(|p| b.owned.contains(p)) {
                    violations.fetch_add(1, Ordering::SeqCst);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    });
    proxy.partition_until_expired(SESSION).await;
    assert!(wait_state(&own_v, OwnershipState::Lost, Duration::from_secs(10)).await);
    // The victim's member node expired; it re-joins on a new session and
    // the group settles again with every partition owned exactly once.
    po.wait_for(Duration::from_secs(20), both).await.unwrap();
    pv.wait_for(Duration::from_secs(20), both).await.unwrap();
    mon.abort();
    assert_eq!(violations.load(Ordering::SeqCst), 0);
    let mut owned: Vec<u32> = [&pv, &po].iter().flat_map(|p| p.assignment().owned).collect();
    owned.sort();
    assert_eq!(owned, (0..n).collect::<Vec<_>>());
    pv.close().await;
    po.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn locking_queue_claim_released_on_expiry() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let base = unique_path("fi-lq");
    let victim = ZkClient::connect(short_session(&proxy.address())).await.unwrap();
    let other = ZkClient::connect(cfg(&server.address())).await.unwrap();
    let q = LockingQueue::new(&other, &base).unwrap();
    q.put(b"job", 0).await.unwrap();
    let claimed = LockingQueue::new(&victim, &base).unwrap().get(Some(Duration::from_secs(5))).await.unwrap().unwrap();
    assert!(q.get(Some(Duration::from_millis(200))).await.unwrap().is_none());
    proxy.partition_until_expired(SESSION).await;
    assert!(wait_state(claimed.ownership(), OwnershipState::Lost, Duration::from_secs(10)).await);
    let again = q.get(Some(Duration::from_secs(10))).await.unwrap().unwrap();
    assert_eq!(again.item.data, b"job");
    // The stale claim cannot consume.
    assert!(claimed.consume().await.is_err());
    again.consume().await.unwrap();
    assert!(q.is_empty().await.unwrap());
}
