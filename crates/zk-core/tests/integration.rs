//! zk-core integration tests against a real ZooKeeper server.

use std::sync::{Arc, Mutex};
use std::time::Duration;

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

#[tokio::test]
async fn crud_and_stat() {
    let server = require_server!();
    let zk = connect(&server.address()).await;
    let root = unique_path("crud");
    let c = zk.create(&root, b"hello", &CreateOptions::persistent()).await.unwrap();
    assert_eq!(c.path, root);
    assert_eq!(c.stat.version, 0);
    assert!(c.sequence.is_none());

    let d = zk.get_data(&root).await.unwrap();
    assert_eq!(d.data, b"hello");
    assert_eq!(d.stat.data_length, 5);
    assert_eq!(d.stat.kind(), NodeKind::Persistent);

    let st = zk.set_data(&root, b"world", Some(0)).await.unwrap();
    assert_eq!(st.version, 1);
    let e = zk.set_data(&root, b"x", Some(0)).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::BadVersion);
    assert_eq!(e.path.as_deref(), Some(root.as_str()));

    assert!(zk.exists(&root).await.unwrap().is_some());
    assert!(zk.exists(&format!("{root}/nope")).await.unwrap().is_none());
    let e = zk.get_data(&format!("{root}/nope")).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::NoNode);

    let e = zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::NodeExists);

    zk.create(&format!("{root}/b"), b"", &CreateOptions::persistent()).await.unwrap();
    zk.create(&format!("{root}/a"), b"", &CreateOptions::persistent()).await.unwrap();
    assert_eq!(zk.children(&root).await.unwrap(), vec!["a", "b"]);
    let (entries, parent, truncated) = zk.children_stats(&root, None).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(parent.num_children, 2);
    assert!(!truncated);
    let (entries, _, truncated) = zk.children_stats(&root, Some(1)).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert!(truncated);

    let e = zk.delete(&root, None).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::NotEmpty);
    assert_eq!(zk.count_descendants(&root).await.unwrap(), 2);

    let e = zk.delete(&format!("{root}/a"), Some(7)).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::BadVersion);
    zk.delete(&format!("{root}/a"), Some(0)).await.unwrap();
    zk.sync(&root).await.unwrap();
    let rep = zk.delete_recursive(&root, &DeleteRecursiveOptions::default()).await.unwrap();
    assert_eq!(rep.paths, vec![format!("{root}/b"), root.clone()]);
    assert!(zk.exists(&root).await.unwrap().is_none());
    zk.close().await;
}

#[tokio::test]
async fn node_types() {
    let server = require_server!();
    let zk = connect(&server.address()).await;
    let root = unique_path("types");
    zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap();

    let s1 = zk.create(&format!("{root}/seq-"), b"", &CreateOptions::mode(CreateMode::PersistentSequential)).await.unwrap();
    let s2 = zk.create(&format!("{root}/seq-"), b"", &CreateOptions::mode(CreateMode::PersistentSequential)).await.unwrap();
    assert!(s1.path.starts_with(&format!("{root}/seq-")));
    assert_eq!(s1.path.len(), format!("{root}/seq-").len() + 10);
    assert_eq!(s2.sequence.unwrap(), s1.sequence.unwrap() + 1);

    let e = zk.create(&format!("{root}/eph"), b"", &CreateOptions::ephemeral()).await.unwrap();
    assert_eq!(e.stat.kind(), NodeKind::Ephemeral);
    let session = zk.current_session().unwrap();
    assert_eq!(e.stat.ephemeral_owner, session.id());
    // A pinned Session keeps the ZooKeeper session open; release it before close.
    drop(session);
    let err = zk.create(&format!("{root}/eph/child"), b"", &CreateOptions::persistent()).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::NoChildrenForEphemerals);
    let es = zk.create(&format!("{root}/eseq-"), b"", &CreateOptions::mode(CreateMode::EphemeralSequential)).await.unwrap();
    let mine = zk.session_ephemerals(&root).await.unwrap();
    assert!(mine.contains(&format!("{root}/eph")));
    assert!(mine.contains(&es.path));

    // Containers are removed by the server once their last child goes away
    // (the testkit sets znode.container.checkIntervalMs=500).
    let container = format!("{root}/container");
    zk.create(&container, b"", &CreateOptions::mode(CreateMode::Container)).await.unwrap();
    zk.create(&format!("{container}/child"), b"", &CreateOptions::persistent()).await.unwrap();
    zk.delete(&format!("{container}/child"), None).await.unwrap();
    let mut gone = false;
    for _ in 0..40 {
        if zk.exists(&container).await.unwrap().is_none() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(gone, "container node should be reaped");

    // TTL nodes expire when unmodified and childless.
    let t = zk
        .create(&format!("{root}/ttl"), b"", &CreateOptions::mode(CreateMode::PersistentWithTtl).with_ttl(Duration::from_millis(500)))
        .await
        .unwrap();
    let mut gone = false;
    for _ in 0..40 {
        if zk.exists(&t.path).await.unwrap().is_none() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(gone, "ttl node should expire");
    let seq_ttl = zk
        .create(&format!("{root}/ttlseq-"), b"", &CreateOptions::mode(CreateMode::PersistentSequentialWithTtl).with_ttl(Duration::from_secs(600)))
        .await
        .unwrap();
    assert!(seq_ttl.sequence.is_some());

    let e = zk.create(&format!("{root}/bad"), b"", &CreateOptions::mode(CreateMode::PersistentWithTtl)).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::BadArguments);

    // Parents.
    let deep = format!("{root}/x/y/z");
    zk.create(&deep, b"d", &CreateOptions::persistent().with_parents()).await.unwrap();
    assert_eq!(zk.get_data(&deep).await.unwrap().data, b"d");

    // Ephemerals vanish with the session.
    zk.close().await;
    let zk2 = connect(&server.address()).await;
    assert!(zk2.exists(&format!("{root}/eph")).await.unwrap().is_none());
    zk2.delete_recursive(&root, &Default::default()).await.unwrap();
    zk2.close().await;
}

#[tokio::test]
async fn transactions() {
    let server = require_server!();
    let zk = connect(&server.address()).await;
    let root = unique_path("txn");
    zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap();
    let a = format!("{root}/a");
    let b = format!("{root}/b");
    let res = zk
        .transaction(&[TxnOp::create(&a, b"1", CreateMode::Persistent), TxnOp::create(&b, b"2", CreateMode::Persistent), TxnOp::check(&root, 0)])
        .await
        .unwrap();
    assert_eq!(res.len(), 3);
    assert!(matches!(&res[0], TxnResult::Create { path, .. } if path == &a));

    // Atomicity: second op fails, first must not apply.
    let c = format!("{root}/c");
    let err = zk
        .transaction(&[TxnOp::create(&c, b"", CreateMode::Persistent), TxnOp::set_data(&a, b"x", Some(42))])
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::BadVersion);
    assert_eq!(err.op_index, Some(1));
    assert!(zk.exists(&c).await.unwrap().is_none());

    let res = zk.transaction(&[TxnOp::set_data(&a, b"x", Some(0)), TxnOp::delete(&b, None)]).await.unwrap();
    assert!(matches!(res[0], TxnResult::SetData { stat } if stat.version == 1));

    // multi-read
    let rr = zk
        .multi_read(&[ReadOp::GetData { path: a.clone() }, ReadOp::GetChildren { path: root.clone() }, ReadOp::GetData { path: b.clone() }])
        .await
        .unwrap();
    assert!(matches!(&rr[0], ReadResult::Data(d) if d.data == b"x"));
    assert!(matches!(&rr[1], ReadResult::Children(c) if c == &vec!["a".to_string()]));
    assert!(matches!(&rr[2], ReadResult::Error(e) if e.kind == ErrorKind::NoNode));

    zk.delete_recursive(&root, &Default::default()).await.unwrap();
    zk.close().await;
}

#[tokio::test]
async fn acl_and_auth() {
    let server = require_server!();
    let admin = ZkClient::connect(cfg(&server.address()).with_digest("alice", "secret")).await.unwrap();
    let root = unique_path("acl");
    let acl = vec![Acl::digest(Perms::ALL, "alice", "secret"), Acl::new(Perms::READ, "world", "anyone")];
    admin.create(&root, b"guarded", &CreateOptions::persistent().with_acl(acl.clone())).await.unwrap();
    let (got, st) = admin.get_acl(&root).await.unwrap();
    assert_eq!(got, acl);
    assert_eq!(st.aversion, 0);
    let who = admin.whoami().await.unwrap();
    assert!(who.iter().any(|w| w.scheme == "digest" && w.user == "alice"));

    let anon = connect(&server.address()).await;
    assert_eq!(anon.get_data(&root).await.unwrap().data, b"guarded");
    let e = anon.set_data(&root, b"x", None).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::NoAuth);

    anon.add_auth("digest", b"alice:secret").await.unwrap();
    anon.set_data(&root, b"ok", None).await.unwrap();

    // set_acl with aversion check
    let e = admin.set_acl(&root, &Acl::anyone_all(), Some(5)).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::BadVersion);
    let st = admin.set_acl(&root, &Acl::anyone_all(), Some(0)).await.unwrap();
    assert_eq!(st.aversion, 1);
    let e = admin.set_acl(&root, &[], None).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::InvalidAcl);

    // Unreadable node.
    let secret = format!("{root}/secret");
    admin.create(&secret, b"s", &CreateOptions::persistent().with_acl(vec![Acl::digest(Perms::ALL, "alice", "secret")])).await.unwrap();
    let anon2 = connect(&server.address()).await;
    let e = anon2.get_data(&secret).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::NoAuth);

    admin.delete_recursive(&root, &Default::default()).await.unwrap();
    for c in [admin, anon, anon2] {
        c.close().await;
    }
}

struct Collect(Mutex<Vec<AuditRecord>>);
impl AuditSink for Collect {
    fn record(&self, r: AuditRecord) {
        self.0.lock().unwrap().push(r);
    }
}

#[tokio::test]
async fn safety_policy_and_audit() {
    let server = require_server!();
    let root = unique_path("safety");
    let mut c = cfg(&server.address());
    c.safety.protected_paths = vec![format!("{root}/prod/**")];
    c.safety.max_recursive_nodes = 3;
    let zk = ZkClient::connect(c).await.unwrap();
    let sink = Arc::new(Collect(Mutex::new(vec![])));
    zk.set_audit_sink(Some(sink.clone()));

    zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap();
    let e = zk.create(&format!("{root}/prod"), b"", &CreateOptions::persistent()).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::PolicyDenied);
    zk.forced().create(&format!("{root}/prod"), b"", &CreateOptions::persistent()).await.unwrap();
    let e = zk.set_data(&format!("{root}/prod"), b"x", None).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::PolicyDenied);

    for i in 0..4 {
        zk.create(&format!("{root}/n{i}"), b"", &CreateOptions::persistent()).await.unwrap();
    }
    let e = zk.delete_recursive(&root, &Default::default()).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::LimitExceeded);
    let dry = zk.delete_recursive(&root, &DeleteRecursiveOptions { dry_run: true, limit: Some(100), version: None }).await.unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.paths.len(), 6);
    assert_eq!(dry.blocked, vec![format!("{root}/prod")]);
    assert!(zk.exists(&root).await.unwrap().is_some());
    // Protected descendant blocks recursive delete before anything is deleted.
    let e = zk.delete_recursive(&root, &DeleteRecursiveOptions { limit: Some(100), ..Default::default() }).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::PolicyDenied);
    assert_eq!(zk.children(&root).await.unwrap().len(), 5);
    zk.forced().delete_recursive(&root, &DeleteRecursiveOptions { limit: Some(100), ..Default::default() }).await.unwrap();

    let records = sink.0.lock().unwrap().clone();
    assert!(records.iter().any(|r| r.operation == "create" && r.outcome == "policy-denied"));
    assert!(records.iter().any(|r| r.operation == "create" && r.forced && r.outcome == "ok"));
    assert!(records.iter().any(|r| r.operation == "delete_recursive" && r.outcome == "ok" && r.paths.len() == 6));

    // Read-only profile.
    let ro = ZkClient::connect(cfg(&server.address()).read_only()).await.unwrap();
    let e = ro.create(&unique_path("ro"), b"", &CreateOptions::persistent()).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::PolicyDenied);
    let e = ro.forced().create(&unique_path("ro"), b"", &CreateOptions::persistent()).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::PolicyDenied);
    assert!(ro.get_data("/").await.is_ok());
    zk.close().await;
    ro.close().await;
}

#[tokio::test]
async fn chroot_support() {
    let server = require_server!();
    let base = unique_path("chroot");
    let zk = connect(&server.address()).await;
    zk.create(&base, b"", &CreateOptions::persistent()).await.unwrap();
    let rooted = ZkClient::connect(cfg(&server.address()).with_chroot(&base)).await.unwrap();
    rooted.create("/inside", b"in", &CreateOptions::persistent()).await.unwrap();
    assert_eq!(zk.get_data(&format!("{base}/inside")).await.unwrap().data, b"in");
    assert_eq!(rooted.children("/").await.unwrap(), vec!["inside"]);
    let missing = ZkClient::connect(cfg(&format!("{}/does-not-exist-{}", server.address(), std::process::id()))).await;
    // Missing chroot: connection works but operations on "/" fail with NoNode.
    if let Ok(m) = missing {
        assert_eq!(m.get_data("/").await.unwrap_err().kind, ErrorKind::NoNode);
        m.close().await;
    }
    zk.delete_recursive(&base, &Default::default()).await.unwrap();
    rooted.close().await;
    zk.close().await;
}

#[tokio::test]
async fn walk_export_import_copy_move() {
    let server = require_server!();
    let zk = connect(&server.address()).await;
    let root = unique_path("tree");
    for p in ["", "/a", "/a/1", "/a/2", "/b"] {
        zk.create(&format!("{root}{p}"), format!("v{p}").as_bytes(), &CreateOptions::persistent()).await.unwrap();
    }
    let w = zk.walk(&root, &WalkOptions::default()).await.unwrap();
    assert_eq!(w.nodes.len(), 5);
    assert!(!w.truncated);
    let w = zk.walk(&root, &WalkOptions { max_depth: Some(1), ..Default::default() }).await.unwrap();
    assert_eq!(w.nodes.len(), 3);
    let w = zk.walk(&root, &WalkOptions { max_nodes: 2, ..Default::default() }).await.unwrap();
    assert!(w.truncated);

    let (nodes, _) = zk.export(&root, true, false, 100).await.unwrap();
    assert_eq!(nodes.iter().map(|n| n.rel.as_str()).collect::<Vec<_>>(), vec!["", "a", "a/1", "a/2", "b"]);

    let copy = unique_path("tree-copy");
    let created = zk.copy_tree(&root, &copy, true, 100).await.unwrap();
    assert_eq!(created.len(), 5);
    assert_eq!(zk.get_data(&format!("{copy}/a/2")).await.unwrap().data, b"v/a/2");
    assert_eq!(zk.copy_tree(&root, &copy, true, 100).await.unwrap_err().kind, ErrorKind::NodeExists);

    let moved = unique_path("tree-moved");
    zk.move_tree(&copy, &moved, false, 100).await.unwrap();
    assert!(zk.exists(&copy).await.unwrap().is_none());
    assert_eq!(zk.get_data(&format!("{moved}/b")).await.unwrap().data, b"v/b");

    let (created, updated) = zk.import(&moved, &nodes, true, false).await.unwrap();
    assert!(created.is_empty());
    assert_eq!(updated.len(), 5);

    for p in [&root, &moved] {
        zk.delete_recursive(p, &Default::default()).await.unwrap();
    }
    zk.close().await;
}

#[tokio::test]
async fn oneshot_and_persistent_watches() {
    let server = require_server!();
    let zk = connect(&server.address()).await;
    let root = unique_path("watch");
    let (st, w) = zk.watch_exists(&root).await.unwrap();
    assert!(st.is_none());
    zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap();
    let ev = w.changed().await;
    assert_eq!(ev.kind, WatchEventKind::NodeCreated);
    assert_eq!(ev.path, root);

    let (_, w) = zk.watch_data(&root).await.unwrap();
    zk.set_data(&root, b"1", None).await.unwrap();
    assert_eq!(w.changed().await.kind, WatchEventKind::NodeDataChanged);

    let (children, _, w) = zk.watch_children(&root).await.unwrap();
    assert!(children.is_empty());
    zk.create(&format!("{root}/c"), b"", &CreateOptions::persistent()).await.unwrap();
    assert_eq!(w.changed().await.kind, WatchEventKind::NodeChildrenChanged);

    let mut s = zk.watch_persistent(&root, PersistentMode::Recursive, 64).await.unwrap();
    zk.set_data(&format!("{root}/c"), b"x", None).await.unwrap();
    zk.create(&format!("{root}/c/d"), b"", &CreateOptions::persistent()).await.unwrap();
    zk.delete(&format!("{root}/c/d"), None).await.unwrap();
    let mut kinds = vec![];
    for _ in 0..3 {
        let ev = tokio::time::timeout(Duration::from_secs(5), s.next()).await.unwrap().unwrap();
        kinds.push((ev.kind, ev.path));
    }
    assert_eq!(
        kinds,
        vec![
            (WatchEventKind::NodeDataChanged, format!("{root}/c")),
            (WatchEventKind::NodeCreated, format!("{root}/c/d")),
            (WatchEventKind::NodeDeleted, format!("{root}/c/d")),
        ]
    );
    drop(s);
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
    zk.close().await;
}

fn short_session(addr: &str) -> ClusterConfig {
    let mut c = cfg(addr);
    c.session_timeout_ms = 2_000;
    c.operation_timeout_ms = 3_000;
    c
}

#[tokio::test]
async fn session_expiry_and_reestablishment() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let zk = ZkClient::connect(short_session(&proxy.address())).await.unwrap();
    let mut events = zk.subscribe_events();
    let root = unique_path("expiry");
    zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap();
    zk.create(&format!("{root}/eph"), b"", &CreateOptions::ephemeral()).await.unwrap();
    let mut stream = zk.watch_persistent(&root, PersistentMode::Recursive, 64).await.unwrap();
    let gen1 = zk.current_session().unwrap().generation();

    proxy.partition_until_expired(Duration::from_secs(2)).await;

    // Lost then SessionReestablished.
    let mut seen = vec![];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !seen.contains(&ConnectionEventKind::SessionReestablished) && tokio::time::Instant::now() < deadline {
        if let Ok(Ok(e)) = tokio::time::timeout(Duration::from_secs(1), events.recv()).await {
            seen.push(e.kind);
        }
    }
    assert!(seen.contains(&ConnectionEventKind::Lost), "{seen:?}");
    assert!(seen.contains(&ConnectionEventKind::SessionReestablished), "{seen:?}");
    let gen2 = zk.current_session().unwrap().generation();
    assert!(gen2 > gen1);

    // Ephemeral is gone; operations work on the new session.
    assert!(zk.exists(&format!("{root}/eph")).await.unwrap().is_none());

    // The persistent watch reported the loss and re-registered.
    let mut kinds = vec![];
    while !kinds.contains(&WatchEventKind::Reregistered) {
        let ev = tokio::time::timeout(Duration::from_secs(10), stream.next()).await.unwrap().unwrap();
        kinds.push(ev.kind);
    }
    assert!(kinds.contains(&WatchEventKind::SessionLost), "{kinds:?}");
    zk.create(&format!("{root}/after"), b"", &CreateOptions::persistent()).await.unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let e = stream.next().await.unwrap();
            if e.is_node_event() {
                return e;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ev.kind, WatchEventKind::NodeCreated);
    assert_eq!(zk.diagnostics().expirations, 1);
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
    zk.close().await;
}

#[tokio::test]
async fn expiry_without_reestablish_closes() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let mut c = short_session(&proxy.address());
    c.reconnect_on_expiry = false;
    let zk = ZkClient::connect(c).await.unwrap();
    proxy.partition_until_expired(Duration::from_secs(2)).await;
    assert!(zk_testkit::eventually(Duration::from_secs(10), || zk.state().is_terminal()).await);
    let e = zk.get_data("/").await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::Closed);
}

#[tokio::test]
async fn suspension_and_recovery_through_proxy() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let mut c = cfg(&proxy.address());
    c.session_timeout_ms = 6_000;
    c.operation_timeout_ms = 2_000;
    let zk = ZkClient::connect(c).await.unwrap();
    let mut events = zk.subscribe_events();
    let root = unique_path("proxy");
    zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap();
    zk.create(&format!("{root}/eph"), b"", &CreateOptions::ephemeral()).await.unwrap();
    let sid = zk.current_session().unwrap().id();

    proxy.set_mode(ProxyMode::Refuse);
    proxy.drop_connections();
    let ev = tokio::time::timeout(Duration::from_secs(5), events.recv()).await.unwrap().unwrap();
    assert_eq!(ev.kind, ConnectionEventKind::Suspended);
    assert_eq!(zk.state().name(), "suspended");
    // Reads fail (with retries exhausted) while disconnected.
    let e = zk.get_data(&root).await.unwrap_err();
    assert!(matches!(e.kind, ErrorKind::ConnectionLoss | ErrorKind::NotConnected | ErrorKind::Timeout), "{e}");

    proxy.set_mode(ProxyMode::Pass);
    let mut got = None;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_secs(1), events.recv()).await {
            Ok(Ok(e)) if e.kind == ConnectionEventKind::Reconnected => {
                got = Some(e);
                break;
            },
            _ => {},
        }
    }
    assert!(got.is_some(), "expected Reconnected");
    // Same session: ephemeral survived.
    assert_eq!(zk.current_session().unwrap().id(), sid);
    assert!(zk.exists(&format!("{root}/eph")).await.unwrap().is_some());
    assert!(zk.diagnostics().suspensions >= 1);
    zk.delete_recursive(&root, &Default::default()).await.unwrap();
    zk.close().await;
}

#[tokio::test]
async fn write_outcome_unknown_on_partition() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let mut c = cfg(&proxy.address());
    c.session_timeout_ms = 8_000;
    c.operation_timeout_ms = 1_000;
    let zk = ZkClient::connect(c).await.unwrap();
    let root = unique_path("outcome");
    proxy.set_mode(ProxyMode::Blackhole);
    let e = zk.create(&root, b"", &CreateOptions::persistent()).await.unwrap_err();
    assert_eq!(e.kind, ErrorKind::OutcomeUnknown, "{e}");
    proxy.set_mode(ProxyMode::Pass);
    zk.close().await;
    let zk = connect(&server.address()).await;
    let _ = zk.delete(&root, None).await;
    zk.close().await;
}

#[tokio::test]
async fn connect_failures() {
    let _server = require_server!();
    let port = zk_testkit::free_port();
    let mut c = ClusterConfig::new(format!("127.0.0.1:{port}"));
    c.connect_deadline_ms = 1_500;
    let e = ZkClient::connect(c).await.unwrap_err();
    assert!(matches!(e.kind, ErrorKind::Timeout | ErrorKind::NotConnected | ErrorKind::Other), "{e}");
    assert!(ZkClient::connect(ClusterConfig::new("")).await.is_err());
}

#[tokio::test]
async fn admin_four_letter_words() {
    let server = require_server!();
    let health = zk_core::admin::ensemble_health(&[server.address()], Duration::from_secs(2)).await;
    assert_eq!(health.len(), 1);
    assert!(health[0].ok);
    let info = health[0].info.as_ref().unwrap();
    assert_eq!(info.mode.as_deref(), Some("standalone"));
    let mntr = zk_core::admin::four_letter(&server.address(), "mntr", Duration::from_secs(2)).await.unwrap();
    assert!(zk_core::admin::parse_mntr(&mntr).contains_key("zk_znode_count"));
    let zk = connect(&server.address()).await;
    let cfgdata = zk.get_config().await;
    // Standalone servers may have an empty /zookeeper/config; it must not error.
    assert!(cfgdata.is_ok(), "{cfgdata:?}");
    let d = zk.diagnostics();
    assert_eq!(d.state.name(), "connected");
    assert!(d.session_id.is_some());
    zk.close().await;
}

#[tokio::test]
async fn binary_and_large_data() {
    let server = require_server!();
    let zk = connect(&server.address()).await;
    let root = unique_path("binary");
    let bin: Vec<u8> = (0..=255u8).cycle().take(512 * 1024).collect();
    zk.create(&root, &bin, &CreateOptions::persistent()).await.unwrap();
    let d = zk.get_data(&root).await.unwrap();
    assert_eq!(d.data, bin);
    // Many children.
    let ops: Vec<TxnOp> = (0..1000).map(|i| TxnOp::create(&format!("{root}/c{i:04}"), b"", CreateMode::Persistent)).collect();
    zk.transaction(&ops).await.unwrap();
    let (entries, _, _) = zk.children_stats(&root, None).await.unwrap();
    assert_eq!(entries.len(), 1000);
    let rep = zk.delete_recursive(&root, &DeleteRecursiveOptions { limit: Some(2000), ..Default::default() }).await.unwrap();
    assert_eq!(rep.paths.len(), 1001);
    zk.close().await;
}
