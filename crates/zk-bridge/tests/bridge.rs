//! End-to-end tests: spawn the real `zk-bridge` binary and talk JSON-RPC over stdio
//! against a real ZooKeeper server.

use std::collections::VecDeque;
use std::process::Stdio;
use std::time::Duration;

use base64::Engine as _;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use zk_testkit::{require_server, unique_path, FaultProxy, ProxyMode};

struct Bridge {
    child: Child,
    stdin: ChildStdin,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    next: u64,
    notifications: VecDeque<Value>,
}

impl Bridge {
    async fn start() -> Bridge {
        Self::start_with(&[], &[]).await
    }

    async fn start_with(args: &[&str], env: &[(&str, &str)]) -> Bridge {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_zk-bridge"));
        cmd.args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut b = Bridge { child, stdin, stdout, next: 1, notifications: VecDeque::new() };
        let init = b.call("initialize", json!({"client": "test", "protocol_version": "1.0"})).await.unwrap();
        assert_eq!(init["protocol_version"], "1.0");
        b
    }

    async fn send_raw(&mut self, line: &str) {
        self.stdin.write_all(line.as_bytes()).await.unwrap();
        self.stdin.write_all(b"\n").await.unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(30), self.stdout.next_line()).await.expect("bridge reply timeout").unwrap().expect("bridge closed stdout");
        serde_json::from_str(&line).unwrap_or_else(|e| panic!("bad json from bridge: {e}: {line}"))
    }

    async fn send(&mut self, method: &str, params: Value) -> u64 {
        let id = self.next;
        self.next += 1;
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.send_raw(&msg.to_string()).await;
        id
    }

    async fn wait_for(&mut self, id: u64) -> Result<Value, Value> {
        loop {
            let v = self.read().await;
            if v.get("id") == Some(&json!(id)) {
                return match v.get("error") {
                    Some(e) => Err(e.clone()),
                    None => Ok(v["result"].clone()),
                };
            }
            if v.get("method").is_some() {
                self.notifications.push_back(v);
            }
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, Value> {
        let id = self.send(method, params).await;
        self.wait_for(id).await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        match self.call(method, params.clone()).await {
            Ok(v) => v,
            Err(e) => panic!("{method} {params} failed: {e}"),
        }
    }

    async fn err_kind(&mut self, method: &str, params: Value) -> String {
        let e = self.call(method, params).await.expect_err("expected error");
        e["data"]["kind"].as_str().unwrap_or_else(|| panic!("no kind in {e}")).to_string()
    }

    /// Waits for a notification matching `pred`.
    async fn notification(&mut self, method: &str, pred: impl Fn(&Value) -> bool) -> Value {
        if let Some(i) = self.notifications.iter().position(|n| n["method"] == method && pred(&n["params"])) {
            return self.notifications.remove(i).unwrap()["params"].clone();
        }
        loop {
            let v = self.read().await;
            if v["method"] == method && pred(&v["params"]) {
                return v["params"].clone();
            }
            if v.get("method").is_some() {
                self.notifications.push_back(v);
            }
        }
    }

    async fn connect(&mut self, addr: &str) -> String {
        self.connect_cfg(json!({"connect": addr, "session_timeout_ms": 4000})).await
    }

    async fn connect_cfg(&mut self, cfg: Value) -> String {
        let r = self.ok("cluster.connect", json!({"config": cfg})).await;
        r["cluster"].as_str().unwrap().to_string()
    }
}

fn b64(s: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(s)
}

#[tokio::test]
async fn protocol_errors_and_lifecycle() {
    let mut b = Bridge::start().await;
    b.send_raw("this is not json").await;
    let v = b.read().await;
    assert_eq!(v["error"]["code"], -32700);
    b.send_raw("[1,2]").await;
    assert_eq!(b.read().await["error"]["code"], -32600);
    let e = b.call("no.such.method", json!({})).await.unwrap_err();
    assert_eq!(e["code"], -32601);
    let e = b.call("node.get", json!({"cluster": 5})).await.unwrap_err();
    assert_eq!(e["code"], -32602);
    let e = b.call("node.get", json!({"cluster": "c999", "path": "/"})).await.unwrap_err();
    assert_eq!(e["code"], -32002);
    let pong = b.ok("$/ping", json!({})).await;
    assert_eq!(pong["pong"], true);
    let r = b.ok("acl.digest", json!({"user": "super", "password": {"value": "admin"}})).await;
    assert_eq!(r["id"], "super:xQJmxLMiHGwaqBvst5y6rkB6HQs=");
    assert_eq!(b.ok("shutdown", json!({})).await, Value::Null);
    let status = tokio::time::timeout(Duration::from_secs(5), b.child.wait()).await.unwrap().unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn version_mismatch_exits() {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_zk-bridge"));
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).kill_on_drop(true);
    let mut child = cmd.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap()).lines();
    stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocol_version\":\"2.0\"}}\n").await.unwrap();
    let line = out.next_line().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["error"]["code"], -32600);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await.unwrap().unwrap();
    assert_eq!(status.code(), Some(3));
}

#[tokio::test]
async fn node_operations_end_to_end() {
    let server = require_server!();
    let mut b = Bridge::start().await;
    let c = b.connect(&server.address()).await;
    let root = unique_path("bridge-nodes");

    let r = b.ok("node.create", json!({"cluster": c, "path": root, "text": "hello"})).await;
    assert_eq!(r["path"], root);
    let audit = b.notification("audit.record", |p| p["operation"] == "create").await;
    assert_eq!(audit["outcome"], "ok");
    assert_eq!(audit["cluster_id"], c);

    let g = b.ok("node.get", json!({"cluster": c, "path": root})).await;
    assert_eq!(g["text"], "hello");
    assert_eq!(g["data"], b64(b"hello"));
    assert_eq!(g["utf8"], true);
    assert_eq!(g["stat"]["version"], 0);

    // Binary data.
    let bin = [0u8, 159, 146, 150, 255];
    let bin_path = format!("{root}/bin");
    b.ok("node.create", json!({"cluster": c, "path": bin_path, "data": b64(&bin)})).await;
    let g = b.ok("node.get", json!({"cluster": c, "path": bin_path})).await;
    assert_eq!(g["utf8"], false);
    assert!(g.get("text").is_none());
    assert_eq!(g["data"], b64(&bin));
    let g = b.ok("node.get", json!({"cluster": c, "path": bin_path, "max_bytes": 2})).await;
    assert_eq!(g["truncated"], true);
    assert_eq!(g["size"], 5);
    assert_eq!(b.call("node.create", json!({"cluster": c, "path": format!("{root}/x"), "text": "a", "data": "YQ=="})).await.unwrap_err()["code"], -32602);

    // Version conflict.
    let s = b.ok("node.set", json!({"cluster": c, "path": root, "text": "v1", "version": 0})).await;
    assert_eq!(s["stat"]["version"], 1);
    assert_eq!(b.err_kind("node.set", json!({"cluster": c, "path": root, "text": "v2", "version": 0})).await, "bad-version");

    // Children paging with stats.
    for i in 0..5 {
        b.ok("node.create", json!({"cluster": c, "path": format!("{root}/kid{i}")})).await;
    }
    let ch = b.ok("node.children", json!({"cluster": c, "path": root, "stats": true, "limit": 2, "offset": 1})).await;
    assert_eq!(ch["total"], 6);
    assert_eq!(ch["truncated"], true);
    assert_eq!(ch["children"].as_array().unwrap().len(), 2);
    assert_eq!(ch["children"][0]["name"], "kid0");
    assert!(ch["children"][0]["stat"]["czxid"].is_number());

    // Sequential + ephemeral.
    let s = b.ok("node.create", json!({"cluster": c, "path": format!("{root}/seq-"), "mode": "ephemeral-sequential"})).await;
    assert!(s["sequence"].is_number());
    if server.at_least(3, 6) {
        let eph = b.ok("node.ephemerals", json!({"cluster": c, "path": root})).await;
        assert_eq!(eph["paths"].as_array().unwrap().len(), 1);
    } else {
        assert_eq!(b.err_kind("node.ephemerals", json!({"cluster": c, "path": root})).await, "unimplemented");
    }

    // Transaction with failing op index.
    let e = b
        .call(
            "node.transaction",
            json!({"cluster": c, "ops": [
                {"op": "create", "path": format!("{root}/t1")},
                {"op": "check", "path": root, "version": 99}
            ]}),
        )
        .await
        .unwrap_err();
    assert_eq!(e["data"]["kind"], "bad-version");
    assert_eq!(e["data"]["op_index"], 1);
    let r = b
        .ok("node.transaction", json!({"cluster": c, "ops": [{"op": "create", "path": format!("{root}/t1"), "data": b64(b"x")}, {"op": "check", "path": root, "version": 1}]}))
        .await;
    assert_eq!(r["results"][0]["op"], "create");

    // ACLs.
    let acl = b.ok("node.acl.get", json!({"cluster": c, "path": root})).await;
    assert_eq!(acl["acl"][0], json!({"perms": "cdrwa", "scheme": "world", "id": "anyone"}));
    b.ok("node.acl.set", json!({"cluster": c, "path": format!("{root}/t1"), "acl": [{"perms": "r", "scheme": "world", "id": "anyone"}], "version": 0})).await;
    assert_eq!(b.err_kind("node.set", json!({"cluster": c, "path": format!("{root}/t1"), "text": "no"})).await, "no-auth");

    // Search, walk, export.
    let s = b.ok("node.search", json!({"cluster": c, "path": root, "name": "^kid[0-2]$"})).await;
    assert_eq!(s["matches"].as_array().unwrap().len(), 3);
    let w = b.ok("node.walk", json!({"cluster": c, "path": root})).await;
    assert!(w["nodes"].as_array().unwrap().len() >= 9);
    let ex = b.ok("node.export", json!({"cluster": c, "path": root, "with_acl": true})).await;
    assert!(ex["nodes"].as_array().unwrap().iter().any(|n| n["rel"] == "kid3"));

    // Copy within cluster and diff.
    let copy = unique_path("bridge-copy");
    b.ok("node.copy", json!({"cluster": c, "path": format!("{root}/kid0"), "to": copy})).await;
    let d = b.ok("node.diff", json!({"left": {"cluster": c, "path": format!("{root}/kid0")}, "right": {"cluster": c, "path": copy}})).await;
    assert_eq!(d["differences"].as_array().unwrap().len(), 0);

    // Recursive delete: dry-run first.
    let dry = b.ok("node.delete", json!({"cluster": c, "path": root, "recursive": true, "dry_run": true})).await;
    assert_eq!(dry["dry_run"], true);
    assert!(b.ok("node.exists", json!({"cluster": c, "path": root})).await["stat"].is_object());
    assert_eq!(b.err_kind("node.delete", json!({"cluster": c, "path": root})).await, "not-empty");
    let del = b.ok("node.delete", json!({"cluster": c, "path": root, "recursive": true})).await;
    assert_eq!(del["paths"].as_array().unwrap().last().unwrap(), &json!(root));
    assert!(b.ok("node.exists", json!({"cluster": c, "path": root})).await["stat"].is_null());
    b.ok("node.delete", json!({"cluster": c, "path": copy})).await;

    let list = b.ok("audit.list", json!({"limit": 1000})).await;
    assert!(list.as_array().unwrap().iter().any(|r| r["operation"] == "delete_recursive"));
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn watches_end_to_end() {
    let server = require_server!();
    zk_testkit::require_version!(server, 3, 6);
    let mut b = Bridge::start().await;
    let c = b.connect(&server.address()).await;
    let root = unique_path("bridge-watch");
    let w = b.ok("watch.add", json!({"cluster": c, "path": root, "kind": "exists"})).await;
    assert!(w["stat"].is_null());
    let wid = w["watch"].as_str().unwrap().to_string();
    b.ok("node.create", json!({"cluster": c, "path": root})).await;
    let ev = b.notification("watch.event", |p| p["watch"] == wid).await;
    assert_eq!(ev["event"]["kind"], "node-created");
    b.notification("watch.removed", |p| p["watch"] == wid).await;

    let w = b.ok("watch.add", json!({"cluster": c, "path": root, "kind": "recursive"})).await;
    let rid = w["watch"].as_str().unwrap().to_string();
    b.ok("node.create", json!({"cluster": c, "path": format!("{root}/a")})).await;
    b.ok("node.set", json!({"cluster": c, "path": format!("{root}/a"), "text": "1"})).await;
    let e1 = b.notification("watch.event", |p| p["watch"] == rid).await;
    let e2 = b.notification("watch.event", |p| p["watch"] == rid).await;
    assert_eq!(e1["event"]["kind"], "node-created");
    assert_eq!(e2["event"]["kind"], "node-data-changed");
    let list = b.ok("watch.list", json!({"cluster": c})).await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    b.ok("watch.remove", json!({"watch": rid})).await;
    assert_eq!(b.ok("watch.list", json!({})).await.as_array().unwrap().len(), 0);
    let e = b.call("watch.remove", json!({"watch": rid})).await.unwrap_err();
    assert_eq!(e["code"], -32002);
    b.ok("node.delete", json!({"cluster": c, "path": root, "recursive": true})).await;
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn watch_rate_limit_collapses_overflow() {
    let server = require_server!();
    zk_testkit::require_version!(server, 3, 6);
    let mut b = Bridge::start_with(&["--watch-rate", "5"], &[]).await;
    let c = b.connect(&server.address()).await;
    let root = unique_path("bridge-rate");
    b.ok("node.create", json!({"cluster": c, "path": root})).await;
    let w = b.ok("watch.add", json!({"cluster": c, "path": root, "kind": "recursive"})).await;
    let wid = w["watch"].as_str().unwrap().to_string();
    let ops: Vec<Value> = (0..50).map(|i| json!({"op": "create", "path": format!("{root}/n{i}")})).collect();
    b.ok("node.transaction", json!({"cluster": c, "ops": ops})).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    b.ok("node.set", json!({"cluster": c, "path": root, "text": "tick"})).await;
    let ov = b.notification("watch.event", |p| p["watch"] == wid && p["event"]["kind"] == "overflow").await;
    assert_eq!(ov["event"]["dropped"], 45);
    b.ok("node.delete", json!({"cluster": c, "path": root, "recursive": true})).await;
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn safety_policy_over_rpc() {
    let server = require_server!();
    let mut b = Bridge::start().await;
    let root = unique_path("bridge-safety");
    let rw = b.connect(&server.address()).await;
    b.ok("node.create", json!({"cluster": rw, "path": root})).await;
    let ro = b.connect_cfg(json!({"connect": server.address(), "safety": {"read_only": true}})).await;
    assert_eq!(b.err_kind("node.set", json!({"cluster": ro, "path": root, "text": "x"})).await, "policy-denied");
    assert_eq!(b.err_kind("node.set", json!({"cluster": ro, "path": root, "text": "x", "force": true})).await, "policy-denied");
    let prot = b.connect_cfg(json!({"connect": server.address(), "safety": {"protected_paths": [format!("{root}/**")]}})).await;
    assert_eq!(b.err_kind("node.create", json!({"cluster": prot, "path": format!("{root}/p")})).await, "policy-denied");
    b.ok("node.create", json!({"cluster": prot, "path": format!("{root}/p"), "force": true})).await;
    let rec = b.notification("audit.record", |p| p["forced"] == true && p["cluster_id"] == prot).await;
    assert_eq!(rec["outcome"], "ok");
    let status = b.ok("cluster.status", json!({"cluster": prot})).await;
    assert_eq!(status["protected_paths"][0], format!("{root}/**"));
    let list = b.ok("cluster.list", json!({})).await;
    assert_eq!(list.as_array().unwrap().len(), 3);
    b.ok("node.delete", json!({"cluster": rw, "path": root, "recursive": true})).await;
    b.ok("cluster.disconnect", json!({"cluster": ro})).await;
    assert_eq!(b.call("node.get", json!({"cluster": ro, "path": "/"})).await.unwrap_err()["code"], -32002);
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn secrets_never_echoed() {
    let server = require_server!();
    let mut b = Bridge::start().await;
    std::env::set_var("ZK_TEST_UNUSED", "x");
    let c = b
        .connect_cfg(json!({"connect": server.address(), "auth": [{"scheme": "digest", "credential": {"value": "bob:TOPSECRET-123"}}]}))
        .await;
    let status = b.ok("cluster.status", json!({"cluster": c})).await;
    assert!(!status.to_string().contains("TOPSECRET"));
    if server.at_least(3, 7) {
        let who = b.ok("auth.whoami", json!({"cluster": c})).await;
        assert!(who.as_array().unwrap().iter().any(|w| w["user"] == "bob"));
    }
    // Failed auth secrets are not echoed in errors either.
    let e = b.call("auth.add", json!({"cluster": c, "scheme": "digest", "credential": {"env": "ZK_NOT_SET_TOPSECRET_VAR"}})).await.unwrap_err();
    assert_eq!(e["data"]["kind"], "config");
    for n in &b.notifications {
        assert!(!n.to_string().contains("TOPSECRET-123"));
    }
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn connection_events_and_outcome_unknown() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let mut b = Bridge::start().await;
    let c = b.connect_cfg(json!({"connect": proxy.address(), "session_timeout_ms": 6000, "operation_timeout_ms": 1500})).await;
    b.notification("connection.event", |p| p["cluster"] == c && p["event"]["kind"] == "connected").await;
    proxy.set_mode(ProxyMode::Blackhole);
    let path = unique_path("bridge-unknown");
    assert_eq!(b.err_kind("node.create", json!({"cluster": c, "path": path})).await, "outcome-unknown");
    proxy.set_mode(ProxyMode::Refuse);
    proxy.drop_connections();
    b.notification("connection.event", |p| p["cluster"] == c && p["event"]["kind"] == "suspended").await;
    proxy.set_mode(ProxyMode::Pass);
    b.notification("connection.event", |p| p["cluster"] == c && p["event"]["kind"] == "reconnected").await;
    let _ = b.call("node.delete", json!({"cluster": c, "path": path})).await;
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn cancellation_and_concurrency() {
    let server = require_server!();
    let proxy = FaultProxy::start(server.address()).await;
    let mut b = Bridge::start().await;
    let c = b.connect_cfg(json!({"connect": proxy.address(), "session_timeout_ms": 10000, "operation_timeout_ms": 20000})).await;
    proxy.set_mode(ProxyMode::Blackhole);
    let id = b.send("node.get", json!({"cluster": c, "path": "/"})).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    b.send_raw(&json!({"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": id}}).to_string()).await;
    let e = b.wait_for(id).await.unwrap_err();
    assert_eq!(e["code"], -32800);
    proxy.set_mode(ProxyMode::Pass);
    // Many concurrent requests are all answered.
    let ids: Vec<u64> = futures_ids(&mut b, &c, 100).await;
    let mut seen = 0;
    while seen < ids.len() {
        let v = b.read().await;
        if v.get("id").and_then(Value::as_u64).map(|i| ids.contains(&i)).unwrap_or(false) {
            assert!(v.get("result").is_some(), "{v}");
            seen += 1;
        }
    }
    b.ok("shutdown", json!({})).await;
}

async fn futures_ids(b: &mut Bridge, c: &str, n: usize) -> Vec<u64> {
    let mut ids = Vec::new();
    for _ in 0..n {
        ids.push(b.send("node.exists", json!({"cluster": c, "path": "/zookeeper"})).await);
    }
    ids
}

#[tokio::test]
async fn eof_closes_session_and_ephemerals() {
    let server = require_server!();
    let mut b = Bridge::start().await;
    let c = b.connect(&server.address()).await;
    let p = unique_path("bridge-eof");
    b.ok("node.create", json!({"cluster": c, "path": p, "mode": "ephemeral"})).await;
    drop(b.stdin);
    let status = tokio::time::timeout(Duration::from_secs(10), b.child.wait()).await.unwrap().unwrap();
    assert!(status.success());
    let mut b2 = Bridge::start().await;
    let c2 = b2.connect(&server.address()).await;
    assert!(b2.ok("node.exists", json!({"cluster": c2, "path": p})).await["stat"].is_null());
    b2.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn admin_and_cross_cluster_copy() {
    let server = require_server!();
    let mut b = Bridge::start().await;
    let c1 = b.connect(&server.address()).await;
    let base = unique_path("bridge-x");
    b.ok("node.create", json!({"cluster": c1, "path": base})).await;
    let c2 = b.connect_cfg(json!({"connect": server.address(), "chroot": base})).await;
    let h = b.ok("cluster.health", json!({"cluster": c1})).await;
    assert_eq!(h[0]["ok"], true);
    let fl = b.ok("cluster.four-letter", json!({"cluster": c1, "command": "mntr"})).await;
    assert!(fl["output"].as_str().unwrap().contains("zk_version"));
    assert_eq!(b.err_kind("cluster.four-letter", json!({"cluster": c1, "command": "srst"})).await, "policy-denied");
    assert_eq!(b.err_kind("cluster.four-letter", json!({"cluster": c1, "command": "ruok", "host": "evil:2181"})).await, "policy-denied");
    let src = format!("{base}-src");
    b.ok("node.create", json!({"cluster": c1, "path": src, "text": "root"})).await;
    b.ok("node.create", json!({"cluster": c1, "path": format!("{src}/child"), "text": "c"})).await;
    let r = b.ok("node.copy", json!({"cluster": c1, "path": src, "to": "/copied", "to_cluster": c2})).await;
    assert_eq!(r["created"].as_array().unwrap().len(), 2);
    let g = b.ok("node.get", json!({"cluster": c1, "path": format!("{base}/copied/child")})).await;
    assert_eq!(g["text"], "c");
    for p in [&base, &src] {
        b.ok("node.delete", json!({"cluster": c1, "path": p, "recursive": true})).await;
    }
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn caches_end_to_end() {
    let server = require_server!();
    zk_testkit::require_version!(server, 3, 6);
    let mut b = Bridge::start().await;
    let c = b.connect(&server.address()).await;
    let root = unique_path("bridge-cache");
    b.ok("node.create", json!({"cluster": c, "path": root, "text": "r"})).await;
    b.ok("node.create", json!({"cluster": c, "path": format!("{root}/a"), "text": "A"})).await;
    let r = b.ok("cache.start", json!({"cluster": c, "path": root, "kind": "tree"})).await;
    let k = r["cache"].as_str().unwrap().to_string();
    assert_eq!(r["status"]["state"], "live");
    assert_eq!(r["status"]["nodes"], 2);
    let got = b.ok("cache.get", json!({"cache": k, "path": format!("{root}/a")})).await;
    assert_eq!(got["text"], "A");
    b.ok("node.create", json!({"cluster": c, "path": format!("{root}/b")})).await;
    let ev = b.notification("cache.event", |p| p["cache"] == k && p["event"]["type"] == "node-added").await;
    assert_eq!(ev["event"]["path"], format!("{root}/b"));
    let ch = b.ok("cache.children", json!({"cache": k, "path": root})).await;
    assert_eq!(ch, json!(["a", "b"]));
    let snap = b.ok("cache.snapshot", json!({"cache": k})).await;
    assert_eq!(snap["nodes"].as_array().unwrap().len(), 3);
    let list = b.ok("cache.list", json!({})).await;
    assert_eq!(list[0]["kind"], "tree");
    b.ok("cache.stop", json!({"cache": k})).await;
    assert_eq!(b.call("cache.status", json!({"cache": k})).await.unwrap_err()["code"], -32002);
    let n = b.ok("cache.start", json!({"cluster": c, "path": format!("{root}/missing"), "kind": "node"})).await;
    let nk = n["cache"].as_str().unwrap().to_string();
    assert_eq!(b.ok("cache.get", json!({"cache": nk})).await, Value::Null);
    b.ok("cluster.disconnect", json!({"cluster": c})).await;
    assert_eq!(b.call("cache.status", json!({"cache": nk})).await.unwrap_err()["code"], -32002);
    let c = b.connect(&server.address()).await;
    b.ok("node.delete", json!({"cluster": c, "path": root, "recursive": true})).await;
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn recipes_end_to_end() {
    let server = require_server!();
    let mut b = Bridge::start().await;
    let mut other = Bridge::start().await;
    let c = b.connect(&server.address()).await;
    let c2 = other.connect(&server.address()).await;
    let base = unique_path("bridge-recipes");

    // Counter.
    let v = b.ok("recipe.counter.add", json!({"cluster": c, "path": format!("{base}/counter"), "delta": 5})).await;
    assert_eq!(v["value"], 5);
    let v = other.ok("recipe.counter.add", json!({"cluster": c2, "path": format!("{base}/counter"), "delta": -2})).await;
    assert_eq!(v["value"], 3);
    assert_eq!(b.ok("recipe.counter.get", json!({"cluster": c, "path": format!("{base}/counter")})).await["value"], 3);

    // Queue FIFO.
    let q = format!("{base}/queue");
    for t in ["one", "two"] {
        b.ok("recipe.queue.put", json!({"cluster": c, "path": q, "text": t})).await;
    }
    assert_eq!(b.ok("recipe.queue.len", json!({"cluster": c, "path": q})).await["len"], 2);
    let item = other.ok("recipe.queue.take", json!({"cluster": c2, "path": q, "timeout_ms": 1000})).await;
    assert_eq!(item["data"], b64(b"one"));
    let ins = b.ok("recipe.inspect", json!({"cluster": c, "path": q})).await;
    assert_eq!(ins["kind"], "queue");
    assert_eq!(ins["size"], 1);

    // Lock held by one bridge; inspect from the other shows the owner; release.
    let lock = format!("{base}/lock");
    let r = b.ok("recipe.lock.acquire", json!({"cluster": c, "path": lock, "timeout_ms": 5000})).await;
    let rid = r["recipe"].as_str().unwrap().to_string();
    assert_eq!(r["ownership"], "held");
    let ins = other.ok("recipe.inspect", json!({"cluster": c2, "path": lock})).await;
    assert_eq!(ins["kind"], "lock");
    assert_eq!(ins["contenders"].as_array().unwrap().len(), 1);
    // The second bridge cannot get the lock while it is held.
    let e = other.call("recipe.lock.acquire", json!({"cluster": c2, "path": lock, "timeout_ms": 500})).await.unwrap_err();
    assert_eq!(e["data"]["kind"], "timeout", "{e}");
    let list = b.ok("recipe.list", json!({})).await;
    assert_eq!(list[0]["kind"], "lock");
    assert_eq!(list[0]["ownership"], "held");
    b.ok("recipe.release", json!({"recipe": rid})).await;
    b.notification("recipe.ownership", |p| p["recipe"] == rid && p["state"] == "released").await;
    let r2 = other.ok("recipe.lock.acquire", json!({"cluster": c2, "path": lock, "timeout_ms": 5000})).await;
    assert_eq!(r2["ownership"], "held");

    // Latch: first starter leads.
    let latch = format!("{base}/latch");
    let l1 = b.ok("recipe.latch.start", json!({"cluster": c, "path": latch, "id": "a"})).await;
    let l1id = l1["recipe"].as_str().unwrap().to_string();
    b.notification("recipe.ownership", |p| p["recipe"] == l1id && p["leader"] == true).await;
    let ins = other.ok("recipe.inspect", json!({"cluster": c2, "path": latch})).await;
    assert_eq!(ins["kind"], "leader-latch");

    // Member join and discovery-style listing via inspect.
    let group = format!("{base}/group");
    let m = b.ok("recipe.member.join", json!({"cluster": c, "path": group, "member_id": "m1", "text": "payload"})).await;
    assert_eq!(m["ownership"], "held");

    // Disconnecting a cluster loses its recipes.
    b.ok("cluster.disconnect", json!({"cluster": c})).await;
    assert_eq!(b.ok("recipe.list", json!({})).await.as_array().unwrap().len(), 0);
    other.ok("shutdown", json!({})).await;
    let mut cleanup = Bridge::start().await;
    let c3 = cleanup.connect(&server.address()).await;
    cleanup.ok("node.delete", json!({"cluster": c3, "path": base, "recursive": true})).await;
    cleanup.ok("shutdown", json!({})).await;
    b.ok("shutdown", json!({})).await;
}

#[tokio::test]
async fn recipes_respect_read_only() {
    let server = require_server!();
    let mut b = Bridge::start().await;
    let ro = b.connect_cfg(json!({"connect": server.address(), "safety": {"read_only": true}})).await;
    let p = unique_path("bridge-ro-recipe");
    assert_eq!(b.err_kind("recipe.lock.acquire", json!({"cluster": ro, "path": p})).await, "policy-denied");
    assert_eq!(b.err_kind("recipe.latch.start", json!({"cluster": ro, "path": p})).await, "policy-denied");
    assert_eq!(b.err_kind("recipe.member.join", json!({"cluster": ro, "path": p, "member_id": "x"})).await, "policy-denied");
    assert_eq!(b.err_kind("recipe.queue.put", json!({"cluster": ro, "path": p, "text": "x"})).await, "policy-denied");
    assert_eq!(b.err_kind("recipe.counter.add", json!({"cluster": ro, "path": p, "delta": 1})).await, "policy-denied");
    // Inspection is allowed.
    let ins = b.ok("recipe.inspect", json!({"cluster": ro, "path": "/zookeeper"})).await;
    assert!(ins["kind"].is_string());
    b.ok("shutdown", json!({})).await;
}
