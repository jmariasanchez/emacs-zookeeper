//! IPC overhead benchmark for `zk-bridge`.
//!
//! Measures round-trip latency of JSON-RPC requests through the real bridge
//! process over stdio:
//!
//! * `$/ping`: pure IPC and dispatch overhead, no ZooKeeper involved. This is
//!   the number checked against the "p95 < 5 ms local IPC overhead" target.
//! * `node.exists`: IPC plus a ZooKeeper round trip, for context.
//! * `node.children` with 10,000 children and stats: large-response handling.
//!
//! Run with `cargo bench -p zk-bridge`. The ZooKeeper parts need a
//! distribution (see CONTRIBUTING.md) and are skipped otherwise.

use std::process::Stdio;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

struct Stats(Vec<Duration>);

impl Stats {
    fn pct(&mut self, p: f64) -> Duration {
        self.0.sort();
        let i = ((self.0.len() as f64 - 1.0) * p).round() as usize;
        self.0[i]
    }

    fn report(&mut self, name: &str) -> Duration {
        let (p50, p95, p99) = (self.pct(0.50), self.pct(0.95), self.pct(0.99));
        println!(
            "{name:<34} n={:<6} p50={:>8.3}ms p95={:>8.3}ms p99={:>8.3}ms",
            self.0.len(),
            p50.as_secs_f64() * 1e3,
            p95.as_secs_f64() * 1e3,
            p99.as_secs_f64() * 1e3
        );
        p95
    }
}

struct Rpc {
    stdin: tokio::process::ChildStdin,
    lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    id: u64,
}

impl Rpc {
    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        let id = self.id;
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string();
        self.stdin.write_all(msg.as_bytes()).await.unwrap();
        self.stdin.write_all(b"\n").await.unwrap();
        self.stdin.flush().await.unwrap();
        loop {
            let line = self.lines.next_line().await.unwrap().unwrap();
            let v: Value = serde_json::from_str(&line).unwrap();
            if v.get("id") == Some(&json!(id)) {
                return v;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let exe = std::env::var("ZK_BRIDGE").unwrap_or_else(|_| {
        let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/release/zk-bridge");
        if p.exists() { p.display().to_string() } else { env!("CARGO_MANIFEST_DIR").to_string() + "/../../target/debug/zk-bridge" }
    });
    let mut child = Command::new(&exe).stdin(Stdio::piped()).stdout(Stdio::piped()).kill_on_drop(true).spawn().expect("spawn zk-bridge (build it first)");
    let mut b = Rpc { stdin: child.stdin.take().unwrap(), lines: BufReader::new(child.stdout.take().unwrap()).lines(), id: 0 };
    b.call("initialize", json!({"protocol_version": "1.0"})).await;
    println!("bridge: {exe}");

    // Warm up.
    for _ in 0..200 {
        b.call("$/ping", json!({})).await;
    }
    let mut s = Stats(Vec::new());
    for _ in 0..5000 {
        let t = Instant::now();
        b.call("$/ping", json!({})).await;
        s.0.push(t.elapsed());
    }
    let p95 = s.report("$/ping (IPC overhead)");
    let target = Duration::from_millis(5);
    println!("target p95 < 5ms: {}", if p95 < target { "MET" } else { "NOT MET" });

    let Some(server) = zk_testkit::shared_server() else {
        println!("no ZooKeeper distribution; skipping ZooKeeper benchmarks");
        return;
    };
    let r = b.call("cluster.connect", json!({"config": {"connect": server.address()}})).await;
    let c = r["result"]["cluster"].as_str().unwrap().to_string();
    let mut s = Stats(Vec::new());
    for _ in 0..2000 {
        let t = Instant::now();
        b.call("node.exists", json!({"cluster": c, "path": "/zookeeper"})).await;
        s.0.push(t.elapsed());
    }
    s.report("node.exists (IPC + ZooKeeper)");

    let root = zk_testkit::unique_path("bench-wide");
    b.call("node.create", json!({"cluster": c, "path": root})).await;
    for chunk in (0..10_000).collect::<Vec<_>>().chunks(1000) {
        let ops: Vec<Value> = chunk.iter().map(|i| json!({"op": "create", "path": format!("{root}/child-{i:05}")})).collect();
        b.call("node.transaction", json!({"cluster": c, "ops": ops})).await;
    }
    let mut s = Stats(Vec::new());
    for _ in 0..10 {
        let t = Instant::now();
        let r = b.call("node.children", json!({"cluster": c, "path": root, "stats": true})).await;
        assert_eq!(r["result"]["total"], 10_000);
        s.0.push(t.elapsed());
    }
    s.report("node.children 10k + stats");
    let mut s = Stats(Vec::new());
    for _ in 0..50 {
        let t = Instant::now();
        b.call("node.children", json!({"cluster": c, "path": root, "stats": true, "limit": 500})).await;
        s.0.push(t.elapsed());
    }
    s.report("node.children 10k, page of 500");
    b.call("node.delete", json!({"cluster": c, "path": root, "recursive": true, "limit": 20000})).await;
    b.call("shutdown", json!({})).await;
}
