# Performance

Measured with `cargo bench -p zk-bridge --bench ipc`, using a release build of
the bridge and a local standalone ZooKeeper 3.9.7. Machine: Apple Silicon
(aarch64), macOS. Date: 2026-11-01.

These numbers are for regression tracking. Real networks add their own round
trip time on top.

| Measure | n | p50 | p95 | p99 | Target | Result |
| --- | --- | --- | --- | --- | --- | --- |
| `$/ping` (stdio IPC and dispatch, no ZooKeeper) | 5000 | 0.029 ms | 0.051 ms | 0.065 ms | p95 < 5 ms | met |
| `node.exists` (IPC plus a local ZooKeeper round trip) | 2000 | 0.100 ms | 0.203 ms | 0.257 ms | n/a | n/a |
| `node.children` of 10,000 children, one page of 500 with stats | 50 | 12.8 ms | 18.5 ms | 25.5 ms | browsing large nodes is responsive | met |
| `node.children` of all 10,000 children with stats | 10 | 216 ms | 329 ms | 329 ms | n/a | n/a (the browser pages instead) |

Notes:

* The Emacs browser fetches children in pages (`zookeeper-browser-page-size`,
  default 500), so expanding a node with 10,000 or more children costs one
  page, not the full listing.
* Every bridge request runs in its own task. Slow requests do not block fast
  ones. The concurrency cap defaults to 256 and can be changed with
  `--max-concurrency`.
* Caches have a memory budget (`max_nodes`, `max_bytes`, `max_data_per_node`).
  `crates/zk-cache/tests/cache.rs::bench_10k_tree` (`--ignored`) measures how
  long a 10,000-node tree takes to load.
* Planned for v1.0: run the benchmark in CI and fail when p95 regresses by
  more than 20% against the stored baseline (parity G7).
