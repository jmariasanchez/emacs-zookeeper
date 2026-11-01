# Feature parity specification

This document is the engineering contract for the project. For each
capability it records:

* the reference behaviour (Apache ZooKeeper client, Curator, Kazoo),
* what the underlying Rust client
  ([`zookeeper-client` 0.11](https://docs.rs/zookeeper-client/0.11)) already
  provides,
* the gap we had to build,
* where it lives, how it is tested, and its status.

Statuses are **measured, not aspirational**. A capability is `done` only when
the tests named in the row exist and pass against a real ZooKeeper server.
"Real" means a JVM ZooKeeper started by `zk-testkit`, never a mock.

| Status | Meaning |
| --- | --- |
| `done` | Implemented, documented, and covered by passing real-server tests (including failure paths where relevant). |
| `partial` | Implemented with documented gaps, or only the main path is tested. |
| `planned` | Not implemented. The design is described here. |
| `n/a` | Deliberately out of scope (see section 8). |

Release targets: `v0.1` is the first public preview. `v1.0` is the stable
release criteria in section 9.

Test locations used below:

* **core-it** `crates/zk-core/tests/integration.rs`
* **sec-it** `crates/zk-core/tests/security.rs`
* **cache-it** `crates/zk-cache/tests/cache.rs`
* **recipes-it** `crates/zk-recipes/tests/*.rs`
* **bridge-e2e** `crates/zk-bridge/tests/bridge.rs` (spawns the real binary)
* **ert** `tests/ert/zookeeper-test.el` (unit, fake bridge)
* **ert-it** `tests/ert/zookeeper-integration-test.el` (real bridge + server)

## A. Connections and sessions

| # | Capability | Reference | Rust client provides | Gap we built | Where | Tests | Status | Target |
|---|---|---|---|---|---|---|---|---|
| A1 | Single and multi-server ensembles | ZK `ZooKeeper(connectString)`; Curator `CuratorFramework`; Kazoo `KazooClient(hosts)` | Connect string parsing, endpoint cycling, per-server `tcp://`/`tcp+tls://` | Config layer, connect deadline, typed errors | `zk-core::session`, `config` | core-it `connect_failures`, every test | done (multi-server tested in unit tests only, see G5) | v0.1 |
| A2 | Multiple simultaneous cluster profiles | Curator: one framework per cluster | Independent `Client`s | Bridge cluster registry with handles | `zk-bridge::dispatch` | bridge-e2e `safety_policy_over_rpc`, `admin_and_cross_cluster_copy` | done | v0.1 |
| A3 | Session and connection timeouts | `sessionTimeout`, Curator `connectionTimeoutMs` | `with_session_timeout`, `with_connection_timeout` | Operation deadlines, connect deadline | `config::ClusterConfig` | core-it `write_outcome_unknown_on_partition`, `connect_failures` | done | v0.1 |
| A4 | Automatic recovery with explicit state transitions | Curator `ConnectionStateListener` (CONNECTED, SUSPENDED, RECONNECTED, LOST, READ_ONLY) | `StateWatcher` (Disconnected, SyncConnected, Expired, ...) | Supervisor that maps to Curator-style events with session `generation` | `session::supervise` | core-it `suspension_and_recovery_through_proxy` (fault proxy) | done | v0.1 |
| A5 | Expiry detection and re-establishment | Curator re-creates the ZooKeeper handle; Kazoo restarts the session | Terminal `Expired`, no re-establishment | New session after expiry, `Lost` then `SessionReestablished`, auth replay | `session::supervise` | core-it `session_expiry_and_reestablishment`, `expiry_without_reestablish_closes` | done | v0.1 |
| A6 | Chroot and namespace | Connect string chroot; Curator `namespace` | Connect string chroot, `Client::chroot` | Config `chroot` joined with the embedded chroot | `config::effective_chroot` | core-it `chroot_support`; bridge-e2e `admin_and_cross_cluster_copy` | done | v0.1 |
| A7 | Per-cluster lifecycle and disconnect | `close()` | Drop closes the session | `close()` waits for terminal state; bridge disconnect tears down watches and caches | `session`, `zk-bridge` | bridge-e2e `eof_closes_session_and_ephemerals`, `caches_end_to_end` | done | v0.1 |
| A8 | Connection-state subscriptions | Curator listeners; Kazoo `add_listener` | `StateWatcher` | Broadcast `ConnectionEvent`, `connection.event` RPC notification | `session`, `zk-bridge` | bridge-e2e `connection_events_and_outcome_unknown` | done | v0.1 |
| A9 | Bounded retry policies and backoff | Curator `RetryPolicy` (`ExponentialBackoffRetry`, ...); Kazoo `KazooRetry` | None (only internal retries in `lock`) | `RetryPolicy` with exponential backoff and full jitter, limited by attempts and elapsed time. Applied to reads only, never to writes | `config::RetryPolicy`, `ops::run` | unit `retry_delay_is_bounded`; core-it proxy tests | done | v0.1 |
| A10 | Connection diagnostics | Curator `getZookeeperClient().getCurrentConnectionString()`; Kazoo `client_state` | Session id and timeout | `ClientDiagnostics` (state, generation, counters, last error, TLS/SASL flags) | `session::diagnostics` | core-it `admin_four_letter_words`; bridge-e2e | done | v0.1 |
| A11 | Read-only server mode | `canBeReadOnly` | `with_readonly` | Config flag `allow_read_only`, `ReadOnly` event | `config`, `session` | not yet tested (needs a partitioned ensemble, G5) | partial | v1.0 |

## B. Znode operations

| # | Capability | Reference | Rust client provides | Gap we built | Where | Tests | Status | Target |
|---|---|---|---|---|---|---|---|---|
| B1 | Get data and stat | `getData` | `get_data` | Typed `NodeData`, serde `Stat` | `ops` | core-it `crud_and_stat` | done | v0.1 |
| B2 | List children with stats | `getChildren(path, stat)`; Curator `getChildren().storingStatIn` | `get_children` | Sorted listing, pipelined per-child stats, paging (`limit`/`offset`) | `ops::children_stats`, `node.children` | core-it `crud_and_stat`, `binary_and_large_data`; bridge-e2e | done | v0.1 |
| B3 | Existence check | `exists` | `check_stat` | Retrying read | `ops::exists` | core-it | done | v0.1 |
| B4 | Persistent and ephemeral create | `create` | `create` | ACL defaults, validation, audit | `ops::create` | core-it `node_types` | done | v0.1 |
| B5 | Sequential create | `PERSISTENT_SEQUENTIAL`, `EPHEMERAL_SEQUENTIAL` | `CreateSequence` | Returns the actual path and sequence | `ops::create` | core-it `node_types`; bridge-e2e | done | v0.1 |
| B6 | Container and TTL nodes | `CONTAINER`, `PERSISTENT_WITH_TTL` (3.5.3+, `extendedTypesEnabled`) | `CreateMode::Container`, `with_ttl` | TTL mode validation, sequential TTL | `types::CreateMode` | core-it `node_types` (asserts that the server reaps both kinds) | done | v0.1 |
| B7 | Conditional set | `setData(path, data, version)` | `set_data` | `BadVersion` surfaced with the path | `ops::set_data` | core-it, bridge-e2e | done | v0.1 |
| B8 | Conditional delete | `delete(path, version)` | `delete` | Same | `ops::delete` | core-it | done | v0.1 |
| B9 | Safe recursive delete | `ZKUtil.deleteRecursive`; Curator `deletingChildrenIfNeeded`; Kazoo `delete(recursive=True)` | None | Bounded walk, policy check of every node *before* deleting anything, dry run with `blocked` list, bounded rescans if children appear concurrently, root version check | `ops::delete_recursive` | core-it `safety_policy_and_audit`, `binary_and_large_data` | done | v0.1 |
| B10 | Create missing parents | Curator `creatingParentsIfNeeded`; Kazoo `ensure_path`, `makepath` | `mkdir` | `create_parents`, `ensure_path` | `ops` | core-it `node_types` | done | v0.1 |
| B11 | Atomic multi-op transactions | `multi`/`Transaction`; Curator `transaction()`; Kazoo `transaction()` | `MultiWriter`, `CheckWriter` | Serde `TxnOp`, the failing `op_index` is reported, `OutcomeUnknown` on lost replies | `ops::transaction` | core-it `transactions`; bridge-e2e | done | v0.1 |
| B12 | Sync and consistency | `sync`; Curator `sync()` | `sync` | `get_data_synced`, `node.get {sync:true}` | `ops` | core-it `crud_and_stat` | done | v0.1 |
| B13 | Binary data and large responses | Byte arrays up to `jute.maxbuffer` | `Vec<u8>` | Base64 over RPC, UTF-8 detection, `max_bytes`/`max_text_bytes` truncation flags | `zk-bridge::data_json` | core-it `binary_and_large_data` (512 KiB, 1000 children); bridge-e2e | done | v0.1 |
| B14 | Snapshot-consistent multi-read | 3.6 `multiRead` | `MultiReader` | Typed `ReadOp`/`ReadResult` | `ops::multi_read` | core-it `transactions` | done | v0.1 |
| B15 | Count descendants, own ephemerals | 3.6 `getAllChildrenNumber`, `getEphemerals` | Yes | Version gating | `ops` | core-it `crud_and_stat`, `node_types` | done | v0.1 |
| B16 | Explicit write outcome on connection loss | Curator `ConnectionLoss` and guaranteed-delete helpers | `ConnectionLoss` | `ErrorKind::OutcomeUnknown` for writes, never retried automatically | `error`, `ops::run` | core-it `write_outcome_unknown_on_partition`; bridge-e2e | done | v0.1 |
| B17 | Copy, move, export, import subtrees | zkCli has none; Kazoo has none (application level) | None | `copy_tree`, atomic `move_tree` (one transaction with version checks, up to 500 nodes), `export`/`import`, cross-cluster copy | `ops`, `node.copy` | core-it `walk_export_import_copy_move`; bridge-e2e `admin_and_cross_cluster_copy` | done | v0.1 |

## C. Watching and caching

| # | Capability | Reference | Rust client provides | Gap we built | Where | Tests | Status | Target |
|---|---|---|---|---|---|---|---|---|
| C1 | One-shot data watch | `getData(watch)` | `get_and_watch_data` | Typed `WatchEvent`; session loss reported as an event | `watch` | core-it `oneshot_and_persistent_watches` | done | v0.1 |
| C2 | One-shot existence watch | `exists(watch)` | `check_and_watch_stat` | Same | `watch` | core-it, bridge-e2e `watches_end_to_end` | done | v0.1 |
| C3 | One-shot children watch | `getChildren(watch)` | `get_and_watch_children` | Same | `watch` | core-it | done | v0.1 |
| C4 | Persistent watch | 3.6 `addWatch(PERSISTENT)` | `watch(Persistent)` | Stream API | `watch::watch_persistent` | cache-it, bridge-e2e | done | v0.1 |
| C5 | Persistent recursive watch | 3.6 `addWatch(PERSISTENT_RECURSIVE)` | `watch(PersistentRecursive)` | Same | `watch` | core-it, bridge-e2e | done | v0.1 |
| C6 | Registration and cancellation | `removeWatches` (3.5) | Drop removes the watcher | `WatchStream` cancel, `watch.remove` | `watch`, bridge | bridge-e2e `watches_end_to_end` | done | v0.1 |
| C7 | Watch lifecycle across reconnects | Server auto-resets watches (ZOOKEEPER-43). Persistent watches can miss events (ZOOKEEPER-4698) | Auto reset | `Resync` event tells consumers that events may have been missed | `watch::persistent_loop` | cache-it `resync_after_disconnect` | done | v0.1 |
| C8 | Session-aware invalidation | Curator caches rebuild after LOST | Terminal event | `SessionLost`, then automatic re-registration on the new session (`Reregistered`) | `watch::persistent_loop` | core-it `session_expiry_and_reestablishment` | done | v0.1 |
| C9 | Node-data cache | Curator `NodeCache` / `CuratorCache` single-node mode | None | `NodeCache` (handles missing nodes) | `zk-cache` | cache-it `node_cache_nonexistent_then_created` | done | v0.1 |
| C10 | Children cache | Curator `PathChildrenCache` | None | `ChildrenCache` | `zk-cache` | cache-it `children_cache` | done | v0.1 |
| C11 | Subtree cache | Curator `TreeCache` / `CuratorCache`; Kazoo `TreeCache` | None | `TreeCache` with depth limit | `zk-cache` | cache-it `initial_load_and_incremental`, `concurrent_writers_converge` | done | v0.1 |
| C12 | Bounded size and invalidation | Curator: none (unbounded) | None | `max_nodes` (sets `truncated`), `max_bytes` (evicts least recently fetched data), `max_data_per_node` | `zk-cache` | cache-it `budgets` | done | v0.1 |
| C13 | Change notifications | Curator `CuratorCacheListener` | None | `CacheEvent` broadcast, `cache.event` RPC | `zk-cache`, bridge | cache-it; bridge-e2e `caches_end_to_end` | done | v0.1 |
| C14 | Consistency and staleness indicators | Not in Curator | None | `CacheStatus.state` (live, suspended, resyncing, stale), `generation`, `last_sync_ms`. Stat ordering guard (czxid, mzxid, pzxid) | `zk-cache` | cache-it `resync_after_disconnect`, `rebuild_after_session_expiry` | done | v0.1 |
| C15 | Watch event rate limiting | n/a | n/a | Per-watch rate limit, collapsed `overflow` events | bridge | bridge-e2e `watch_rate_limit_collapses_overflow` | done | v0.1 |

## D. Security and administration

| # | Capability | Reference | Rust client provides | Gap we built | Where | Tests | Status | Target |
|---|---|---|---|---|---|---|---|---|
| D1 | Digest authentication | `addAuthInfo("digest", ...)` | `with_auth`, `auth` | Credentials resolved from secret sources, replayed on new sessions, digest id helper | `config::Secret`, `ops::add_auth`, `types::digest_id` | core-it `acl_and_auth`; unit `digest_matches_zookeeper` | done | v0.1 |
| D2 | SASL | JAAS `Client` section (DIGEST-MD5, Kerberos) | `sasl-digest-md5`, `sasl-gssapi` features | Config mapping. GSSAPI behind the `gssapi` cargo feature | `session::build_connector` | sec-it `sasl_digest_md5`, `enforced_sasl_rejects_anonymous` | partial: DIGEST-MD5 done; GSSAPI builds but is untested (needs a KDC) | v1.0 |
| D3 | TLS and certificate verification | `zookeeper.client.secure`, `ssl.*` | `tls` feature (rustls) | PEM file configuration, hostname verification toggle shown in diagnostics | `config::TlsConfig` | sec-it `tls_server_authentication`, `tls_hostname_verification`, `mutual_tls` | done | v0.1 |
| D4 | ACL inspection | `getACL` | `get_acl` | `cdrwa` perms, serde | `types::Acl` | core-it `acl_and_auth` | done | v0.1 |
| D5 | ACL modification | `setACL(path, acl, aversion)` | `set_acl` | `zkCli`-syntax parser, aversion check | `ops::set_acl`, `Acl::parse` | core-it, bridge-e2e | done | v0.1 |
| D6 | Authorization error reporting | `NoAuthException` | `NoAuth` | Stable kind `no-auth` with path; unreadable nodes counted in search and walk | `error`, `search` | core-it `acl_and_auth`, bridge-e2e | done | v0.1 |
| D7 | Credential storage integration | JAAS files | None | Secrets from `env`, `file`, `command` (argv, no shell). Emacs: auth-source and `pass`-style commands. Redacted in Debug, Serialize and errors | `config::Secret`, `zookeeper.el` | unit `secrets_are_redacted`, `secret_sources`; bridge-e2e `secrets_never_echoed`; ert config tests | done | v0.1 |
| D8 | Read-only and protected profiles | n/a | n/a | `safety.read_only` (cannot be overridden), `protected_paths` glob patterns (`force` overrides them and is audited) | `ops::check_write_allowed` | core-it `safety_policy_and_audit`; bridge-e2e `safety_policy_over_rpc` | done | v0.1 |
| D9 | Audit trail for destructive commands | n/a | n/a | `AuditSink` for every mutation (including denied ones), bridge ring buffer, `audit.record` notification, optional JSONL file | `session::AuditSink`, bridge | core-it `safety_policy_and_audit`; bridge-e2e | done | v0.1 |
| D10 | Server diagnostics | `zkCli` four-letter words, AdminServer | None | Safe four-letter-word allowlist (no `srst`, `crst`, `stmk`), restricted to the cluster's own hosts, `ensemble_health` | `admin` | core-it `admin_four_letter_words`; bridge-e2e `admin_and_cross_cluster_copy` | done | v0.1 |
| D11 | Reconfiguration | `ZooKeeperAdmin.reconfigure` | `update_ensemble`, `get_config` | Read-only policy check, audited | `ops::reconfig` | `get_config` tested; `reconfig` untested (needs a dynamic ensemble, G5) | partial | v1.0 |

## E. Distributed coordination recipes

Recipe semantics share one ownership model (`zk-recipes::ownership`).
Ownership is bound to the pinned session that created the recipe's ephemeral
nodes. The states are `Held`, `Suspended`, `Lost` and `Released`, and session
expiry always means `Lost`. This follows Curator's documented guidance that
leadership must be considered lost on SUSPENDED/LOST. `SuspendPolicy::LoseOnSuspend`
gives the conservative behaviour.

| # | Capability | Curator | Kazoo | Rust client provides | Where | Tests (real server) | Status | Target |
|---|---|---|---|---|---|---|---|---|
| E1 | Exclusive lock | `InterProcessMutex` | `Lock` | `Client::lock` (Curator-compatible prefix) | `locks::Mutex`. Uses the Curator node layout `_c_{uuid}-lock-{seq}`, so it interoperates with Curator. A protected create recovers from lost replies, and acquire is cancel-safe | `mutex_mutual_exclusion`, `mutex_timeout_cancellation_and_participants`; faults `mutex_holder_expiry`; bridge-e2e `recipes_end_to_end` | done | v0.1 |
| E2 | Reentrant lock | `InterProcessMutex` (reentrant) | not reentrant | No | `locks::ReentrantMutex` | `reentrant_mutex` | done. Reentrancy is per instance, not per thread | v0.1 |
| E3 | Read/write lock | `InterProcessReadWriteLock` | `ReadLock`, `WriteLock` | No | `locks::ReadWriteLock` (`__READ__`/`__WRIT__`) | `read_write_lock` | done | v0.1 |
| E4 | Semaphore | `InterProcessSemaphoreV2` | `Semaphore` | No | `semaphore` (`locks/` and `leases/` layout) | `semaphore_limits_concurrency`; faults `semaphore_lease_expiry` | done. The lease limit is fixed at construction | v0.1 |
| E5 | Leader election | `LeaderSelector` | `Election` | No | `election::LeaderElection::run` (requeues automatically) | `leader_election_requeues`; faults `leader_expiry_failover_and_requeue` | done. The closure is not interrupted when leadership is lost: it must watch its `Ownership` | v0.1 |
| E6 | Leader latch | `LeaderLatch` | n/a | `latch-` prefix | `election::LeaderLatch` | `leader_latch_single_leader_and_failover`; faults; bridge-e2e | done | v0.1 |
| E7 | Barrier and double barrier | `DistributedBarrier`, `DistributedDoubleBarrier` | `Barrier`, `DoubleBarrier` | No | `barrier` | `barriers` | done | v0.1 |
| E8 | Atomic counter | `DistributedAtomicLong` | `Counter` | No | `counter::SharedCounter`. CAS with bounded retries. A lost reply surfaces as `OutcomeUnknown` and is never applied twice | `counters_are_atomic`; bridge-e2e | done | v0.1 |
| E9 | Shared counter | `SharedCount` | `Counter` | No | `counter` (`Int32Be`, `Int64Be` and `Decimal` encodings) | `counters_are_atomic` | done | v0.1 |
| E10 | Distributed queue | `DistributedQueue` | `Queue` | No | `queue::DistributedQueue`. `take` claims an item with a versioned delete | `queue_fifo_and_exactly_once` (5 concurrent consumers, each item taken exactly once); bridge-e2e | done | v0.1 |
| E11 | Locking queue | lock path option on `DistributedQueue` | `LockingQueue` | No | `queue::LockingQueue` (ephemeral `taken/` claims) | `locking_queue`; faults `locking_queue_claim_released_on_expiry` | done | v0.1 |
| E12 | Priority and delayed queues | `DistributedPriorityQueue`, `DistributedDelayQueue` | priority in `put` | No | `queue::PriorityQueue`, `put_delayed` | `delayed_and_priority_queue` | done | v0.1 |
| E13 | Distributed IDs | sequential nodes | sequential nodes | `CreateSequence` | `id` (sequential and counter-block generators) | `id_generators_unique` | done | v0.1 |
| E14 | Group membership | `GroupMember` | `Party` | No | `membership::GroupMember` (re-creates its node on a new session) | `group_membership`; faults `persistent_node_member_and_discovery_reregister` | done | v0.1 |
| E15 | Service discovery | `curator-x-discovery` | n/a | No | `discovery` (Curator JSON wire format, `ServiceDiscovery`, `ServiceCache`) | `service_discovery`; faults (re-registers after expiry) | done. The payload is plain JSON. `ServiceCache` also polls every 0.5 to 1 s to catch data-only changes | v0.1 |
| E16 | Persistent (self-healing) node | `PersistentNode` | n/a | No | `persistent_node` | faults `persistent_node_member_and_discovery_reregister` | done. Only ephemeral modes are supported, no TTL variant | v0.1 |
| E17 | Partition and lease coordination | application level | n/a | No | `lease::PartitionAssigner` (rendezvous hashing, exclusive ephemeral claims, epoch) | `partition_assignment_rebalances`; faults `partition_owner_expiry_moves_partitions` | done. Polls to detect membership data changes | v0.1 |
| E18 | Correct cancellation and session-loss handling | documented per recipe | partial | cancel-safe `lock` | `ownership`, `pinned` (session-bound operations never move to a new session) | every faults test asserts `Lost` after expiry, that another contender takes over, and that two `Held` owners never coexist | done | v0.1 |
| E19 | Read-only recipe inspection | n/a | n/a | n/a | `inspect`, `recipe.inspect` | `inspect_unknown_and_counter_paths`; bridge-e2e `recipes_end_to_end`, `recipes_respect_read_only` | done | v0.1 |

Run the suites with `cargo test -p zk-recipes`. The tests live in
`tests/recipes.rs`, which has multi-client tests using independent sessions,
and `tests/faults.rs`, which uses `FaultProxy` to expire holders. Lock
revocation (Curator `Revocable`) and child reapers are not implemented. They
are tracked for v1.0.

## F. Emacs-native user experience

| # | Capability | Where | Tests | Status | Target |
|---|---|---|---|---|---|
| F1 | Interactive cluster connection manager | `zookeeper-connection.el` (`zookeeper-connect`, `zookeeper-connections` buffer, mode-line state) | ert, ert-it | done | v0.1 |
| F2 | Lazy expandable znode tree | `zookeeper-browser.el` (TAB/RET, paging with "more...") | ert `tree-rendering`; ert-it | done | v0.1 |
| F3 | Znode inspector | `zookeeper-node.el` (stat with human times, ephemeral owner, ACL, data) | ert-it | done | v0.1 |
| F4 | Editable data with version checking | `zookeeper-node.el` (`C-c C-c` saves with the version read; conflict buffer offers overwrite, reload, or ediff) | ert-it (version conflict) | done | v0.1 |
| F5 | JSON, text, hex and base64 views | `zookeeper-node.el` | ert data-view tests | done | v0.1 |
| F6 | Create, delete, copy and move workflows | browser `c`, `D`, `C`, `R` | ert-it | done | v0.1 |
| F7 | ACL inspection and editing | `zookeeper-acl.el` | ert perms tests | done | v0.1 |
| F8 | Live watch and event buffer | `zookeeper-watch.el` (labelled "locally observed") | ert | done | v0.1 |
| F9 | Tree diff and comparison | browser `=` (`node.diff`, also across clusters) | bridge-e2e; ert | done | v0.1 |
| F10 | Subtree search | browser `s` (`node.search`, with explicit partial-result reasons) | bridge-e2e; ert | done | v0.1 |
| F11 | Bookmarks and recent paths | `bookmark-make-record-function`, `zookeeper-visit-recent` | ert | done | v0.1 |
| F12 | TRAMP-compatible bridge execution | `zookeeper-bridge-remote` with `start-file-process` | none (manual test only) | partial | v1.0 |
| F13 | Transient menus | `zookeeper-dispatch` | byte-compile | done | v0.1 |
| F14 | Embark and completion integration | `zookeeper-path` completion category; Embark target (guarded) | ert | done | v0.1 |
| F15 | Dired-like mark and act | `m`, `u`, `U`, `d`, `x`, `*` | ert mark tests | done | v0.1 |
| F16 | Session and error diagnostics | `zookeeper-diagnostics.el` (health, four-letter words, status, audit, bridge log) | ert | done | v0.1 |
| F17 | Keyboard-centric workflows | All commands bound, `?` opens the transient | n/a | done | v0.1 |
| F18 | Customizable faces and hooks | `defface`s, mode hooks, notification hook variables | byte-compile | done | v0.1 |
| F19 | Read-only and destructive-operation controls | Profile read-only, dry-run preview, typed confirmation for recursive or protected deletes, second confirmation before `force` | ert | done | v0.1 |
| F20 | Multi-cluster browsing and comparison | One browser per cluster, cross-cluster diff and copy | bridge-e2e | done | v0.1 |
| F21 | Cache-driven live refresh and freshness display | One `children` cache per expanded browser node, freshness shown in the header with a STALE warning, falling back to watches when the bridge has no caches. `zookeeper-node-follow-mode` never overwrites local edits | ert `zookeeper-test-cache-fallback-to-watch`, `zookeeper-test-browser-freshness`, `zookeeper-test-follow-never-clobbers`; ert-it `zookeeper-integration-caches` | done | v0.1 |
| F22 | Recipe inspection and controlled participation UI | `zookeeper-recipe.el`, backed by `recipe.*` (inspect, discovery, counters, queues, bridge-owned lock, latch and member, `recipe.ownership` notifications) | bridge-e2e `recipes_end_to_end`; ert | partial: bridge side done, an Emacs real-bridge test for recipes is pending | v0.1 |

## G. Quality and distribution

| # | Capability | Where | Status | Target |
|---|---|---|---|---|
| G1 | Stable versioned bridge protocol | `docs/api/protocol.md`; `initialize` version handshake; mismatch exits with code 3 | done (draft-stable 1.0) | v1.0 freeze |
| G2 | Typed request and response schemas | serde types in `zk-core`, a strict `params` parser (`-32602`) | done; machine-readable JSON Schema is planned | v1.0 |
| G3 | Unit tests (Rust and Lisp) | `cargo test`, `tests/ert` | done | v0.1 |
| G4 | Integration tests against real ZooKeeper | `zk-testkit` launches the JVM server; no mocks | done | v0.1 |
| G5 | Fault injection and partitions | `zk-testkit::FaultProxy` (refuse, blackhole, drop, `partition_until_expired`). Multi-node ensemble harness is planned | partial | v1.0 |
| G6 | Compatibility tests | `scripts/compat-matrix.sh`; see `docs/compatibility/matrix.md` | done for 3.5 to 3.9 standalone | v0.1 |
| G7 | Benchmarks and performance regression | `crates/zk-bridge/benches` (IPC overhead), cache 10k bench (`#[ignore]`) | partial | v1.0 |
| G8 | Cross-platform binaries | `.github/workflows/release.yml` (Linux x86_64/aarch64, macOS x86_64/aarch64) | done in config, not yet exercised by a tag | v0.1 |
| G9 | Emacs package and binary discovery | `zookeeper-bridge-executable` discovery (defcustom, `exec-path`, package `bin/`, repo `target/`) | done | v0.1 |
| G10 | Automated releases and documentation | release workflow, `docs/` | partial | v1.0 |
| G11 | Threat model and redaction tests | `docs/architecture/threat-model.md`; redaction tests in unit, bridge-e2e and ert | done | v0.1 |
| G12 | Contribution guide and compatibility matrix | `CONTRIBUTING.md`, `docs/compatibility/matrix.md` | done | v0.1 |
