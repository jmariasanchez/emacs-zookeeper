# ZooKeeper for Emacs

An Apache ZooKeeper toolkit in Rust, with Emacs as its first fully integrated
interface. It ships as three products:

| Product | What it is |
| --- | --- |
| **zookeeper.el** (`lisp/`) | Emacs UI for browsing, inspecting, editing, monitoring, diffing and safely administering ZooKeeper clusters. |
| **zk-bridge** (`crates/zk-bridge`) | A Rust executable that speaks JSON-RPC 2.0 over stdio. It exposes typed operations, watch, cache and connection events, and an audit trail. |
| **zk-core / zk-cache / zk-recipes** (`crates/`) | Reusable async Rust libraries with no Emacs or RPC dependency: supervised sessions, operations, transactions, ACLs, watches, caches, and coordination recipes in the style of Curator and Kazoo. |

The engineering contract is [`docs/parity.md`](docs/parity.md). It maps every
Curator, Kazoo and ZooKeeper capability to what the underlying Rust client
already provides, the gap built here, the tests that prove it, and its
measured status.

## Status

Pre-release (`0.1.0-dev`). Everything listed as `done` in the parity spec is
tested against real ZooKeeper servers. The tests never use mocks: `zk-testkit`
launches a JVM ZooKeeper, and a fault-injecting proxy covers partitions,
disconnects and session expiry.

* ZooKeeper 3.6, 3.7, 3.8 and 3.9 are supported. 3.5 has limited support. See
  [`docs/compatibility/matrix.md`](docs/compatibility/matrix.md).
* The test suites cover plain TCP, TLS (CA and hostname verification), mutual
  TLS, digest auth and SASL DIGEST-MD5.
* Emacs 29.1 or newer.

## Quick start

```sh
# 1. Build the bridge
cargo build --release -p zk-bridge          # -> target/release/zk-bridge

# 2. Optional: a throwaway local ZooKeeper (needs Java + a ZooKeeper tarball
#    under ~/.cache/zk or ZOOKEEPER_HOME)
eval "$(scripts/dev-zk.sh start)"; echo "$ZK_CONNECT"
```

```elisp
;; 3. Emacs
(add-to-list 'load-path "/path/to/emacs-zookeeper/lisp")
(require 'zookeeper)
(setq zookeeper-bridge-executable "/path/to/emacs-zookeeper/target/release/zk-bridge")
(setq zookeeper-clusters
      '(("local" :connect "127.0.0.1:2181")
        ("prod"  :connect "zk1:2181,zk2:2181,zk3:2181/app"
                 :read-only t
                 :protected-paths ("/kafka/**" "/zookeeper/**")
                 :auth ((:scheme "digest"
                         :credential (:command ("pass" "show" "zk/prod")))))))
```

Run `M-x zookeeper`, pick a cluster, then press `?` for the command menu.

The [user guide](docs/user-guide.md) covers keybindings, editing, safety
controls, watches, caches, search, diff and recipes.

## Repository layout

```
crates/zk-core      sessions, operations, transactions, ACL, watches, safety, admin, search/diff
crates/zk-cache     NodeCache / ChildrenCache / TreeCache with budgets and freshness state
crates/zk-recipes   locks, elections, semaphores, barriers, counters, queues, discovery, ...
crates/zk-bridge    JSON-RPC stdio server (docs/api/protocol.md)
crates/zk-testkit   real-ZooKeeper launcher + fault-injecting TCP proxy (tests only)
lisp/               zookeeper.el
tests/ert/          ERT unit tests (fake bridge) and real-bridge integration tests
docs/               parity spec, protocol, architecture, threat model, compatibility
scripts/            dev-zk.sh, compat-matrix.sh
```

## Development

```sh
. ./env.sh                  # macOS + Nix toolchains only: use the system linker
make rust-test              # all Rust tests (starts ZooKeeper automatically)
make lisp-compile           # byte-compile with warnings as errors
make lisp-test-integration  # ERT, including real bridge + ZooKeeper
make compat                 # run the suites against every cached ZooKeeper version
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before sending changes.

## License

Dual licensed under MIT ([LICENSE-MIT](LICENSE-MIT)) or Apache-2.0
([LICENSE-APACHE](LICENSE-APACHE)), at your option.
