# Contributing

## Ground rules

1. **No mocks for ZooKeeper behaviour.** Correctness tests run against a real
   server through `zk-testkit` (`require_server!()`). Failure paths use
   `FaultProxy`, which supports refuse, blackhole and drop modes plus
   `partition_until_expired`. Mocks are fine for pure logic: parsers, the
   Emacs rendering code and the fake bridge in `tests/ert`.
2. **The parity spec is the contract.** Any change that adds or changes a
   capability must update its row in `docs/parity.md`: where it lives, which
   tests cover it, and its status. A row is only `done` when its tests exist
   and pass.
3. **The dependency rule.** `zk-core`, `zk-cache` and `zk-recipes` must not
   depend on Emacs, JSON-RPC or `zk-bridge`. Domain logic belongs in the
   libraries. The bridge only adapts it.
4. **Protocol changes** go in `docs/api/protocol.md` first. Additive changes
   bump the minor `PROTOCOL_VERSION`. Breaking changes bump the major version.
   Error `kind` strings are stable and must never be renamed.
5. **Write semantics.** Never retry a write automatically. Surface
   `outcome-unknown` instead. Every mutation goes through the safety policy and
   the audit sink.
6. **Session-bound recipes** must express ownership through
   `zk_recipes::Ownership`. They need a fault-injection test showing that
   ownership is `Lost` after expiry and that two owners never hold at once.

## Setup

* A Rust stable toolchain (MSRV 1.80).
* Java 11 or newer, plus a ZooKeeper binary distribution. Point
  `ZOOKEEPER_HOME` at it, or unpack it under `~/.cache/zk/`:

  ```sh
  mkdir -p ~/.cache/zk && curl -fsSL https://dlcdn.apache.org/zookeeper/zookeeper-3.9.7/apache-zookeeper-3.9.7-bin.tar.gz | tar xz -C ~/.cache/zk
  ```

* Emacs 29.1 or newer for the Lisp side.
* On macOS with Nix toolchains, run `. ./env.sh` so that the system linker is
  used.

Tests that need a server skip with a message when none is found. CI sets
`ZK_TESTS_REQUIRED=1`, which turns those skips into failures.

## Checklist before sending a change

```sh
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
make lisp-compile lisp-test-integration
```

For changes that affect compatibility, also run `make compat` and update
`docs/compatibility/matrix.md`.

## Style

* Rust: follow the existing style, write rustdoc for public items, and use
  typed errors (`ZkError` with a stable `ErrorKind`).
* Elisp: use `lexical-binding`. Give every command a docstring. Byte-compiling
  must produce no warnings. Do not add external package dependencies beyond
  the ones listed in `Package-Requires`.
