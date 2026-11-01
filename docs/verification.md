# Local acceptance record

Verified on November 1, 2026 on macOS aarch64 with Rust 1.97.1 and Emacs 31.1.

- `ZK_TESTS_REQUIRED=1 cargo test --workspace -- --test-threads 4`: passed all test binaries against real ZooKeeper. The 10,000-node cache benchmark is intentionally ignored in normal tests.
- `make lisp-compile lisp-test-integration`: warnings-as-errors compilation passed and 23/23 ERT tests passed, including real bridge CRUD, cache and recipe workflows.
- `ZOOKEEPER_HOME=.../apache-zookeeper-3.6.4-bin ZK_TESTS_REQUIRED=1 cargo test -p zk-core --test security -- --test-threads 4`: 6/6 passed after gating identity inspection on ZooKeeper 3.7+. TLS, mutual TLS, SASL DIGEST-MD5 and authorization tests still execute on 3.6.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps`: passed.
- Real local IPC and 10,000-child browsing measurements are recorded in `performance.md`.

This is a development preview, not a declaration of complete v1.0 parity. See `parity.md` and `compatibility/matrix.md` for feature evidence and limitations. Outstanding release gates include three-node ensemble testing, broader independent-process and reference-client recipe interoperability tests, GSSAPI validation, CI execution on advertised platforms, a verified minimum Rust version, and published reproducible release artifacts. Workflow definitions alone are not evidence that those gates passed.

Recipe start operations can return before asynchronous registration has created a contender. An immediate schema-detecting inspection can therefore report `unknown` for an empty latch path. Ownership notifications and subsequent inspection provide the eventual state. Consumers must never infer leadership solely from a successful start response.
