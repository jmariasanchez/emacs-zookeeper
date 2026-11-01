# Local acceptance record

Verified on November 1, 2026 on macOS aarch64 with Rust 1.97.1 and Emacs 31.1.

- `ZK_TESTS_REQUIRED=1 cargo test --workspace -- --test-threads 4`: passed all test binaries against real ZooKeeper. The 10,000-node cache benchmark is intentionally ignored in normal tests.
- `make lisp-compile lisp-test-integration`: warnings-as-errors compilation passed and 23/23 ERT tests passed, including real bridge CRUD, cache and recipe workflows.
- `ZOOKEEPER_HOME=.../apache-zookeeper-3.6.4-bin ZK_TESTS_REQUIRED=1 cargo test -p zk-core --test security -- --test-threads 4`: 6/6 passed after gating identity inspection on ZooKeeper 3.7+. TLS, mutual TLS, SASL DIGEST-MD5 and authorization tests still execute on 3.6.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps`: passed.
- Real local IPC and 10,000-child browsing measurements are recorded in `performance.md`.
- MSRV: `ZK_TESTS_REQUIRED=1 cargo +1.85 test --workspace --locked -- --test-threads 4` passed against real ZooKeeper. The lockfile is MSRV-resolved (`uuid` pinned to 1.26.1).

## CI and release record

- GitHub Actions CI ([run 41289556547](https://github.com/jmariasanchez/emacs-zookeeper/actions/runs/41289556547)) passed all 9 jobs on Linux x86_64: lint (clippy and rustdoc with warnings denied), MSRV 1.85 build, the full Rust suite against ZooKeeper 3.6.4, 3.7.2, 3.8.8 and 3.9.7, and ERT with the real bridge on Emacs 29.4, 30.2 and 31.1.
- The `v0.1.0` tag ran the release workflow ([run 41289872281](https://github.com/jmariasanchez/emacs-zookeeper/actions/runs/41289872281)). It built and smoke-tested `zk-bridge` natively on Linux x86_64/aarch64 and macOS x86_64/aarch64, and published them with SHA-256 checksums and the Emacs package tarball as a GitHub prerelease.
- The downloaded artifacts were checked locally: all four checksums matched, the macOS aarch64 binary reported `zk-bridge 0.1.0 (protocol 1.0)`, `package-install-file` installed `zookeeper-0.1.0.tar` into a clean `package-user-dir`, and the 23 ERT tests (including the real-bridge flow) passed using the released binary.

This is a development preview, not a declaration of complete v1.0 parity. See `parity.md` and `compatibility/matrix.md` for feature evidence and limitations. Outstanding release gates include three-node ensemble testing, broader independent-process and reference-client recipe interoperability tests, GSSAPI validation, and bit-for-bit reproducible builds. The release binaries are built from the locked graph on clean runners, but builds have not been independently reproduced.

Recipe start operations can return before asynchronous registration has created a contender. An immediate schema-detecting inspection can therefore report `unknown` for an empty latch path. Ownership notifications and subsequent inspection provide the eventual state. Consumers must never infer leadership solely from a successful start response.
