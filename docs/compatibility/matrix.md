# Compatibility matrix

The rows are measured with `scripts/compat-matrix.sh`, which runs the real-server
suites against each ZooKeeper distribution found under `~/.cache/zk`. A row
counts as supported only when CI runs it. The CI matrix in
`.github/workflows/ci.yml` runs 3.6, 3.7, 3.8 and 3.9 on every push.

## ZooKeeper server versions

Measured on 2026-11-01 (macOS aarch64, standalone servers, Java 27):

| Server | zk-core | zk-bridge e2e | zk-cache | zk-recipes | Level | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| 3.9.7 | pass | pass | pass | pass | Supported | Watch events carry `zxid` |
| 3.8.8 | pass | pass | pass | pass | Supported | |
| 3.7.2 | pass | pass | pass | pass | Supported | |
| 3.6.4 | pass | pass | pass | pass | Supported | `whoami` returns `unimplemented` |
| 3.5.10 | pass | pass | n/a | pass | Limited | No persistent watches, so no caches and no live browser refresh. No `multiRead`, `getEphemerals` or `getAllChildrenNumber`. Set `server_version` (see below) |
| 3.4.x | not tested | not tested | n/a | not tested | Unsupported | Set `server_version: "3.4.x"` so creates use the old opcode. No containers or TTL |

On 3.5 the suites run the version-gated paths and assert the documented
`unimplemented` errors. Tests that need a newer server log `skipping:
requires ZooKeeper 3.6+` and count as passed.

### Version-gated features

| Feature | Minimum version |
| --- | --- |
| Containers and TTL nodes | 3.5.3, and TTL also needs `extendedTypesEnabled=true` |
| Persistent and recursive watches, caches | 3.6.0 |
| `multiRead`, `getEphemerals`, `getAllChildrenNumber` | 3.6.0 |
| `whoami` | 3.7.0 |
| Watch event `zxid` | 3.9.0 |

When a profile sets `server_version`, unsupported calls fail locally with
`unimplemented` and are never sent. Setting it matters on 3.5: those servers
drop the connection when they receive an unknown opcode.

## Topologies

| Topology | Status |
| --- | --- |
| Standalone | Tested |
| Three-node ensemble (failover, leader loss, read-only mode) | Planned. The multi-node harness is tracked in parity G5. Connect-string handling for several hosts is unit-tested |
| Dynamic reconfiguration | Planned (parity D11) |

## Transports and authentication

| Combination | Status | Test |
| --- | --- | --- |
| Plain TCP | Tested | all suites |
| TLS, server authentication (CA, DNS and IP SAN) | Tested on 3.9 | `security::tls_server_authentication` |
| TLS with hostname verification failure and explicit opt-out | Tested on 3.9 | `security::tls_hostname_verification` |
| Mutual TLS (x509 provider) | Tested on 3.9 | `security::mutual_tls` |
| Digest (`addauth`) | Tested on 3.5 to 3.9 | `integration::acl_and_auth` |
| SASL DIGEST-MD5 | Tested on 3.9 | `security::sasl_digest_md5` |
| SASL with `enforce.auth` | Tested on 3.9 | `security::enforced_sasl_rejects_anonymous` |
| SASL GSSAPI (Kerberos) | Builds (`--features gssapi`), not tested | needs a KDC fixture |

ZooKeeper 3.9 runs the server in FIPS mode by default, and in that mode the
server refuses DIGEST-MD5. Servers that use DIGEST-MD5 SASL need
`-Dzookeeper.fips-mode=false`.

## Platforms

| Platform | Rust crates | Emacs package | Release binary |
| --- | --- | --- | --- |
| macOS aarch64 | Tested locally | Tested (Emacs 31.1) | Release workflow |
| Linux x86_64 | CI | CI (Emacs 29.4, 30.1) | Release workflow |
| Linux aarch64 | Builds (cross) | Should work | Release workflow |
| macOS x86_64 | Builds | Should work | Release workflow |
| Windows | Untested, not advertised | Untested | not built |

The minimum Emacs version is 29.1, because the package needs native JSON and
`transient`. CI runs the ERT suite on 29.4 and 30.1.

The minimum supported Rust version is 1.85. It is the floor imposed by
edition-2024 dependencies (`getrandom` 0.4 and others), and the lockfile is
resolved so that every locked crate supports it. The `msrv` CI job builds all
targets with Rust 1.85, and the full real-server suite was also run locally on
1.85.
