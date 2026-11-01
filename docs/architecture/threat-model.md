# Threat model

Scope: `zookeeper.el` running in a user's Emacs, the local `zk-bridge`
process it spawns, and the ZooKeeper ensembles the bridge connects to. The
goal is to let an engineer administer production clusters from Emacs without
the tool becoming a new way to leak credentials or cause damage.

## Assets

| Asset | Why it matters |
| --- | --- |
| ZooKeeper credentials (digest secrets, SASL passwords, TLS private keys) | They grant cluster access, often admin. |
| Cluster data | It can include configuration secrets stored in znodes. |
| Cluster integrity | Locks, leaders and configs that production services depend on. |
| Audit trail | It records who changed what during an incident. |

## Trust boundaries

1. **Emacs to bridge.** The pipe belongs to the same user. The bridge trusts
   its parent process. It never opens a listening socket. When it runs over
   TRAMP, the transport is the user's SSH session.
2. **Bridge to ZooKeeper.** Over the network. It can be plain TCP or TLS.
3. **Bridge to the local system.** The bridge reads certificate and secret
   files and runs secret helper commands named in the profile.

## Threats and mitigations

| # | Threat | Mitigation | Verified by |
| --- | --- | --- | --- |
| T1 | Secrets end up in logs, `*Messages*`, RPC responses, error messages or audit records | `Secret`'s `Debug` and `Serialize` print `<redacted>`. Secrets are resolved inside the bridge only when a session is created. Errors name the source (for example `env VAR`), never the value. The Emacs side resolves auth-source credentials only at connect time and never stores them in buffers. | `config::tests::secrets_are_redacted`; bridge-e2e `secrets_never_echoed`; `security::digest_auth_failure_is_terminal_and_redacted`; ert `zookeeper-test-auth-source-only-at-connect`, `zookeeper-test-config-conversion` |
| T2 | Secret helper commands are used for shell injection | `Secret::Command` takes an argv vector and runs it without a shell. stdin is `/dev/null`. | code review (`config::Secret::resolve`) |
| T3 | Man-in-the-middle on the ZooKeeper connection | TLS uses rustls with CA pinning through `ca_file`. Hostname verification is on by default. Turning it off is an explicit setting, and diagnostics report it. | `security::tls_hostname_verification`, `tls_server_authentication` (untrusted CA rejected, no plaintext downgrade) |
| T4 | An accidental destructive change to production | Read-only profiles that `force` cannot override. Protected path patterns. Recursive deletes check every node against policy before deleting anything, and have a node limit and a dry-run preview. Emacs asks for typed confirmation on recursive or protected deletes and a second confirmation before `force`. Writes are never retried automatically, so a write whose reply was lost cannot apply twice. | `integration::safety_policy_and_audit`; bridge-e2e `safety_policy_over_rpc`; ert confirmation tests |
| T5 | Silently overwriting someone else's change | The editor saves with the version it read. `bad-version` opens a conflict buffer. `outcome-unknown` tells the user to re-read. | bridge-e2e `node_operations_end_to_end`; ert-it version conflict |
| T6 | Destructive server administration | The four-letter-word allowlist excludes `srst`, `crst` and `stmk`. The target host must belong to the cluster profile, so the bridge cannot be used as a port scanner. `reconfig` is refused on read-only profiles and is audited. | bridge-e2e `admin_and_cross_cluster_copy` |
| T7 | Resource exhaustion from huge trees, event storms or large nodes | Walks, search, export and caches have node and byte limits. Requests have a line-length limit. A semaphore caps concurrency. Watch events are rate limited per watch and collapse into `overflow`. Truncation is always reported. | bridge-e2e `watch_rate_limit_collapses_overflow`; cache-it `budgets`; core-it walk limits |
| T8 | Assuming ownership after a session is lost | The recipe ownership model treats expiry as `Lost`, and that state is terminal. The Emacs UI shows ownership state and never infers it from a stale handle. | recipes-it fault-injection tests |
| T9 | Leftover ephemeral nodes or sessions keep locks held | When stdin reaches EOF or the bridge gets `shutdown`, it closes every session. Clients close sessions when the last handle is dropped. | bridge-e2e `eof_closes_session_and_ephemerals` |
| T10 | Hiding the audit trail | Every mutation is audited, including denied ones. Forced operations are flagged. `ZK_BRIDGE_AUDIT_LOG` adds an append-only local file. | `integration::safety_policy_and_audit`; bridge-e2e |
| T11 | Malicious data in a znode attacks the editor | Data travels as base64. Display decodes it as text only when it is valid UTF-8, and otherwise falls back to hex. Data is never evaluated. JSON view uses `json-parse-string`. | ert data-view tests |

## Out of scope

* Server-side ZooKeeper vulnerabilities, ensemble hardening and network ACLs.
* A compromised local user account. The bridge runs with the user's
  privileges.
* Protecting secrets in the user's own Emacs configuration when they choose
  `{:value ...}`. The documentation recommends `:command`, `:env` or
  auth-source instead.
