# zk-bridge protocol (v1)

`zk-bridge` is a long-running process that speaks **JSON-RPC 2.0** over its
standard input and output. It is the only component that talks to ZooKeeper.
Editors (first of all `zookeeper.el`) start one bridge process per editor
session and multiplex any number of clusters over it.

Status: **draft-stable**. The method set below is versioned by
`protocol_version`. Additive changes (new methods, new optional params, new
result fields) bump the minor version. Removing or renaming anything bumps the
major version.

| Item | Value |
| --- | --- |
| Transport | stdio, UTF-8 |
| Framing | one JSON object per line (`\n` terminated, no embedded newlines) |
| Protocol version | `1.0` |
| Logging | stderr only, never stdout. Level via `ZK_BRIDGE_LOG` (`error`..`trace`), default `warn` |

## Messages

Requests: `{"jsonrpc":"2.0","id":<int|string>,"method":"...","params":{...}}`.
Responses: `{"jsonrpc":"2.0","id":...,"result":...}` or
`{"jsonrpc":"2.0","id":...,"error":{"code":...,"message":"...","data":{...}}}`.
Notifications (server to client) have no `id`.

Requests are processed **concurrently**; responses may arrive in any order.
Requests that touch the same cluster are sent to ZooKeeper in arrival order
(the underlying client preserves request order per session), so a `node.set`
followed by a `node.get` on the same cluster observes the write.

### Version-dependent features

Some operations need newer servers:

| Operation | Minimum server version |
| --- | --- |
| `node.count` (`getAllChildrenNumber`) | 3.6 |
| `node.ephemerals` | 3.6 |
| `node.multi-read` | 3.6 |
| Persistent and recursive watches, including caches | 3.6 |
| `auth.whoami` | 3.7 |
| Watch event `zxid` | 3.9 |

On older servers these return `unimplemented`.

If `server_version` is set in the cluster config, the bridge refuses the call
locally and never sends it. This matters because 3.5 servers drop the
connection when they receive an unknown opcode.

### Error codes

| Code | Meaning |
| --- | --- |
| -32700 | Parse error |
| -32600 | Invalid request |
| -32601 | Method not found |
| -32602 | Invalid params |
| -32603 | Internal error |
| -32800 | Request cancelled (`$/cancelRequest`) |
| -32001 | ZooKeeper or policy error. `data` is a `ZkError` |
| -32002 | Unknown cluster/watch/cache/recipe handle |

A `ZkError` object is `{"kind": "...", "message": "...", "path"?: "...", "op_index"?: n}`.
`kind` is one of the stable kebab-case strings below. Clients must branch on
`kind`, never on `message`.

`no-node`, `node-exists`, `bad-version`, `not-empty`,
`no-children-for-ephemerals`, `no-auth`, `auth-failed`, `invalid-acl`,
`session-expired`, `session-moved`, `connection-loss`, `outcome-unknown`,
`not-connected`, `timeout`, `closed`, `not-read-only`, `unimplemented`,
`reconfig-disabled`, `quota-exceeded`, `throttled`, `bad-arguments`,
`policy-denied`, `cancelled`, `inconsistent`, `limit-exceeded`, `io`,
`config`, `protocol`, `other`.

`outcome-unknown` is returned when a **write** was sent but its reply was not
received (connection lost or deadline hit). The write may or may not have
been applied. Clients must re-read before retrying. The bridge never retries
writes on its own. Reads are retried with bounded backoff per the cluster's
retry policy.

## Common shapes

**Stat**
```json
{"czxid":2,"mzxid":5,"pzxid":2,"ctime":1730419200000,"mtime":1730419260000,
 "version":3,"cversion":0,"aversion":0,"ephemeral_owner":0,
 "data_length":5,"num_children":0}
```
`ephemeral_owner` is a JSON number. Session ids up to 2^63 can exceed the
exact integer range of some JSON parsers. Emacs handles them exactly. For
display, results that carry session ids also include a hex string (for
example `session_id: "0x1000..."`).

**Data.** Node data is always sent as base64 in `data`. Responses that
carry data also include:

* `utf8`: `true` if the bytes are valid UTF-8.
* `text`: the decoded string when `utf8` is true and the size is within
  `max_text_bytes` (default 1 MiB).
* `truncated`: `true` if `max_bytes` cut the data.

Requests that carry data accept either `data` (base64) or `text` (UTF-8 string).
Giving both is an error.

**ACL entry** `{"perms":"cdrwa","scheme":"world","id":"anyone"}`. On input,
`perms` may also be a bit mask integer.

**Create modes** `persistent`, `persistent-sequential`, `ephemeral`,
`ephemeral-sequential`, `container`, `persistent-with-ttl`,
`persistent-sequential-with-ttl`.

Ephemeral nodes belong to the bridge's session for that cluster. They vanish
when the cluster is disconnected, the bridge exits, or the session expires.

**ClusterConfig** is passed as is to `zk-core`. Fields (all optional except `connect`):

```json
{
  "name": "prod-eu",
  "connect": "zk1:2181,zk2:2181,zk3:2181/app",
  "chroot": "/sub",
  "session_timeout_ms": 30000,
  "connection_timeout_ms": 5000,
  "operation_timeout_ms": 15000,
  "connect_deadline_ms": 30000,
  "allow_read_only": false,
  "reconnect_on_expiry": true,
  "auth": [{"scheme": "digest", "credential": {"env": "ZK_PROD_DIGEST"}}],
  "sasl": {"mechanism": "digest-md5", "username": "u", "password": {"file": "/run/secrets/zk"}},
  "tls": {"ca_file": "/etc/zk/ca.pem", "cert_file": "...", "key_file": "...", "verify_hostname": true},
  "retry": {"max_attempts": 4, "base_delay_ms": 100, "max_delay_ms": 2000, "max_elapsed_ms": 15000},
  "safety": {"read_only": true, "protected_paths": ["/kafka/**", "/zookeeper/**"], "max_recursive_nodes": 10000},
  "server_version": "3.9.2",
  "admin_hosts": ["zk1:2181"]
}
```

A secret (`credential`, `password`) is one of `{"value": "..."}`, `{"env": "VAR"}`,
`{"file": "/path"}` or `{"command": ["argv0", "arg1"]}`. The bridge resolves
secrets itself. Secrets never appear in any response, notification or log.
Editors should prefer `command` (for example `["pass", "show", "zk/prod"]`) or
`env` over `value`.

## Lifecycle

### `initialize`
params `{"client": "zookeeper.el 0.1", "protocol_version": "1.0"}`
result `{"server": "zk-bridge", "server_version": "0.1.0", "protocol_version": "1.0", "methods": [...], "features": {"tls": true, "sasl_digest_md5": true, "sasl_gssapi": false}}`

If the major versions differ, the bridge answers with error `-32600` and
exits.

### `shutdown`
Closes every cluster (deleting this bridge's ephemerals), then responds `null`.
The process exits after flushing. EOF on stdin has the same effect.

### `$/cancelRequest` (notification)
params `{"id": <request id>}`. The request is aborted and answered with
`-32800`. Cancellation is best effort for writes: a write already sent may
still apply, and the response then reports `outcome-unknown`.

### `$/ping`
result `{"pong": true, "uptime_ms": n}`.

## Clusters

| Method | Params | Result |
| --- | --- | --- |
| `cluster.connect` | `{config, wait?: bool=true}` | `{cluster: id, state}` |
| `cluster.disconnect` | `{cluster}` | `null` |
| `cluster.list` | `{}` | `[{cluster, name, connect, state}]` |
| `cluster.status` | `{cluster}` | `ClientDiagnostics` |
| `cluster.health` | `{cluster, timeout_ms?}` | `[{host, ok, info?, error?}]` |
| `cluster.four-letter` | `{cluster, command, host?}` | `{host, output}` (safe commands only) |
| `cluster.config` | `{cluster}` | dynamic config `/zookeeper/config` as node data |
| `cluster.reconfig` | `{cluster, joining?, leaving?, members?, from_config_version?}` | node data |

`cluster` ids are small strings ("c1", "c2", ...) issued by the bridge.

Notification **`connection.event`**: `{cluster, event: {kind, generation, session_id?, at_ms, message?}, state}`.
`kind` is one of `connected`, `suspended`, `reconnected`, `read-only`, `lost`,
`session-reestablished`, `auth-failed`, `closed`.

## Nodes

All node methods take `cluster` and `path` unless noted. Writes accept
`force: true` to bypass protected-path rules (never `read_only`). Every write
produces an `audit.record` notification.

| Method | Extra params | Result |
| --- | --- | --- |
| `node.get` | `sync?`, `max_bytes?`, `max_text_bytes?` | `{path, stat, data, utf8, text?, truncated, size, ephemeral_owner_hex?}` |
| `node.exists` | | `{path, stat \| null}` |
| `node.children` | `stats?: bool`, `limit?`, `offset?` | `{path, stat, total, offset, children: [{name, path, stat?}], truncated}` |
| `node.create` | `data \| text`, `mode?`, `acl?`, `ttl_ms?`, `create_parents?` | `{path, stat, sequence?}` |
| `node.set` | `data \| text`, `version?` | `{path, stat}` |
| `node.delete` | `version?`, `recursive?`, `dry_run?`, `limit?` | `{paths, dry_run, rescans, blocked?}` |
| `node.sync` | | `null` |
| `node.count` | | `{path, descendants}` |
| `node.acl.get` | | `{path, acl, stat}` |
| `node.acl.set` | `acl`, `version?` (aversion) | `{path, stat}` |
| `node.ephemerals` | | `{paths}` (own session only) |
| `node.transaction` | `ops` | `{results}` |
| `node.multi-read` | `ops` | `{results: [{data...} \| {children} \| {error}]}` |
| `node.walk` | `max_depth?`, `max_nodes?`, `include_system?` | `{nodes: [{path, depth, stat}], truncated, skipped}` |
| `node.export` | `with_acl?`, `with_stat?`, `max_nodes?` | `{root, nodes: [{rel, data, acl?, stat?}], truncated}` |
| `node.import` | `nodes`, `overwrite?`, `keep_acl?` | `{created, updated}` |
| `node.copy` | `to`, `to_cluster?`, `with_acl?`, `max_nodes?` | `{created}` |
| `node.move` | `to`, `allow_non_atomic?`, `max_nodes?` | `{paths}` |
| `node.search` | see below | `{matches, scanned, truncated, partial_reasons}` |
| `node.diff` | see below | `{differences, left_count, right_count, truncated}` |

`node.transaction` ops: `{"op":"create","path","data"?,"mode"?,"acl"?,"ttl_ms"?}`,
`{"op":"set-data","path","data"?,"version"?}`, `{"op":"delete","path","version"?}`,
`{"op":"check","path","version"}`. Here `data` is base64. A failure reports
`op_index`.

**`node.search`** params: `{cluster, path, name?: regex, data?: regex,
data_contains?: string, acl?: "scheme" or "scheme:id", kind?: "ephemeral" | "persistent",
min_data_bytes?, max_nodes?: 100000, max_results?: 1000, max_depth?, include_system?}`.
Each match is `{path, stat, reasons: ["name", "data", "acl", "kind"]}`. The
search is a bounded subtree walk (it can be cancelled). `partial_reasons` lists
why results might be incomplete, for example `node-limit`, `result-limit`, or
`unreadable:3`.

**`node.diff`** params: `{left: {cluster, path}, right: {cluster, path},
data?: true, acl?: false, stat?: false, max_nodes?: 100000}`. Differences:
`{rel, kind: "only-left" | "only-right" | "data" | "acl" | "ephemeral", left?, right?}`.
`left` and `right` hold summaries with base64 data for `data` differences.

## Auth

| Method | Params | Result |
| --- | --- | --- |
| `auth.add` | `{cluster, scheme, credential: Secret}` | `null` (replayed on re-established sessions) |
| `auth.whoami` | `{cluster}` | `[{scheme, user}]` |
| `acl.digest` | `{user, password}` | `{id}` (server-side digest id, computed locally) |

## Watches

`watch.add` params `{cluster, path, kind}`. `kind` is one of:

* `data`: one-shot on data change or delete. The node must exist.
* `exists`: one-shot on create, delete or data change.
* `children`: one-shot on child set change or delete.
* `persistent`: 3.6+. Data, children and existence of the node.
* `recursive`: 3.6+. Data and existence of the node and every descendant.

The result is `{watch: id, stat?, children?}` (the current value at
registration).

One-shot watches fire once, then emit `watch.removed {watch}`. Persistent watches stay
until removed and survive reconnects and session expiry.

Notification **`watch.event`**: `{watch, cluster, event: {kind, path, zxid, at_ms, message?}}`.
Event kinds: `node-created`, `node-deleted`, `node-data-changed`,
`node-children-changed`, `resync` (events may have been missed while
disconnected), `session-lost`, `reregistered`, `closed`.

| Method | Params | Result |
| --- | --- | --- |
| `watch.remove` | `{watch}` | `null` |
| `watch.list` | `{cluster?}` | `[{watch, cluster, path, kind, events}]` |

Watch events are rate limited per watch (default 500 events/s). Excess events
collapse into one `{kind: "overflow", dropped: n}` event.

## Caches

`cache.start` params `{cluster, path, kind: "node" | "children" | "tree", options?: CacheOptions}`.
The result is `{cache: id, status}` once the initial load completes.

| Method | Params | Result |
| --- | --- | --- |
| `cache.get` | `{cache, path?, with_data?: true}` | cached node `{path, stat, has_data, data?, text?, fetched_at_ms}` or null |
| `cache.children` | `{cache, path}` | `[names]` |
| `cache.snapshot` | `{cache, with_data?: false}` | `{nodes: [...], status}` |
| `cache.status` | `{cache}` | `CacheStatus` |
| `cache.stop` | `{cache}` | `null` |

`cache.list {}` returns `[{cache, cluster, kind, path, status}]`.

`CacheOptions`: `{cache_data: true, max_nodes: 100000, max_bytes: 67108864,
max_depth?: n, max_data_per_node: 1048576, include_system: false}`.

`CacheStatus`: `{state, generation, last_sync_ms, nodes, bytes, evictions, truncated}`,
where `state` is one of:

* `initializing`
* `live`: connected, the watch is registered, and the contents were rebuilt
  after the last gap.
* `suspended`: the connection is lost, so the contents may be stale.
* `resyncing`
* `stale`: a rebuild failed and is being retried.
* `closed`

Notification **`cache.event`**: `{cache, event}`. `event` is tagged by `type`:

* `node-added {path, stat}`
* `node-updated {path, stat, old_stat}`
* `node-removed {path}`
* `initialized {nodes}`
* `resyncing {reason}`
* `resynced {changed}`
* `stale {reason}`
* `error {error}`
* `closed`
* `overflow {dropped}`: notifications were dropped, so the client must
  re-snapshot.

## Recipes

Inspection is read-only and safe on production clusters.

| Method | Params | Result |
| --- | --- | --- |
| `recipe.inspect` | `{cluster, path}` | recipe description (structure is detected automatically) |
| `recipe.discovery.services` | `{cluster, base}` | `[names]` |
| `recipe.discovery.instances` | `{cluster, base, service}` | `[ServiceInstance]` |
| `recipe.counter.get` | `{cluster, path, encoding?: "i64" \| "i32" \| "text"}` | `{value, version}` |
| `recipe.counter.add` | `{cluster, path, delta, encoding?}` | `{value, version}` |
| `recipe.queue.put` | `{cluster, path, data \| text, priority?}` | `{path}` |
| `recipe.queue.peek` | `{cluster, path}` | item or null |
| `recipe.queue.take` | `{cluster, path, timeout_ms?}` | item or null |
| `recipe.queue.len` | `{cluster, path}` | `{len}` |

Queue items have the shape `{node, sequence, data (base64), priority?, ready_at_ms?}`.

`recipe.queue.put` also accepts `ready_at_ms`, which creates a delayed item
that `take` only returns after that time. Give `priority` or `ready_at_ms`,
not both.

Counter results are `{value, version}`, where `version` is -1 if the node
does not exist yet.

`recipe.inspect` returns an object tagged by `kind`:

* `lock {contenders, owner}`
* `read-write-lock {contenders, readers_holding, writer_holding}`
* `leader-latch {participants, leader}`
* `semaphore {leases, lock_contenders}`
* `queue {size, ready, head}`
* `locking-queue {size, claimed, head}`
* `service-registry {services}`
* `service {name, instances}`
* `counter {int32, int64, decimal}`
* `unknown {children}`

All recipe writes follow the cluster safety policy. On read-only profiles
every one of them fails with `policy-denied`. Recipe writes also accept
`force`.

Controlled execution of session-bound recipes: the bridge owns the
participant, and the editor holds a handle.

| Method | Params | Result |
| --- | --- | --- |
| `recipe.lock.acquire` | `{cluster, path, timeout_ms?, data?}` | `{recipe: id, node, ownership}` |
| `recipe.latch.start` | `{cluster, path, id?}` | `{recipe: id, participant}` |
| `recipe.member.join` | `{cluster, path, member_id, data? \| text?, timeout_ms?}` | `{recipe: id, ownership}` |
| `recipe.release` | `{recipe}` | `null` |
| `recipe.list` | `{}` | `[{recipe, kind, cluster, path, ownership, node?, leader?}]` |

Notification **`recipe.ownership`**: `{recipe, state: "held" | "suspended" | "lost" | "released", leader?: bool}`.
Losing the cluster connection makes every recipe on it `lost`. Editors must
never assume ownership from a stale handle.

## Audit

Every mutating request produces **`audit.record`**:
`{at_ms, cluster, cluster_id, operation, paths, outcome, error?, forced?}`.
Here `cluster` is the profile name and `cluster_id` is the bridge handle.
`audit.list {limit?: 200}` returns the in-memory ring buffer (last 1000
records). The bridge also appends records as JSON lines to `$ZK_BRIDGE_AUDIT_LOG`
when that variable is set.

## Limits

| Limit | Default | Override |
| --- | --- | --- |
| Max line length (request) | 64 MiB | `--max-request-bytes` |
| Concurrent requests | 256 | `--max-concurrency` |
| Watch events per watch per second | 500 | `--watch-rate` |
| Audit ring buffer | 1000 | - |
