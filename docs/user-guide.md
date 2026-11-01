# zookeeper.el user guide

## Installation

1. Build or download `zk-bridge`. Releases ship prebuilt binaries for Linux and
   macOS. To build it yourself, run `cargo build --release -p zk-bridge`.
2. Put `lisp/` on your `load-path` and `(require 'zookeeper)`.
3. Tell Emacs where the bridge is. The search order is:

   1. `zookeeper-bridge-executable`, if set.
   2. `zk-bridge` on `exec-path`.
   3. `bin/zk-bridge` inside the package directory.
   4. `target/release/zk-bridge` or `target/debug/zk-bridge` in a source
      checkout.

To run the bridge on a remote host over TRAMP, set `zookeeper-bridge-remote`
to `t` (use the remote `default-directory`) or to a TRAMP prefix such as
`"/ssh:bastion:"`. The process starts with `start-file-process`, so the
remote host must have `zk-bridge` installed.

## Cluster profiles

```elisp
(setq zookeeper-clusters
      '(("local" :connect "127.0.0.1:2181")
        ("staging" :connect "zk-s1:2181,zk-s2:2181/app"
                   :session-timeout-ms 30000
                   :auth ((:scheme "digest" :credential (:auth-source (:host "zk-staging")))))
        ("prod" :connect "zk1:2181,zk2:2181,zk3:2181"
                :read-only t
                :protected-paths ("/kafka/**" "/zookeeper/**" "/services/*/config")
                :max-recursive-nodes 2000
                :tls (:ca-file "~/.zk/ca.pem" :cert-file "~/.zk/me.pem" :key-file "~/.zk/me.key")
                :auth ((:scheme "digest" :credential (:command ("pass" "show" "zk/prod")))))))
```

| Key | Meaning |
| --- | --- |
| `:connect` | `host:port[,host:port...][/chroot]`. Per-host `tcp://` and `tcp+tls://` prefixes are allowed. |
| `:chroot` | Extra chroot, appended after any chroot in `:connect`. |
| `:read-only` | Refuse every write. The bridge enforces this too, and `force` cannot override it. Defaults to `zookeeper-default-read-only`. |
| `:protected-paths` | Glob patterns. `/a/**` matches a node and all its descendants, and `*` matches within one path component. Writing to a protected path needs typed confirmation, and the write is sent with `force`, which is audited. |
| `:max-recursive-nodes` | Upper limit for recursive delete and copy. |
| `:session-timeout-ms`, `:connection-timeout-ms`, `:operation-timeout-ms`, `:connect-deadline-ms` | Timeouts. |
| `:allow-read-only` | Accept connections to read-only (partitioned) servers. |
| `:reconnect-on-expiry` | Re-establish a new session after expiry. Default `t`. |
| `:auth` | A list of `(:scheme "digest" :credential SECRET)`. Credentials are replayed on every new session. |
| `:sasl` | `(:mechanism "digest-md5" :username "u" :password SECRET)`. |
| `:tls` | `(:ca-file F :cert-file F :key-file F :verify-hostname t)`. |
| `:retry` | `(:max-attempts 4 :base-delay-ms 100 :max-delay-ms 2000 :max-elapsed-ms 15000)`. Applies to reads only. Writes are never retried. |
| `:server-version` | For example `"3.5.10"`. Features the server cannot handle are refused locally instead of being sent. |
| `:admin-hosts` | Hosts used for four-letter-word diagnostics. Defaults to the hosts in `:connect`. |

A SECRET is one of the following. Prefer `:command`, `:env` or `:auth-source`.

* `(:command ("pass" "show" "zk/prod"))`: run without a shell.
* `(:env "VAR")`: read from the bridge's environment.
* `(:file "/run/secrets/zk")`
* `(:auth-source (:host "zk-prod"))`: resolved in Emacs, only at connect
  time.
* `(:value "user:pw")`: kept in your configuration as plain text.

The bridge never echoes secrets in logs, errors or responses.

## Commands

`M-x zookeeper` picks a cluster and opens its tree browser. `?` in any
zookeeper buffer (or `M-x zookeeper-dispatch`) opens the main menu.

### Tree browser

| Key | Command |
| --- | --- |
| `TAB`, `o` | Expand or collapse the node (children load lazily, in pages of `zookeeper-browser-page-size`) |
| `<backtab>` | Collapse all |
| `RET`, `v` | Open the node inspector |
| `^` | Go to the parent |
| `j` | Go to a path, with completion over children |
| `g` | Refresh |
| `m` / `u` / `U` | Mark / unmark / unmark all |
| `*` | Mark by regexp |
| `d` then `x` | Flag for deletion, then execute |
| `c` | Create a node (choose the mode: persistent, ephemeral, sequential, container, TTL) |
| `e` | Edit node data |
| `D` | Delete. Recursive deletes first show a dry-run listing of every path |
| `C` / `R` | Copy / move (rename) a subtree |
| `s` | Search the subtree (name regexp, data, ACL, kind) |
| `=` | Diff two nodes or subtrees, possibly on different clusters |
| `a` | ACL editor |
| `r` | Inspect the recipe structure at point (lock, election, queue, ...) |
| `W` | Add a watch on the node at point |
| `w` | Copy the path |

The header line shows the cluster state (connected, suspended, lost) and how
fresh the view is. When caches back the browser (ZooKeeper 3.6 or newer), the
header shows `live`, `suspended`, `resyncing` or `stale`. In any state other
than `live`, the view may be out of date.

Bookmarks (`C-x r m`) work in browser buffers. `M-x zookeeper-visit-recent`
reopens recent nodes.

### Node inspector and editor

| Key | Command |
| --- | --- |
| `C-c C-c` | Save. The save is conditional on the version that was read |
| `C-c C-k` | Revert (refetch) |
| `C-c C-r` | Reload from the server |
| `C-c C-f` | Toggle `zookeeper-node-follow-mode`, which updates the buffer live from a node cache |
| `C-c C-v` | Cycle the data view |
| `C-c C-t` / `C-c C-j` / `C-c C-x` / `C-c C-b` | Text, JSON, hex or base64 view |
| `C-c C-a` | ACL editor |
| `C-c C-w` | Copy the path |

If someone else changed the node since you read it, the save fails with
`bad-version` and a conflict buffer opens. You can overwrite with the new
version, reload, or ediff your text against the server's. If the connection
dropped after the write was sent, you get `outcome-unknown`: the write may or
may not have applied, so reload before retrying.

The JSON view is validated when you save. Follow mode never overwrites
unsaved local edits. When the server changes under you, the header line says
so instead.

### ACL editor

`+` or `i` adds an entry, `k` removes one, and `e` edits one. The letters
`c`, `d`, `r`, `w` and `a` toggle permissions on the entry at point. `D` adds
a digest entry from a user name and password. The digest is computed locally
and the password is not stored. `C-c C-c` saves, checking the ACL version.
`g` reloads.

### Events and watches

`M-x zookeeper-events` opens a live log of watch, connection, cache and audit
events. It is labelled *locally observed*: it shows what this Emacs saw, and
is not an authoritative history.

In the log, `/` filters, `f` toggles follow, `C` clears, `+` and `-` add and
remove watches, and `L` lists them.

| Watch kind | Fires on | Version |
| --- | --- | --- |
| `data` | Data change or delete. One shot | any |
| `exists` | Create, delete or data change. One shot | any |
| `children` | Child list change or delete. One shot | any |
| `persistent` | Changes to the node, until removed | 3.6+ |
| `recursive` | Changes to the node and every descendant, until removed | 3.6+ |

After a reconnect a persistent watch reports `resync`, because events may
have been missed. After session expiry it reports `session-lost`, then
`reregistered`.

### Recipes

`M-x zookeeper-recipe-inspect` describes the coordination structure at a
path. It is read-only and safe on production. It covers:

* lock contenders, in order, and the current holder
* latch and election participants, and the leader
* semaphore leases
* queue sizes
* service discovery instances

`M-x zookeeper-discovery-browse` lists registered services. `M-x
zookeeper-counter-view` shows counters.

The recipes buffer (`M-x zookeeper-recipes`) can take part in a lock, a
leader latch or a group membership:

| Key | Command |
| --- | --- |
| `l` | Acquire a lock |
| `L` | Start a leader latch |
| `j` | Join a group |
| `k` | Release |

Each participant is owned by the bridge's session. Its ownership state is
shown as `held`, `suspended`, `lost` or `released`. Ownership is lost when
the session is lost, including when Emacs or the bridge exits. Never treat a
stale handle as proof of ownership.

### Diagnostics

| Command | Shows |
| --- | --- |
| `M-x zookeeper-cluster-health` | `ruok` and `srvr` for each server |
| `M-x zookeeper-four-letter` | Read-only four-letter words. `srst`, `crst` and `stmk` are refused |
| `M-x zookeeper-cluster-status` | Session, state, counters and last error |
| `M-x zookeeper-audit-log` | Every mutation made in this bridge, including denied and forced ones |
| `M-x zookeeper-bridge-show-log` | The bridge's stderr. Set its level with `zookeeper-bridge-log-level` |
| `M-x zookeeper-bridge-restart` | Restarts the bridge |

`M-x zookeeper-list-connections` lists connections: `RET` browses, `c`
connects, `d` disconnects, and `i` shows diagnostics.

## Safety model

* Read-only profiles cannot write at all. No override exists.
* Protected paths need typed confirmation, and the write is audited as
  forced.
* Recursive deletes always show a dry run first, along with any paths policy
  would block. Large or protected deletes need you to type `yes`.
* Writes are conditional on the version you read, wherever the UI knows that
  version.
* Writes are never retried automatically. You get `outcome-unknown` instead.
* Every mutation produces an audit record. Set `ZK_BRIDGE_AUDIT_LOG` in the
  bridge's environment to keep a JSONL file of them.
