# Recipes (`zk-recipes`)

Coordination recipes in the style of Curator and Kazoo, built on `zk-core`.
Every recipe is a library type with rustdoc that explains its semantics and
its differences from Curator and Kazoo. This page covers what all of them
have in common.

## The ownership rule

A recipe that holds something holds it **through one ZooKeeper session**.
That covers locks, leadership, semaphore leases, queue claims, memberships,
registrations and partition claims. Session-bound operations go through
`zk_recipes::pinned`, so they never quietly switch to a newer session.

Each such recipe exposes an `Ownership`:

```rust
let guard = Mutex::new(&client, "/locks/report")?.acquire(Some(Duration::from_secs(10))).await?;
tokio::select! {
    r = do_protected_work() => r?,
    s = guard.ownership().lost() => bail!("lock lost ({s:?}); aborting"),
}
guard.release().await?;
```

| State | Meaning | What callers must do |
| --- | --- | --- |
| `Held` | The session is connected, so ownership is valid | Work |
| `Suspended` | The connection is lost, but the session may survive | Pause protected work. It may come back to `Held` |
| `Lost` | The session expired or the client closed | Stop. Another process may already be owner |
| `Released` | You released it | Stop |

`SuspendPolicy::LoseOnSuspend` moves straight from `Suspended` to `Lost`.
Choose it when the protected resource cannot tolerate two owners even
briefly. A disconnected owner then gives up before the server can expire it.

## Interoperability

| Recipe | Node layout | Interoperates with |
| --- | --- | --- |
| `Mutex` | `{base}/_c_{uuid}-lock-{seq}` | Curator `InterProcessMutex` |
| `ReadWriteLock` | `_c_{uuid}-__READ__{seq}` and `_c_{uuid}-__WRIT__{seq}` | Curator `InterProcessReadWriteLock` |
| `LeaderLatch` | `_c_{uuid}-latch-{seq}` | Curator `LeaderLatch` |
| `Semaphore` | `{base}/locks`, `{base}/leases` | Curator `InterProcessSemaphoreV2` |
| `ServiceDiscovery` | `{base}/{service}/{id}`, holding Curator JSON | `curator-x-discovery` |
| `SharedCounter` | `Int32Be`, `Int64Be` or decimal text | Curator `SharedCount` (`Int32Be`) and `DistributedAtomicLong` (`Int64Be`) |
| `DistributedQueue` | `qn-{seq}` | Kazoo/Curator queue naming. Payloads are raw bytes |

## Fault tolerance evidence

`crates/zk-recipes/tests/faults.rs` puts the owner behind a `FaultProxy`
and partitions it until its session expires. Each test then checks that:

1. the owner's `Ownership` becomes `Lost`,
2. another contender acquires,
3. two `Held` owners never exist at the same moment (a background monitor
   samples every 5 ms), and
4. self-healing recipes (`PersistentNode`, `GroupMember`, discovery
   registrations) re-create their nodes on the new session.

## Known differences and limitations

* Reentrancy is per `ReentrantMutex` instance. Curator tracks it per thread.
* `LeaderElection::run` does not interrupt the closure when leadership is
  lost. The closure must watch the `Ownership` it receives.
* There is no lock revocation (`Revocable`) and no child reaper.
* The semaphore's maximum number of leases is fixed when it is created.
* `PersistentNode` supports only ephemeral modes. There is no TTL variant.
* `ServiceCache` and `PartitionAssigner` poll every 0.5 to 1 s in addition to
  using watches, so that they also notice data-only changes.
