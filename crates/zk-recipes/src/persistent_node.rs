//! [`PersistentNode`]: an ephemeral node kept present across session loss.
//!
//! Curator `PersistentNode` semantics: a background task creates the node,
//! watches it through the creating session and re-creates it if it is deleted
//! or when the session expires and the client establishes a new one. The
//! current incarnation is reported through [`Ownership`]: `Held` while our
//! node exists on a live session, `Lost` between expiry and re-creation.
//!
//! Modes ([`PersistentNodeMode`]):
//!
//! * `Ephemeral`: fixed path. If another session owns a node at that path,
//!   we wait for it to disappear (no stealing).
//! * `ProtectedEphemeralSequential`: `{parent}/_c_{uuid}-{name}{seq}`; the
//!   actual path changes on each incarnation ([`PersistentNode::actual_path`]).
//!
//! Differences vs Curator: only ephemeral modes are supported (persistent
//! nodes do not need a supervisor); TTL modes are not supported.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use zk_core::{path as zpath, raw as zk, Acl, ErrorKind, Result, ZkClient, ZkError};

use crate::ownership::{Ownership, SuspendPolicy};
use crate::pinned::Pinned;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PersistentNodeMode {
    Ephemeral,
    ProtectedEphemeralSequential,
}

struct State {
    data: Vec<u8>,
    current: Option<(Pinned, String)>,
}

struct Inner {
    client: ZkClient,
    path: String,
    mode: PersistentNodeMode,
    uuid: String,
    policy: SuspendPolicy,
    state: StdMutex<State>,
    ownership: watch::Sender<Option<Ownership>>,
    incarnations: watch::Sender<u64>,
    cancel: CancellationToken,
    poke: Notify,
}

/// A supervised ephemeral node. Cloning shares the same node.
#[derive(Clone)]
pub struct PersistentNode {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for PersistentNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentNode").field("path", &self.inner.path).field("actual", &self.actual_path()).finish()
    }
}

impl PersistentNode {
    /// Starts supervising a node at `path` with `data`.
    pub fn start(client: &ZkClient, path: &str, mode: PersistentNodeMode, data: &[u8]) -> Result<PersistentNode> {
        Self::start_with_policy(client, path, mode, data, SuspendPolicy::default())
    }

    pub fn start_with_policy(client: &ZkClient, path: &str, mode: PersistentNodeMode, data: &[u8], policy: SuspendPolicy) -> Result<PersistentNode> {
        zpath::validate(path)?;
        if path == "/" || zpath::parent(path).is_none() {
            return Err(ZkError::bad_arguments("persistent node path must have a parent"));
        }
        let (otx, _) = watch::channel(None);
        let (itx, _) = watch::channel(0);
        let inner = Arc::new(Inner {
            client: client.clone(),
            path: path.to_string(),
            mode,
            uuid: uuid::Uuid::new_v4().to_string(),
            policy,
            state: StdMutex::new(State { data: data.to_vec(), current: None }),
            ownership: otx,
            incarnations: itx,
            cancel: CancellationToken::new(),
            poke: Notify::new(),
        });
        let i = inner.clone();
        tokio::spawn(async move { supervise(i).await });
        Ok(PersistentNode { inner })
    }

    /// Requested path (for sequential modes, the prefix).
    pub fn path(&self) -> &str {
        &self.inner.path
    }

    /// Actual path of the current incarnation.
    pub fn actual_path(&self) -> Option<String> {
        self.inner.state.lock().unwrap().current.as_ref().map(|(_, p)| p.clone())
    }

    /// Ownership of the current incarnation, if created.
    pub fn ownership(&self) -> Option<Ownership> {
        self.inner.ownership.borrow().clone()
    }

    /// Subscribes to incarnation changes.
    pub fn subscribe(&self) -> watch::Receiver<Option<Ownership>> {
        self.inner.ownership.subscribe()
    }

    /// Number of times the node has been (re-)created.
    pub fn incarnations(&self) -> u64 {
        *self.inner.incarnations.borrow()
    }

    /// Waits until the node exists on a live session (`Held`).
    pub async fn wait_for_created(&self, timeout: Duration) -> Result<Ownership> {
        let mut rx = self.inner.ownership.subscribe();
        let fut = async {
            loop {
                if let Some(o) = rx.borrow_and_update().clone() {
                    if o.is_held() {
                        return Ok(o);
                    }
                }
                tokio::select! {
                    r = rx.changed() => if r.is_err() { return Err(ZkError::new(ErrorKind::Closed, "persistent node closed")) },
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
                if self.inner.cancel.is_cancelled() {
                    return Err(ZkError::new(ErrorKind::Closed, "persistent node closed"));
                }
            }
        };
        tokio::time::timeout(timeout, fut).await.map_err(|_| ZkError::timeout("persistent node creation"))?
    }

    /// Current data.
    pub fn data(&self) -> Vec<u8> {
        self.inner.state.lock().unwrap().data.clone()
    }

    /// Updates the data, writing it to the current incarnation (if any) and
    /// using it for future incarnations.
    pub async fn set_data(&self, data: &[u8]) -> Result<()> {
        let current = {
            let mut st = self.inner.state.lock().unwrap();
            st.data = data.to_vec();
            st.current.clone()
        };
        if let Some((pinned, path)) = current {
            match pinned.set_data(&path, data, None).await {
                Ok(_) => {}
                // Being re-created: the supervisor writes the new data.
                Err(e) if e.kind.is_session_terminal() || e.is(ErrorKind::NoNode) => self.inner.poke.notify_one(),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Stops supervising and deletes the node.
    pub async fn close(&self) -> Result<()> {
        self.inner.cancel.cancel();
        self.inner.poke.notify_one();
        let current = self.inner.state.lock().unwrap().current.take();
        if let Some(o) = self.inner.ownership.send_replace(None) {
            o.mark_released();
        }
        if let Some((pinned, path)) = current {
            match pinned.delete_if_exists(&path).await {
                Err(e) if !e.kind.is_session_terminal() => return Err(e),
                _ => {}
            }
        }
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn create_once(inner: &Inner, pinned: &Pinned) -> Result<Option<String>> {
    let data = inner.state.lock().unwrap().data.clone();
    let parent = zpath::parent(&inner.path).unwrap_or("/").to_string();
    match inner.mode {
        PersistentNodeMode::Ephemeral => loop {
            match pinned.create(&inner.path, &data, zk::CreateMode::Ephemeral).await {
                Ok((p, _)) => return Ok(Some(p)),
                Err(e) if e.is(ErrorKind::NoNode) => inner.client.ensure_path(&parent, &Acl::anyone_all()).await?,
                Err(e) if e.is(ErrorKind::NodeExists) || e.is(ErrorKind::OutcomeUnknown) => {
                    let (stat, w) = pinned.watch_exists(&inner.path).await?;
                    match stat {
                        Some(st) if st.ephemeral_owner == pinned.session().id() => {
                            pinned.set_data(&inner.path, &data, None).await?;
                            return Ok(Some(inner.path.clone()));
                        }
                        Some(_) => {
                            // Someone else's node: wait for it to go away.
                            tokio::select! {
                                r = pinned.wait_watch(w, None) => { r?; }
                                _ = inner.cancel.cancelled() => return Ok(None),
                            }
                        }
                        None => {}
                    }
                }
                Err(e) => return Err(e),
            }
        },
        PersistentNodeMode::ProtectedEphemeralSequential => {
            let name = inner.path.rsplit('/').next().unwrap_or_default();
            // Reuse a node this session already owns (e.g. after LoseOnSuspend).
            if let Some((p, st)) = pinned.find_protected(&parent, &inner.uuid).await.unwrap_or(None) {
                if st.ephemeral_owner == pinned.session().id() {
                    pinned.set_data(&p, &data, None).await?;
                    return Ok(Some(p));
                }
            }
            let (p, _) = pinned.create_protected(&parent, name, &data, zk::CreateMode::EphemeralSequential, &inner.uuid).await?;
            Ok(Some(p))
        }
    }
}

async fn supervise(inner: Arc<Inner>) {
    let cancel = inner.cancel.clone();
    'outer: while !cancel.is_cancelled() {
        let pinned = tokio::select! {
            _ = cancel.cancelled() => break,
            p = Pinned::current(&inner.client) => match p {
                Ok(p) => p,
                Err(e) if e.is(ErrorKind::Closed) || e.is(ErrorKind::AuthFailed) => break,
                Err(_) => continue,
            },
        };
        let mut ownership: Option<Ownership> = None;
        loop {
            if cancel.is_cancelled() {
                break 'outer;
            }
            let path = match create_once(&inner, &pinned).await {
                Ok(Some(p)) => p,
                Ok(None) => break 'outer,
                Err(e) => {
                    tracing::debug!(path = %inner.path, error = %e, "persistent node: create failed");
                    if !pinned.is_alive() {
                        break;
                    }
                    tokio::select! {
                        _ = cancel.cancelled() => break 'outer,
                        _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
                    }
                }
            };
            if cancel.is_cancelled() {
                let _ = pinned.delete_if_exists(&path).await;
                break 'outer;
            }
            inner.state.lock().unwrap().current = Some((pinned.clone(), path.clone()));
            let own = match &ownership {
                Some(o) if !o.state().is_terminal() => o.clone(),
                _ => {
                    let o = Ownership::track(&inner.client, pinned.session().clone(), inner.policy);
                    ownership = Some(o.clone());
                    inner.ownership.send_replace(Some(o.clone()));
                    o
                }
            };
            inner.incarnations.send_modify(|n| *n += 1);
            // Watch the node until it is deleted or the session dies.
            loop {
                let (stat, w) = match pinned.watch_exists(&path).await {
                    Ok(v) => v,
                    Err(_) if !pinned.is_alive() => break,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                };
                if stat.is_none() {
                    break;
                }
                tokio::select! {
                    _ = cancel.cancelled() => break 'outer,
                    _ = inner.poke.notified() => {
                        // Data changed while the node was unreachable: rewrite it.
                        let data = inner.state.lock().unwrap().data.clone();
                        let _ = pinned.set_data(&path, &data, None).await;
                    }
                    r = pinned.wait_watch(w, None) => if r.is_err() { break },
                    _ = own.lost() => break,
                }
            }
            inner.state.lock().unwrap().current = None;
            if !pinned.is_alive() || own.state().is_terminal() {
                own.mark_lost();
                break;
            }
            // Deleted by someone else: re-create on the same session.
        }
    }
    if let Some(o) = inner.ownership.send_replace(None) {
        o.mark_released();
    }
}
