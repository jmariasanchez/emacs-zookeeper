//! Operations pinned to one ZooKeeper session.
//!
//! [`zk_core::ZkClient`] operations transparently move to a replacement
//! session after expiry. That is right for plain reads and persistent writes
//! but wrong for recipes: an ephemeral node created on behalf of a lock must
//! be created, watched and deleted through *the* session the lock is bound
//! to. [`Pinned`] wraps a [`Session`] and issues requests through its
//! underlying client ([`Session::raw`]), so once that session is gone every
//! operation fails with a session-terminal error instead of silently acting
//! on a new session.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use zk_core::{raw as zk, Acl, ErrorKind, NodeData, Result, Session, Stat, ZkClient, ZkError};

use crate::common::lost;

/// A client plus a pinned session.
#[derive(Clone, Debug)]
pub struct Pinned {
    client: ZkClient,
    session: Session,
    timeout: Duration,
}

impl Pinned {
    /// Pins the client's current session (waiting up to the operation timeout for one).
    pub async fn current(client: &ZkClient) -> Result<Pinned> {
        let timeout = client.config().operation_timeout();
        let session = client.session(timeout).await?;
        Ok(Pinned { client: client.clone(), session, timeout })
    }

    pub fn client(&self) -> &ZkClient {
        &self.client
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn is_alive(&self) -> bool {
        self.session.is_alive()
    }

    async fn run<T>(&self, what: &str, path: &str, write: bool, f: impl Future<Output = std::result::Result<T, zk::Error>>) -> Result<T> {
        if write {
            self.client.check_write_allowed(path)?;
        }
        let r = match tokio::time::timeout(self.timeout, f).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) => ZkError::from(e).with_path(path),
            Err(_) => ZkError::timeout(what).with_path(path),
        };
        Err(if write { r.into_write_outcome() } else { r })
    }

    /// Creates a node; returns the actual path (with sequence suffix) and stat.
    pub async fn create(&self, path: &str, data: &[u8], mode: zk::CreateMode) -> Result<(String, Stat)> {
        let opts = mode.with_acls(zk::Acls::anyone_all());
        let (stat, seq) = self.run("create", path, true, self.session.raw().create(path, data, &opts)).await?;
        let actual = match mode {
            zk::CreateMode::PersistentSequential | zk::CreateMode::EphemeralSequential => format!("{path}{seq}"),
            _ => path.to_string(),
        };
        Ok((actual, stat.into()))
    }

    /// Creates a Curator-style *protected* node `{dir}/_c_{uuid}-{name}`.
    ///
    /// When the create outcome is unknown (connection lost after sending), the
    /// node is searched for by its uuid prefix before retrying, so at most one
    /// node is ever created per call (Curator's `withProtection()`).
    pub async fn create_protected(&self, dir: &str, name: &str, data: &[u8], mode: zk::CreateMode, uuid: &str) -> Result<(String, Stat)> {
        let full = format!("{dir}/_c_{uuid}-{name}");
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.create(&full, data, mode).await {
                Ok(r) => return Ok(r),
                Err(e) if e.is(ErrorKind::NoNode) && attempt < 5 => {
                    self.client.ensure_path(dir, &Acl::anyone_all()).await?;
                    continue;
                }
                Err(e) if matches!(e.kind, ErrorKind::OutcomeUnknown | ErrorKind::ConnectionLoss | ErrorKind::NotConnected) => {
                    if !self.is_alive() {
                        return Err(lost("protected create"));
                    }
                    if attempt > 20 {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(100 * attempt.min(10) as u64)).await;
                    match self.find_protected(dir, uuid).await {
                        Ok(Some(found)) => return Ok(found),
                        Ok(None) => continue,
                        Err(e) if e.kind.is_session_terminal() => return Err(e),
                        Err(_) => continue,
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Finds a protected node created with `uuid` under `dir`.
    pub async fn find_protected(&self, dir: &str, uuid: &str) -> Result<Option<(String, Stat)>> {
        let prefix = format!("_c_{uuid}-");
        for name in self.children(dir).await? {
            if name.starts_with(&prefix) {
                let p = format!("{dir}/{name}");
                if let Some(st) = self.exists(&p).await? {
                    return Ok(Some((p, st)));
                }
            }
        }
        Ok(None)
    }

    pub async fn delete(&self, path: &str, version: Option<i32>) -> Result<()> {
        self.run("delete", path, true, self.session.raw().delete(path, version)).await
    }

    /// Deletes, treating `NoNode` as success.
    pub async fn delete_if_exists(&self, path: &str) -> Result<()> {
        match self.delete(path, None).await {
            Err(e) if e.is(ErrorKind::NoNode) => Ok(()),
            r => r,
        }
    }

    pub async fn set_data(&self, path: &str, data: &[u8], version: Option<i32>) -> Result<Stat> {
        self.run("set_data", path, true, self.session.raw().set_data(path, data, version)).await.map(Stat::from)
    }

    pub async fn get_data(&self, path: &str) -> Result<NodeData> {
        let (data, stat) = self.run("get_data", path, false, self.session.raw().get_data(path)).await?;
        Ok(NodeData { data, stat: stat.into() })
    }

    pub async fn exists(&self, path: &str) -> Result<Option<Stat>> {
        self.run("exists", path, false, self.session.raw().check_stat(path)).await.map(|o| o.map(Stat::from))
    }

    pub async fn children(&self, path: &str) -> Result<Vec<String>> {
        let mut v = self.run("children", path, false, self.session.raw().list_children(path)).await?;
        v.sort();
        Ok(v)
    }

    pub async fn watch_exists(&self, path: &str) -> Result<(Option<Stat>, zk::OneshotWatcher)> {
        let (st, w) = self.run("watch_exists", path, false, self.session.raw().check_and_watch_stat(path)).await?;
        Ok((st.map(Stat::from), w))
    }

    pub async fn watch_children(&self, path: &str) -> Result<(Vec<String>, zk::OneshotWatcher)> {
        let (mut v, w) = self.run("watch_children", path, false, self.session.raw().list_and_watch_children(path)).await?;
        v.sort();
        Ok((v, w))
    }

    /// Atomically deletes all `paths` (multi-op) through this session.
    pub async fn delete_all(&self, paths: &[&str]) -> Result<()> {
        let first = paths.first().copied().unwrap_or("/");
        let mut w = self.session.raw().new_multi_writer();
        for p in paths {
            self.client.check_write_allowed(p)?;
            w.add_delete(p, None).map_err(ZkError::from)?;
        }
        self.run("multi", first, true, async move { w.commit().await.map(|_| ()).map_err(zk::Error::from) }).await
    }

    /// Waits for a one-shot watch. Returns `Ok(false)` if `deadline` passed
    /// first, `Err` if the session terminated.
    pub async fn wait_watch(&self, w: zk::OneshotWatcher, deadline: Option<Instant>) -> Result<bool> {
        let handle = |ev: zk::WatchedEvent| {
            if ev.event_type == zk::EventType::Session && ev.session_state.is_terminated() {
                Err(lost("watch"))
            } else {
                Ok(true)
            }
        };
        match deadline {
            Some(d) => tokio::select! {
                ev = w.changed() => handle(ev),
                _ = tokio::time::sleep_until(d) => Ok(false),
            },
            None => handle(w.changed().await),
        }
    }

    /// Deletes `path` in the background (best effort, used from `Drop`).
    pub fn spawn_delete(&self, path: String) {
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            let p = self.clone();
            h.spawn(async move {
                for _ in 0..3 {
                    match p.delete_if_exists(&path).await {
                        Ok(()) => return,
                        Err(e) if e.kind.is_session_terminal() => return,
                        Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                    }
                }
            });
        }
    }

    /// Deletes the protected node with `uuid` under `dir` in the background.
    pub fn spawn_delete_protected(&self, dir: String, uuid: String) {
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            let p = self.clone();
            h.spawn(async move {
                for _ in 0..5 {
                    match p.find_protected(&dir, &uuid).await {
                        Ok(Some((path, _))) => {
                            let _ = p.delete_if_exists(&path).await;
                            return;
                        }
                        Ok(None) => tokio::time::sleep(Duration::from_millis(300)).await,
                        Err(e) if e.kind.is_session_terminal() => return,
                        Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
                    }
                }
            });
        }
    }
}

/// Deletes a contender node when an acquisition is abandoned (error,
/// timeout or the future being dropped) unless disarmed.
pub(crate) struct ContenderCleanup {
    pub pinned: Pinned,
    pub dir: String,
    pub uuid: String,
    pub path: Option<String>,
    pub armed: bool,
}

impl Drop for ContenderCleanup {
    fn drop(&mut self) {
        if !self.armed || !self.pinned.is_alive() {
            return;
        }
        match self.path.take() {
            Some(p) => self.pinned.spawn_delete(p),
            None => self.pinned.spawn_delete_protected(self.dir.clone(), self.uuid.clone()),
        }
    }
}
