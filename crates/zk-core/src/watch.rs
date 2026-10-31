//! Watches.
//!
//! Two layers:
//!
//! * One-shot watches ([`ZkClient::watch_data`], [`ZkClient::watch_exists`],
//!   [`ZkClient::watch_children`]) return the current value plus a
//!   [`OneshotWatch`] that resolves on the next change.
//! * [`ZkClient::watch_persistent`] returns a [`WatchStream`] of
//!   [`WatchEvent`]s, built on ZooKeeper 3.6+ persistent (optionally recursive)
//!   watches. The stream is session-aware: on reconnect it emits
//!   [`WatchEventKind::Resync`] (events may have been missed, ZOOKEEPER-4698), on
//!   expiry it emits [`WatchEventKind::SessionLost`] and transparently
//!   re-registers on the replacement session.
//!
//! Dropping any watch handle unregisters it locally.

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zookeeper_client as zk;

use crate::error::{ErrorKind, Result, ZkError};
use crate::path;
use crate::session::{now_ms, ConnectionEventKind, ZkClient};
use crate::types::{NodeData, Stat};

/// Event classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WatchEventKind {
    NodeCreated,
    NodeDeleted,
    NodeDataChanged,
    NodeChildrenChanged,
    /// Reconnected after a disconnect: events may have been lost. Consumers
    /// that maintain derived state must re-read.
    Resync,
    /// The session expired. The watch is re-registered on the next session
    /// (for persistent streams); state must be rebuilt from scratch.
    SessionLost,
    /// The watch has been re-registered on a new session.
    Reregistered,
    /// The stream ended (client closed or registration failed permanently).
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchEvent {
    pub kind: WatchEventKind,
    /// Affected path (the watched root for session-level kinds).
    pub path: String,
    /// Transaction id (3.9+ servers; -1 otherwise or for session-level kinds).
    pub zxid: i64,
    pub at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl WatchEvent {
    fn session(kind: WatchEventKind, path: &str, message: Option<String>) -> Self {
        WatchEvent { kind, path: path.to_string(), zxid: -1, at_ms: now_ms(), message }
    }

    fn from_zk(ev: zk::WatchedEvent) -> Option<Self> {
        let kind = match ev.event_type {
            zk::EventType::NodeCreated => WatchEventKind::NodeCreated,
            zk::EventType::NodeDeleted => WatchEventKind::NodeDeleted,
            zk::EventType::NodeDataChanged => WatchEventKind::NodeDataChanged,
            zk::EventType::NodeChildrenChanged => WatchEventKind::NodeChildrenChanged,
            zk::EventType::Session => return None,
        };
        Some(WatchEvent { kind, path: ev.path, zxid: ev.zxid, at_ms: now_ms(), message: None })
    }

    /// Whether this event is a change notification for a node.
    pub fn is_node_event(&self) -> bool {
        matches!(
            self.kind,
            WatchEventKind::NodeCreated
                | WatchEventKind::NodeDeleted
                | WatchEventKind::NodeDataChanged
                | WatchEventKind::NodeChildrenChanged
        )
    }
}

/// A one-shot watch. Resolves once with the triggering event.
#[derive(Debug)]
pub struct OneshotWatch {
    inner: zk::OneshotWatcher,
    path: String,
}

impl OneshotWatch {
    /// Waits for the event. Session expiry resolves with [`WatchEventKind::SessionLost`].
    pub async fn changed(self) -> WatchEvent {
        let path = self.path;
        let ev = self.inner.changed().await;
        match ev.event_type {
            zk::EventType::Session => match ev.session_state {
                zk::SessionState::Closed => WatchEvent::session(WatchEventKind::Closed, &path, None),
                s => WatchEvent::session(WatchEventKind::SessionLost, &path, Some(format!("session {s}"))),
            },
            _ => WatchEvent::from_zk(ev).expect("node event"),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Persistent watch mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PersistentMode {
    /// Data, children and existence of the node itself.
    Node,
    /// Data and existence of the node and all descendants.
    Recursive,
}

/// Stream of events from a persistent watch. Drop to cancel.
pub struct WatchStream {
    rx: mpsc::Receiver<WatchEvent>,
    cancel: CancellationToken,
    path: String,
    mode: PersistentMode,
}

impl std::fmt::Debug for WatchStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchStream").field("path", &self.path).field("mode", &self.mode).finish()
    }
}

impl WatchStream {
    /// Next event, or `None` after the stream closed.
    pub async fn next(&mut self) -> Option<WatchEvent> {
        self.rx.recv().await
    }

    pub fn try_next(&mut self) -> Option<WatchEvent> {
        self.rx.try_recv().ok()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn mode(&self) -> PersistentMode {
        self.mode
    }

    /// Cancels the watch.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl Drop for WatchStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl ZkClient {
    /// Reads data and registers a one-shot data watch (triggered by data change or deletion).
    pub async fn watch_data(&self, path: &str) -> Result<(NodeData, OneshotWatch)> {
        path::validate(path)?;
        let s = self.op_session().await?;
        let (data, stat, w) = tokio::time::timeout(self.inner.config.operation_timeout(), s.client.get_and_watch_data(path))
            .await
            .map_err(|_| ZkError::timeout("watch_data"))?
            .map_err(|e| ZkError::from(e).with_path(path))?;
        Ok((NodeData { data, stat: stat.into() }, OneshotWatch { inner: w, path: path.into() }))
    }

    /// Checks existence and registers a one-shot watch (creation, deletion, data change).
    pub async fn watch_exists(&self, path: &str) -> Result<(Option<Stat>, OneshotWatch)> {
        path::validate(path)?;
        let s = self.op_session().await?;
        let (stat, w) = tokio::time::timeout(self.inner.config.operation_timeout(), s.client.check_and_watch_stat(path))
            .await
            .map_err(|_| ZkError::timeout("watch_exists"))?
            .map_err(|e| ZkError::from(e).with_path(path))?;
        Ok((stat.map(Stat::from), OneshotWatch { inner: w, path: path.into() }))
    }

    /// Lists children and registers a one-shot children watch (child set change or deletion).
    pub async fn watch_children(&self, path: &str) -> Result<(Vec<String>, Stat, OneshotWatch)> {
        path::validate(path)?;
        let s = self.op_session().await?;
        let (mut children, stat, w) =
            tokio::time::timeout(self.inner.config.operation_timeout(), s.client.get_and_watch_children(path))
                .await
                .map_err(|_| ZkError::timeout("watch_children"))?
                .map_err(|e| ZkError::from(e).with_path(path))?;
        children.sort();
        Ok((children, stat.into(), OneshotWatch { inner: w, path: path.into() }))
    }

    /// Registers a persistent (3.6+) watch. `path` need not exist.
    ///
    /// The returned stream survives disconnects and session expiry; see the
    /// module docs for the session-level events it emits.
    pub async fn watch_persistent(&self, path: &str, mode: PersistentMode, buffer: usize) -> Result<WatchStream> {
        path::validate(path)?;
        let zmode = match mode {
            PersistentMode::Node => zk::AddWatchMode::Persistent,
            PersistentMode::Recursive => zk::AddWatchMode::PersistentRecursive,
        };
        // Register synchronously first so that errors (e.g. Unimplemented on old servers) surface.
        let session = self.op_session().await?;
        let watcher = tokio::time::timeout(self.inner.config.operation_timeout(), session.client.watch(path, zmode))
            .await
            .map_err(|_| ZkError::timeout("watch_persistent"))?
            .map_err(|e| ZkError::from(e).with_path(path))?;
        let (tx, rx) = mpsc::channel(buffer.max(16));
        let cancel = CancellationToken::new();
        let client = self.clone();
        let p = path.to_string();
        let c = cancel.clone();
        tokio::spawn(async move {
            persistent_loop(client, p, zmode, session.generation, watcher, tx, c).await;
        });
        Ok(WatchStream { rx, cancel, path: path.into(), mode })
    }
}

async fn persistent_loop(
    client: ZkClient,
    path: String,
    mode: zk::AddWatchMode,
    mut generation: u64,
    mut watcher: zk::PersistentWatcher,
    tx: mpsc::Sender<WatchEvent>,
    cancel: CancellationToken,
) {
    let mut events = client.subscribe_events();
    loop {
        // Serve events of the current registration until the session ends.
        let lost = loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tx.closed() => return,
                ev = watcher.changed() => {
                    if ev.event_type == zk::EventType::Session {
                        match ev.session_state {
                            zk::SessionState::Disconnected => {}
                            zk::SessionState::SyncConnected | zk::SessionState::ConnectedReadOnly => {
                                if tx.send(WatchEvent::session(WatchEventKind::Resync, &path, Some("reconnected; events may have been missed".into()))).await.is_err() { return; }
                            }
                            zk::SessionState::Closed => {
                                let _ = tx.send(WatchEvent::session(WatchEventKind::Closed, &path, None)).await;
                                return;
                            }
                            s if s.is_terminated() => break format!("session {s}"),
                            _ => {}
                        }
                        continue;
                    }
                    if let Some(e) = WatchEvent::from_zk(ev) {
                        if tx.send(e).await.is_err() { return; }
                    }
                }
            }
        };
        // PersistentWatcher panics if polled after a terminal state; drop it.
        drop(watcher);
        if tx.send(WatchEvent::session(WatchEventKind::SessionLost, &path, Some(lost))).await.is_err() {
            return;
        }
        // Wait for a replacement session and re-register.
        loop {
            if cancel.is_cancelled() || client.is_closed() {
                let _ = tx.send(WatchEvent::session(WatchEventKind::Closed, &path, Some("client closed".into()))).await;
                return;
            }
            let session = tokio::select! {
                _ = cancel.cancelled() => return,
                s = client.session(client.config().session_timeout() * 4) => s,
            };
            match session {
                Ok(s) if s.generation > generation => {
                    match s.client.watch(&path, mode).await {
                        Ok(w) => {
                            generation = s.generation;
                            watcher = w;
                            if tx
                                .send(WatchEvent::session(WatchEventKind::Reregistered, &path, Some(format!("generation {generation}"))))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            break;
                        },
                        Err(e) => {
                            tracing::warn!(%path, error = %e, "re-registering persistent watch failed");
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        },
                    }
                },
                Ok(_) => {
                    // Still the old session object; wait for an event.
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        e = events.recv() => {
                            if let Ok(e) = e { if e.kind == ConnectionEventKind::Closed { continue; } }
                        }
                        _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
                    }
                },
                Err(e) if e.kind == ErrorKind::Closed || e.kind == ErrorKind::AuthFailed => {
                    let _ = tx.send(WatchEvent::session(WatchEventKind::Closed, &path, Some(e.to_string()))).await;
                    return;
                },
                Err(_) => {},
            }
        }
    }
}
