//! Ownership guards shared by every session-bound recipe.
//!
//! A ZooKeeper recipe that "holds" something (a lock, leadership, a lease, a
//! semaphore permit, a registration) holds it *through a specific session*.
//! [`Ownership`] captures that: it is created with the pinned
//! [`Session`] that created the recipe's ephemeral nodes and tracks the
//! client's connection events to expose:
//!
//! * [`OwnershipState::Held`]: the session is connected; ownership is valid.
//! * [`OwnershipState::Suspended`]: the connection is lost but the session may
//!   survive. Following Curator's guidance, callers must stop acting as owner
//!   until it is `Held` again (or treat suspension as loss, see
//!   [`SuspendPolicy`]).
//! * [`OwnershipState::Lost`]: the session expired or the client closed. The
//!   ephemeral nodes are gone (or will be), so ownership is irrevocably lost.
//! * [`OwnershipState::Released`]: released voluntarily.
//!
//! Recipes return an `Ownership` (or embed one) so that callers can
//! `select!` on [`Ownership::lost`] while doing protected work.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zk_core::{ConnectionEventKind, ConnectionState, Session, ZkClient};

/// Ownership lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OwnershipState {
    Held,
    Suspended,
    Lost,
    Released,
}

impl OwnershipState {
    pub fn is_terminal(self) -> bool {
        matches!(self, OwnershipState::Lost | OwnershipState::Released)
    }
}

/// How a recipe treats connection suspension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SuspendPolicy {
    /// Report `Suspended`, return to `Held` on reconnect to the same session
    /// (Curator's default "standard" error policy).
    #[default]
    Standard,
    /// Treat suspension as loss (Curator's `SessionConnectionStateErrorPolicy`
    /// inverse: the most conservative choice for leadership).
    LoseOnSuspend,
}

/// Tracks whether a session-bound ownership is still valid.
#[derive(Clone)]
pub struct Ownership {
    state: watch::Receiver<OwnershipState>,
    sender: Arc<watch::Sender<OwnershipState>>,
    session: Session,
    stop: CancellationToken,
}

impl std::fmt::Debug for Ownership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ownership").field("state", &self.state()).field("session", &self.session.id_hex()).finish()
    }
}

impl Ownership {
    /// Starts tracking ownership held through `session`.
    pub fn track(client: &ZkClient, session: Session, policy: SuspendPolicy) -> Ownership {
        let initial = if session.is_alive() {
            if session.is_connected() { OwnershipState::Held } else { OwnershipState::Suspended }
        } else {
            OwnershipState::Lost
        };
        let (tx, rx) = watch::channel(initial);
        let tx = Arc::new(tx);
        let stop = CancellationToken::new();
        let own = Ownership { state: rx, sender: tx.clone(), session: session.clone(), stop: stop.clone() };
        if initial.is_terminal() {
            return own;
        }
        let mut events = client.subscribe_events();
        let mut conn = client.state_receiver();
        let generation = session.generation();
        tokio::spawn(async move {
            let set = |s: OwnershipState| {
                tx.send_if_modified(|cur| {
                    if cur.is_terminal() || *cur == s {
                        false
                    } else {
                        *cur = s;
                        true
                    }
                });
            };
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    st = session.terminated() => {
                        let _ = st;
                        set(OwnershipState::Lost);
                        return;
                    }
                    ev = events.recv() => match ev {
                        Ok(ev) if ev.generation == generation || ev.kind == ConnectionEventKind::Closed => match ev.kind {
                            ConnectionEventKind::Suspended => {
                                if policy == SuspendPolicy::LoseOnSuspend { set(OwnershipState::Lost); return; }
                                set(OwnershipState::Suspended);
                            }
                            ConnectionEventKind::Reconnected | ConnectionEventKind::Connected => set(OwnershipState::Held),
                            ConnectionEventKind::ReadOnly => set(OwnershipState::Suspended),
                            ConnectionEventKind::Lost | ConnectionEventKind::Closed | ConnectionEventKind::AuthFailed => {
                                set(OwnershipState::Lost);
                                return;
                            }
                            _ => {}
                        },
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // Re-derive from the coarse state.
                            match &*conn.borrow_and_update() {
                                ConnectionState::Suspended { generation: g, .. } if *g == generation => set(OwnershipState::Suspended),
                                ConnectionState::Connected { generation: g, .. } if *g == generation => set(OwnershipState::Held),
                                ConnectionState::Connected { .. } | ConnectionState::Connecting { .. } | ConnectionState::Closed { .. } | ConnectionState::AuthFailed { .. } => {
                                    set(OwnershipState::Lost);
                                    return;
                                }
                                _ => {}
                            }
                        }
                        Err(_) => { set(OwnershipState::Lost); return; }
                    },
                }
            }
        });
        own
    }

    pub fn state(&self) -> OwnershipState {
        *self.state.borrow()
    }

    /// `true` while ownership may be exercised (state is `Held`).
    pub fn is_held(&self) -> bool {
        self.state() == OwnershipState::Held
    }

    /// The session ownership is bound to.
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Resolves when ownership becomes `Lost` or `Released`.
    pub async fn lost(&self) -> OwnershipState {
        let mut rx = self.state.clone();
        loop {
            let s = *rx.borrow_and_update();
            if s.is_terminal() {
                return s;
            }
            if rx.changed().await.is_err() {
                return OwnershipState::Lost;
            }
        }
    }

    /// Resolves when ownership is no longer `Held` (suspended or terminal).
    pub async fn interrupted(&self) -> OwnershipState {
        let mut rx = self.state.clone();
        loop {
            let s = *rx.borrow_and_update();
            if s != OwnershipState::Held {
                return s;
            }
            if rx.changed().await.is_err() {
                return OwnershipState::Lost;
            }
        }
    }

    /// Subscribes to state changes.
    pub fn subscribe(&self) -> watch::Receiver<OwnershipState> {
        self.state.clone()
    }

    /// Marks ownership as voluntarily released and stops tracking.
    pub fn mark_released(&self) {
        self.sender.send_if_modified(|cur| {
            if cur.is_terminal() {
                false
            } else {
                *cur = OwnershipState::Released;
                true
            }
        });
        self.stop.cancel();
    }

    /// Marks ownership as lost (e.g. the recipe observed its node was deleted).
    pub fn mark_lost(&self) {
        self.sender.send_if_modified(|cur| {
            if cur.is_terminal() {
                false
            } else {
                *cur = OwnershipState::Lost;
                true
            }
        });
        self.stop.cancel();
    }
}
