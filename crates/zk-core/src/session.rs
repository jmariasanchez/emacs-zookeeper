//! Session supervision.
//!
//! [`ZkClient`] owns one logical connection to a cluster. Underneath, it holds
//! a sequence of ZooKeeper *sessions*, each identified by a monotonically
//! increasing `generation`. A supervisor task watches the current session and:
//!
//! * publishes [`ConnectionState`] and [`ConnectionEvent`]s (Curator-style
//!   `Connected`, `Suspended`, `Reconnected`, `Lost`, `ReadOnly`, `Closed`),
//! * re-establishes a brand-new session after expiration when
//!   [`ClusterConfig::reconnect_on_expiry`] is set,
//! * re-applies `addauth` credentials to every new session.
//!
//! Anything that depends on session identity (ephemeral nodes, locks,
//! leadership) must pin a [`Session`] and treat its expiry as loss of
//! ownership. Plain operations simply use whichever session is current.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use zookeeper_client as zk;

use crate::config::{parse_version, ClusterConfig, SaslConfig};
use crate::error::{ErrorKind, Result, ZkError};

/// Coarse connection state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum ConnectionState {
    /// No session yet (initial connect or re-creating after expiry).
    Connecting { generation: u64 },
    /// Session established and connected.
    Connected { generation: u64, session_id: String, read_only: bool },
    /// Connection lost; the session may still be alive on the server.
    Suspended { generation: u64, session_id: String, since_ms: i64 },
    /// Authentication failed. Terminal.
    AuthFailed { message: String },
    /// Closed, either explicitly or because the session expired and
    /// re-establishment is disabled. Terminal.
    Closed { reason: String },
}

impl ConnectionState {
    pub fn is_connected(&self) -> bool {
        matches!(self, ConnectionState::Connected { .. })
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, ConnectionState::AuthFailed { .. } | ConnectionState::Closed { .. })
    }

    pub fn generation(&self) -> Option<u64> {
        match self {
            ConnectionState::Connecting { generation }
            | ConnectionState::Connected { generation, .. }
            | ConnectionState::Suspended { generation, .. } => Some(*generation),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            ConnectionState::Connecting { .. } => "connecting",
            ConnectionState::Connected { .. } => "connected",
            ConnectionState::Suspended { .. } => "suspended",
            ConnectionState::AuthFailed { .. } => "auth-failed",
            ConnectionState::Closed { .. } => "closed",
        }
    }
}

/// Connection lifecycle events, modelled after Curator's `ConnectionState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectionEventKind {
    /// First session established.
    Connected,
    /// Connection lost; session may survive.
    Suspended,
    /// Reconnected to the same session (ephemerals and watches survive).
    Reconnected,
    /// Connected to a read-only server.
    ReadOnly,
    /// Session expired. Ephemeral nodes are gone and every ownership-based
    /// recipe must consider itself released.
    Lost,
    /// A new session replaced an expired one.
    SessionReestablished,
    /// Authentication failed. Terminal.
    AuthFailed,
    /// Closed. Terminal.
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionEvent {
    pub kind: ConnectionEventKind,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Wall clock time in milliseconds since the epoch.
    pub at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// A pinned ZooKeeper session.
///
/// Cloning is cheap. Operations through a pinned session fail with
/// [`ErrorKind::SessionExpired`] once it is gone; they never silently move to a
/// newer session.
#[derive(Clone, Debug)]
pub struct Session {
    pub(crate) client: zk::Client,
    pub(crate) generation: u64,
}

impl Session {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn id(&self) -> i64 {
        self.client.session_id().0
    }

    /// Session id formatted like the Java client (`0x...`).
    pub fn id_hex(&self) -> String {
        format!("{:#x}", self.client.session_id().0)
    }

    pub fn timeout(&self) -> Duration {
        self.client.session_timeout()
    }

    pub fn is_alive(&self) -> bool {
        !self.client.state().is_terminated()
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.client.state(), zk::SessionState::SyncConnected | zk::SessionState::ConnectedReadOnly)
    }

    /// Escape hatch to the underlying `zookeeper-client` handle.
    pub fn raw(&self) -> &zk::Client {
        &self.client
    }

    /// Resolves when this session terminates (expired, closed, auth failed).
    pub async fn terminated(&self) -> zk::SessionState {
        let mut w = self.client.state_watcher();
        let mut s = w.peek_state();
        while !s.is_terminated() {
            s = w.changed().await;
        }
        s
    }

    /// Resolves at the next state change, returning it.
    pub fn state_watcher(&self) -> zk::StateWatcher {
        self.client.state_watcher()
    }
}

/// Mutating-operation audit record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditRecord {
    pub at_ms: i64,
    pub cluster: String,
    pub operation: String,
    pub paths: Vec<String>,
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub forced: bool,
}

/// Receiver of audit records for mutating operations.
pub trait AuditSink: Send + Sync + 'static {
    fn record(&self, record: AuditRecord);
}

pub(crate) struct Inner {
    pub(crate) config: ClusterConfig,
    pub(crate) session: watch::Sender<Option<Session>>,
    pub(crate) state: watch::Sender<ConnectionState>,
    pub(crate) events: broadcast::Sender<ConnectionEvent>,
    pub(crate) cancel: CancellationToken,
    pub(crate) audit: Mutex<Option<Arc<dyn AuditSink>>>,
    pub(crate) stats: Stats,
    pub(crate) last_error: Mutex<Option<ZkError>>,
    /// Credentials added at runtime with `add_auth`, replayed on new sessions.
    pub(crate) runtime_auth: Mutex<Vec<(String, Vec<u8>)>>,
}

/// Counters for diagnostics.
#[derive(Default)]
pub(crate) struct Stats {
    pub(crate) sessions_created: AtomicU64,
    pub(crate) suspensions: AtomicU64,
    pub(crate) expirations: AtomicU64,
    pub(crate) requests: AtomicU64,
    pub(crate) failures: AtomicU64,
    pub(crate) retries: AtomicU64,
}

/// Snapshot of client diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientDiagnostics {
    pub cluster: String,
    pub connect: String,
    pub chroot: Option<String>,
    pub hosts: Vec<String>,
    pub state: ConnectionState,
    pub session_id: Option<String>,
    pub negotiated_session_timeout_ms: Option<u64>,
    pub generation: u64,
    pub sessions_created: u64,
    pub suspensions: u64,
    pub expirations: u64,
    pub requests: u64,
    pub failures: u64,
    pub retries: u64,
    pub read_only_policy: bool,
    pub protected_paths: Vec<String>,
    pub tls: bool,
    pub tls_hostname_verification: Option<bool>,
    pub sasl: Option<String>,
    pub auth_schemes: Vec<String>,
    pub last_error: Option<ZkError>,
}

/// A supervised, cloneable ZooKeeper client.
#[derive(Clone)]
pub struct ZkClient {
    pub(crate) inner: Arc<Inner>,
    /// When set, protected-path checks are bypassed for this handle.
    pub(crate) forced: bool,
}

impl std::fmt::Debug for ZkClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZkClient").field("cluster", &self.inner.config.name).field("state", &self.state()).finish()
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

impl ZkClient {
    /// Connects and waits for the first session (bounded by `connect_deadline_ms`).
    ///
    /// On failure nothing keeps running in the background.
    pub async fn connect(config: ClusterConfig) -> Result<ZkClient> {
        let client = Self::start(config)?;
        let deadline = Duration::from_millis(client.inner.config.connect_deadline_ms);
        match client.wait_connected(deadline).await {
            Ok(()) => Ok(client),
            Err(err) => {
                let err = client.inner.last_error.lock().unwrap().clone().unwrap_or(err);
                client.close().await;
                Err(err)
            },
        }
    }

    /// Starts the supervisor without waiting for a session.
    pub fn start(config: ClusterConfig) -> Result<ZkClient> {
        config.validate()?;
        let (session, _) = watch::channel(None);
        let (state, _) = watch::channel(ConnectionState::Connecting { generation: 1 });
        let (events, _) = broadcast::channel(256);
        let inner = Arc::new(Inner {
            config,
            session,
            state,
            events,
            cancel: CancellationToken::new(),
            audit: Mutex::new(None),
            stats: Stats::default(),
            last_error: Mutex::new(None),
            runtime_auth: Mutex::new(Vec::new()),
        });
        tokio::spawn(supervise(Arc::downgrade(&inner), inner.cancel.clone()));
        Ok(ZkClient { inner, forced: false })
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.inner.config
    }

    pub fn name(&self) -> &str {
        &self.inner.config.name
    }

    /// Installs an audit sink receiving a record per mutating operation.
    pub fn set_audit_sink(&self, sink: Option<Arc<dyn AuditSink>>) {
        *self.inner.audit.lock().unwrap() = sink;
    }

    /// A handle that bypasses protected-path checks (but never `read_only`).
    /// Every write through it is audited as `forced`.
    pub fn forced(&self) -> ZkClient {
        ZkClient { inner: self.inner.clone(), forced: true }
    }

    pub fn is_forced(&self) -> bool {
        self.forced
    }

    pub fn state(&self) -> ConnectionState {
        self.inner.state.borrow().clone()
    }

    pub fn state_receiver(&self) -> watch::Receiver<ConnectionState> {
        self.inner.state.subscribe()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.inner.events.subscribe()
    }

    /// The current session, if any.
    pub fn current_session(&self) -> Option<Session> {
        self.inner.session.borrow().clone()
    }

    /// Waits (up to `timeout`) for a usable session and returns it pinned.
    pub async fn session(&self, timeout: Duration) -> Result<Session> {
        let mut rx = self.inner.session.subscribe();
        let fut = async {
            loop {
                if let Some(s) = rx.borrow_and_update().clone() {
                    if s.is_alive() {
                        return Ok(s);
                    }
                }
                if let Some(err) = self.terminal_error() {
                    return Err(err);
                }
                let mut st = self.inner.state.subscribe();
                tokio::select! {
                    r = rx.changed() => if r.is_err() { return Err(ZkError::new(ErrorKind::Closed, "client closed")) },
                    _ = st.changed() => {},
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {},
                }
            }
        };
        match tokio::time::timeout(timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(ZkError::new(ErrorKind::NotConnected, format!("no session within {}ms", timeout.as_millis()))),
        }
    }

    /// The session to use for the next operation, honoring the operation timeout.
    pub(crate) async fn op_session(&self) -> Result<Session> {
        self.session(self.inner.config.operation_timeout()).await
    }

    fn terminal_error(&self) -> Option<ZkError> {
        match &*self.inner.state.borrow() {
            ConnectionState::AuthFailed { message } => Some(ZkError::new(ErrorKind::AuthFailed, message.clone())),
            ConnectionState::Closed { reason } => Some(ZkError::new(ErrorKind::Closed, reason.clone())),
            _ => None,
        }
    }

    /// Waits for `Connected` state.
    pub async fn wait_connected(&self, timeout: Duration) -> Result<()> {
        let mut rx = self.inner.state.subscribe();
        let fut = async {
            loop {
                let s = rx.borrow_and_update().clone();
                match s {
                    ConnectionState::Connected { .. } => return Ok(()),
                    ConnectionState::AuthFailed { message } => return Err(ZkError::new(ErrorKind::AuthFailed, message)),
                    ConnectionState::Closed { reason } => return Err(ZkError::new(ErrorKind::Closed, reason)),
                    _ => {},
                }
                if rx.changed().await.is_err() {
                    return Err(ZkError::new(ErrorKind::Closed, "client closed"));
                }
            }
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| ZkError::new(ErrorKind::Timeout, format!("not connected within {}ms", timeout.as_millis())))?
    }

    /// Closes the client and its session. Ephemeral nodes are removed by the server.
    ///
    /// The ZooKeeper session is closed once the last pinned [`Session`]
    /// obtained from this client is dropped as well.
    pub async fn close(&self) {
        self.inner.cancel.cancel();
        let session = self.inner.session.send_replace(None);
        drop(session);
        // Give the supervisor a moment to observe cancellation and publish Closed.
        let mut rx = self.inner.state.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            while !rx.borrow_and_update().is_terminal() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
    }

    pub fn is_closed(&self) -> bool {
        self.inner.cancel.is_cancelled() || self.state().is_terminal()
    }

    pub fn diagnostics(&self) -> ClientDiagnostics {
        let c = &self.inner.config;
        let s = &self.inner.stats;
        let session = self.current_session();
        let (hosts, _) = c.hosts_and_chroot();
        ClientDiagnostics {
            cluster: c.name.clone(),
            connect: c.connect.clone(),
            chroot: c.effective_chroot(),
            hosts,
            state: self.state(),
            session_id: session.as_ref().map(|s| s.id_hex()),
            negotiated_session_timeout_ms: session.as_ref().map(|s| s.timeout().as_millis() as u64),
            generation: session.as_ref().map(|s| s.generation).unwrap_or(0),
            sessions_created: s.sessions_created.load(Ordering::Relaxed),
            suspensions: s.suspensions.load(Ordering::Relaxed),
            expirations: s.expirations.load(Ordering::Relaxed),
            requests: s.requests.load(Ordering::Relaxed),
            failures: s.failures.load(Ordering::Relaxed),
            retries: s.retries.load(Ordering::Relaxed),
            read_only_policy: c.safety.read_only,
            protected_paths: c.safety.protected_paths.clone(),
            tls: c.tls.is_some() || c.connect.contains("tcp+tls://"),
            tls_hostname_verification: c.tls.as_ref().map(|t| t.verify_hostname),
            sasl: c.sasl.as_ref().map(|s| match s {
                SaslConfig::DigestMd5 { .. } => "DIGEST-MD5".to_string(),
                SaslConfig::Gssapi { .. } => "GSSAPI".to_string(),
            }),
            auth_schemes: c.auth.iter().map(|a| a.scheme.clone()).collect(),
            last_error: self.inner.last_error.lock().unwrap().clone(),
        }
    }

    pub(crate) fn audit(&self, operation: &str, paths: Vec<String>, result: std::result::Result<(), &ZkError>) {
        let sink = self.inner.audit.lock().unwrap().clone();
        if let Some(sink) = sink {
            sink.record(AuditRecord {
                at_ms: now_ms(),
                cluster: self.inner.config.name.clone(),
                operation: operation.to_string(),
                paths,
                outcome: match result {
                    Ok(()) => "ok".into(),
                    Err(e) => e.kind.as_str().into(),
                },
                error: result.err().map(|e| e.to_string()),
                forced: self.forced,
            });
        }
    }

    pub(crate) fn note_error(&self, err: &ZkError) {
        self.inner.stats.failures.fetch_add(1, Ordering::Relaxed);
        *self.inner.last_error.lock().unwrap() = Some(err.clone());
    }

    pub(crate) fn connector(&self) -> Result<zk::Connector> {
        build_connector(&self.inner.config)
    }
}

fn read_file(path: &std::path::Path, what: &str) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| ZkError::config(format!("cannot read {what} {}: {e}", path.display())))
}

pub(crate) fn build_connector(config: &ClusterConfig) -> Result<zk::Connector> {
    let mut c = zk::Client::connector().with_session_timeout(config.session_timeout());
    if let Some(ms) = config.connection_timeout_ms {
        c = c.with_connection_timeout(Duration::from_millis(ms));
    }
    c = c.with_readonly(config.allow_read_only);
    if let Some(v) = &config.server_version {
        let (a, b, p) = parse_version(v)?;
        c = c.with_server_version(a, b, p);
    }
    for auth in &config.auth {
        let cred = auth.credential.resolve()?;
        c = c.with_auth(&auth.scheme, cred.as_bytes());
    }
    if let Some(tls) = &config.tls {
        let mut opts = zk::TlsOptions::new();
        if let Some(ca) = &tls.ca_file {
            opts = opts.with_pem_ca(&read_file(ca, "CA file")?).map_err(|e| ZkError::config(format!("bad CA: {e}")))?;
        }
        if let (Some(cert), Some(key)) = (&tls.cert_file, &tls.key_file) {
            opts = opts
                .with_pem_identity(&read_file(cert, "certificate")?, &read_file(key, "private key")?)
                .map_err(|e| ZkError::config(format!("bad client identity: {e}")))?;
        }
        if !tls.verify_hostname {
            // SAFETY: explicitly requested by configuration; surfaced in diagnostics.
            opts = unsafe { opts.with_no_hostname_verification() };
        }
        c = c.with_tls(opts);
    }
    if let Some(sasl) = &config.sasl {
        match sasl {
            SaslConfig::DigestMd5 { username, password } => {
                let pw = password.resolve()?;
                c = c.with_sasl(zk::SaslOptions::digest_md5(username.clone(), pw));
            },
            #[cfg(feature = "gssapi")]
            SaslConfig::Gssapi { username, hostname } => {
                let mut o = zk::SaslOptions::gssapi();
                if let Some(u) = username {
                    o = o.with_username(u.clone());
                }
                if let Some(h) = hostname {
                    o = o.with_hostname(h.clone());
                }
                c = c.with_sasl(o);
            },
            #[cfg(not(feature = "gssapi"))]
            SaslConfig::Gssapi { .. } => {
                return Err(ZkError::config("GSSAPI SASL requires building with the `gssapi` feature"));
            },
        }
    }
    Ok(c)
}

fn connect_string(config: &ClusterConfig) -> String {
    let (hosts, _) = config.hosts_and_chroot();
    let mut s = hosts.join(",");
    if let Some(chroot) = config.effective_chroot() {
        if chroot != "/" {
            s.push_str(&chroot);
        }
    }
    s
}

async fn establish(config: &ClusterConfig, extra_auth: &[(String, Vec<u8>)]) -> Result<zk::Client> {
    let mut connector = build_connector(config)?;
    for (scheme, cred) in extra_auth {
        connector = connector.with_auth(scheme, cred);
    }
    let cs = connect_string(config);
    let secure = config.tls.is_some();
    let fut = async move {
        if secure {
            connector.secure_connect(&cs).await
        } else {
            connector.connect(&cs).await
        }
    };
    let deadline = Duration::from_millis(config.connect_deadline_ms.max(1_000));
    match tokio::time::timeout(deadline, fut).await {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(zk::Error::NoNode)) => Err(ZkError::new(ErrorKind::NoNode, "chroot does not exist")),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(ZkError::new(ErrorKind::Timeout, format!("connect timed out after {}ms", deadline.as_millis()))),
    }
}

fn emit(inner: &Inner, kind: ConnectionEventKind, generation: u64, session: Option<&Session>, message: Option<String>) {
    let ev = ConnectionEvent {
        kind,
        generation,
        session_id: session.map(|s| s.id_hex()),
        at_ms: now_ms(),
        message,
    };
    tracing::info!(cluster = %inner.config.name, ?kind, generation, "connection event");
    let _ = inner.events.send(ev);
}

async fn supervise(weak: std::sync::Weak<Inner>, cancel: CancellationToken) {
    let mut generation: u64 = 0;
    let mut backoff = Duration::from_millis(100);
    loop {
        generation += 1;
        let Some(inner) = weak.upgrade() else { return };
        inner.state.send_replace(ConnectionState::Connecting { generation });
        let config = inner.config.clone();
        let extra_auth = inner.runtime_auth.lock().unwrap().clone();
        drop(inner);

        let started = Instant::now();
        let result = tokio::select! {
            _ = cancel.cancelled() => {
                finish(&weak, "closed by client".into());
                return;
            }
            r = establish(&config, &extra_auth) => r,
        };
        let Some(inner) = weak.upgrade() else { return };
        let client = match result {
            Ok(c) => c,
            Err(err) => {
                *inner.last_error.lock().unwrap() = Some(err.clone());
                if err.kind == ErrorKind::AuthFailed {
                    inner.state.send_replace(ConnectionState::AuthFailed { message: err.message.clone() });
                    emit(&inner, ConnectionEventKind::AuthFailed, generation, None, Some(err.message));
                    return;
                }
                tracing::warn!(cluster = %config.name, error = %err, "session establishment failed");
                drop(inner);
                generation -= 1;
                let delay = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                tokio::select! {
                    _ = cancel.cancelled() => { finish(&weak, "closed by client".into()); return; }
                    _ = tokio::time::sleep(delay) => continue,
                }
            },
        };
        backoff = Duration::from_millis(100);
        let session = Session { client, generation };
        inner.stats.sessions_created.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(elapsed_ms = started.elapsed().as_millis() as u64, "session established");
        let read_only = session.client.state() == zk::SessionState::ConnectedReadOnly;
        inner.session.send_replace(Some(session.clone()));
        inner.state.send_replace(ConnectionState::Connected {
            generation,
            session_id: session.id_hex(),
            read_only,
        });
        if generation == 1 {
            emit(&inner, ConnectionEventKind::Connected, generation, Some(&session), None);
        } else {
            emit(&inner, ConnectionEventKind::SessionReestablished, generation, Some(&session), None);
        }
        if read_only {
            emit(&inner, ConnectionEventKind::ReadOnly, generation, Some(&session), None);
        }
        drop(inner);

        // Monitor this session until it terminates.
        let mut watcher = session.client.state_watcher();
        let terminal = loop {
            let state = tokio::select! {
                _ = cancel.cancelled() => {
                    if let Some(inner) = weak.upgrade() { inner.session.send_replace(None); }
                    drop(session);
                    finish(&weak, "closed by client".into());
                    return;
                }
                s = watcher.changed() => s,
            };
            let Some(inner) = weak.upgrade() else { return };
            match state {
                zk::SessionState::Disconnected => {
                    inner.stats.suspensions.fetch_add(1, Ordering::Relaxed);
                    inner.state.send_replace(ConnectionState::Suspended {
                        generation,
                        session_id: session.id_hex(),
                        since_ms: now_ms(),
                    });
                    emit(&inner, ConnectionEventKind::Suspended, generation, Some(&session), None);
                },
                zk::SessionState::SyncConnected | zk::SessionState::ConnectedReadOnly => {
                    let read_only = state == zk::SessionState::ConnectedReadOnly;
                    inner.state.send_replace(ConnectionState::Connected {
                        generation,
                        session_id: session.id_hex(),
                        read_only,
                    });
                    emit(
                        &inner,
                        if read_only { ConnectionEventKind::ReadOnly } else { ConnectionEventKind::Reconnected },
                        generation,
                        Some(&session),
                        None,
                    );
                },
                s if s.is_terminated() => break s,
                _ => {},
            }
        };

        let Some(inner) = weak.upgrade() else { return };
        inner.session.send_replace(None);
        drop(session);
        match terminal {
            zk::SessionState::Expired => {
                inner.stats.expirations.fetch_add(1, Ordering::Relaxed);
                emit(&inner, ConnectionEventKind::Lost, generation, None, Some("session expired".into()));
                if !inner.config.reconnect_on_expiry {
                    drop(inner);
                    finish(&weak, "session expired".into());
                    return;
                }
            },
            zk::SessionState::AuthFailed => {
                inner.state.send_replace(ConnectionState::AuthFailed { message: "authentication failed".into() });
                emit(&inner, ConnectionEventKind::AuthFailed, generation, None, None);
                return;
            },
            _ => {
                // Closed underneath us (should not happen while we hold a handle).
                emit(&inner, ConnectionEventKind::Lost, generation, None, Some(format!("session {terminal}")));
                if cancel.is_cancelled() || !inner.config.reconnect_on_expiry {
                    drop(inner);
                    finish(&weak, format!("session {terminal}"));
                    return;
                }
            },
        }
    }
}

fn finish(weak: &std::sync::Weak<Inner>, reason: String) {
    if let Some(inner) = weak.upgrade() {
        let generation = inner.state.borrow().generation().unwrap_or(0);
        inner.session.send_replace(None);
        if !inner.state.borrow().is_terminal() {
            inner.state.send_replace(ConnectionState::Closed { reason: reason.clone() });
            emit(&inner, ConnectionEventKind::Closed, generation, None, Some(reason));
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
