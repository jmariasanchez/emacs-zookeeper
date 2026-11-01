//! Znode operations on a supervised [`ZkClient`].
//!
//! Semantics:
//!
//! * Every operation has a deadline ([`ClusterConfig::operation_timeout_ms`] by default).
//! * Idempotent reads are retried per [`RetryPolicy`] on connection loss.
//! * Writes are never retried automatically. If the connection drops after a
//!   write was sent, the error is [`ErrorKind::OutcomeUnknown`].
//! * The local [`SafetyPolicy`] is enforced before any write is sent.
//!
//! [`ClusterConfig::operation_timeout_ms`]: crate::ClusterConfig::operation_timeout_ms
//! [`RetryPolicy`]: crate::RetryPolicy
//! [`SafetyPolicy`]: crate::SafetyPolicy

use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use zookeeper_client as zk;

use crate::error::{ErrorKind, Result, ZkError};
use crate::path;
use crate::session::{Session, ZkClient};
use crate::types::{Acl, AuthIdentity, ChildEntry, CreateMode, Created, NodeData, Stat};

/// Options for [`ZkClient::create`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateOptions {
    #[serde(default)]
    pub mode: CreateMode,
    /// ACL for the node. Defaults to `world:anyone:cdrwa`.
    #[serde(default = "Acl::anyone_all")]
    pub acl: Vec<Acl>,
    /// TTL in milliseconds for TTL modes.
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    /// Create missing ancestors as persistent nodes (`mkdir -p`).
    #[serde(default)]
    pub create_parents: bool,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self { mode: CreateMode::Persistent, acl: Acl::anyone_all(), ttl_ms: None, create_parents: false }
    }
}

impl CreateOptions {
    pub fn mode(mode: CreateMode) -> Self {
        Self { mode, ..Default::default() }
    }

    pub fn persistent() -> Self {
        Self::mode(CreateMode::Persistent)
    }

    pub fn ephemeral() -> Self {
        Self::mode(CreateMode::Ephemeral)
    }

    pub fn with_parents(mut self) -> Self {
        self.create_parents = true;
        self
    }

    pub fn with_acl(mut self, acl: Vec<Acl>) -> Self {
        self.acl = acl;
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl_ms = Some(ttl.as_millis() as u64);
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.acl.is_empty() {
            return Err(ZkError::new(ErrorKind::InvalidAcl, "ACL must not be empty"));
        }
        match (self.mode.needs_ttl(), self.ttl_ms) {
            (true, None) | (true, Some(0)) => Err(ZkError::bad_arguments("TTL mode requires a positive ttl_ms")),
            (false, Some(_)) => Err(ZkError::bad_arguments("ttl_ms is only valid with a TTL create mode")),
            _ => Ok(()),
        }
    }
}

/// One operation of a multi-op transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum TxnOp {
    Create {
        path: String,
        #[serde(default, with = "crate::b64")]
        data: Vec<u8>,
        #[serde(default)]
        mode: CreateMode,
        #[serde(default = "Acl::anyone_all")]
        acl: Vec<Acl>,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    SetData {
        path: String,
        #[serde(default, with = "crate::b64")]
        data: Vec<u8>,
        #[serde(default)]
        version: Option<i32>,
    },
    Delete {
        path: String,
        #[serde(default)]
        version: Option<i32>,
    },
    Check {
        path: String,
        version: i32,
    },
}

impl TxnOp {
    pub fn path(&self) -> &str {
        match self {
            TxnOp::Create { path, .. }
            | TxnOp::SetData { path, .. }
            | TxnOp::Delete { path, .. }
            | TxnOp::Check { path, .. } => path,
        }
    }

    pub fn is_write(&self) -> bool {
        !matches!(self, TxnOp::Check { .. })
    }

    pub fn create(path: &str, data: &[u8], mode: CreateMode) -> Self {
        TxnOp::Create { path: path.into(), data: data.into(), mode, acl: Acl::anyone_all(), ttl_ms: None }
    }

    pub fn set_data(path: &str, data: &[u8], version: Option<i32>) -> Self {
        TxnOp::SetData { path: path.into(), data: data.into(), version }
    }

    pub fn delete(path: &str, version: Option<i32>) -> Self {
        TxnOp::Delete { path: path.into(), version }
    }

    pub fn check(path: &str, version: i32) -> Self {
        TxnOp::Check { path: path.into(), version }
    }
}

/// Per-operation transaction result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum TxnResult {
    Create { path: String, stat: Option<Stat> },
    SetData { stat: Stat },
    Delete,
    Check,
}

/// Read operation for [`ZkClient::multi_read`] (snapshot-consistent read, 3.6+).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum ReadOp {
    GetData { path: String },
    GetChildren { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadResult {
    Data(NodeData),
    Children(Vec<String>),
    Error(ZkError),
}

/// Options for a recursive delete.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeleteRecursiveOptions {
    /// Only report what would be deleted.
    #[serde(default)]
    pub dry_run: bool,
    /// Override the policy's `max_recursive_nodes`.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Version the root must have, checked before anything is deleted.
    #[serde(default)]
    pub version: Option<i32>,
}

/// Outcome of a recursive delete.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteRecursiveReport {
    /// Deleted (or, in a dry run, to-be-deleted) paths, deepest first.
    pub paths: Vec<String>,
    pub dry_run: bool,
    /// Nodes created concurrently during the delete that forced a re-scan.
    pub rescans: usize,
    /// Dry run only: paths the safety policy would refuse to delete.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked: Vec<String>,
}

/// A serialized subtree, used for export/import and copy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeNode {
    /// Path relative to the export root ("" for the root itself).
    pub rel: String,
    #[serde(with = "crate::b64")]
    pub data: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acl: Option<Vec<Acl>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stat: Option<Stat>,
}

/// Result of a subtree walk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalkResult {
    pub nodes: Vec<WalkEntry>,
    /// `true` if a limit stopped the walk early.
    pub truncated: bool,
    /// Nodes that disappeared or could not be read during the walk.
    pub skipped: Vec<(String, ErrorKind)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalkEntry {
    pub path: String,
    pub depth: usize,
    pub stat: Stat,
}

/// Walk options.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalkOptions {
    #[serde(default)]
    pub max_depth: Option<usize>,
    #[serde(default = "default_walk_limit")]
    pub max_nodes: usize,
    /// Include `/zookeeper` when walking from `/`.
    #[serde(default)]
    pub include_system: bool,
    /// Maximum number of requests in flight.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_walk_limit() -> usize {
    100_000
}
fn default_concurrency() -> usize {
    64
}

impl Default for WalkOptions {
    fn default() -> Self {
        Self { max_depth: None, max_nodes: default_walk_limit(), include_system: false, concurrency: default_concurrency() }
    }
}

/// Request outcome classes for retry logic.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Idempotency {
    Read,
    Write,
}

impl ZkClient {
    /// Runs one request against the current session with deadline and retry.
    pub(crate) async fn run<T, F, Fut>(&self, what: &str, path: Option<&str>, kind: Idempotency, f: F) -> Result<T>
    where
        F: Fn(Session) -> Fut,
        Fut: Future<Output = std::result::Result<T, zk::Error>>,
    {
        let policy = &self.inner.config.retry;
        let started = Instant::now();
        let budget = Duration::from_millis(policy.max_elapsed_ms);
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            self.inner.stats.requests.fetch_add(1, Ordering::Relaxed);
            let result = async {
                let session = self.op_session().await?;
                match tokio::time::timeout(self.inner.config.operation_timeout(), f(session)).await {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(ZkError::from(e)),
                    Err(_) => Err(ZkError::timeout(what)),
                }
            }
            .await;
            let err = match result {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };
            let mut err = match path {
                Some(p) => err.with_path(p),
                None => err,
            };
            let may_retry = kind == Idempotency::Read
                && err.kind.is_retryable()
                && attempt < policy.max_attempts
                && started.elapsed() < budget;
            if may_retry {
                self.inner.stats.retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(policy.delay(attempt)).await;
                continue;
            }
            if kind == Idempotency::Write {
                err = err.into_write_outcome();
            }
            if !matches!(err.kind, ErrorKind::NoNode | ErrorKind::NodeExists | ErrorKind::BadVersion | ErrorKind::NotEmpty) {
                self.note_error(&err);
            }
            return Err(err);
        }
    }

    /// Fails fast with `Unimplemented` when `server_version` is configured and
    /// older than `major.minor`. Older servers (3.5) drop the connection when
    /// they receive an unknown opcode, so not sending it at all is kinder.
    pub(crate) fn require_server(&self, feature: &str, major: u32, minor: u32) -> Result<()> {
        if let Some(v) = &self.inner.config.server_version {
            if let Ok((a, b, _)) = crate::config::parse_version(v) {
                if (a, b) < (major, minor) {
                    return Err(ZkError::new(
                        ErrorKind::Unimplemented,
                        format!("{feature} requires ZooKeeper {major}.{minor} or newer (configured server_version {v})"),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Checks the local safety policy for a write to `path`.
    pub fn check_write_allowed(&self, path: &str) -> Result<()> {
        let safety = &self.inner.config.safety;
        if safety.read_only {
            return Err(ZkError::policy(format!(
                "cluster profile {:?} is read-only; refusing to modify {path}",
                self.inner.config.name
            ))
            .with_path(path));
        }
        if !self.forced {
            if let Some(p) = safety.protected_paths.iter().find(|p| path::matches_pattern(path, p)) {
                return Err(ZkError::policy(format!("{path} is protected by pattern {p}; an explicit override is required"))
                    .with_path(path));
            }
        }
        Ok(())
    }

    fn audited<T>(&self, op: &str, paths: Vec<String>, r: Result<T>) -> Result<T> {
        self.audit(op, paths, r.as_ref().map(|_| ()));
        r
    }

    // ---------------------------------------------------------------- reads

    /// Data and stat of a node.
    pub async fn get_data(&self, path: &str) -> Result<NodeData> {
        path::validate(path)?;
        self.run("get_data", Some(path), Idempotency::Read, |s| async move {
            s.client.get_data(path).await.map(|(data, stat)| NodeData { data, stat: stat.into() })
        })
        .await
    }

    /// Stat of a node, `None` if it does not exist.
    pub async fn exists(&self, path: &str) -> Result<Option<Stat>> {
        path::validate(path)?;
        self.run("exists", Some(path), Idempotency::Read, |s| async move {
            s.client.check_stat(path).await.map(|o| o.map(Stat::from))
        })
        .await
    }

    /// Child names, sorted.
    pub async fn children(&self, path: &str) -> Result<Vec<String>> {
        let (mut c, _) = self.children_with_stat(path).await?;
        c.sort();
        Ok(c)
    }

    /// Child names (server order) plus the parent stat.
    pub async fn children_with_stat(&self, path: &str) -> Result<(Vec<String>, Stat)> {
        path::validate(path)?;
        self.run("get_children", Some(path), Idempotency::Read, |s| async move {
            s.client.get_children(path).await.map(|(c, st)| (c, st.into()))
        })
        .await
    }

    /// Children with their stats, sorted by name. Stats are fetched with
    /// pipelined `exists` requests. Children deleted in between are omitted.
    pub async fn children_stats(&self, path: &str, limit: Option<usize>) -> Result<(Vec<ChildEntry>, Stat, bool)> {
        let (mut names, parent) = self.children_with_stat(path).await?;
        names.sort();
        let truncated = limit.map(|l| names.len() > l).unwrap_or(false);
        if let Some(l) = limit {
            names.truncate(l);
        }
        let session = self.op_session().await?;
        let mut out = Vec::with_capacity(names.len());
        for chunk in names.chunks(512) {
            let futs: Vec<_> = chunk
                .iter()
                .map(|n| {
                    let p = path::join(path, n);
                    let c = session.client.clone();
                    async move {
                        let r = c.check_stat(&p).await;
                        (p, r)
                    }
                })
                .collect();
            let results = tokio::time::timeout(self.inner.config.operation_timeout(), join_all(futs))
                .await
                .map_err(|_| ZkError::timeout("children_stats").with_path(path))?;
            for (p, r) in results {
                match r {
                    Ok(Some(st)) => out.push(ChildEntry { name: path::basename(&p).to_string(), path: p, stat: Some(st.into()) }),
                    Ok(None) | Err(zk::Error::NoNode) => {},
                    Err(e) => return Err(ZkError::from(e).with_path(p)),
                }
            }
        }
        Ok((out, parent, truncated))
    }

    pub async fn get_acl(&self, path: &str) -> Result<(Vec<Acl>, Stat)> {
        path::validate(path)?;
        self.run("get_acl", Some(path), Idempotency::Read, |s| async move {
            s.client.get_acl(path).await.map(|(acl, st)| (acl.iter().map(Acl::from_zk).collect(), st.into()))
        })
        .await
    }

    /// `sync` the connected server with the leader for `path`.
    pub async fn sync(&self, path: &str) -> Result<()> {
        path::validate(path)?;
        self.run("sync", Some(path), Idempotency::Read, |s| async move { s.client.sync(path).await }).await
    }

    /// `sync` then read: the freshest read ZooKeeper offers without a write.
    pub async fn get_data_synced(&self, path: &str) -> Result<NodeData> {
        self.sync(path).await?;
        self.get_data(path).await
    }

    /// Number of descendants (3.6+, `getAllChildrenNumber`).
    pub async fn count_descendants(&self, path: &str) -> Result<usize> {
        path::validate(path)?;
        self.require_server("getAllChildrenNumber", 3, 6)?;
        self.run("count_descendants", Some(path), Idempotency::Read, |s| async move {
            s.client.count_descendants_number(path).await
        })
        .await
        .map_err(|e| e.requires_version("getAllChildrenNumber", "3.6"))
    }

    /// Ephemeral nodes of the *current* session under `path` (3.6+).
    pub async fn session_ephemerals(&self, path: &str) -> Result<Vec<String>> {
        path::validate(path)?;
        self.require_server("getEphemerals", 3, 6)?;
        self.run("get_ephemerals", Some(path), Idempotency::Read, |s| async move {
            s.client.list_ephemerals(path).await
        })
        .await
        .map_err(|e| e.requires_version("getEphemerals", "3.6"))
    }

    /// Identities authenticated on the current session (`whoami`, 3.7+).
    pub async fn whoami(&self) -> Result<Vec<AuthIdentity>> {
        self.require_server("whoami", 3, 7)?;
        self.run("whoami", None, Idempotency::Read, |s| async move {
            s.client.list_auth_users().await.map(|v| {
                v.iter().map(|u| AuthIdentity { scheme: u.scheme().into(), user: u.user().into() }).collect()
            })
        })
        .await
        .map_err(|e| e.requires_version("whoami", "3.7"))
    }

    /// Adds authentication to the current session and remembers it so that
    /// sessions re-established after expiration authenticate the same way.
    pub async fn add_auth(&self, scheme: &str, credential: &[u8]) -> Result<()> {
        if scheme.is_empty() {
            return Err(ZkError::bad_arguments("auth scheme must not be empty"));
        }
        let session = self.op_session().await?;
        let r = tokio::time::timeout(self.inner.config.operation_timeout(), session.client.auth(scheme, credential))
            .await
            .map_err(|_| ZkError::timeout("add_auth"))?
            .map_err(ZkError::from);
        if r.is_ok() {
            self.inner.runtime_auth.lock().unwrap().push((scheme.to_string(), credential.to_vec()));
        }
        r
    }

    /// Dynamic configuration (`/zookeeper/config`).
    pub async fn get_config(&self) -> Result<NodeData> {
        self.run("get_config", None, Idempotency::Read, |s| async move {
            s.client.get_config().await.map(|(data, stat)| NodeData { data, stat: stat.into() })
        })
        .await
    }

    /// Snapshot-consistent multi-read (3.6+).
    pub async fn multi_read(&self, ops: &[ReadOp]) -> Result<Vec<ReadResult>> {
        self.require_server("multiRead", 3, 6)?;
        for op in ops {
            match op {
                ReadOp::GetData { path } | ReadOp::GetChildren { path } => path::validate(path)?,
            }
        }
        self.run("multi_read", None, Idempotency::Read, |s| async move {
            let mut reader = s.client.new_multi_reader();
            for op in ops {
                match op {
                    ReadOp::GetData { path } => reader.add_get_data(path)?,
                    ReadOp::GetChildren { path } => reader.add_get_children(path)?,
                }
            }
            let results = reader.commit().await?;
            Ok(results
                .into_iter()
                .map(|r| match r {
                    zk::MultiReadResult::Data { data, stat } => ReadResult::Data(NodeData { data, stat: stat.into() }),
                    zk::MultiReadResult::Children { children } => ReadResult::Children(children),
                    zk::MultiReadResult::Error { err } => ReadResult::Error(err.into()),
                    _ => ReadResult::Error(ZkError::new(ErrorKind::Protocol, "unknown multi-read result")),
                })
                .collect())
        })
        .await
        .map_err(|e| e.requires_version("multiRead", "3.6"))
    }

    // --------------------------------------------------------------- writes

    /// Creates a node. With `create_parents`, missing ancestors are created as
    /// empty persistent nodes with the same ACL.
    pub async fn create(&self, path: &str, data: &[u8], options: &CreateOptions) -> Result<Created> {
        let r = self.create_inner(path, data, options).await;
        let paths = match &r {
            Ok(c) => vec![c.path.clone()],
            Err(_) => vec![path.to_string()],
        };
        self.audited("create", paths, r)
    }

    async fn create_inner(&self, path: &str, data: &[u8], options: &CreateOptions) -> Result<Created> {
        options.validate()?;
        if options.mode.is_sequential() {
            path::validate_sequential_prefix(path)?;
        } else {
            path::validate(path)?;
        }
        if path == "/" {
            return Err(ZkError::bad_arguments("cannot create the root node"));
        }
        self.check_write_allowed(path)?;
        if options.create_parents {
            if let Some(parent) = path::parent(path.trim_end_matches('/')).filter(|p| *p != "/") {
                self.ensure_path(parent, &options.acl).await?;
            }
        }
        let acl: Vec<zk::Acl> = options.acl.iter().map(Acl::to_zk).collect();
        let mode = options.mode;
        let ttl = options.ttl_ms;
        let (stat, seq) = self
            .run("create", Some(path), Idempotency::Write, |s| {
                let acl = acl.clone();
                async move {
                    let mut o = mode.to_zk().with_acls(zk::Acls::new(&acl));
                    if let Some(ms) = ttl {
                        o = o.with_ttl(Duration::from_millis(ms));
                    }
                    s.client.create(path, data, &o).await
                }
            })
            .await?;
        let sequence = mode.is_sequential().then(|| seq.into_i64());
        let actual = match sequence {
            Some(_) => format!("{path}{seq}"),
            None => path.to_string(),
        };
        Ok(Created { path: actual, stat: stat.into(), sequence })
    }

    /// Creates `path` and all missing ancestors as empty persistent nodes.
    /// Succeeds if it already exists.
    pub async fn ensure_path(&self, path: &str, acl: &[Acl]) -> Result<()> {
        path::validate(path)?;
        if path == "/" {
            return Ok(());
        }
        if self.exists(path).await?.is_some() {
            return Ok(());
        }
        self.check_write_allowed(path)?;
        let zacl: Vec<zk::Acl> = acl.iter().map(Acl::to_zk).collect();
        let r = self
            .run("mkdir", Some(path), Idempotency::Write, |s| {
                let zacl = zacl.clone();
                async move { s.client.mkdir(path, &zk::CreateMode::Persistent.with_acls(zk::Acls::new(&zacl))).await }
            })
            .await;
        self.audited("ensure_path", vec![path.to_string()], r)
    }

    /// Sets data, optionally conditional on `version`. Returns the new stat.
    pub async fn set_data(&self, path: &str, data: &[u8], version: Option<i32>) -> Result<Stat> {
        let r = async {
            path::validate(path)?;
            self.check_write_allowed(path)?;
            self.run("set_data", Some(path), Idempotency::Write, |s| async move {
                s.client.set_data(path, data, version).await.map(Stat::from)
            })
            .await
        }
        .await;
        self.audited("set_data", vec![path.to_string()], r)
    }

    /// Deletes a node, optionally conditional on `version`.
    pub async fn delete(&self, path: &str, version: Option<i32>) -> Result<()> {
        let r = async {
            path::validate(path)?;
            if path == "/" {
                return Err(ZkError::bad_arguments("cannot delete the root node"));
            }
            self.check_write_allowed(path)?;
            self.run("delete", Some(path), Idempotency::Write, |s| async move { s.client.delete(path, version).await })
                .await
        }
        .await;
        self.audited("delete", vec![path.to_string()], r)
    }

    /// Sets the ACL, optionally conditional on the ACL version (`aversion`).
    pub async fn set_acl(&self, path: &str, acl: &[Acl], aversion: Option<i32>) -> Result<Stat> {
        let r = async {
            path::validate(path)?;
            if acl.is_empty() {
                return Err(ZkError::new(ErrorKind::InvalidAcl, "ACL must not be empty"));
            }
            self.check_write_allowed(path)?;
            let zacl: Vec<zk::Acl> = acl.iter().map(Acl::to_zk).collect();
            self.run("set_acl", Some(path), Idempotency::Write, |s| {
                let zacl = zacl.clone();
                async move { s.client.set_acl(path, &zacl, aversion).await.map(Stat::from) }
            })
            .await
        }
        .await;
        self.audited("set_acl", vec![path.to_string()], r)
    }

    /// Commits an atomic multi-operation transaction.
    ///
    /// On failure, `ZkError::op_index` identifies the failing operation.
    pub async fn transaction(&self, ops: &[TxnOp]) -> Result<Vec<TxnResult>> {
        let paths: Vec<String> = ops.iter().map(|o| o.path().to_string()).collect();
        let r = self.transaction_inner(ops).await;
        self.audited("transaction", paths, r)
    }

    async fn transaction_inner(&self, ops: &[TxnOp]) -> Result<Vec<TxnResult>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        for (i, op) in ops.iter().enumerate() {
            let validated = match op {
                TxnOp::Create { mode, .. } if mode.is_sequential() => path::validate_sequential_prefix(op.path()),
                _ => path::validate(op.path()),
            };
            validated.map_err(|e| e.with_op_index(i))?;
            if let TxnOp::Create { acl, mode, ttl_ms, .. } = op {
                CreateOptions { mode: *mode, acl: acl.clone(), ttl_ms: *ttl_ms, create_parents: false }
                    .validate()
                    .map_err(|e| e.with_op_index(i))?;
            }
            if op.is_write() {
                self.check_write_allowed(op.path()).map_err(|e| e.with_op_index(i))?;
            }
        }
        let zacls: Vec<Vec<zk::Acl>> = ops
            .iter()
            .map(|op| match op {
                TxnOp::Create { acl, .. } => acl.iter().map(Acl::to_zk).collect(),
                _ => Vec::new(),
            })
            .collect();
        let session = self.op_session().await?;
        self.inner.stats.requests.fetch_add(1, Ordering::Relaxed);
        let fut = async {
            let mut w = session.client.new_multi_writer();
            for (i, op) in ops.iter().enumerate() {
                let r = match op {
                    TxnOp::Create { path, data, mode, ttl_ms, .. } => {
                        let mut o = mode.to_zk().with_acls(zk::Acls::new(&zacls[i]));
                        if let Some(ms) = ttl_ms {
                            o = o.with_ttl(Duration::from_millis(*ms));
                        }
                        w.add_create(path, data, &o)
                    },
                    TxnOp::SetData { path, data, version } => w.add_set_data(path, data, *version),
                    TxnOp::Delete { path, version } => w.add_delete(path, *version),
                    TxnOp::Check { path, version } => w.add_check_version(path, *version),
                };
                r.map_err(|e| ZkError::from(e).with_op_index(i))?;
            }
            w.commit().await.map_err(|e| match e {
                zk::MultiWriteError::RequestFailed { source } => ZkError::from(source),
                zk::MultiWriteError::OperationFailed { index, source } => {
                    ZkError::from(source).with_op_index(index).with_path(ops[index].path())
                },
            })
        };
        let results = match tokio::time::timeout(self.inner.config.operation_timeout(), fut).await {
            Ok(r) => r,
            Err(_) => Err(ZkError::timeout("transaction")),
        }
        .map_err(|e| e.into_write_outcome())?;
        Ok(results
            .into_iter()
            .map(|r| match r {
                zk::MultiWriteResult::Create { path, stat } => {
                    let stat = Stat::from(stat);
                    TxnResult::Create { path, stat: (!stat.is_invalid()).then_some(stat) }
                },
                zk::MultiWriteResult::SetData { stat } => TxnResult::SetData { stat: stat.into() },
                zk::MultiWriteResult::Delete => TxnResult::Delete,
                zk::MultiWriteResult::Check => TxnResult::Check,
                _ => TxnResult::Check,
            })
            .collect())
    }

    // ------------------------------------------------------------ subtrees

    /// Breadth-first walk of a subtree with bounded concurrency.
    pub async fn walk(&self, root: &str, options: &WalkOptions) -> Result<WalkResult> {
        path::validate(root)?;
        let root_stat = self.exists(root).await?.ok_or_else(|| {
            ZkError::new(ErrorKind::NoNode, "walk root does not exist").with_path(root)
        })?;
        let mut nodes = vec![WalkEntry { path: root.to_string(), depth: 0, stat: root_stat }];
        let mut skipped = Vec::new();
        let mut frontier = vec![(root.to_string(), 0usize, root_stat)];
        let mut truncated = false;
        let session = self.op_session().await?;
        'outer: while !frontier.is_empty() {
            let mut next = Vec::new();
            for chunk in frontier.chunks(options.concurrency.max(1)) {
                let futs: Vec<_> = chunk
                    .iter()
                    .filter(|(_, d, st)| st.num_children > 0 && options.max_depth.map(|m| *d < m).unwrap_or(true))
                    .map(|(p, d, _)| {
                        let c = session.client.clone();
                        let p = p.clone();
                        let d = *d;
                        async move {
                            let r = c.get_children(&p).await;
                            (p, d, r)
                        }
                    })
                    .collect();
                let listed = tokio::time::timeout(self.inner.config.operation_timeout(), join_all(futs))
                    .await
                    .map_err(|_| ZkError::timeout("walk"))?;
                let mut child_paths = Vec::new();
                for (p, d, r) in listed {
                    match r {
                        Ok((mut names, _)) => {
                            names.sort();
                            for n in names {
                                let cp = path::join(&p, &n);
                                if !options.include_system && root == "/" && cp == "/zookeeper" {
                                    continue;
                                }
                                child_paths.push((cp, d + 1));
                            }
                        },
                        Err(zk::Error::NoNode) => skipped.push((p, ErrorKind::NoNode)),
                        Err(zk::Error::NoAuth) => skipped.push((p, ErrorKind::NoAuth)),
                        Err(e) => return Err(ZkError::from(e).with_path(p)),
                    }
                }
                for sub in child_paths.chunks(512) {
                    let futs: Vec<_> = sub
                        .iter()
                        .map(|(p, d)| {
                            let c = session.client.clone();
                            let p = p.clone();
                            let d = *d;
                            async move {
                                let r = c.check_stat(&p).await;
                                (p, d, r)
                            }
                        })
                        .collect();
                    let stats = tokio::time::timeout(self.inner.config.operation_timeout(), join_all(futs))
                        .await
                        .map_err(|_| ZkError::timeout("walk"))?;
                    for (p, d, r) in stats {
                        match r {
                            Ok(Some(st)) => {
                                if nodes.len() >= options.max_nodes {
                                    truncated = true;
                                    break 'outer;
                                }
                                let st = Stat::from(st);
                                nodes.push(WalkEntry { path: p.clone(), depth: d, stat: st });
                                next.push((p, d, st));
                            },
                            Ok(None) => skipped.push((p, ErrorKind::NoNode)),
                            Err(e) => return Err(ZkError::from(e).with_path(p)),
                        }
                    }
                }
            }
            frontier = next;
        }
        Ok(WalkResult { nodes, truncated, skipped })
    }

    /// Deletes a subtree, deepest nodes first.
    ///
    /// Safety properties: the safety policy is checked for every node before
    /// anything is deleted; the node count is bounded; nodes created
    /// concurrently cause a bounded number of re-scans; `version` (if given)
    /// is checked on the root before starting and enforced on the final delete.
    pub async fn delete_recursive(&self, root: &str, options: &DeleteRecursiveOptions) -> Result<DeleteRecursiveReport> {
        let r = self.delete_recursive_inner(root, options).await;
        let paths = match &r {
            Ok(rep) if !rep.dry_run => rep.paths.clone(),
            Ok(_) => return r,
            Err(_) => vec![root.to_string()],
        };
        self.audited("delete_recursive", paths, r)
    }

    async fn delete_recursive_inner(&self, root: &str, options: &DeleteRecursiveOptions) -> Result<DeleteRecursiveReport> {
        path::validate(root)?;
        if root == "/" {
            return Err(ZkError::bad_arguments("refusing to delete the root node recursively"));
        }
        if path::is_within(root, "/zookeeper") {
            return Err(ZkError::policy("refusing to delete ZooKeeper system nodes"));
        }
        let limit = options.limit.unwrap_or(self.inner.config.safety.max_recursive_nodes);
        let walk = self.walk(root, &WalkOptions { max_nodes: limit.saturating_add(1), ..Default::default() }).await?;
        if walk.nodes.len() > limit || walk.truncated {
            return Err(ZkError::new(
                ErrorKind::LimitExceeded,
                format!("subtree at {root} has more than {limit} nodes; raise the limit to delete it"),
            )
            .with_path(root));
        }
        if let Some(v) = options.version {
            if walk.nodes[0].stat.version != v {
                return Err(ZkError::new(ErrorKind::BadVersion, format!("expected version {v}, found {}", walk.nodes[0].stat.version))
                    .with_path(root));
            }
        }
        let mut order: Vec<&WalkEntry> = walk.nodes.iter().collect();
        order.sort_by(|a, b| b.depth.cmp(&a.depth).then_with(|| a.path.cmp(&b.path)));
        let mut paths: Vec<String> = order.iter().map(|e| e.path.clone()).collect();
        if options.dry_run {
            let blocked = paths.iter().filter(|p| self.check_write_allowed(p).is_err()).cloned().collect();
            return Ok(DeleteRecursiveReport { paths, dry_run: true, rescans: 0, blocked });
        }
        for p in &paths {
            self.check_write_allowed(p)?;
        }
        let mut deleted = Vec::with_capacity(paths.len());
        let mut rescans = 0;
        // Group by depth: nodes at the same depth are deleted concurrently.
        loop {
            let mut retry_needed = false;
            let mut i = 0;
            while i < paths.len() {
                let depth = path::depth(&paths[i]);
                let mut j = i;
                while j < paths.len() && path::depth(&paths[j]) == depth {
                    j += 1;
                }
                let level = &paths[i..j];
                let session = self.op_session().await?;
                let futs: Vec<_> = level
                    .iter()
                    .map(|p| {
                        let c = session.client.clone();
                        let p = p.clone();
                        let version = if p == root { options.version } else { None };
                        async move {
                            let r = c.delete(&p, version).await;
                            (p, r)
                        }
                    })
                    .collect();
                let results = tokio::time::timeout(self.inner.config.operation_timeout(), join_all(futs))
                    .await
                    .map_err(|_| ZkError::timeout("delete_recursive").into_write_outcome())?;
                for (p, r) in results {
                    match r {
                        Ok(()) => deleted.push(p),
                        Err(zk::Error::NoNode) => {},
                        Err(zk::Error::NotEmpty) => retry_needed = true,
                        Err(e) => return Err(ZkError::from(e).with_path(p).into_write_outcome()),
                    }
                }
                i = j;
            }
            if !retry_needed {
                break;
            }
            rescans += 1;
            if rescans > 5 {
                return Err(ZkError::new(ErrorKind::Inconsistent, "subtree keeps changing during recursive delete").with_path(root));
            }
            let walk = self.walk(root, &WalkOptions { max_nodes: limit.saturating_add(1), ..Default::default() }).await;
            let walk = match walk {
                Err(e) if e.kind == ErrorKind::NoNode => break,
                other => other?,
            };
            if walk.nodes.len() > limit {
                return Err(ZkError::new(ErrorKind::LimitExceeded, "subtree grew past the limit during delete").with_path(root));
            }
            let mut order: Vec<&WalkEntry> = walk.nodes.iter().collect();
            order.sort_by(|a, b| b.depth.cmp(&a.depth).then_with(|| a.path.cmp(&b.path)));
            for e in &order {
                self.check_write_allowed(&e.path)?;
            }
            paths = order.iter().map(|e| e.path.clone()).collect();
        }
        Ok(DeleteRecursiveReport { paths: deleted, dry_run: false, rescans, blocked: Vec::new() })
    }

    /// Exports a subtree (data, and optionally ACLs and stats).
    pub async fn export(&self, root: &str, with_acl: bool, with_stat: bool, max_nodes: usize) -> Result<(Vec<TreeNode>, bool)> {
        let walk = self.walk(root, &WalkOptions { max_nodes, ..Default::default() }).await?;
        let session = self.op_session().await?;
        let mut out = Vec::with_capacity(walk.nodes.len());
        for chunk in walk.nodes.chunks(256) {
            let futs: Vec<_> = chunk
                .iter()
                .map(|e| {
                    let c = session.client.clone();
                    let p = e.path.clone();
                    async move {
                        let d = c.get_data(&p).await;
                        let a = if with_acl { Some(c.get_acl(&p).await) } else { None };
                        (p, d, a)
                    }
                })
                .collect();
            let results = tokio::time::timeout(self.inner.config.operation_timeout() * 4, join_all(futs))
                .await
                .map_err(|_| ZkError::timeout("export"))?;
            for (p, d, a) in results {
                let (data, stat) = match d {
                    Ok(v) => v,
                    Err(zk::Error::NoNode) => continue,
                    Err(e) => return Err(ZkError::from(e).with_path(p)),
                };
                let acl = match a {
                    Some(Ok((acl, _))) => Some(acl.iter().map(Acl::from_zk).collect()),
                    Some(Err(zk::Error::NoNode)) => continue,
                    Some(Err(e)) => return Err(ZkError::from(e).with_path(p)),
                    None => None,
                };
                let rel = path::relative(&p, root).unwrap_or("").to_string();
                out.push(TreeNode { rel, data, acl, stat: with_stat.then(|| stat.into()) });
            }
        }
        out.sort_by(|a, b| a.rel.cmp(&b.rel));
        Ok((out, walk.truncated))
    }

    /// Imports nodes under `root`. Existing nodes are updated only if `overwrite`.
    /// Returns created and updated paths.
    pub async fn import(&self, root: &str, nodes: &[TreeNode], overwrite: bool, keep_acl: bool) -> Result<(Vec<String>, Vec<String>)> {
        path::validate(root)?;
        let mut sorted: Vec<&TreeNode> = nodes.iter().collect();
        sorted.sort_by_key(|n| (if n.rel.is_empty() { 0 } else { n.rel.matches('/').count() + 1 }, n.rel.clone()));
        for n in &sorted {
            let p = if n.rel.is_empty() { root.to_string() } else { path::join(root, &n.rel) };
            path::validate(&p)?;
            self.check_write_allowed(&p)?;
        }
        let (mut created, mut updated) = (Vec::new(), Vec::new());
        if let Some(parent) = path::parent(root).filter(|p| *p != "/") {
            self.ensure_path(parent, &Acl::anyone_all()).await?;
        }
        for n in sorted {
            let p = if n.rel.is_empty() { root.to_string() } else { path::join(root, &n.rel) };
            let acl = if keep_acl { n.acl.clone().unwrap_or_else(Acl::anyone_all) } else { Acl::anyone_all() };
            match self.create(&p, &n.data, &CreateOptions::persistent().with_acl(acl)).await {
                Ok(_) => created.push(p),
                Err(e) if e.kind == ErrorKind::NodeExists => {
                    if overwrite {
                        self.set_data(&p, &n.data, None).await?;
                        updated.push(p);
                    }
                },
                Err(e) => return Err(e),
            }
        }
        Ok((created, updated))
    }

    /// Copies a subtree. Fails if the destination exists. Ephemeral sources are
    /// copied as persistent nodes.
    pub async fn copy_tree(&self, from: &str, to: &str, with_acl: bool, max_nodes: usize) -> Result<Vec<String>> {
        path::validate(from)?;
        path::validate(to)?;
        if path::is_within(to, from) {
            return Err(ZkError::bad_arguments("destination lies inside the source subtree"));
        }
        if self.exists(to).await?.is_some() {
            return Err(ZkError::new(ErrorKind::NodeExists, "destination exists").with_path(to));
        }
        let (nodes, truncated) = self.export(from, with_acl, false, max_nodes.saturating_add(1)).await?;
        if truncated || nodes.len() > max_nodes {
            return Err(ZkError::new(ErrorKind::LimitExceeded, format!("source has more than {max_nodes} nodes")).with_path(from));
        }
        let (created, _) = self.import(to, &nodes, false, with_acl).await?;
        Ok(created)
    }

    /// Moves a subtree. Small subtrees (<= 500 nodes) move atomically in one
    /// transaction with version checks; larger ones are refused unless
    /// `allow_non_atomic` is set, in which case copy-then-delete is used.
    pub async fn move_tree(&self, from: &str, to: &str, allow_non_atomic: bool, max_nodes: usize) -> Result<Vec<String>> {
        path::validate(from)?;
        path::validate(to)?;
        if from == "/" || path::is_within(to, from) {
            return Err(ZkError::bad_arguments("invalid move destination"));
        }
        let walk = self.walk(from, &WalkOptions { max_nodes: max_nodes.saturating_add(1), ..Default::default() }).await?;
        if walk.truncated || walk.nodes.len() > max_nodes {
            return Err(ZkError::new(ErrorKind::LimitExceeded, format!("source has more than {max_nodes} nodes")).with_path(from));
        }
        if walk.nodes.iter().any(|e| e.stat.is_ephemeral()) {
            return Err(ZkError::bad_arguments("cannot move a subtree containing ephemeral nodes"));
        }
        if walk.nodes.len() <= 500 {
            let (nodes, _) = self.export(from, true, true, max_nodes).await?;
            let mut ops = Vec::new();
            for n in &nodes {
                let dst = if n.rel.is_empty() { to.to_string() } else { path::join(to, &n.rel) };
                ops.push(TxnOp::Create {
                    path: dst,
                    data: n.data.clone(),
                    mode: CreateMode::Persistent,
                    acl: n.acl.clone().unwrap_or_else(Acl::anyone_all),
                    ttl_ms: None,
                });
            }
            let mut deletes: Vec<&TreeNode> = nodes.iter().collect();
            deletes.sort_by_key(|n| std::cmp::Reverse(if n.rel.is_empty() { 0 } else { n.rel.matches('/').count() + 1 }));
            for n in deletes {
                let src = if n.rel.is_empty() { from.to_string() } else { path::join(from, &n.rel) };
                ops.push(TxnOp::Delete { path: src, version: n.stat.map(|s| s.version) });
            }
            self.transaction(&ops).await?;
            return Ok(nodes.iter().map(|n| if n.rel.is_empty() { to.to_string() } else { path::join(to, &n.rel) }).collect());
        }
        if !allow_non_atomic {
            return Err(ZkError::new(
                ErrorKind::LimitExceeded,
                "subtree too large for an atomic move; pass allow_non_atomic to copy then delete",
            ));
        }
        let created = self.copy_tree(from, to, true, max_nodes).await?;
        self.delete_recursive(from, &DeleteRecursiveOptions { limit: Some(max_nodes), ..Default::default() }).await?;
        Ok(created)
    }

    /// Updates the ensemble membership (`reconfig`, 3.5+, requires reconfigEnabled).
    pub async fn reconfig(
        &self,
        joining: &[String],
        leaving: &[String],
        new_members: Option<&[String]>,
        expected_config_version: Option<i64>,
    ) -> Result<NodeData> {
        if self.inner.config.safety.read_only {
            return Err(ZkError::policy("cluster profile is read-only"));
        }
        let r = self
            .run("reconfig", None, Idempotency::Write, |s| async move {
                let update = match new_members {
                    Some(m) => zk::EnsembleUpdate::New { ensemble: m.iter().map(String::as_str) },
                    None => zk::EnsembleUpdate::Incremental {
                        joinings: joining.iter().map(String::as_str),
                        leavings: leaving.iter().map(String::as_str),
                    },
                };
                s.client.update_ensemble(update, expected_config_version).await.map(|(data, stat)| NodeData { data, stat: stat.into() })
            })
            .await;
        self.audited("reconfig", vec!["/zookeeper/config".into()], r)
    }
}
