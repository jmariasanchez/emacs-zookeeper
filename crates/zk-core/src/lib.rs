//! `zk-core`: a session-supervised Apache ZooKeeper client toolkit.
//!
//! Built over [`zookeeper_client`], this crate adds the pieces a
//! Curator/Kazoo-class toolkit needs on top of the wire protocol:
//!
//! * [`ZkClient`]: connection supervision with explicit state transitions,
//!   automatic session re-establishment after expiry, credential replay and
//!   diagnostics ([`session`]).
//! * Typed, serializable operations and errors with clear write-outcome
//!   semantics ([`ops`], [`error`]).
//! * Watches that survive reconnects and session loss ([`watch`]).
//! * Local safety policy (read-only profiles, protected paths, bounded
//!   recursion) and an audit hook for every mutation.
//! * Four-letter-word server diagnostics ([`admin`]).
//!
//! It deliberately has no dependency on Emacs or any RPC transport.

pub mod admin;
pub mod config;
pub mod error;
pub mod ops;
pub mod path;
pub mod search;
pub mod session;
pub mod types;
pub mod watch;

pub use config::{AuthCredential, ClusterConfig, RetryPolicy, SafetyPolicy, SaslConfig, Secret, TlsConfig};
pub use error::{ErrorKind, Result, ZkError};
pub use ops::{
    CreateOptions, DeleteRecursiveOptions, DeleteRecursiveReport, ReadOp, ReadResult, TreeNode, TxnOp, TxnResult,
    WalkEntry, WalkOptions, WalkResult,
};
pub use search::{diff_trees, DiffOptions, DiffResult, Difference, NodeSummary, SearchMatch, SearchQuery, SearchResult};
pub use session::{
    AuditRecord, AuditSink, ClientDiagnostics, ConnectionEvent, ConnectionEventKind, ConnectionState, Session, ZkClient,
};
pub use types::{digest_id, Acl, AuthIdentity, ChildEntry, CreateMode, Created, NodeData, NodeKind, Perms, Stat};
pub use watch::{OneshotWatch, PersistentMode, WatchEvent, WatchEventKind, WatchStream};

/// Re-export of the underlying protocol client for escape hatches.
pub use zookeeper_client as raw;

/// Serde helper: `Vec<u8>` as base64 strings.
pub mod b64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD.decode(s.as_bytes()).map_err(serde::de::Error::custom)
    }

    pub fn encode(v: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(v)
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
        base64::engine::general_purpose::STANDARD.decode(s.as_bytes())
    }
}
