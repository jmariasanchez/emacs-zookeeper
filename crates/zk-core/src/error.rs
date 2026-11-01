//! Typed, serializable errors.
//!
//! Every failure surfaced by `zk-core` is a [`ZkError`] carrying a stable
//! [`ErrorKind`]. Kinds are part of the public contract (they are serialized
//! into the bridge protocol as kebab-case strings), so new kinds may be added
//! but existing ones must not be renamed.

use serde::{Deserialize, Serialize};
use zookeeper_client as zk;

/// Stable error classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum ErrorKind {
    /// The node does not exist.
    NoNode,
    /// The node already exists.
    NodeExists,
    /// The expected version did not match.
    BadVersion,
    /// The node has children.
    NotEmpty,
    /// Ephemeral nodes may not have children.
    NoChildrenForEphemerals,
    /// The session lacks permission for this operation.
    NoAuth,
    /// Authentication failed. This is terminal for the session.
    AuthFailed,
    /// The ACL is invalid or empty.
    InvalidAcl,
    /// The session expired.
    SessionExpired,
    /// The session moved to another server.
    SessionMoved,
    /// The connection was lost before a reply was received (reads only).
    ConnectionLoss,
    /// A write was sent but no reply was received. It may or may not have been
    /// applied. Callers must re-read before deciding to retry.
    OutcomeUnknown,
    /// There is currently no usable session (connecting or reconnecting).
    NotConnected,
    /// The operation did not complete within its deadline.
    Timeout,
    /// The session or client has been closed.
    Closed,
    /// A write was sent to a read-only server.
    NotReadOnly,
    /// The server does not implement this operation.
    Unimplemented,
    /// Dynamic reconfiguration is disabled on the server.
    ReconfigDisabled,
    /// A quota was exceeded.
    QuotaExceeded,
    /// The server throttled the request.
    Throttled,
    /// Invalid arguments (bad path, bad mode combination, ...).
    BadArguments,
    /// A local safety policy (read-only profile, protected path) refused the operation.
    PolicyDenied,
    /// The operation was cancelled locally.
    Cancelled,
    /// Observed state violates an invariant (e.g. a recipe's node vanished).
    Inconsistent,
    /// A configured limit (node count, bytes) was exceeded.
    LimitExceeded,
    /// Local I/O failure (reading certificates, admin sockets...).
    Io,
    /// Invalid configuration.
    Config,
    /// Unexpected data on the wire or in a znode.
    Protocol,
    /// Anything else.
    Other,
}

impl ErrorKind {
    /// Stable kebab-case name.
    pub fn as_str(self) -> &'static str {
        use ErrorKind::*;
        match self {
            NoNode => "no-node",
            NodeExists => "node-exists",
            BadVersion => "bad-version",
            NotEmpty => "not-empty",
            NoChildrenForEphemerals => "no-children-for-ephemerals",
            NoAuth => "no-auth",
            AuthFailed => "auth-failed",
            InvalidAcl => "invalid-acl",
            SessionExpired => "session-expired",
            SessionMoved => "session-moved",
            ConnectionLoss => "connection-loss",
            OutcomeUnknown => "outcome-unknown",
            NotConnected => "not-connected",
            Timeout => "timeout",
            Closed => "closed",
            NotReadOnly => "not-read-only",
            Unimplemented => "unimplemented",
            ReconfigDisabled => "reconfig-disabled",
            QuotaExceeded => "quota-exceeded",
            Throttled => "throttled",
            BadArguments => "bad-arguments",
            PolicyDenied => "policy-denied",
            Cancelled => "cancelled",
            Inconsistent => "inconsistent",
            LimitExceeded => "limit-exceeded",
            Io => "io",
            Config => "config",
            Protocol => "protocol",
            Other => "other",
        }
    }

    /// Whether an idempotent operation failing with this kind may be retried.
    pub fn is_retryable(self) -> bool {
        matches!(self, ErrorKind::ConnectionLoss | ErrorKind::Throttled | ErrorKind::NotConnected)
    }

    /// Whether this kind means the session that issued the request is gone.
    pub fn is_session_terminal(self) -> bool {
        matches!(self, ErrorKind::SessionExpired | ErrorKind::AuthFailed | ErrorKind::Closed)
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error type of every `zk-core` operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub struct ZkError {
    pub kind: ErrorKind,
    pub message: String,
    /// The path the failing operation targeted, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Index of the failing operation in a multi-op transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_index: Option<usize>,
}

impl std::fmt::Display for ZkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)?;
        if let Some(path) = &self.path {
            write!(f, " (path {path})")?;
        }
        if let Some(i) = self.op_index {
            write!(f, " (operation #{i})")?;
        }
        Ok(())
    }
}

impl ZkError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into(), path: None, op_index: None }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        if self.path.is_none() {
            self.path = Some(path.into());
        }
        self
    }

    pub fn with_op_index(mut self, index: usize) -> Self {
        self.op_index = Some(index);
        self
    }

    pub fn bad_arguments(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::BadArguments, message)
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, message)
    }

    pub fn policy(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::PolicyDenied, message)
    }

    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "operation cancelled")
    }

    pub fn timeout(what: &str) -> Self {
        Self::new(ErrorKind::Timeout, format!("{what} timed out"))
    }

    /// Converts a transport-level failure of a *write* into
    /// [`ErrorKind::OutcomeUnknown`]: the request may have been applied.
    pub fn into_write_outcome(mut self) -> Self {
        if matches!(self.kind, ErrorKind::ConnectionLoss | ErrorKind::Timeout) {
            self.message = format!(
                "{} after the write was sent; it may or may not have been applied, re-read before retrying",
                self.kind
            );
            self.kind = ErrorKind::OutcomeUnknown;
        }
        self
    }

    /// Annotates `Unimplemented` errors with the server version that introduced the feature.
    pub fn requires_version(mut self, feature: &str, version: &str) -> Self {
        if self.kind == ErrorKind::Unimplemented {
            self.message = format!("{feature} requires ZooKeeper {version} or newer");
        }
        self
    }

    pub fn is(&self, kind: ErrorKind) -> bool {
        self.kind == kind
    }
}

impl From<zk::Error> for ZkError {
    fn from(err: zk::Error) -> Self {
        use zk::Error as E;
        let kind = match &err {
            E::NoNode => ErrorKind::NoNode,
            E::NodeExists => ErrorKind::NodeExists,
            E::BadVersion => ErrorKind::BadVersion,
            E::NotEmpty => ErrorKind::NotEmpty,
            E::NoChildrenForEphemerals => ErrorKind::NoChildrenForEphemerals,
            E::NoAuth => ErrorKind::NoAuth,
            E::AuthFailed => ErrorKind::AuthFailed,
            E::InvalidAcl => ErrorKind::InvalidAcl,
            E::SessionExpired => ErrorKind::SessionExpired,
            E::SessionMoved => ErrorKind::SessionMoved,
            E::ConnectionLoss => ErrorKind::ConnectionLoss,
            E::Timeout => ErrorKind::Timeout,
            E::ClientClosed => ErrorKind::Closed,
            E::NotReadOnly => ErrorKind::NotReadOnly,
            E::Unimplemented => ErrorKind::Unimplemented,
            E::ReconfigDisabled => ErrorKind::ReconfigDisabled,
            E::QuotaExceeded => ErrorKind::QuotaExceeded,
            E::Throttled => ErrorKind::Throttled,
            E::BadArguments(_) => ErrorKind::BadArguments,
            E::NoHosts => ErrorKind::NotConnected,
            E::RuntimeInconsistent => ErrorKind::Inconsistent,
            E::UnmarshalError { .. } | E::MarshallingError => ErrorKind::Protocol,
            _ => ErrorKind::Other,
        };
        ZkError::new(kind, err.to_string())
    }
}

impl From<std::io::Error> for ZkError {
    fn from(err: std::io::Error) -> Self {
        ZkError::new(ErrorKind::Io, err.to_string())
    }
}

/// Result alias used throughout `zk-core`.
pub type Result<T, E = ZkError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_serialize_as_kebab_case() {
        let json = serde_json::to_string(&ErrorKind::NoChildrenForEphemerals).unwrap();
        assert_eq!(json, "\"no-children-for-ephemerals\"");
        for kind in [ErrorKind::NoNode, ErrorKind::OutcomeUnknown, ErrorKind::PolicyDenied] {
            let json = serde_json::to_string(&kind).unwrap();
            assert_eq!(json, format!("\"{}\"", kind.as_str()));
        }
    }

    #[test]
    fn write_outcome_conversion() {
        let e = ZkError::from(zk::Error::ConnectionLoss).into_write_outcome();
        assert_eq!(e.kind, ErrorKind::OutcomeUnknown);
        let e = ZkError::from(zk::Error::BadVersion).into_write_outcome();
        assert_eq!(e.kind, ErrorKind::BadVersion);
    }

    #[test]
    fn display_includes_context() {
        let e = ZkError::new(ErrorKind::NoNode, "node not exists").with_path("/a").with_op_index(2);
        assert_eq!(e.to_string(), "no-node: node not exists (path /a) (operation #2)");
    }
}
