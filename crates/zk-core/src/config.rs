//! Cluster profile configuration.
//!
//! A [`ClusterConfig`] fully describes how to reach one ZooKeeper ensemble and
//! how this toolkit should behave against it (timeouts, retries, credentials,
//! safety policy). It is serde-deserializable so front-ends (the JSON-RPC
//! bridge, a CLI) can pass it through unchanged.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Result, ZkError};

/// A secret value. Its `Debug` and `Serialize` forms never reveal the content.
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Secret {
    /// Inline value. Prefer the other sources for anything long-lived.
    Value(String),
    /// Read from an environment variable of the bridge process.
    Env(String),
    /// Read from a file (trailing newline trimmed).
    File(PathBuf),
    /// Standard output of a command (argv form, no shell), trailing newline trimmed.
    Command(Vec<String>),
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Secret::Value(_) => f.write_str("Secret::Value(<redacted>)"),
            Secret::Env(name) => write!(f, "Secret::Env({name})"),
            Secret::File(p) => write!(f, "Secret::File({})", p.display()),
            Secret::Command(argv) => write!(f, "Secret::Command({})", argv.first().map(String::as_str).unwrap_or("")),
        }
    }
}

impl Serialize for Secret {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(Some(1))?;
        match self {
            Secret::Value(_) => map.serialize_entry("value", "<redacted>")?,
            Secret::Env(n) => map.serialize_entry("env", n)?,
            Secret::File(p) => map.serialize_entry("file", p)?,
            Secret::Command(a) => map.serialize_entry("command", a)?,
        }
        map.end()
    }
}

impl Secret {
    /// Resolves the secret to its value.
    pub fn resolve(&self) -> Result<String> {
        match self {
            Secret::Value(v) => Ok(v.clone()),
            Secret::Env(name) => std::env::var(name)
                .map_err(|_| ZkError::config(format!("environment variable {name} is not set"))),
            Secret::File(path) => {
                let s = std::fs::read_to_string(path)
                    .map_err(|e| ZkError::config(format!("cannot read secret file {}: {e}", path.display())))?;
                Ok(s.trim_end_matches(['\n', '\r']).to_string())
            },
            Secret::Command(argv) => {
                let (prog, args) =
                    argv.split_first().ok_or_else(|| ZkError::config("secret command must not be empty"))?;
                let out = std::process::Command::new(prog)
                    .args(args)
                    .stdin(std::process::Stdio::null())
                    .output()
                    .map_err(|e| ZkError::config(format!("cannot run secret command {prog}: {e}")))?;
                if !out.status.success() {
                    return Err(ZkError::config(format!("secret command {prog} exited with {}", out.status)));
                }
                let s = String::from_utf8(out.stdout)
                    .map_err(|_| ZkError::config(format!("secret command {prog} produced non UTF-8 output")))?;
                Ok(s.trim_end_matches(['\n', '\r']).to_string())
            },
        }
    }
}

/// A `addauth`-style credential, e.g. scheme `digest` with `user:password`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthCredential {
    pub scheme: String,
    pub credential: Secret,
}

/// SASL configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mechanism", rename_all = "kebab-case")]
pub enum SaslConfig {
    /// DIGEST-MD5, matching a server `DigestLoginModule` JAAS section.
    DigestMd5 { username: String, password: Secret },
    /// Kerberos GSSAPI. Requires the `gssapi` cargo feature and a valid ticket cache.
    Gssapi {
        #[serde(default)]
        username: Option<String>,
        #[serde(default)]
        hostname: Option<String>,
    },
}

/// TLS configuration. Files are PEM encoded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TlsConfig {
    /// CA bundle used to verify the server. Required unless only system roots are wanted.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// Client certificate for mutual TLS.
    #[serde(default)]
    pub cert_file: Option<PathBuf>,
    /// Client private key for mutual TLS.
    #[serde(default)]
    pub key_file: Option<PathBuf>,
    /// Verify that the server certificate matches the host name. Disabling this
    /// is insecure and is reported in diagnostics.
    #[serde(default = "default_true")]
    pub verify_hostname: bool,
}

fn default_true() -> bool {
    true
}

/// Bounded retry policy with exponential backoff and full jitter. Applied only
/// to idempotent operations (reads, `sync`, existence checks).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum attempts including the first. `1` disables retries.
    pub max_attempts: u32,
    /// First backoff delay in milliseconds.
    pub base_delay_ms: u64,
    /// Upper bound for one backoff delay in milliseconds.
    pub max_delay_ms: u64,
    /// Total time budget for all attempts in milliseconds.
    pub max_elapsed_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 4, base_delay_ms: 100, max_delay_ms: 2_000, max_elapsed_ms: 15_000 }
    }
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self { max_attempts: 1, ..Self::default() }
    }

    /// Delay before attempt `attempt` (1-based retry number), with full jitter.
    pub fn delay(&self, attempt: u32) -> Duration {
        let exp = self.base_delay_ms.saturating_mul(1u64 << attempt.min(20));
        let cap = exp.min(self.max_delay_ms).max(1);
        let jittered = rand::random::<u64>() % (cap + 1);
        Duration::from_millis(jittered.max(self.base_delay_ms.min(cap) / 2))
    }
}

/// Local safety policy enforced before any request is sent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafetyPolicy {
    /// Refuse every mutating operation.
    #[serde(default)]
    pub read_only: bool,
    /// Path patterns (see [`crate::path::matches_pattern`]) where writes require an explicit override.
    #[serde(default)]
    pub protected_paths: Vec<String>,
    /// Maximum number of nodes a recursive delete/copy may touch.
    #[serde(default = "default_recursive_limit")]
    pub max_recursive_nodes: usize,
}

fn default_recursive_limit() -> usize {
    10_000
}

impl Default for SafetyPolicy {
    fn default() -> Self {
        Self { read_only: false, protected_paths: Vec::new(), max_recursive_nodes: default_recursive_limit() }
    }
}

/// One cluster profile.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    /// Profile name, used for display and audit.
    #[serde(default = "default_name")]
    pub name: String,
    /// Connection string: `host1:2181,host2:2181[/chroot]`. Per-server `tcp://` and
    /// `tcp+tls://` prefixes are accepted.
    pub connect: String,
    /// Optional chroot applied in addition to (after) any chroot in `connect`.
    #[serde(default)]
    pub chroot: Option<String>,
    /// Requested session timeout. The server negotiates the final value.
    #[serde(default = "default_session_timeout")]
    pub session_timeout_ms: u64,
    /// Timeout for establishing a TCP connection and handshake to one server.
    #[serde(default)]
    pub connection_timeout_ms: Option<u64>,
    /// Default deadline for one operation.
    #[serde(default = "default_operation_timeout")]
    pub operation_timeout_ms: u64,
    /// Give up the initial connection after this long.
    #[serde(default = "default_connect_deadline")]
    pub connect_deadline_ms: u64,
    /// Allow connecting to read-only (partitioned) servers.
    #[serde(default)]
    pub allow_read_only: bool,
    /// Re-establish a new session automatically after expiration.
    #[serde(default = "default_true")]
    pub reconnect_on_expiry: bool,
    /// Credentials added after the session is established (`addauth`).
    #[serde(default)]
    pub auth: Vec<AuthCredential>,
    #[serde(default)]
    pub sasl: Option<SaslConfig>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub safety: SafetyPolicy,
    /// Server version hint `major.minor.patch` for protocol compatibility (e.g. "3.4.14").
    #[serde(default)]
    pub server_version: Option<String>,
    /// Hosts for four-letter-word / admin diagnostics; defaults to the hosts from `connect`.
    #[serde(default)]
    pub admin_hosts: Vec<String>,
}

fn default_name() -> String {
    "default".into()
}
fn default_session_timeout() -> u64 {
    30_000
}
fn default_operation_timeout() -> u64 {
    15_000
}
fn default_connect_deadline() -> u64 {
    30_000
}

impl ClusterConfig {
    /// A configuration with defaults for the given connection string.
    pub fn new(connect: impl Into<String>) -> Self {
        Self {
            name: default_name(),
            connect: connect.into(),
            chroot: None,
            session_timeout_ms: default_session_timeout(),
            connection_timeout_ms: None,
            operation_timeout_ms: default_operation_timeout(),
            connect_deadline_ms: default_connect_deadline(),
            allow_read_only: false,
            reconnect_on_expiry: true,
            auth: Vec::new(),
            sasl: None,
            tls: None,
            retry: RetryPolicy::default(),
            safety: SafetyPolicy::default(),
            server_version: None,
            admin_hosts: Vec::new(),
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_chroot(mut self, chroot: impl Into<String>) -> Self {
        self.chroot = Some(chroot.into());
        self
    }

    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout_ms = timeout.as_millis() as u64;
        self
    }

    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout_ms = timeout.as_millis() as u64;
        self
    }

    pub fn with_digest(mut self, user: &str, password: &str) -> Self {
        self.auth.push(AuthCredential {
            scheme: "digest".into(),
            credential: Secret::Value(format!("{user}:{password}")),
        });
        self
    }

    pub fn read_only(mut self) -> Self {
        self.safety.read_only = true;
        self
    }

    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }

    pub fn session_timeout(&self) -> Duration {
        Duration::from_millis(self.session_timeout_ms)
    }

    /// Splits `connect` into hosts and the embedded chroot.
    pub fn hosts_and_chroot(&self) -> (Vec<String>, Option<String>) {
        let (hosts, chroot) = match self.connect.find('/') {
            // Skip "tcp://" style scheme separators when searching for the chroot.
            _ if self.connect.contains("://") => split_schemed(&self.connect),
            Some(i) => (&self.connect[..i], Some(self.connect[i..].to_string())),
            None => (self.connect.as_str(), None),
        };
        let hosts = hosts.split(',').map(|h| h.trim().to_string()).filter(|h| !h.is_empty()).collect();
        (hosts, chroot)
    }

    /// The effective chroot (connect-string chroot joined with `chroot`).
    pub fn effective_chroot(&self) -> Option<String> {
        let (_, embedded) = self.hosts_and_chroot();
        match (embedded, &self.chroot) {
            (None, None) => None,
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some(crate::path::join(&a, b)),
        }
    }

    /// Validates configuration without connecting.
    pub fn validate(&self) -> Result<()> {
        let (hosts, _) = self.hosts_and_chroot();
        if hosts.is_empty() {
            return Err(ZkError::config("connect string has no hosts"));
        }
        if let Some(chroot) = self.effective_chroot() {
            crate::path::validate(&chroot).map_err(|e| ZkError::config(format!("bad chroot: {}", e.message)))?;
        }
        if self.session_timeout_ms < 1_000 {
            return Err(ZkError::config("session_timeout_ms must be at least 1000"));
        }
        if self.retry.max_attempts == 0 {
            return Err(ZkError::config("retry.max_attempts must be at least 1"));
        }
        if let Some(v) = &self.server_version {
            parse_version(v)?;
        }
        if let Some(tls) = &self.tls {
            if tls.cert_file.is_some() != tls.key_file.is_some() {
                return Err(ZkError::config("tls.cert_file and tls.key_file must be given together"));
            }
        }
        for p in &self.safety.protected_paths {
            if !p.starts_with('/') {
                return Err(ZkError::config(format!("protected path pattern {p:?} must be absolute")));
            }
        }
        Ok(())
    }
}

fn split_schemed(connect: &str) -> (&str, Option<String>) {
    // With schemes, the chroot is the first '/' that is not part of "://".
    let bytes = connect.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'/' {
            let is_scheme = i >= 1 && bytes[i - 1] == b':' || (i >= 2 && bytes[i - 1] == b'/' && bytes[i - 2] == b':');
            if !is_scheme {
                return (&connect[..i], Some(connect[i..].to_string()));
            }
        }
        i += 1;
    }
    (connect, None)
}

/// Parses `major.minor.patch`.
pub fn parse_version(v: &str) -> Result<(u32, u32, u32)> {
    let parts: Vec<&str> = v.split(['.', '-']).collect();
    let num = |i: usize| -> Result<u32> {
        parts
            .get(i)
            .unwrap_or(&"0")
            .parse::<u32>()
            .map_err(|_| ZkError::config(format!("invalid server_version {v:?}")))
    };
    Ok((num(0)?, num(1)?, num(2)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hosts_and_chroot() {
        let c = ClusterConfig::new("a:2181,b:2181/app");
        assert_eq!(c.hosts_and_chroot(), (vec!["a:2181".into(), "b:2181".into()], Some("/app".into())));
        let c = ClusterConfig::new("tcp://a:2181,tcp+tls://b:2182/app/x");
        assert_eq!(
            c.hosts_and_chroot(),
            (vec!["tcp://a:2181".into(), "tcp+tls://b:2182".into()], Some("/app/x".into()))
        );
        let c = ClusterConfig::new("a:2181/app").with_chroot("/sub");
        assert_eq!(c.effective_chroot(), Some("/app/sub".into()));
    }

    #[test]
    fn json_defaults() {
        let c: ClusterConfig = serde_json::from_str(r#"{"connect":"localhost:2181"}"#).unwrap();
        assert_eq!(c.session_timeout_ms, 30_000);
        assert!(c.reconnect_on_expiry);
        assert_eq!(c.safety.max_recursive_nodes, 10_000);
        c.validate().unwrap();
    }

    #[test]
    fn secrets_are_redacted() {
        let s = Secret::Value("hunter2".into());
        assert!(!format!("{s:?}").contains("hunter2"));
        assert!(!serde_json::to_string(&s).unwrap().contains("hunter2"));
        let c = ClusterConfig::new("h:1").with_digest("u", "hunter2");
        assert!(!format!("{c:?}").contains("hunter2"));
        assert!(!serde_json::to_string(&c).unwrap().contains("hunter2"));
    }

    #[test]
    fn secret_sources() {
        std::env::set_var("ZK_CORE_TEST_SECRET", "s3");
        assert_eq!(Secret::Env("ZK_CORE_TEST_SECRET".into()).resolve().unwrap(), "s3");
        assert_eq!(Secret::Command(vec!["echo".into(), "pw".into()]).resolve().unwrap(), "pw");
        let dir = std::env::temp_dir().join(format!("zkcore-secret-{}", std::process::id()));
        std::fs::write(&dir, "filepw\n").unwrap();
        assert_eq!(Secret::File(dir.clone()).resolve().unwrap(), "filepw");
        std::fs::remove_file(dir).ok();
    }

    #[test]
    fn validation_errors() {
        assert!(ClusterConfig::new("").validate().is_err());
        let mut c = ClusterConfig::new("h:1");
        c.session_timeout_ms = 10;
        assert!(c.validate().is_err());
        let mut c = ClusterConfig::new("h:1");
        c.server_version = Some("3.x".into());
        assert!(c.validate().is_err());
        assert_eq!(parse_version("3.4.14").unwrap(), (3, 4, 14));
    }

    #[test]
    fn retry_delay_is_bounded() {
        let p = RetryPolicy::default();
        for i in 0..30 {
            assert!(p.delay(i) <= Duration::from_millis(p.max_delay_ms));
        }
    }
}
