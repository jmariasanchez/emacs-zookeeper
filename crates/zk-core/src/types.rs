//! Serializable domain types: stats, ACLs, create modes, node data.

use serde::{Deserialize, Serialize};
use zookeeper_client as zk;

use crate::error::{Result, ZkError};

/// Znode metadata (`Stat`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Stat {
    pub czxid: i64,
    pub mzxid: i64,
    pub pzxid: i64,
    /// Creation time, milliseconds since the epoch.
    pub ctime: i64,
    /// Last modification time, milliseconds since the epoch.
    pub mtime: i64,
    pub version: i32,
    pub cversion: i32,
    pub aversion: i32,
    /// Session id owning this node if ephemeral, else 0.
    pub ephemeral_owner: i64,
    pub data_length: i32,
    pub num_children: i32,
}

impl Stat {
    pub fn is_ephemeral(&self) -> bool {
        self.ephemeral_owner != 0 && !self.is_ttl() && !self.is_container()
    }

    /// TTL nodes encode `0xFF` in the top byte of `ephemeralOwner`
    /// (see ZooKeeper `EphemeralType`).
    ///
    /// Note: servers report `ephemeralOwner = 0` to clients for container and
    /// TTL nodes (`DataTree.getClientEphemeralOwner`), so over the wire these
    /// are indistinguishable from persistent nodes. These helpers only apply to
    /// persisted stats (snapshots, transaction logs).
    pub fn is_ttl(&self) -> bool {
        (self.ephemeral_owner as u64) >> 56 == 0xFF
    }

    /// Container nodes use `Long.MIN_VALUE` as `ephemeralOwner`.
    pub fn is_container(&self) -> bool {
        self.ephemeral_owner == i64::MIN
    }

    /// Node kind derived from the stat. Over the client protocol this is
    /// either `Persistent` or `Ephemeral`; see [`Stat::is_ttl`].
    pub fn kind(&self) -> NodeKind {
        if self.is_container() {
            NodeKind::Container
        } else if self.is_ttl() {
            NodeKind::Ttl
        } else if self.ephemeral_owner != 0 {
            NodeKind::Ephemeral
        } else {
            NodeKind::Persistent
        }
    }

    /// TTL in milliseconds for TTL nodes.
    pub fn ttl_ms(&self) -> Option<i64> {
        self.is_ttl().then_some(self.ephemeral_owner & 0x00FF_FFFF_FFFF_FFFF)
    }

    /// Whether the server returned a placeholder stat (old servers, see ZOOKEEPER-1297).
    pub fn is_invalid(&self) -> bool {
        self.czxid == -1
    }
}

impl From<zk::Stat> for Stat {
    fn from(s: zk::Stat) -> Self {
        Stat {
            czxid: s.czxid,
            mzxid: s.mzxid,
            pzxid: s.pzxid,
            ctime: s.ctime,
            mtime: s.mtime,
            version: s.version,
            cversion: s.cversion,
            aversion: s.aversion,
            ephemeral_owner: s.ephemeral_owner,
            data_length: s.data_length,
            num_children: s.num_children,
        }
    }
}

/// Node kind as observed through its stat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeKind {
    Persistent,
    Ephemeral,
    Container,
    Ttl,
}

/// Znode creation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum CreateMode {
    #[default]
    Persistent,
    PersistentSequential,
    Ephemeral,
    EphemeralSequential,
    /// Deleted by the server once its last child is removed (3.5.3+).
    Container,
    /// Persistent with a TTL (requires `zookeeper.extendedTypesEnabled`).
    PersistentWithTtl,
    /// Persistent sequential with a TTL.
    PersistentSequentialWithTtl,
}

impl CreateMode {
    pub fn is_sequential(self) -> bool {
        matches!(self, Self::PersistentSequential | Self::EphemeralSequential | Self::PersistentSequentialWithTtl)
    }

    pub fn is_ephemeral(self) -> bool {
        matches!(self, Self::Ephemeral | Self::EphemeralSequential)
    }

    pub fn needs_ttl(self) -> bool {
        matches!(self, Self::PersistentWithTtl | Self::PersistentSequentialWithTtl)
    }

    pub(crate) fn to_zk(self) -> zk::CreateMode {
        match self {
            Self::Persistent | Self::PersistentWithTtl => zk::CreateMode::Persistent,
            Self::PersistentSequential | Self::PersistentSequentialWithTtl => zk::CreateMode::PersistentSequential,
            Self::Ephemeral => zk::CreateMode::Ephemeral,
            Self::EphemeralSequential => zk::CreateMode::EphemeralSequential,
            Self::Container => zk::CreateMode::Container,
        }
    }
}

/// Permission bit set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Perms(pub i32);

impl Perms {
    pub const READ: Perms = Perms(1);
    pub const WRITE: Perms = Perms(2);
    pub const CREATE: Perms = Perms(4);
    pub const DELETE: Perms = Perms(8);
    pub const ADMIN: Perms = Perms(16);
    pub const ALL: Perms = Perms(31);
    pub const NONE: Perms = Perms(0);

    pub fn has(self, other: Perms) -> bool {
        self.0 & other.0 == other.0
    }

    /// `zkCli`-style letters: `cdrwa`.
    pub fn to_letters(self) -> String {
        let mut s = String::new();
        for (bit, c) in [(Self::CREATE, 'c'), (Self::DELETE, 'd'), (Self::READ, 'r'), (Self::WRITE, 'w'), (Self::ADMIN, 'a')] {
            if self.has(bit) {
                s.push(c);
            }
        }
        s
    }

    /// Parses `cdrwa` letters (any order) or the words `all` / `none`.
    pub fn from_letters(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "all" => return Ok(Self::ALL),
            "none" | "" => return Ok(Self::NONE),
            _ => {},
        }
        let mut p = 0;
        for c in s.chars() {
            p |= match c.to_ascii_lowercase() {
                'c' => 4,
                'd' => 8,
                'r' => 1,
                'w' => 2,
                'a' => 16,
                _ => return Err(ZkError::bad_arguments(format!("invalid permission letter {c:?} in {s:?}"))),
            };
        }
        Ok(Perms(p))
    }
}

impl Serialize for Perms {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_letters())
    }
}

impl<'de> Deserialize<'de> for Perms {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Bits(i32),
            Letters(String),
        }
        match Repr::deserialize(d)? {
            Repr::Bits(b) if (0..=31).contains(&b) => Ok(Perms(b)),
            Repr::Bits(b) => Err(serde::de::Error::custom(format!("permission bits out of range: {b}"))),
            Repr::Letters(s) => Perms::from_letters(&s).map_err(|e| serde::de::Error::custom(e.message)),
        }
    }
}

/// One ACL entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Acl {
    pub perms: Perms,
    pub scheme: String,
    pub id: String,
}

impl Acl {
    pub fn new(perms: Perms, scheme: &str, id: &str) -> Self {
        Self { perms, scheme: scheme.into(), id: id.into() }
    }

    /// `world:anyone:cdrwa`
    pub fn anyone_all() -> Vec<Acl> {
        vec![Acl::new(Perms::ALL, "world", "anyone")]
    }

    /// `world:anyone:r`
    pub fn anyone_read() -> Vec<Acl> {
        vec![Acl::new(Perms::READ, "world", "anyone")]
    }

    /// `auth::cdrwa` (all authenticated identities of the creating session).
    pub fn creator_all() -> Vec<Acl> {
        vec![Acl::new(Perms::ALL, "auth", "")]
    }

    /// A `digest` ACL for `user` with `password`, computing the server-side
    /// `user:base64(sha1(user:password))` identity.
    pub fn digest(perms: Perms, user: &str, password: &str) -> Acl {
        Acl::new(perms, "digest", &digest_id(user, password))
    }

    /// Parses `scheme:id:perms` (zkCli syntax). The id may itself contain ':'
    /// (e.g. digest `user:hash`, ip ranges). `perms` is the final component.
    pub fn parse(s: &str) -> Result<Acl> {
        let (rest, perms) =
            s.rsplit_once(':').ok_or_else(|| ZkError::bad_arguments(format!("invalid ACL {s:?}: expected scheme:id:perms")))?;
        let (scheme, id) =
            rest.split_once(':').ok_or_else(|| ZkError::bad_arguments(format!("invalid ACL {s:?}: expected scheme:id:perms")))?;
        if scheme.is_empty() {
            return Err(ZkError::bad_arguments(format!("invalid ACL {s:?}: empty scheme")));
        }
        Ok(Acl::new(Perms::from_letters(perms)?, scheme, id))
    }

    /// Parses a comma-separated ACL list.
    pub fn parse_list(s: &str) -> Result<Vec<Acl>> {
        // Commas cannot appear inside scheme/id for the built-in schemes.
        s.split(',').map(str::trim).filter(|p| !p.is_empty()).map(Acl::parse).collect()
    }

    pub fn to_cli_string(&self) -> String {
        format!("{}:{}:{}", self.scheme, self.id, self.perms.to_letters())
    }

    pub(crate) fn to_zk(&self) -> zk::Acl {
        zk::Acl::new(zk::Permission::from(self.perms), zk::AuthId::new(&self.scheme, &self.id))
    }

    pub(crate) fn from_zk(acl: &zk::Acl) -> Self {
        Acl { perms: Perms::from(acl.permission()), scheme: acl.scheme().into(), id: acl.id().into() }
    }
}

impl From<Perms> for zk::Permission {
    fn from(p: Perms) -> Self {
        let mut out = zk::Permission::NONE;
        for (bit, zp) in [
            (Perms::READ, zk::Permission::READ),
            (Perms::WRITE, zk::Permission::WRITE),
            (Perms::CREATE, zk::Permission::CREATE),
            (Perms::DELETE, zk::Permission::DELETE),
            (Perms::ADMIN, zk::Permission::ADMIN),
        ] {
            if p.has(bit) {
                out = out | zp;
            }
        }
        out
    }
}

impl From<zk::Permission> for Perms {
    fn from(p: zk::Permission) -> Self {
        let mut out = 0;
        for (bit, zp) in [
            (1, zk::Permission::READ),
            (2, zk::Permission::WRITE),
            (4, zk::Permission::CREATE),
            (8, zk::Permission::DELETE),
            (16, zk::Permission::ADMIN),
        ] {
            if p.has(zp) {
                out |= bit;
            }
        }
        Perms(out)
    }
}

/// `user:base64(sha1("user:password"))`, the server's `DigestAuthenticationProvider` id.
pub fn digest_id(user: &str, password: &str) -> String {
    use base64::Engine as _;
    use sha1::{Digest, Sha1};
    let hash = Sha1::digest(format!("{user}:{password}").as_bytes());
    format!("{user}:{}", base64::engine::general_purpose::STANDARD.encode(hash))
}

/// An authenticated identity attached to the session (`whoami`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthIdentity {
    pub scheme: String,
    pub user: String,
}

/// Node data plus stat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeData {
    pub data: Vec<u8>,
    pub stat: Stat,
}

/// Result of a `create`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Created {
    /// Actual path (differs from the request for sequential nodes).
    pub path: String,
    pub stat: Stat,
    /// Sequence number for sequential nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<i64>,
}

/// Child name with optional stat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildEntry {
    pub name: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stat: Option<Stat>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perms_roundtrip() {
        assert_eq!(Perms::ALL.to_letters(), "cdrwa");
        assert_eq!(Perms::from_letters("rw").unwrap(), Perms(3));
        assert_eq!(Perms::from_letters("ALL").unwrap(), Perms::ALL);
        assert!(Perms::from_letters("rx").is_err());
        let p: Perms = serde_json::from_str("\"ra\"").unwrap();
        assert_eq!(p, Perms(17));
        let p: Perms = serde_json::from_str("31").unwrap();
        assert_eq!(p, Perms::ALL);
        assert_eq!(zk::Permission::from(Perms(5)), zk::Permission::READ | zk::Permission::CREATE);
        assert_eq!(Perms::from(zk::Permission::ALL), Perms::ALL);
    }

    #[test]
    fn acl_parse() {
        let a = Acl::parse("world:anyone:cdrwa").unwrap();
        assert_eq!(a, Acl::new(Perms::ALL, "world", "anyone"));
        let a = Acl::parse("digest:bob:abc=:r").unwrap();
        assert_eq!(a.id, "bob:abc=");
        assert_eq!(a.perms, Perms::READ);
        let a = Acl::parse("ip:10.0.0.0/8:rw").unwrap();
        assert_eq!(a.id, "10.0.0.0/8");
        assert!(Acl::parse("nonsense").is_err());
        let l = Acl::parse_list("world:anyone:r, auth::cdrwa").unwrap();
        assert_eq!(l.len(), 2);
        assert_eq!(l[1].to_cli_string(), "auth::cdrwa");
    }

    #[test]
    fn digest_matches_zookeeper() {
        // Value produced by org.apache.zookeeper.server.auth.DigestAuthenticationProvider.generateDigest("super:admin")
        assert_eq!(digest_id("super", "admin"), "super:xQJmxLMiHGwaqBvst5y6rkB6HQs=");
    }

    #[test]
    fn stat_kinds() {
        let mut s = Stat { ephemeral_owner: 0x1234, ..Default::default() };
        assert_eq!(s.kind(), NodeKind::Ephemeral);
        s.ephemeral_owner = i64::MIN;
        assert_eq!(s.kind(), NodeKind::Container);
        s.ephemeral_owner = (0xFFu64 << 56 | 5000) as i64;
        assert_eq!(s.kind(), NodeKind::Ttl);
        assert_eq!(s.ttl_ms(), Some(5000));
        s.ephemeral_owner = 0;
        assert_eq!(s.kind(), NodeKind::Persistent);
    }
}
