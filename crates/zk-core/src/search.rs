//! Subtree search and comparison.
//!
//! Both operate on bounded walks and report explicitly when results are
//! partial (limits hit, unreadable nodes), so a UI never presents an
//! incomplete answer as complete.

use std::collections::BTreeMap;

use futures::future::join_all;
use serde::{Deserialize, Serialize};

use crate::error::{ErrorKind, Result, ZkError};
use crate::ops::WalkOptions;
use crate::path;
use crate::session::ZkClient;
use crate::types::{Acl, Stat};

/// Search criteria. All given criteria must match (logical AND).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchQuery {
    /// Regex matched against the node name (last path component).
    #[serde(default)]
    pub name: Option<String>,
    /// Regex matched against node data (lossy UTF-8).
    #[serde(default)]
    pub data: Option<String>,
    /// Substring matched against raw node data bytes.
    #[serde(default)]
    pub data_contains: Option<String>,
    /// `scheme` or `scheme:id`; matches nodes with such an ACL entry.
    #[serde(default)]
    pub acl: Option<String>,
    /// `ephemeral` or `persistent`.
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub min_data_bytes: Option<i32>,
    #[serde(default = "default_max_nodes")]
    pub max_nodes: usize,
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    #[serde(default)]
    pub max_depth: Option<usize>,
    #[serde(default)]
    pub include_system: bool,
}

fn default_max_nodes() -> usize {
    100_000
}
fn default_max_results() -> usize {
    1_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchMatch {
    pub path: String,
    pub stat: Stat,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchResult {
    pub matches: Vec<SearchMatch>,
    pub scanned: usize,
    pub truncated: bool,
    /// Why the result may be incomplete.
    pub partial_reasons: Vec<String>,
}

impl ZkClient {
    /// Searches a subtree.
    pub async fn search(&self, root: &str, q: &SearchQuery) -> Result<SearchResult> {
        let re = |s: &Option<String>, what: &str| -> Result<Option<regex::Regex>> {
            s.as_deref()
                .map(|p| regex::Regex::new(p).map_err(|e| ZkError::bad_arguments(format!("invalid {what} regex: {e}"))))
                .transpose()
        };
        let name_re = re(&q.name, "name")?;
        let data_re = re(&q.data, "data")?;
        let kind = match q.kind.as_deref() {
            None => None,
            Some("ephemeral") => Some(true),
            Some("persistent") => Some(false),
            Some(k) => return Err(ZkError::bad_arguments(format!("unknown kind {k:?}"))),
        };
        let acl_filter = q.acl.as_deref().map(|a| match a.split_once(':') {
            Some((s, i)) => (s.to_string(), Some(i.to_string())),
            None => (a.to_string(), None),
        });
        let walk = self
            .walk(
                root,
                &WalkOptions { max_depth: q.max_depth, max_nodes: q.max_nodes, include_system: q.include_system, ..Default::default() },
            )
            .await?;
        let mut partial = Vec::new();
        if walk.truncated {
            partial.push("node-limit".to_string());
        }
        let unreadable = walk.skipped.iter().filter(|(_, k)| *k == ErrorKind::NoAuth).count();
        let need_data = data_re.is_some() || q.data_contains.is_some();
        let need_acl = acl_filter.is_some();
        // Cheap filters first.
        let candidates: Vec<_> = walk
            .nodes
            .iter()
            .filter(|e| name_re.as_ref().map(|r| r.is_match(path::basename(&e.path))).unwrap_or(true))
            .filter(|e| kind.map(|k| e.stat.is_ephemeral() == k).unwrap_or(true))
            .filter(|e| q.min_data_bytes.map(|m| e.stat.data_length >= m).unwrap_or(true))
            .collect();
        let mut matches = Vec::new();
        let mut unreadable_data = 0usize;
        let session = self.op_session().await?;
        for chunk in candidates.chunks(256) {
            let futs = chunk.iter().map(|e| {
                let c = session.client.clone();
                let p = e.path.clone();
                async move {
                    let d = if need_data { Some(c.get_data(&p).await) } else { None };
                    let a = if need_acl { Some(c.get_acl(&p).await) } else { None };
                    (d, a)
                }
            });
            let results = tokio::time::timeout(self.config().operation_timeout() * 4, join_all(futs))
                .await
                .map_err(|_| ZkError::timeout("search"))?;
            for (e, (d, a)) in chunk.iter().zip(results) {
                let mut reasons = Vec::new();
                if name_re.is_some() {
                    reasons.push("name".to_string());
                }
                if kind.is_some() {
                    reasons.push("kind".to_string());
                }
                if q.min_data_bytes.is_some() {
                    reasons.push("size".to_string());
                }
                match d {
                    Some(Ok((data, _))) => {
                        if let Some(r) = &data_re {
                            if !r.is_match(&String::from_utf8_lossy(&data)) {
                                continue;
                            }
                        }
                        if let Some(sub) = &q.data_contains {
                            if !contains(&data, sub.as_bytes()) {
                                continue;
                            }
                        }
                        reasons.push("data".to_string());
                    },
                    Some(Err(zookeeper_client::Error::NoAuth)) => {
                        unreadable_data += 1;
                        continue;
                    },
                    Some(Err(_)) => continue,
                    None => {},
                }
                match a {
                    Some(Ok((acl, _))) => {
                        let (scheme, id) = acl_filter.as_ref().unwrap();
                        let hit = acl.iter().any(|x| x.scheme() == scheme && id.as_ref().map(|i| x.id() == i).unwrap_or(true));
                        if !hit {
                            continue;
                        }
                        reasons.push("acl".to_string());
                    },
                    Some(Err(_)) => continue,
                    None => {},
                }
                matches.push(SearchMatch { path: e.path.clone(), stat: e.stat, reasons });
                if matches.len() >= q.max_results {
                    break;
                }
            }
            if matches.len() >= q.max_results {
                partial.push("result-limit".to_string());
                break;
            }
        }
        if unreadable + unreadable_data > 0 {
            partial.push(format!("unreadable:{}", unreadable + unreadable_data));
        }
        Ok(SearchResult { truncated: !partial.is_empty(), matches, scanned: walk.nodes.len(), partial_reasons: partial })
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
}

/// What to compare.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffOptions {
    #[serde(default = "yes")]
    pub data: bool,
    #[serde(default)]
    pub acl: bool,
    /// Report persistent/ephemeral mismatches.
    #[serde(default = "yes")]
    pub kind: bool,
    #[serde(default = "default_max_nodes")]
    pub max_nodes: usize,
}

fn yes() -> bool {
    true
}

impl Default for DiffOptions {
    fn default() -> Self {
        Self { data: true, acl: false, kind: true, max_nodes: default_max_nodes() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeSummary {
    pub stat: Stat,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_b64")]
    pub data: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acl: Option<Vec<Acl>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Difference {
    /// Path relative to the compared roots ("" for the roots).
    pub rel: String,
    /// `only-left`, `only-right`, `data`, `acl`, `kind`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left: Option<NodeSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right: Option<NodeSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiffResult {
    pub differences: Vec<Difference>,
    pub left_count: usize,
    pub right_count: usize,
    pub truncated: bool,
}

async fn snapshot(client: &ZkClient, root: &str, o: &DiffOptions) -> Result<(BTreeMap<String, NodeSummary>, bool)> {
    let (nodes, truncated) = client.export(root, o.acl, true, o.max_nodes).await?;
    Ok((
        nodes
            .into_iter()
            .map(|n| (n.rel, NodeSummary { stat: n.stat.unwrap_or_default(), data: Some(n.data), acl: n.acl }))
            .collect(),
        truncated,
    ))
}

/// Compares two subtrees, possibly on different clusters.
pub async fn diff_trees(left: &ZkClient, lroot: &str, right: &ZkClient, rroot: &str, o: &DiffOptions) -> Result<DiffResult> {
    let ((l, lt), (r, rt)) = tokio::try_join!(snapshot(left, lroot, o), snapshot(right, rroot, o))?;
    let mut out = Vec::new();
    let strip = |mut s: NodeSummary, keep_data: bool| {
        if !keep_data {
            s.data = None;
        }
        s
    };
    for (rel, ln) in &l {
        match r.get(rel) {
            None => out.push(Difference { rel: rel.clone(), kind: "only-left".into(), left: Some(strip(ln.clone(), false)), right: None }),
            Some(rn) => {
                if o.data && ln.data != rn.data {
                    out.push(Difference { rel: rel.clone(), kind: "data".into(), left: Some(ln.clone()), right: Some(rn.clone()) });
                }
                if o.acl && ln.acl != rn.acl {
                    out.push(Difference {
                        rel: rel.clone(),
                        kind: "acl".into(),
                        left: Some(strip(ln.clone(), false)),
                        right: Some(strip(rn.clone(), false)),
                    });
                }
                if o.kind && ln.stat.is_ephemeral() != rn.stat.is_ephemeral() {
                    out.push(Difference {
                        rel: rel.clone(),
                        kind: "kind".into(),
                        left: Some(strip(ln.clone(), false)),
                        right: Some(strip(rn.clone(), false)),
                    });
                }
            },
        }
    }
    for (rel, rn) in &r {
        if !l.contains_key(rel) {
            out.push(Difference { rel: rel.clone(), kind: "only-right".into(), left: None, right: Some(strip(rn.clone(), false)) });
        }
    }
    out.sort_by(|a, b| a.rel.cmp(&b.rel).then(a.kind.cmp(&b.kind)));
    Ok(DiffResult { differences: out, left_count: l.len(), right_count: r.len(), truncated: lt || rt })
}

mod opt_b64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(b) => s.serialize_str(&crate::b64::encode(b)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let s = Option::<String>::deserialize(d)?;
        s.map(|s| crate::b64::decode(&s).map_err(serde::de::Error::custom)).transpose()
    }
}
