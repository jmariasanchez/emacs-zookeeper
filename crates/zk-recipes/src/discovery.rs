//! Curator x-discovery compatible service registry.
//!
//! Instances are stored as ephemeral nodes `{base}/{service}/{id}` whose data
//! is Curator's `JsonInstanceSerializer` format:
//!
//! ```json
//! {"name":"api","id":"7f3c...","address":"10.0.0.1","port":8080,"sslPort":null,
//!  "payload":null,"registrationTimeUTC":1700000000000,"serviceType":"DYNAMIC",
//!  "uriSpec":{"parts":[{"value":"scheme","variable":true},{"value":"://","variable":false}]},
//!  "enabled":true}
//! ```
//!
//! Registrations use [`PersistentNode`] so they are re-created after
//! session expiry (Curator re-registers on `RECONNECTED` after loss).
//!
//! Differences vs Curator: payloads are arbitrary JSON values (Curator
//! deserializes into a configured payload class and may add `@class` type
//! info, which is preserved here as plain JSON); no provider strategies
//! (round robin, sticky) beyond [`ServiceCache::instances`].

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use zk_core::{path as zpath, Acl, ErrorKind, Result, ZkClient, ZkError};

use crate::common::validate_base;
use crate::persistent_node::{PersistentNode, PersistentNodeMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ServiceType {
    #[default]
    Dynamic,
    Static,
    Permanent,
    DynamicSequential,
}

/// One part of a Curator `UriSpec`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UriPart {
    pub value: String,
    pub variable: bool,
}

/// Curator `UriSpec`, e.g. `{scheme}://{address}:{port}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UriSpec {
    pub parts: Vec<UriPart>,
}

impl UriSpec {
    /// Parses Curator template syntax (`{var}` placeholders).
    pub fn parse(template: &str) -> UriSpec {
        let mut parts = Vec::new();
        let mut rest = template;
        while !rest.is_empty() {
            match rest.find('{') {
                Some(0) => match rest.find('}') {
                    Some(end) => {
                        parts.push(UriPart { value: rest[1..end].to_string(), variable: true });
                        rest = &rest[end + 1..];
                    }
                    None => {
                        parts.push(UriPart { value: rest.to_string(), variable: false });
                        break;
                    }
                },
                Some(i) => {
                    parts.push(UriPart { value: rest[..i].to_string(), variable: false });
                    rest = &rest[i..];
                }
                None => {
                    parts.push(UriPart { value: rest.to_string(), variable: false });
                    break;
                }
            }
        }
        UriSpec { parts }
    }

    /// Renders using an instance (`name`, `id`, `address`, `port`, `ssl-port`, `scheme`).
    pub fn build(&self, inst: &ServiceInstance) -> String {
        let mut out = String::new();
        for p in &self.parts {
            if !p.variable {
                out.push_str(&p.value);
                continue;
            }
            let v = match p.value.as_str() {
                "name" => inst.name.clone(),
                "id" => inst.id.clone(),
                "address" => inst.address.clone(),
                "port" => inst.port.map(|p| p.to_string()).unwrap_or_default(),
                "ssl-port" => inst.ssl_port.map(|p| p.to_string()).unwrap_or_default(),
                "scheme" => if inst.ssl_port.is_some() { "https" } else { "http" }.to_string(),
                _ => String::new(),
            };
            out.push_str(&v);
        }
        out
    }
}

/// Curator `ServiceInstance`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInstance {
    pub name: String,
    pub id: String,
    pub address: String,
    pub port: Option<u16>,
    pub ssl_port: Option<u16>,
    pub payload: Option<serde_json::Value>,
    #[serde(rename = "registrationTimeUTC")]
    pub registration_time_utc: i64,
    pub service_type: ServiceType,
    pub uri_spec: Option<UriSpec>,
    #[serde(default = "yes")]
    pub enabled: bool,
}

fn yes() -> bool {
    true
}

impl ServiceInstance {
    /// A dynamic instance with a fresh UUID id and the current time.
    pub fn new(name: &str, address: &str, port: Option<u16>) -> ServiceInstance {
        ServiceInstance {
            name: name.to_string(),
            id: uuid::Uuid::new_v4().to_string(),
            address: address.to_string(),
            port,
            ssl_port: None,
            payload: None,
            registration_time_utc: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            service_type: ServiceType::Dynamic,
            uri_spec: None,
            enabled: true,
        }
    }

    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| ZkError::new(ErrorKind::Protocol, e.to_string()))
    }

    pub fn from_json(data: &[u8]) -> Result<ServiceInstance> {
        serde_json::from_slice(data).map_err(|e| ZkError::new(ErrorKind::Protocol, format!("invalid service instance: {e}")))
    }

    /// Renders the instance URI using its `uri_spec`.
    pub fn uri(&self) -> Option<String> {
        self.uri_spec.as_ref().map(|s| s.build(self))
    }
}

/// Service registry rooted at `base`.
#[derive(Clone)]
pub struct ServiceDiscovery {
    client: ZkClient,
    base: String,
    registered: Arc<StdMutex<HashMap<(String, String), PersistentNode>>>,
}

impl std::fmt::Debug for ServiceDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceDiscovery").field("base", &self.base).finish()
    }
}

impl ServiceDiscovery {
    pub fn new(client: &ZkClient, base: &str) -> Result<ServiceDiscovery> {
        validate_base(base)?;
        Ok(ServiceDiscovery { client: client.clone(), base: base.to_string(), registered: Default::default() })
    }

    fn instance_path(&self, name: &str, id: &str) -> String {
        format!("{}/{}/{}", self.base, name, id)
    }

    /// Registers an instance (ephemeral, re-registered after session loss;
    /// `Static`/`Permanent` types are persistent nodes like Curator).
    /// Waits up to `timeout` for the node to exist.
    pub async fn register(&self, inst: &ServiceInstance, timeout: Duration) -> Result<()> {
        if inst.name.is_empty() || inst.name.contains('/') || inst.id.is_empty() || inst.id.contains('/') {
            return Err(ZkError::bad_arguments("service name and id must be non-empty and contain no '/'"));
        }
        let path = self.instance_path(&inst.name, &inst.id);
        let data = inst.to_json()?;
        if matches!(inst.service_type, ServiceType::Static | ServiceType::Permanent) {
            self.client.ensure_path(&format!("{}/{}", self.base, inst.name), &Acl::anyone_all()).await?;
            return match self.client.create(&path, &data, &zk_core::CreateOptions::persistent()).await {
                Ok(_) => Ok(()),
                Err(e) if e.is(ErrorKind::NodeExists) => self.client.set_data(&path, &data, None).await.map(|_| ()),
                Err(e) => Err(e),
            };
        }
        let node = PersistentNode::start(&self.client, &path, PersistentNodeMode::Ephemeral, &data)?;
        let old = self.registered.lock().unwrap().insert((inst.name.clone(), inst.id.clone()), node.clone());
        if let Some(old) = old {
            let _ = old.close().await;
        }
        node.wait_for_created(timeout).await.map(|_| ())
    }

    /// Updates a registered instance's data.
    pub async fn update(&self, inst: &ServiceInstance) -> Result<()> {
        let node = self.registered.lock().unwrap().get(&(inst.name.clone(), inst.id.clone())).cloned();
        let data = inst.to_json()?;
        match node {
            Some(n) => n.set_data(&data).await,
            None => self.client.set_data(&self.instance_path(&inst.name, &inst.id), &data, None).await.map(|_| ()),
        }
    }

    /// Unregisters an instance (also works for persistent ones).
    pub async fn unregister(&self, name: &str, id: &str) -> Result<()> {
        let node = self.registered.lock().unwrap().remove(&(name.to_string(), id.to_string()));
        if let Some(n) = node {
            n.close().await?;
        }
        match self.client.delete(&self.instance_path(name, id), None).await {
            Err(e) if !e.is(ErrorKind::NoNode) => Err(e),
            _ => Ok(()),
        }
    }

    /// Registration handle (for ownership / re-registration inspection).
    pub fn registration(&self, name: &str, id: &str) -> Option<PersistentNode> {
        self.registered.lock().unwrap().get(&(name.to_string(), id.to_string())).cloned()
    }

    pub async fn query_for_names(&self) -> Result<Vec<String>> {
        match self.client.children(&self.base).await {
            Ok(v) => Ok(v),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    pub async fn query_for_instances(&self, name: &str) -> Result<Vec<ServiceInstance>> {
        query_instances(&self.client, &zpath::join(&self.base, name)).await
    }

    pub async fn query_for_instance(&self, name: &str, id: &str) -> Result<Option<ServiceInstance>> {
        match self.client.get_data(&self.instance_path(name, id)).await {
            Ok(d) => Ok(Some(ServiceInstance::from_json(&d.data)?)),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Starts a cache of `name`'s instances.
    pub fn service_cache(&self, name: &str) -> ServiceCache {
        ServiceCache::start(&self.client, &zpath::join(&self.base, name))
    }

    /// Unregisters everything registered through this instance.
    pub async fn close(&self) {
        let all: Vec<_> = self.registered.lock().unwrap().drain().collect();
        for ((_, _), n) in all {
            let _ = n.close().await;
        }
    }
}

/// Reads and parses all instances under a service path. Unparseable nodes are skipped.
pub async fn query_instances(client: &ZkClient, service_path: &str) -> Result<Vec<ServiceInstance>> {
    let ids = match client.children(service_path).await {
        Ok(v) => v,
        Err(e) if e.is(ErrorKind::NoNode) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for id in ids {
        match client.get_data(&zpath::join(service_path, &id)).await {
            Ok(d) => {
                if let Ok(i) = ServiceInstance::from_json(&d.data) {
                    out.push(i);
                }
            }
            Err(e) if e.is(ErrorKind::NoNode) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// A watched view of one service's instances (Curator `ServiceCache`).
/// Refreshes on child changes and periodically (to catch data updates).
#[derive(Clone)]
pub struct ServiceCache {
    rx: watch::Receiver<BTreeMap<String, ServiceInstance>>,
    _guard: Arc<tokio_util::sync::DropGuard>,
}

impl ServiceCache {
    fn start(client: &ZkClient, service_path: &str) -> ServiceCache {
        let (tx, rx) = watch::channel(BTreeMap::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let c = cancel.clone();
        let client = client.clone();
        let path = service_path.to_string();
        tokio::spawn(async move {
            loop {
                if c.is_cancelled() {
                    return;
                }
                let w = client.watch_exists(&path).await.ok().map(|(_, w)| w);
                let cw = client.watch_children(&path).await.ok().map(|(_, _, w)| w);
                if let Ok(v) = query_instances(&client, &path).await {
                    let m: BTreeMap<String, ServiceInstance> = v.into_iter().map(|i| (i.id.clone(), i)).collect();
                    tx.send_if_modified(|cur| if *cur != m { *cur = m; true } else { false });
                }
                let tick = tokio::time::sleep(Duration::from_millis(500));
                let ev = async {
                    match (cw, w) {
                        (Some(cw), _) => { cw.changed().await; }
                        (None, Some(w)) => { w.changed().await; }
                        _ => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    _ = c.cancelled() => return,
                    _ = ev => {}
                    _ = tick => {}
                }
            }
        });
        ServiceCache { rx, _guard: Arc::new(cancel.drop_guard()) }
    }

    /// Current instances.
    pub fn instances(&self) -> Vec<ServiceInstance> {
        self.rx.borrow().values().cloned().collect()
    }

    /// Change notifications.
    pub fn subscribe(&self) -> watch::Receiver<BTreeMap<String, ServiceInstance>> {
        self.rx.clone()
    }

    /// Waits until `pred` holds for the instance set.
    pub async fn wait_for(&self, timeout: Duration, pred: impl Fn(&[ServiceInstance]) -> bool) -> Result<Vec<ServiceInstance>> {
        let mut rx = self.rx.clone();
        let fut = async {
            loop {
                let v: Vec<ServiceInstance> = rx.borrow_and_update().values().cloned().collect();
                if pred(&v) {
                    return v;
                }
                if rx.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::time::timeout(timeout, fut).await.map_err(|_| ZkError::timeout("service cache wait"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn curator_json_roundtrip() {
        let java = r#"{"name":"test","id":"abc","address":"10.0.0.1","port":1234,"sslPort":null,"payload":null,"registrationTimeUTC":1325129459728,"serviceType":"STATIC","uriSpec":{"parts":[{"value":"scheme","variable":true},{"value":"://foo","variable":false}]}}"#;
        let i = ServiceInstance::from_json(java.as_bytes()).unwrap();
        assert_eq!(i.service_type, ServiceType::Static);
        assert_eq!(i.port, Some(1234));
        assert!(i.enabled);
        assert_eq!(i.uri().unwrap(), "http://foo");
        let v: serde_json::Value = serde_json::from_slice(&i.to_json().unwrap()).unwrap();
        assert_eq!(v["registrationTimeUTC"], 1325129459728i64);
        assert_eq!(v["sslPort"], serde_json::Value::Null);
        assert_eq!(v["serviceType"], "STATIC");
    }

    #[test]
    fn uri_spec() {
        let s = UriSpec::parse("{scheme}://{address}:{port}/x");
        assert_eq!(s.parts.len(), 6);
        let mut i = ServiceInstance::new("svc", "h", Some(80));
        i.uri_spec = Some(s);
        assert_eq!(i.uri().unwrap(), "http://h:80/x");
    }
}
