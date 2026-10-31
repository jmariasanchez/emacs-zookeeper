//! Server administration diagnostics via four-letter-word commands.
//!
//! Commands must be allowed by the server's `4lw.commands.whitelist`. Each
//! request opens a fresh TCP connection with a deadline. TLS-only client ports
//! do not accept four-letter words; configure `admin_hosts` to a plain port or
//! use the AdminServer instead.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{ErrorKind, Result, ZkError};

/// Commands considered safe to issue (read-only). Destructive ones (`srst`,
/// `crst`, `stmk`) are intentionally excluded.
pub const SAFE_COMMANDS: &[&str] =
    &["ruok", "srvr", "stat", "mntr", "conf", "cons", "envi", "wchs", "wchc", "wchp", "dump", "isro", "dirs", "gtmk", "hash"];

/// Sends a four-letter-word command and returns the raw response.
pub async fn four_letter(host: &str, command: &str, timeout: Duration) -> Result<String> {
    if command.len() != 4 || !command.chars().all(|c| c.is_ascii_lowercase()) {
        return Err(ZkError::bad_arguments(format!("invalid four letter word {command:?}")));
    }
    if !SAFE_COMMANDS.contains(&command) {
        return Err(ZkError::policy(format!("four letter word {command:?} is not in the safe list")));
    }
    let addr = normalize_host(host);
    let fut = async {
        let mut stream = tokio::net::TcpStream::connect(&addr).await?;
        stream.write_all(command.as_bytes()).await?;
        stream.flush().await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        Ok::<_, std::io::Error>(buf)
    };
    let bytes = tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| ZkError::timeout(&format!("{command} on {addr}")))?
        .map_err(|e| ZkError::new(ErrorKind::Io, format!("{command} on {addr}: {e}")))?;
    let text = String::from_utf8_lossy(&bytes).into_owned();
    if text.contains("is not executed because it is not in the whitelist") {
        return Err(ZkError::policy(format!("server {addr} does not allow {command}: {}", text.trim())));
    }
    Ok(text)
}

fn normalize_host(host: &str) -> String {
    let h = host.trim_start_matches("tcp://").trim_start_matches("tcp+tls://");
    if h.rsplit_once(':').map(|(_, p)| p.parse::<u16>().is_ok()).unwrap_or(false) {
        h.to_string()
    } else {
        format!("{h}:2181")
    }
}

/// Parses `mntr` output (tab-separated key/value lines).
pub fn parse_mntr(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|l| l.split_once('\t'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

/// Parsed `srvr` output.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerInfo {
    pub host: String,
    pub version: Option<String>,
    pub mode: Option<String>,
    pub zxid: Option<String>,
    pub node_count: Option<u64>,
    pub connections: Option<u64>,
    pub outstanding: Option<u64>,
    pub latency: Option<String>,
    #[serde(default)]
    pub raw: BTreeMap<String, String>,
}

pub fn parse_srvr(host: &str, text: &str) -> ServerInfo {
    let mut raw = BTreeMap::new();
    for line in text.lines() {
        if let Some((k, v)) = line.split_once(':') {
            raw.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    let num = |k: &str| raw.get(k).and_then(|v| v.parse::<u64>().ok());
    ServerInfo {
        host: host.to_string(),
        version: raw.get("Zookeeper version").cloned(),
        mode: raw.get("Mode").cloned(),
        zxid: raw.get("Zxid").cloned(),
        node_count: num("Node count"),
        connections: num("Connections"),
        outstanding: num("Outstanding"),
        latency: raw.get("Latency min/avg/max").cloned(),
        raw: raw.clone(),
    }
}

/// Health of one ensemble member.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberHealth {
    pub host: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<ServerInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Queries `ruok` and `srvr` on each host concurrently.
pub async fn ensemble_health(hosts: &[String], timeout: Duration) -> Vec<MemberHealth> {
    let futs = hosts.iter().map(|h| async move {
        let ruok = four_letter(h, "ruok", timeout).await;
        match ruok {
            Ok(r) if r.trim() == "imok" => match four_letter(h, "srvr", timeout).await {
                Ok(text) => MemberHealth { host: h.clone(), ok: true, info: Some(parse_srvr(h, &text)), error: None },
                Err(e) => MemberHealth { host: h.clone(), ok: true, info: None, error: Some(e.to_string()) },
            },
            Ok(other) => MemberHealth {
                host: h.clone(),
                ok: false,
                info: None,
                error: Some(format!("unexpected ruok reply {:?}", other.trim())),
            },
            Err(e) => MemberHealth { host: h.clone(), ok: false, info: None, error: Some(e.to_string()) },
        }
    });
    futures::future::join_all(futs).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mntr_and_srvr() {
        let m = parse_mntr("zk_version\t3.9.2\nzk_server_state\tstandalone\n");
        assert_eq!(m.get("zk_server_state").unwrap(), "standalone");
        let s = parse_srvr("h", "Zookeeper version: 3.9.2-x\nLatency min/avg/max: 0/0.1/3\nMode: leader\nNode count: 42\n");
        assert_eq!(s.mode.as_deref(), Some("leader"));
        assert_eq!(s.node_count, Some(42));
        assert_eq!(s.latency.as_deref(), Some("0/0.1/3"));
    }

    #[test]
    fn host_normalization() {
        assert_eq!(normalize_host("a"), "a:2181");
        assert_eq!(normalize_host("tcp://a:1"), "a:1");
    }

    #[tokio::test]
    async fn refuses_unsafe_commands() {
        let e = four_letter("localhost:1", "srst", Duration::from_millis(10)).await.unwrap_err();
        assert_eq!(e.kind, ErrorKind::PolicyDenied);
        let e = four_letter("localhost:1", "x", Duration::from_millis(10)).await.unwrap_err();
        assert_eq!(e.kind, ErrorKind::BadArguments);
    }
}
