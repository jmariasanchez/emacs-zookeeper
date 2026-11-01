//! JSON-RPC 2.0 message types and the stdout writer.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use zk_core::ZkError;

pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;
pub const REQUEST_CANCELLED: i64 = -32800;
pub const ZK_ERROR: i64 = -32001;
pub const UNKNOWN_HANDLE: i64 = -32002;

/// An incoming message (request or notification).
#[derive(Debug, Deserialize)]
pub struct Incoming {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), data: None }
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS, message)
    }

    pub fn unknown(what: &str, id: &str) -> Self {
        Self::new(UNKNOWN_HANDLE, format!("unknown {what} {id:?}"))
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(INTERNAL_ERROR, message)
    }
}

impl From<ZkError> for RpcError {
    fn from(e: ZkError) -> Self {
        RpcError { code: ZK_ERROR, message: e.to_string(), data: Some(serde_json::to_value(&e).unwrap_or(Value::Null)) }
    }
}

pub type RpcResult = Result<Value, RpcError>;

/// Serialized outgoing lines.
#[derive(Clone)]
pub struct Outbox {
    tx: mpsc::UnboundedSender<String>,
}

impl Outbox {
    pub fn new() -> (Outbox, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Outbox { tx }, rx)
    }

    fn send(&self, v: Value) {
        // serde_json never emits raw newlines inside strings, so one value is one line.
        if let Ok(s) = serde_json::to_string(&v) {
            let _ = self.tx.send(s);
        }
    }

    pub fn respond(&self, id: Value, result: RpcResult) {
        match result {
            Ok(r) => self.send(json!({"jsonrpc": "2.0", "id": id, "result": r})),
            Err(e) => self.send(json!({"jsonrpc": "2.0", "id": id, "error": e})),
        }
    }

    pub fn notify(&self, method: &str, params: Value) {
        self.send(json!({"jsonrpc": "2.0", "method": method, "params": params}));
    }
}

/// Parses params into `T`, mapping failures to `-32602`.
pub fn params<T: serde::de::DeserializeOwned>(v: Value) -> Result<T, RpcError> {
    let v = if v.is_null() { json!({}) } else { v };
    serde_json::from_value(v).map_err(|e| RpcError::invalid_params(e.to_string()))
}

/// Request id as a map key.
pub fn id_key(id: &Value) -> String {
    match id {
        Value::String(s) => format!("s:{s}"),
        other => format!("n:{other}"),
    }
}
