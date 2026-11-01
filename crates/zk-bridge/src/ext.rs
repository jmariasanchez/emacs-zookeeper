//! Cache and recipe methods (backed by zk-cache and zk-recipes).

use std::sync::Arc;

use serde_json::Value;

use crate::dispatch::Bridge;
use crate::rpc::{RpcError, RpcResult};

pub const METHODS: &[&str] = &[];

#[derive(Default)]
pub struct ExtState {}

impl ExtState {
    pub fn shutdown(&self) {}
    pub fn drop_cluster(&self, _cluster: &str) {}
}

pub async fn dispatch(_b: &Arc<Bridge>, method: &str, _p: Value) -> RpcResult {
    Err(RpcError::new(crate::rpc::METHOD_NOT_FOUND, format!("method not found: {method}")))
}
