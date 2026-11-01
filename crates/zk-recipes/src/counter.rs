//! Shared counters over a single znode using versioned compare-and-set.
//!
//! [`SharedCounter`] stores an integer in one node and updates it with
//! `set_data(.., expected_version)`. Encodings ([`CounterEncoding`]):
//!
//! * `Int32Be`: 4-byte big-endian, Curator `SharedCount` compatible.
//! * `Int64Be`: 8-byte big-endian, Curator `DistributedAtomicLong`
//!   compatible (the default).
//! * `Decimal`: ASCII decimal text, readable in `zkCli` and Kazoo's
//!   `Counter` default (`repr(int)`).
//!
//! An empty or missing node reads as 0.
//!
//! # Outcome-unknown writes
//!
//! When an update's reply is lost ([`ErrorKind::OutcomeUnknown`]), the
//! counter does **not** retry: retrying a blind increment might apply it
//! twice. The error is returned so the caller can re-read and decide.
//! (`compare_and_set` callers can safely re-check the value; Curator instead
//! retries through its `RetryPolicy`, which may double-apply.)
//!
//! `BadVersion` conflicts are retried up to `max_retries` times with jitter;
//! exhausting them yields `BadVersion`.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use zk_core::{CreateOptions, ErrorKind, Result, ZkClient, ZkError};

use crate::common::validate_base;

/// Value encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CounterEncoding {
    /// 4-byte big-endian (Curator `SharedCount`).
    Int32Be,
    /// 8-byte big-endian (Curator `DistributedAtomicLong`).
    #[default]
    Int64Be,
    /// Decimal text.
    Decimal,
}

impl CounterEncoding {
    pub fn encode(self, v: i64) -> Result<Vec<u8>> {
        Ok(match self {
            CounterEncoding::Int32Be => {
                let v = i32::try_from(v).map_err(|_| ZkError::bad_arguments(format!("{v} does not fit a 32-bit counter")))?;
                v.to_be_bytes().to_vec()
            }
            CounterEncoding::Int64Be => v.to_be_bytes().to_vec(),
            CounterEncoding::Decimal => v.to_string().into_bytes(),
        })
    }

    pub fn decode(self, data: &[u8]) -> Result<i64> {
        if data.is_empty() {
            return Ok(0);
        }
        let bad = || ZkError::new(ErrorKind::Protocol, format!("counter data {data:?} is not {self:?}"));
        match self {
            CounterEncoding::Int32Be => Ok(i32::from_be_bytes(data.try_into().map_err(|_| bad())?) as i64),
            CounterEncoding::Int64Be => Ok(i64::from_be_bytes(data.try_into().map_err(|_| bad())?)),
            CounterEncoding::Decimal => std::str::from_utf8(data).ok().and_then(|s| s.trim().parse().ok()).ok_or_else(bad),
        }
    }
}

/// A value plus the node version it was read at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct VersionedValue {
    pub value: i64,
    /// Node version; `-1` if the node does not exist yet.
    pub version: i32,
}

/// CAS counter over one node.
#[derive(Clone, Debug)]
pub struct SharedCounter {
    client: ZkClient,
    path: String,
    encoding: CounterEncoding,
    max_retries: u32,
}

impl SharedCounter {
    pub fn new(client: &ZkClient, path: &str, encoding: CounterEncoding) -> Result<SharedCounter> {
        validate_base(path)?;
        Ok(SharedCounter { client: client.clone(), path: path.to_string(), encoding, max_retries: 64 })
    }

    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Current value and version.
    pub async fn get_versioned(&self) -> Result<VersionedValue> {
        match self.client.get_data(&self.path).await {
            Ok(d) => Ok(VersionedValue { value: self.encoding.decode(&d.data)?, version: d.stat.version }),
            Err(e) if e.is(ErrorKind::NoNode) => Ok(VersionedValue { value: 0, version: -1 }),
            Err(e) => Err(e),
        }
    }

    pub async fn get(&self) -> Result<i64> {
        Ok(self.get_versioned().await?.value)
    }

    /// Writes `new` if the node is still at `expected.version`. Returns
    /// `Ok(false)` on a version conflict.
    pub async fn try_set(&self, expected: VersionedValue, new: i64) -> Result<bool> {
        let data = self.encoding.encode(new)?;
        let r = if expected.version < 0 {
            self.client.create(&self.path, &data, &CreateOptions::persistent().with_parents()).await.map(|_| ())
        } else {
            self.client.set_data(&self.path, &data, Some(expected.version)).await.map(|_| ())
        };
        match r {
            Ok(()) => Ok(true),
            Err(e) if e.is(ErrorKind::BadVersion) || e.is(ErrorKind::NodeExists) || e.is(ErrorKind::NoNode) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Sets to `new` if the current value equals `expected` (value CAS, retried
    /// across concurrent no-op version bumps). Returns whether it was set.
    pub async fn compare_and_set(&self, expected: i64, new: i64) -> Result<bool> {
        for attempt in 0..=self.max_retries {
            let cur = self.get_versioned().await?;
            if cur.value != expected {
                return Ok(false);
            }
            if self.try_set(cur, new).await? {
                return Ok(true);
            }
            backoff(attempt).await;
        }
        Err(ZkError::new(ErrorKind::BadVersion, "compare_and_set: too much contention").with_path(&self.path))
    }

    /// Unconditionally sets the value.
    pub async fn set(&self, v: i64) -> Result<()> {
        self.update(|_| Ok(v)).await.map(|_| ())
    }

    /// Adds `delta`, returning the new value.
    pub async fn add(&self, delta: i64) -> Result<i64> {
        self.update(|cur| cur.checked_add(delta).ok_or_else(|| ZkError::bad_arguments("counter overflow"))).await.map(|(_, n)| n)
    }

    pub async fn increment(&self) -> Result<i64> {
        self.add(1).await
    }

    pub async fn decrement(&self) -> Result<i64> {
        self.add(-1).await
    }

    /// Applies `f` atomically via CAS, returning `(old, new)`.
    pub async fn update(&self, f: impl Fn(i64) -> Result<i64>) -> Result<(i64, i64)> {
        for attempt in 0..=self.max_retries {
            let cur = self.get_versioned().await?;
            let new = f(cur.value)?;
            if self.try_set(cur, new).await? {
                return Ok((cur.value, new));
            }
            backoff(attempt).await;
        }
        Err(ZkError::new(ErrorKind::BadVersion, "counter update: too much contention").with_path(&self.path))
    }
}

async fn backoff(attempt: u32) {
    let jitter = (uuid::Uuid::new_v4().as_u128() % 10) as u64;
    tokio::time::sleep(Duration::from_millis((attempt.min(5) as u64) * 5 + jitter)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodings() {
        for e in [CounterEncoding::Int32Be, CounterEncoding::Int64Be, CounterEncoding::Decimal] {
            assert_eq!(e.decode(&e.encode(-42).unwrap()).unwrap(), -42);
            assert_eq!(e.decode(b"").unwrap(), 0);
        }
        assert_eq!(CounterEncoding::Int32Be.encode(1).unwrap(), vec![0, 0, 0, 1]);
        assert!(CounterEncoding::Int32Be.encode(i64::MAX).is_err());
        assert!(CounterEncoding::Int64Be.decode(&[1, 2]).is_err());
    }
}
