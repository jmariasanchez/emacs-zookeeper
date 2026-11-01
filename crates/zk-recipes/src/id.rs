//! Distributed unique id generation.
//!
//! * [`SequentialIdGenerator`]: creates a persistent-sequential node under
//!   `{base}/id-` and immediately deletes it; the server-assigned sequence
//!   (monotonic per parent, unique across all clients) is the id. Ids are
//!   unique and increasing but not gap-free. The parent's `cversion`
//!   overflows after 2^31 ids (ZooKeeper limitation); use
//!   [`SequentialIdGenerator::with_epoch`] to rotate parents, or the counter
//!   generator.
//! * [`CounterIdGenerator`]: block allocation over a [`SharedCounter`]
//!   (64-bit): each call reserves `block` ids with one CAS and hands them out
//!   locally. Unique across clients; ids from different clients interleave.
//!   An `OutcomeUnknown` reservation is never reused (the block is skipped),
//!   so ids stay unique at the cost of a possible gap.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;
use zk_core::{CreateMode, CreateOptions, ErrorKind, Result, ZkClient};

use crate::common::validate_base;
use crate::counter::{CounterEncoding, SharedCounter};

/// Ids from sequential node creation.
#[derive(Clone, Debug)]
pub struct SequentialIdGenerator {
    client: ZkClient,
    base: String,
}

impl SequentialIdGenerator {
    pub fn new(client: &ZkClient, base: &str) -> Result<SequentialIdGenerator> {
        validate_base(base)?;
        Ok(SequentialIdGenerator { client: client.clone(), base: base.to_string() })
    }

    /// Uses `{base}/e{epoch}` as the parent; ids are `(epoch << 32) | seq`.
    pub fn with_epoch(mut self, epoch: u32) -> Self {
        self.base = format!("{}/e{epoch}", self.base);
        self
    }

    fn epoch(&self) -> i64 {
        self.base.rsplit('/').next().and_then(|n| n.strip_prefix('e')).and_then(|e| e.parse::<i64>().ok()).unwrap_or(0)
    }

    /// Generates the next id.
    pub async fn next_id(&self) -> Result<i64> {
        let opts = CreateOptions::mode(CreateMode::PersistentSequential).with_parents();
        let c = self.client.create(&format!("{}/id-", self.base), b"", &opts).await?;
        // Best effort: a leftover node is harmless (ids are unique regardless).
        let _ = self.client.delete(&c.path, None).await;
        Ok((self.epoch() << 32) | c.sequence.unwrap_or(0))
    }
}

struct Block {
    next: i64,
    end: i64,
}

/// Block-allocating counter-based ids.
#[derive(Clone)]
pub struct CounterIdGenerator {
    counter: SharedCounter,
    block: i64,
    state: Arc<AsyncMutex<Block>>,
}

impl CounterIdGenerator {
    pub fn new(client: &ZkClient, path: &str, block: u32) -> Result<CounterIdGenerator> {
        let block = block.max(1) as i64;
        Ok(CounterIdGenerator {
            counter: SharedCounter::new(client, path, CounterEncoding::Int64Be)?,
            block,
            state: Arc::new(AsyncMutex::new(Block { next: 0, end: 0 })),
        })
    }

    /// Next id (`>= 1`).
    pub async fn next_id(&self) -> Result<i64> {
        let mut st = self.state.lock().await;
        if st.next >= st.end {
            let end = match self.counter.add(self.block).await {
                Ok(v) => v,
                Err(e) if e.is(ErrorKind::OutcomeUnknown) => {
                    // The reservation may or may not have happened; skip ahead
                    // to a fresh one rather than reuse an ambiguous block.
                    self.counter.add(self.block).await?
                }
                Err(e) => return Err(e),
            };
            st.next = end - self.block + 1;
            st.end = end + 1;
        }
        let id = st.next;
        st.next += 1;
        Ok(id)
    }
}
