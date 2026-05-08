//! redb-backed persistent storage for Paxos and Raft.
//!
//! Available behind the `redb` cargo feature. redb is a pure-Rust embedded
//! ACID key-value store; no system library or C toolchain is required, so
//! this is a good default when you want durable storage without the build
//! footprint of `duckdb` / `duckdb-bundled`.
//!
//! [`RedbPaxosStorage`] and [`RedbRaftStorage`] use disjoint sets of tables
//! (`paxos_*` and `raft_*` respectively) and can therefore share the same
//! database file, or each live in its own.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use redb::{
    backends::InMemoryBackend, Builder, Database, ReadableDatabase, ReadableTable, TableDefinition,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::config::NodeId;
use crate::error::StorageError;
use crate::message::LogEntry;
use crate::storage::{PaxosStorage, RaftStorage};

const PAXOS_DECISIONS: TableDefinition<u64, &str> = TableDefinition::new("paxos_decisions");

const RAFT_DECISIONS: TableDefinition<u64, &str> = TableDefinition::new("raft_decisions");
const RAFT_LOG: TableDefinition<u64, (u64, &str)> = TableDefinition::new("raft_log");
const RAFT_TERM: TableDefinition<&str, u64> = TableDefinition::new("raft_term");
const RAFT_VOTED_FOR: TableDefinition<&str, (&str, u64)> = TableDefinition::new("raft_voted_for");
const RAFT_COMMIT_INDEX: TableDefinition<&str, u64> = TableDefinition::new("raft_commit_index");

// Single-row meta tables key all entries under this constant.
const META_KEY: &str = "k";

fn open_database(path: Option<&Path>) -> Result<Database, StorageError> {
    match path {
        Some(p) => {
            Database::create(p).map_err(|e| StorageError::Persist(format!("open redb: {e}")))
        }
        None => Builder::new()
            .create_with_backend(InMemoryBackend::new())
            .map_err(|e| StorageError::Persist(format!("open redb in-memory: {e}"))),
    }
}

fn persist<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Persist(e.to_string())
}

fn load<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Load(e.to_string())
}

async fn run_blocking<F, R>(db: Arc<Database>, f: F) -> Result<R, StorageError>
where
    F: FnOnce(&Database) -> Result<R, StorageError> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || f(&db))
        .await
        .map_err(|e| StorageError::Persist(format!("blocking task panicked: {e}")))?
}

// ============================================================================
// Paxos
// ============================================================================

/// redb-backed [`PaxosStorage`] implementation.
///
/// The table does not encode the value type `V`; values round-trip as JSON
/// strings. Reopening an existing file with a different `V` will either
/// silently misdeserialize (if the JSON happens to be compatible) or fail at
/// row-load — `V` must remain stable across reopens of the same database.
pub struct RedbPaxosStorage<V> {
    db: Arc<Database>,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedbPaxosStorage<V> {
    /// Open or create a redb database at `path` and prepare the Paxos table.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::from_database(open_database(Some(path.as_ref()))?)
    }

    /// Open an in-memory redb database. Decisions are lost on drop.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::from_database(open_database(None)?)
    }

    fn from_database(db: Database) -> Result<Self, StorageError> {
        let txn = db.begin_write().map_err(persist)?;
        {
            let _ = txn.open_table(PAXOS_DECISIONS).map_err(persist)?;
        }
        txn.commit().map_err(persist)?;
        Ok(Self {
            db: Arc::new(db),
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<V> PaxosStorage<V> for RedbPaxosStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        let serialized = serde_json::to_string(&value).map_err(persist)?;
        run_blocking(self.db.clone(), move |db| {
            let txn = db.begin_write().map_err(persist)?;
            {
                let mut table = txn.open_table(PAXOS_DECISIONS).map_err(persist)?;
                table.insert(slot, serialized.as_str()).map_err(persist)?;
            }
            txn.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        run_blocking(self.db.clone(), |db| {
            let txn = db.begin_read().map_err(load)?;
            let table = txn.open_table(PAXOS_DECISIONS).map_err(load)?;
            let mut out = Vec::new();
            for entry in table.iter().map_err(load)? {
                let (k, v) = entry.map_err(load)?;
                let slot = k.value();
                let raw = v.value();
                let value: V = serde_json::from_str(raw).map_err(load)?;
                out.push((slot, value));
            }
            Ok(out)
        })
        .await
    }
}

// ============================================================================
// Raft
// ============================================================================

/// redb-backed [`RaftStorage`] implementation.
///
/// The tables do not encode the value type `V`; log entries and decisions
/// round-trip as JSON strings. Reopening an existing file with a different
/// `V` will either silently misdeserialize (if the JSON happens to be
/// compatible) or fail at row-load — `V` must remain stable across reopens of
/// the same database.
///
/// `voted_for` is stored as a single `(name, incarnation)` tuple per row;
/// because the tuple is written and read atomically by redb, the partial-state
/// hazard that requires explicit handling in the SQL backend cannot occur.
pub struct RedbRaftStorage<V> {
    db: Arc<Database>,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedbRaftStorage<V> {
    /// Open or create a redb database at `path` and prepare the Raft tables.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::from_database(open_database(Some(path.as_ref()))?)
    }

    /// Open an in-memory redb database. State is lost on drop.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::from_database(open_database(None)?)
    }

    fn from_database(db: Database) -> Result<Self, StorageError> {
        let txn = db.begin_write().map_err(persist)?;
        {
            let _ = txn.open_table(RAFT_DECISIONS).map_err(persist)?;
            let _ = txn.open_table(RAFT_LOG).map_err(persist)?;
            let _ = txn.open_table(RAFT_TERM).map_err(persist)?;
            let _ = txn.open_table(RAFT_VOTED_FOR).map_err(persist)?;
            let _ = txn.open_table(RAFT_COMMIT_INDEX).map_err(persist)?;
        }
        txn.commit().map_err(persist)?;
        Ok(Self {
            db: Arc::new(db),
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<V> RaftStorage<V> for RedbRaftStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        let serialized = serde_json::to_string(&value).map_err(persist)?;
        run_blocking(self.db.clone(), move |db| {
            let txn = db.begin_write().map_err(persist)?;
            {
                let mut table = txn.open_table(RAFT_DECISIONS).map_err(persist)?;
                table.insert(slot, serialized.as_str()).map_err(persist)?;
            }
            txn.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        run_blocking(self.db.clone(), |db| {
            let txn = db.begin_read().map_err(load)?;
            let table = txn.open_table(RAFT_DECISIONS).map_err(load)?;
            let mut out = Vec::new();
            for entry in table.iter().map_err(load)? {
                let (k, v) = entry.map_err(load)?;
                let slot = k.value();
                let raw = v.value();
                let value: V = serde_json::from_str(raw).map_err(load)?;
                out.push((slot, value));
            }
            Ok(out)
        })
        .await
    }

    async fn save_term(&mut self, term: u64) -> Result<(), StorageError> {
        run_blocking(self.db.clone(), move |db| {
            let txn = db.begin_write().map_err(persist)?;
            {
                let mut table = txn.open_table(RAFT_TERM).map_err(persist)?;
                table.insert(META_KEY, term).map_err(persist)?;
            }
            txn.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn load_term(&self) -> Result<u64, StorageError> {
        run_blocking(self.db.clone(), |db| {
            let txn = db.begin_read().map_err(load)?;
            let table = txn.open_table(RAFT_TERM).map_err(load)?;
            Ok(table
                .get(META_KEY)
                .map_err(load)?
                .map(|g| g.value())
                .unwrap_or(0))
        })
        .await
    }

    async fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> Result<(), StorageError> {
        let value = voted_for
            .as_ref()
            .map(|id| (id.name().to_string(), id.incarnation()));
        run_blocking(self.db.clone(), move |db| {
            let txn = db.begin_write().map_err(persist)?;
            {
                let mut table = txn.open_table(RAFT_VOTED_FOR).map_err(persist)?;
                match &value {
                    Some((name, inc)) => {
                        table
                            .insert(META_KEY, (name.as_str(), *inc))
                            .map_err(persist)?;
                    }
                    None => {
                        table.remove(META_KEY).map_err(persist)?;
                    }
                }
            }
            txn.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn load_voted_for(&self) -> Result<Option<NodeId>, StorageError> {
        run_blocking(self.db.clone(), |db| {
            let txn = db.begin_read().map_err(load)?;
            let table = txn.open_table(RAFT_VOTED_FOR).map_err(load)?;
            match table.get(META_KEY).map_err(load)? {
                Some(g) => {
                    let (name, inc) = g.value();
                    Ok(Some(NodeId::new(name, inc)))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn append_log(&mut self, entries: &[LogEntry<V>]) -> Result<(), StorageError> {
        let serialized: Vec<(u64, String)> = entries
            .iter()
            .map(|e| {
                serde_json::to_string(&e.value)
                    .map(|v| (e.term, v))
                    .map_err(persist)
            })
            .collect::<Result<_, _>>()?;
        if serialized.is_empty() {
            return Ok(());
        }
        run_blocking(self.db.clone(), move |db| {
            let txn = db.begin_write().map_err(persist)?;
            {
                let mut table = txn.open_table(RAFT_LOG).map_err(persist)?;
                let next_index: u64 = match table.last().map_err(persist)? {
                    Some((k, _)) => k.value() + 1,
                    None => 0,
                };
                for (i, (term, json)) in serialized.iter().enumerate() {
                    let idx = next_index + i as u64;
                    table.insert(idx, (*term, json.as_str())).map_err(persist)?;
                }
            }
            txn.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn truncate_log_from(&mut self, index: u64) -> Result<(), StorageError> {
        run_blocking(self.db.clone(), move |db| {
            let txn = db.begin_write().map_err(persist)?;
            {
                let mut table = txn.open_table(RAFT_LOG).map_err(persist)?;
                let extracted = table
                    .extract_from_if(index.., |_, _| true)
                    .map_err(persist)?;
                for entry in extracted {
                    let _ = entry.map_err(persist)?;
                }
            }
            txn.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn load_log(&self) -> Result<Vec<LogEntry<V>>, StorageError> {
        run_blocking(self.db.clone(), |db| {
            let txn = db.begin_read().map_err(load)?;
            let table = txn.open_table(RAFT_LOG).map_err(load)?;
            let mut out = Vec::new();
            for entry in table.iter().map_err(load)? {
                let (_k, v) = entry.map_err(load)?;
                let (term, raw) = v.value();
                let value: V = serde_json::from_str(raw).map_err(load)?;
                out.push(LogEntry { term, value });
            }
            Ok(out)
        })
        .await
    }

    async fn save_commit_index(&mut self, commit_index: Option<u64>) -> Result<(), StorageError> {
        run_blocking(self.db.clone(), move |db| {
            let txn = db.begin_write().map_err(persist)?;
            {
                let mut table = txn.open_table(RAFT_COMMIT_INDEX).map_err(persist)?;
                match commit_index {
                    Some(n) => {
                        table.insert(META_KEY, n).map_err(persist)?;
                    }
                    None => {
                        table.remove(META_KEY).map_err(persist)?;
                    }
                }
            }
            txn.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn load_commit_index(&self) -> Result<Option<u64>, StorageError> {
        run_blocking(self.db.clone(), |db| {
            let txn = db.begin_read().map_err(load)?;
            let table = txn.open_table(RAFT_COMMIT_INDEX).map_err(load)?;
            Ok(table.get(META_KEY).map_err(load)?.map(|g| g.value()))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_load_err(err: StorageError) {
        match err {
            StorageError::Load(_) => {}
            other => panic!("expected StorageError::Load, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn paxos_load_decisions_returns_err_on_malformed_json() {
        let storage = RedbPaxosStorage::<String>::open_in_memory().unwrap();
        {
            let txn = storage.db.begin_write().unwrap();
            {
                let mut table = txn.open_table(PAXOS_DECISIONS).unwrap();
                table.insert(0u64, "{not valid json").unwrap();
            }
            txn.commit().unwrap();
        }
        assert_load_err(storage.load_decisions().await.unwrap_err());
    }

    #[tokio::test]
    async fn raft_load_decisions_returns_err_on_malformed_json() {
        let storage = RedbRaftStorage::<String>::open_in_memory().unwrap();
        {
            let txn = storage.db.begin_write().unwrap();
            {
                let mut table = txn.open_table(RAFT_DECISIONS).unwrap();
                table.insert(0u64, "{not valid json").unwrap();
            }
            txn.commit().unwrap();
        }
        assert_load_err(storage.load_decisions().await.unwrap_err());
    }

    #[tokio::test]
    async fn raft_load_log_returns_err_on_malformed_json() {
        let storage = RedbRaftStorage::<String>::open_in_memory().unwrap();
        {
            let txn = storage.db.begin_write().unwrap();
            {
                let mut table = txn.open_table(RAFT_LOG).unwrap();
                table.insert(0u64, (1u64, "{not valid json")).unwrap();
            }
            txn.commit().unwrap();
        }
        assert_load_err(storage.load_log().await.unwrap_err());
    }
}
