//! DuckDB-backed persistent storage for Paxos and Raft.
//!
//! Available behind the `duckdb` cargo feature. The DuckDB library is bundled
//! (statically compiled) so no system `libduckdb` is required.
//!
//! [`DuckdbPaxosStorage`] and [`DuckdbRaftStorage`] use disjoint sets of
//! tables (`paxos_*` and `raft_*` respectively) and can therefore share the
//! same database file, or each live in its own.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::{params, Connection};
use serde::{de::DeserializeOwned, Serialize};

use crate::config::NodeId;
use crate::error::StorageError;
use crate::message::LogEntry;
use crate::storage::{PaxosStorage, RaftStorage};

const PAXOS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS paxos_decisions (
    slot  UBIGINT PRIMARY KEY,
    value TEXT NOT NULL
);
";

const RAFT_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS raft_decisions (
    slot  UBIGINT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raft_meta (
    id                    UBIGINT PRIMARY KEY,
    term                  UBIGINT NOT NULL,
    voted_for_name        TEXT,
    voted_for_incarnation UBIGINT,
    commit_index          UBIGINT
);

CREATE TABLE IF NOT EXISTS raft_log (
    log_index UBIGINT PRIMARY KEY,
    term      UBIGINT NOT NULL,
    value     TEXT NOT NULL
);
";

const RAFT_META_SEED: &str =
    "INSERT INTO raft_meta (id, term, voted_for_name, voted_for_incarnation, commit_index) \
     VALUES (0, 0, NULL, NULL, NULL) ON CONFLICT (id) DO NOTHING";

fn open_connection(path: Option<&Path>) -> Result<Connection, StorageError> {
    match path {
        Some(p) => Connection::open(p),
        None => Connection::open_in_memory(),
    }
    .map_err(|e| StorageError::Persist(format!("open duckdb: {e}")))
}

fn persist<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Persist(e.to_string())
}

fn load<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Load(e.to_string())
}

async fn run_blocking<F, R>(conn: Arc<Mutex<Connection>>, f: F) -> Result<R, StorageError>
where
    F: FnOnce(&mut Connection) -> Result<R, StorageError> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut guard = conn
            .lock()
            .map_err(|e| StorageError::Persist(format!("duckdb storage mutex poisoned: {e}")))?;
        f(&mut guard)
    })
    .await
    .map_err(|e| StorageError::Persist(format!("blocking task panicked: {e}")))?
}

// ============================================================================
// Paxos
// ============================================================================

/// DuckDB-backed [`PaxosStorage`] implementation.
///
/// The schema does not encode the value type `V`; values round-trip as JSON
/// `TEXT`. Reopening an existing file with a different `V` will either
/// silently misdeserialize (if the JSON happens to be compatible) or fail at
/// row-load — `V` must remain stable across reopens of the same database.
pub struct DuckdbPaxosStorage<V> {
    conn: Arc<Mutex<Connection>>,
    _marker: PhantomData<fn() -> V>,
}

impl<V> DuckdbPaxosStorage<V> {
    /// Open or create a DuckDB database at `path` and prepare the Paxos schema.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::from_connection(open_connection(Some(path.as_ref()))?)
    }

    /// Open an in-memory DuckDB database. Decisions are lost on drop.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::from_connection(open_connection(None)?)
    }

    fn from_connection(conn: Connection) -> Result<Self, StorageError> {
        conn.execute_batch(PAXOS_SCHEMA).map_err(persist)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<V> PaxosStorage<V> for DuckdbPaxosStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        let serialized = serde_json::to_string(&value).map_err(persist)?;
        run_blocking(self.conn.clone(), move |conn| {
            conn.execute(
                "INSERT INTO paxos_decisions (slot, value) VALUES (?, ?) \
                 ON CONFLICT (slot) DO UPDATE SET value = excluded.value",
                params![slot, serialized],
            )
            .map(|_| ())
            .map_err(persist)
        })
        .await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        run_blocking(self.conn.clone(), |conn| {
            let mut stmt = conn
                .prepare("SELECT slot, value FROM paxos_decisions")
                .map_err(load)?;
            let rows = stmt
                .query_map([], |row| {
                    let slot: u64 = row.get(0)?;
                    let raw: String = row.get(1)?;
                    Ok((slot, raw))
                })
                .map_err(load)?;
            let mut out = Vec::new();
            for row in rows {
                let (slot, raw) = row.map_err(load)?;
                let value: V = serde_json::from_str(&raw).map_err(load)?;
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

/// DuckDB-backed [`RaftStorage`] implementation.
///
/// The schema does not encode the value type `V`; log entries and decisions
/// round-trip as JSON `TEXT`. Reopening an existing file with a different `V`
/// will either silently misdeserialize (if the JSON happens to be compatible)
/// or fail at row-load — `V` must remain stable across reopens of the same
/// database.
pub struct DuckdbRaftStorage<V> {
    conn: Arc<Mutex<Connection>>,
    _marker: PhantomData<fn() -> V>,
}

impl<V> DuckdbRaftStorage<V> {
    /// Open or create a DuckDB database at `path` and prepare the Raft schema.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::from_connection(open_connection(Some(path.as_ref()))?)
    }

    /// Open an in-memory DuckDB database. State is lost on drop.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::from_connection(open_connection(None)?)
    }

    fn from_connection(conn: Connection) -> Result<Self, StorageError> {
        conn.execute_batch(RAFT_SCHEMA).map_err(persist)?;
        conn.execute(RAFT_META_SEED, []).map_err(persist)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<V> RaftStorage<V> for DuckdbRaftStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        let serialized = serde_json::to_string(&value).map_err(persist)?;
        run_blocking(self.conn.clone(), move |conn| {
            conn.execute(
                "INSERT INTO raft_decisions (slot, value) VALUES (?, ?) \
                 ON CONFLICT (slot) DO UPDATE SET value = excluded.value",
                params![slot, serialized],
            )
            .map(|_| ())
            .map_err(persist)
        })
        .await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        run_blocking(self.conn.clone(), |conn| {
            let mut stmt = conn
                .prepare("SELECT slot, value FROM raft_decisions")
                .map_err(load)?;
            let rows = stmt
                .query_map([], |row| {
                    let slot: u64 = row.get(0)?;
                    let raw: String = row.get(1)?;
                    Ok((slot, raw))
                })
                .map_err(load)?;
            let mut out = Vec::new();
            for row in rows {
                let (slot, raw) = row.map_err(load)?;
                let value: V = serde_json::from_str(&raw).map_err(load)?;
                out.push((slot, value));
            }
            Ok(out)
        })
        .await
    }

    async fn save_term(&mut self, term: u64) -> Result<(), StorageError> {
        run_blocking(self.conn.clone(), move |conn| {
            conn.execute("UPDATE raft_meta SET term = ? WHERE id = 0", params![term])
                .map(|_| ())
                .map_err(persist)
        })
        .await
    }

    async fn load_term(&self) -> Result<u64, StorageError> {
        run_blocking(self.conn.clone(), |conn| {
            conn.query_row("SELECT term FROM raft_meta WHERE id = 0", [], |row| {
                row.get::<_, u64>(0)
            })
            .map_err(load)
        })
        .await
    }

    async fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> Result<(), StorageError> {
        let (name, incarnation) = match &voted_for {
            Some(id) => (Some(id.name().to_string()), Some(id.incarnation())),
            None => (None::<String>, None::<u64>),
        };
        run_blocking(self.conn.clone(), move |conn| {
            conn.execute(
                "UPDATE raft_meta SET voted_for_name = ?, voted_for_incarnation = ? WHERE id = 0",
                params![name, incarnation],
            )
            .map(|_| ())
            .map_err(persist)
        })
        .await
    }

    async fn load_voted_for(&self) -> Result<Option<NodeId>, StorageError> {
        run_blocking(self.conn.clone(), |conn| {
            let row: (Option<String>, Option<u64>) = conn
                .query_row(
                    "SELECT voted_for_name, voted_for_incarnation FROM raft_meta WHERE id = 0",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(load)?;
            // Partial state would be a Raft safety hazard: if we forgot a
            // vote we already cast in the current term, we could grant a
            // second vote and split the cluster. Fail-stop instead.
            match row {
                (Some(name), Some(inc)) => Ok(Some(NodeId::new(name, inc))),
                (None, None) => Ok(None),
                (Some(_), None) | (None, Some(_)) => Err(StorageError::Load(
                    "raft_meta.voted_for_name and voted_for_incarnation are inconsistent"
                        .into(),
                )),
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
        run_blocking(self.conn.clone(), move |conn| {
            let tx = conn.transaction().map_err(persist)?;
            let next_index: u64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(log_index) + 1, 0) FROM raft_log",
                    [],
                    |row| row.get(0),
                )
                .map_err(persist)?;
            for (i, (term, value)) in serialized.into_iter().enumerate() {
                let idx = next_index + i as u64;
                tx.execute(
                    "INSERT INTO raft_log (log_index, term, value) VALUES (?, ?, ?)",
                    params![idx, term, value],
                )
                .map_err(persist)?;
            }
            tx.commit().map_err(persist)?;
            Ok(())
        })
        .await
    }

    async fn truncate_log_from(&mut self, index: u64) -> Result<(), StorageError> {
        run_blocking(self.conn.clone(), move |conn| {
            conn.execute("DELETE FROM raft_log WHERE log_index >= ?", params![index])
                .map(|_| ())
                .map_err(persist)
        })
        .await
    }

    async fn load_log(&self) -> Result<Vec<LogEntry<V>>, StorageError> {
        run_blocking(self.conn.clone(), |conn| {
            let mut stmt = conn
                .prepare("SELECT term, value FROM raft_log ORDER BY log_index ASC")
                .map_err(load)?;
            let rows = stmt
                .query_map([], |row| {
                    let term: u64 = row.get(0)?;
                    let raw: String = row.get(1)?;
                    Ok((term, raw))
                })
                .map_err(load)?;
            let mut out = Vec::new();
            for row in rows {
                let (term, raw) = row.map_err(load)?;
                let value: V = serde_json::from_str(&raw).map_err(load)?;
                out.push(LogEntry { term, value });
            }
            Ok(out)
        })
        .await
    }

    async fn save_commit_index(&mut self, commit_index: Option<u64>) -> Result<(), StorageError> {
        run_blocking(self.conn.clone(), move |conn| {
            conn.execute(
                "UPDATE raft_meta SET commit_index = ? WHERE id = 0",
                params![commit_index],
            )
            .map(|_| ())
            .map_err(persist)
        })
        .await
    }

    async fn load_commit_index(&self) -> Result<Option<u64>, StorageError> {
        run_blocking(self.conn.clone(), |conn| {
            conn.query_row(
                "SELECT commit_index FROM raft_meta WHERE id = 0",
                [],
                |row| row.get::<_, Option<u64>>(0),
            )
            .map_err(load)
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
        let storage = DuckdbPaxosStorage::<String>::open_in_memory().unwrap();
        {
            let conn = storage.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO paxos_decisions (slot, value) VALUES (?, ?)",
                params![0u64, "{not valid json"],
            )
            .unwrap();
        }
        assert_load_err(storage.load_decisions().await.unwrap_err());
    }

    #[tokio::test]
    async fn raft_load_decisions_returns_err_on_malformed_json() {
        let storage = DuckdbRaftStorage::<String>::open_in_memory().unwrap();
        {
            let conn = storage.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO raft_decisions (slot, value) VALUES (?, ?)",
                params![0u64, "{not valid json"],
            )
            .unwrap();
        }
        assert_load_err(storage.load_decisions().await.unwrap_err());
    }

    #[tokio::test]
    async fn raft_load_log_returns_err_on_malformed_json() {
        let storage = DuckdbRaftStorage::<String>::open_in_memory().unwrap();
        {
            let conn = storage.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO raft_log (log_index, term, value) VALUES (?, ?, ?)",
                params![0u64, 1u64, "{not valid json"],
            )
            .unwrap();
        }
        assert_load_err(storage.load_log().await.unwrap_err());
    }

    #[tokio::test]
    async fn raft_load_voted_for_returns_err_on_partial_state() {
        // name set, incarnation NULL
        {
            let storage = DuckdbRaftStorage::<String>::open_in_memory().unwrap();
            {
                let conn = storage.conn.lock().unwrap();
                conn.execute(
                    "UPDATE raft_meta SET voted_for_name = ?, voted_for_incarnation = NULL \
                     WHERE id = 0",
                    params!["a"],
                )
                .unwrap();
            }
            assert_load_err(storage.load_voted_for().await.unwrap_err());
        }

        // incarnation set, name NULL
        {
            let storage = DuckdbRaftStorage::<String>::open_in_memory().unwrap();
            {
                let conn = storage.conn.lock().unwrap();
                conn.execute(
                    "UPDATE raft_meta SET voted_for_name = NULL, voted_for_incarnation = ? \
                     WHERE id = 0",
                    params![5u64],
                )
                .unwrap();
            }
            assert_load_err(storage.load_voted_for().await.unwrap_err());
        }
    }
}
