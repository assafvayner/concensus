use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::params;
use serde::{de::DeserializeOwned, Serialize};

use super::{AcceptorState, Storage};
use crate::error::StorageError;
use crate::message::ProposalNumber;

/// DuckDB-backed [`Storage`] implementation for crash recovery.
///
/// Persists decisions and acceptor state to a DuckDB database file. All
/// database operations run inside [`tokio::task::spawn_blocking`] so that the
/// synchronous DuckDB driver does not block the async runtime.
pub struct DuckDbStorage<V> {
    conn: Arc<Mutex<duckdb::Connection>>,
    _phantom: PhantomData<V>,
}

impl<V> DuckDbStorage<V> {
    /// Opens or creates a DuckDB database at `path` and ensures the schema
    /// tables exist.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let conn = duckdb::Connection::open(path)
            .map_err(|e| StorageError::Load(format!("failed to open DuckDB: {e}")))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS decisions (
                slot UBIGINT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS acceptor_state (
                slot UBIGINT PRIMARY KEY,
                highest_promised TEXT,
                accepted TEXT
            );",
        )
        .map_err(|e| StorageError::Load(format!("failed to create tables: {e}")))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<V> Storage<V> for DuckDbStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        let value_json = serde_json::to_string(&value)
            .map_err(|e| StorageError::Persist(format!("failed to serialize value: {e}")))?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| StorageError::Persist(format!("failed to lock connection: {e}")))?;
            conn.execute(
                "INSERT OR REPLACE INTO decisions (slot, value) VALUES (?, ?)",
                params![slot, value_json],
            )
            .map_err(|e| StorageError::Persist(format!("failed to insert decision: {e}")))?;
            conn.execute("DELETE FROM acceptor_state WHERE slot = ?", params![slot])
                .map_err(|e| {
                    StorageError::Delete(format!("failed to delete acceptor state: {e}"))
                })?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Persist(format!("spawn_blocking failed: {e}")))?
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| StorageError::Load(format!("failed to lock connection: {e}")))?;
            let mut stmt = conn
                .prepare("SELECT slot, value FROM decisions")
                .map_err(|e| StorageError::Load(format!("failed to prepare query: {e}")))?;
            let rows = stmt
                .query_map([], |row| {
                    let slot: u64 = row.get(0)?;
                    let value_json: String = row.get(1)?;
                    Ok((slot, value_json))
                })
                .map_err(|e| StorageError::Load(format!("failed to query decisions: {e}")))?;

            let mut decisions = Vec::new();
            for row in rows {
                let (slot, value_json) =
                    row.map_err(|e| StorageError::Load(format!("failed to read row: {e}")))?;
                match serde_json::from_str::<V>(&value_json) {
                    Ok(value) => decisions.push((slot, value)),
                    Err(e) => {
                        tracing::warn!(slot, "skipping decision with bad JSON: {e}");
                    }
                }
            }
            Ok(decisions)
        })
        .await
        .map_err(|e| StorageError::Load(format!("spawn_blocking failed: {e}")))?
    }

    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<ProposalNumber>,
        accepted: Option<(ProposalNumber, V)>,
    ) -> Result<(), StorageError> {
        let hp_json = highest_promised
            .map(|hp| serde_json::to_string(&hp))
            .transpose()
            .map_err(|e| {
                StorageError::Persist(format!("failed to serialize highest_promised: {e}"))
            })?;
        let accepted_json = accepted
            .map(|a| serde_json::to_string(&a))
            .transpose()
            .map_err(|e| StorageError::Persist(format!("failed to serialize accepted: {e}")))?;

        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                StorageError::Persist(format!("failed to lock connection: {e}"))
            })?;
            conn.execute(
                "INSERT OR REPLACE INTO acceptor_state (slot, highest_promised, accepted) VALUES (?, ?, ?)",
                params![slot, hp_json, accepted_json],
            )
            .map_err(|e| {
                StorageError::Persist(format!("failed to insert acceptor state: {e}"))
            })?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Persist(format!("spawn_blocking failed: {e}")))?
    }

    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| StorageError::Load(format!("failed to lock connection: {e}")))?;
            let mut stmt = conn
                .prepare("SELECT slot, highest_promised, accepted FROM acceptor_state")
                .map_err(|e| StorageError::Load(format!("failed to prepare query: {e}")))?;
            let rows = stmt
                .query_map([], |row| {
                    let slot: u64 = row.get(0)?;
                    let hp_json: Option<String> = row.get(1)?;
                    let accepted_json: Option<String> = row.get(2)?;
                    Ok((slot, hp_json, accepted_json))
                })
                .map_err(|e| StorageError::Load(format!("failed to query acceptor states: {e}")))?;

            let mut states = Vec::new();
            for row in rows {
                let (slot, hp_json, accepted_json) =
                    row.map_err(|e| StorageError::Load(format!("failed to read row: {e}")))?;

                let highest_promised = match hp_json {
                    Some(json) => match serde_json::from_str::<ProposalNumber>(&json) {
                        Ok(hp) => Some(hp),
                        Err(e) => {
                            tracing::warn!(
                                slot,
                                "skipping acceptor state with bad highest_promised JSON: {e}"
                            );
                            continue;
                        }
                    },
                    None => None,
                };

                let accepted = match accepted_json {
                    Some(json) => match serde_json::from_str::<(ProposalNumber, V)>(&json) {
                        Ok(a) => Some(a),
                        Err(e) => {
                            tracing::warn!(
                                slot,
                                "skipping acceptor state with bad accepted JSON: {e}"
                            );
                            continue;
                        }
                    },
                    None => None,
                };

                states.push(AcceptorState {
                    slot,
                    highest_promised,
                    accepted,
                });
            }
            Ok(states)
        })
        .await
        .map_err(|e| StorageError::Load(format!("spawn_blocking failed: {e}")))?
    }

    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| StorageError::Delete(format!("failed to lock connection: {e}")))?;
            conn.execute("DELETE FROM acceptor_state WHERE slot = ?", params![slot])
                .map_err(|e| {
                    StorageError::Delete(format!("failed to delete acceptor state: {e}"))
                })?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Delete(format!("spawn_blocking failed: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeId;

    fn temp_db_path() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("concensus-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{}.db", rand::random::<u64>()))
    }

    fn test_node_id() -> NodeId {
        NodeId::new("node-1", 1000)
    }

    fn make_pn(round: u64) -> ProposalNumber {
        (round, test_node_id())
    }

    #[tokio::test]
    async fn duckdb_create_new_db() {
        let path = temp_db_path();
        let _storage = DuckDbStorage::<String>::new(&path).unwrap();
        assert!(path.exists());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_save_and_load_decisions() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::new(&path).unwrap();

        storage.save_decision(0, "hello".to_string()).await.unwrap();
        storage.save_decision(2, "world".to_string()).await.unwrap();

        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions.len(), 2);
        assert!(decisions.contains(&(0, "hello".to_string())));
        assert!(decisions.contains(&(2, "world".to_string())));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_save_and_load_acceptor_state() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();

        storage
            .save_acceptor_state(1, Some(make_pn(1)), None)
            .await
            .unwrap();

        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 1);
        assert_eq!(states[0].highest_promised, Some(make_pn(1)));
        assert!(states[0].accepted.is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_save_acceptor_state_with_accepted_value() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::new(&path).unwrap();

        storage
            .save_acceptor_state(1, Some(make_pn(2)), Some((make_pn(1), "hello".to_string())))
            .await
            .unwrap();

        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 1);
        assert_eq!(states[0].highest_promised, Some(make_pn(2)));
        assert_eq!(states[0].accepted, Some((make_pn(1), "hello".to_string())));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_save_decision_cleans_acceptor_state() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::new(&path).unwrap();

        storage
            .save_acceptor_state(1, Some(make_pn(1)), Some((make_pn(1), "hello".to_string())))
            .await
            .unwrap();
        assert_eq!(storage.load_acceptor_states().await.unwrap().len(), 1);

        storage.save_decision(1, "hello".to_string()).await.unwrap();
        assert!(storage.load_acceptor_states().await.unwrap().is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_delete_acceptor_state() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();

        storage
            .save_acceptor_state(5, Some(make_pn(2)), None)
            .await
            .unwrap();
        assert_eq!(storage.load_acceptor_states().await.unwrap().len(), 1);

        storage.delete_acceptor_state(5).await.unwrap();
        assert!(storage.load_acceptor_states().await.unwrap().is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_save_acceptor_state_upserts() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();

        storage
            .save_acceptor_state(1, Some(make_pn(1)), None)
            .await
            .unwrap();
        storage
            .save_acceptor_state(1, Some(make_pn(2)), None)
            .await
            .unwrap();

        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].highest_promised, Some(make_pn(2)));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_persistence_survives_reopen() {
        let path = temp_db_path();

        // Write data and drop
        {
            let mut storage = DuckDbStorage::new(&path).unwrap();
            storage.save_decision(0, "hello".to_string()).await.unwrap();
            storage
                .save_acceptor_state(5, Some(make_pn(3)), Some((make_pn(2), "world".to_string())))
                .await
                .unwrap();
        }

        // Reopen and verify
        {
            let storage = DuckDbStorage::<String>::new(&path).unwrap();
            let decisions = storage.load_decisions().await.unwrap();
            assert_eq!(decisions.len(), 1);
            assert!(decisions.contains(&(0, "hello".to_string())));

            let states = storage.load_acceptor_states().await.unwrap();
            assert_eq!(states.len(), 1);
            assert_eq!(states[0].slot, 5);
            assert_eq!(states[0].highest_promised, Some(make_pn(3)));
            assert_eq!(states[0].accepted, Some((make_pn(2), "world".to_string())));
        }

        let _ = std::fs::remove_file(&path);
    }
}
