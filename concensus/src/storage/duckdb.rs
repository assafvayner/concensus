use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::params;
use serde::{de::DeserializeOwned, Serialize};

use super::{AcceptorState, Storage};
use crate::config::NodeId;
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
                highest_promised_round UBIGINT,
                highest_promised_node_name TEXT,
                highest_promised_node_incarnation UBIGINT,
                accepted_round UBIGINT,
                accepted_node_name TEXT,
                accepted_node_incarnation UBIGINT,
                accepted_value TEXT
            );",
        )
        .map_err(|e| StorageError::Load(format!("failed to create tables: {e}")))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _phantom: PhantomData,
        })
    }
}

struct ProposalNumberColumns {
    round: Option<u64>,
    node_name: Option<String>,
    node_incarnation: Option<u64>,
}

impl ProposalNumberColumns {
    fn empty() -> Self {
        Self {
            round: None,
            node_name: None,
            node_incarnation: None,
        }
    }

    fn from_proposal_number(proposal_number: ProposalNumber) -> Self {
        let (round, node_id) = proposal_number;
        Self {
            round: Some(round),
            node_name: Some(node_id.name().to_string()),
            node_incarnation: Some(node_id.incarnation()),
        }
    }

    fn into_proposal_number(self, slot: u64, label: &str) -> Option<Option<ProposalNumber>> {
        match (self.round, self.node_name, self.node_incarnation) {
            (None, None, None) => Some(None),
            (Some(round), Some(node_name), Some(node_incarnation)) => {
                Some(Some((round, NodeId::new(node_name, node_incarnation))))
            }
            _ => {
                tracing::warn!(
                    slot,
                    label,
                    "skipping acceptor state with incomplete proposal number"
                );
                None
            }
        }
    }
}

fn split_proposal_number(proposal_number: Option<ProposalNumber>) -> ProposalNumberColumns {
    proposal_number.map_or_else(
        ProposalNumberColumns::empty,
        ProposalNumberColumns::from_proposal_number,
    )
}

struct AcceptedColumns {
    proposal_number: ProposalNumberColumns,
    value_json: Option<String>,
}

fn split_accepted<V>(accepted: Option<(ProposalNumber, V)>) -> Result<AcceptedColumns, StorageError>
where
    V: Serialize,
{
    match accepted {
        Some((proposal_number, value)) => {
            let value_json = serde_json::to_string(&value).map_err(|e| {
                StorageError::Persist(format!("failed to serialize accepted value: {e}"))
            })?;
            Ok(AcceptedColumns {
                proposal_number: ProposalNumberColumns::from_proposal_number(proposal_number),
                value_json: Some(value_json),
            })
        }
        None => Ok(AcceptedColumns {
            proposal_number: ProposalNumberColumns::empty(),
            value_json: None,
        }),
    }
}

fn restore_accepted<V>(
    slot: u64,
    proposal_number: ProposalNumberColumns,
    value_json: Option<String>,
) -> Option<Option<(ProposalNumber, V)>>
where
    V: DeserializeOwned,
{
    match (
        proposal_number.into_proposal_number(slot, "accepted"),
        value_json,
    ) {
        (Some(None), None) => Some(None),
        (Some(Some(proposal_number)), Some(value_json)) => {
            match serde_json::from_str::<V>(&value_json) {
                Ok(value) => Some(Some((proposal_number, value))),
                Err(e) => {
                    tracing::warn!(
                        slot,
                        "skipping acceptor state with bad accepted value JSON: {e}"
                    );
                    None
                }
            }
        }
        _ => {
            tracing::warn!(
                slot,
                "skipping acceptor state with incomplete accepted value"
            );
            None
        }
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
        let highest_promised = split_proposal_number(highest_promised);
        let accepted = split_accepted(accepted)?;

        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| StorageError::Persist(format!("failed to lock connection: {e}")))?;
            conn.execute(
                "INSERT OR REPLACE INTO acceptor_state (
                    slot,
                    highest_promised_round,
                    highest_promised_node_name,
                    highest_promised_node_incarnation,
                    accepted_round,
                    accepted_node_name,
                    accepted_node_incarnation,
                    accepted_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    slot,
                    highest_promised.round,
                    highest_promised.node_name,
                    highest_promised.node_incarnation,
                    accepted.proposal_number.round,
                    accepted.proposal_number.node_name,
                    accepted.proposal_number.node_incarnation,
                    accepted.value_json,
                ],
            )
            .map_err(|e| StorageError::Persist(format!("failed to insert acceptor state: {e}")))?;
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
                .prepare(
                    "SELECT
                        slot,
                        highest_promised_round,
                        highest_promised_node_name,
                        highest_promised_node_incarnation,
                        accepted_round,
                        accepted_node_name,
                        accepted_node_incarnation,
                        accepted_value
                    FROM acceptor_state",
                )
                .map_err(|e| StorageError::Load(format!("failed to prepare query: {e}")))?;
            let rows = stmt
                .query_map([], |row| {
                    let slot: u64 = row.get(0)?;
                    Ok((
                        slot,
                        ProposalNumberColumns {
                            round: row.get(1)?,
                            node_name: row.get(2)?,
                            node_incarnation: row.get(3)?,
                        },
                        ProposalNumberColumns {
                            round: row.get(4)?,
                            node_name: row.get(5)?,
                            node_incarnation: row.get(6)?,
                        },
                        row.get(7)?,
                    ))
                })
                .map_err(|e| StorageError::Load(format!("failed to query acceptor states: {e}")))?;

            let mut states = Vec::new();
            for row in rows {
                let (slot, highest_promised_columns, accepted_columns, accepted_value_json) =
                    row.map_err(|e| StorageError::Load(format!("failed to read row: {e}")))?;

                let highest_promised =
                    match highest_promised_columns.into_proposal_number(slot, "highest_promised") {
                        Some(highest_promised) => highest_promised,
                        None => continue,
                    };

                let accepted = match restore_accepted(slot, accepted_columns, accepted_value_json) {
                    Some(accepted) => accepted,
                    None => continue,
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
    use std::collections::HashSet;
    use std::path::PathBuf;

    use crate::config::NodeId;
    use crate::message::ProposalNumber;
    use crate::storage::{DuckDbStorage, Storage};

    use duckdb::params;

    fn temp_db_path() -> PathBuf {
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
    async fn duckdb_acceptor_state_schema_uses_typed_columns() {
        let path = temp_db_path();
        let _storage = DuckDbStorage::<String>::new(&path).unwrap();
        let conn = duckdb::Connection::open(&path).unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info('acceptor_state')").unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
            })
            .unwrap();
        let columns: HashSet<(String, String)> = rows.map(|row| row.unwrap()).collect();
        let column_names: HashSet<&str> = columns.iter().map(|(name, _)| name.as_str()).collect();

        assert!(column_names.contains("slot"));
        assert!(columns.contains(&("slot".to_string(), "UBIGINT".to_string())));
        assert!(column_names.contains("highest_promised_round"));
        assert!(columns.contains(&("highest_promised_round".to_string(), "UBIGINT".to_string())));
        assert!(column_names.contains("highest_promised_node_name"));
        assert!(columns.contains(&(
            "highest_promised_node_name".to_string(),
            "VARCHAR".to_string()
        )));
        assert!(column_names.contains("highest_promised_node_incarnation"));
        assert!(columns.contains(&(
            "highest_promised_node_incarnation".to_string(),
            "UBIGINT".to_string()
        )));
        assert!(column_names.contains("accepted_round"));
        assert!(columns.contains(&("accepted_round".to_string(), "UBIGINT".to_string())));
        assert!(column_names.contains("accepted_node_name"));
        assert!(columns.contains(&("accepted_node_name".to_string(), "VARCHAR".to_string())));
        assert!(column_names.contains("accepted_node_incarnation"));
        assert!(columns.contains(&(
            "accepted_node_incarnation".to_string(),
            "UBIGINT".to_string()
        )));
        assert!(column_names.contains("accepted_value"));
        assert!(columns.contains(&("accepted_value".to_string(), "VARCHAR".to_string())));
        assert!(!column_names.contains("highest_promised"));
        assert!(!column_names.contains("accepted"));

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
    async fn duckdb_save_decision_replaces_existing_value() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::new(&path).unwrap();

        storage.save_decision(7, "old".to_string()).await.unwrap();
        storage.save_decision(7, "new".to_string()).await.unwrap();

        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions, vec![(7, "new".to_string())]);

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

        let conn = duckdb::Connection::open(&path).unwrap();
        let row = conn
            .query_row(
                "SELECT
                    highest_promised_round,
                    highest_promised_node_name,
                    highest_promised_node_incarnation,
                    accepted_round,
                    accepted_node_name,
                    accepted_node_incarnation,
                    accepted_value
                FROM acceptor_state
                WHERE slot = ?",
                params![1u64],
                |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u64>(2)?,
                        row.get::<_, u64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, u64>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row.0, 2);
        assert_eq!(row.1, "node-1");
        assert_eq!(row.2, 1000);
        assert_eq!(row.3, 1);
        assert_eq!(row.4, "node-1");
        assert_eq!(row.5, 1000);
        assert_eq!(row.6, serde_json::to_string(&"hello".to_string()).unwrap());

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
    async fn duckdb_save_decision_only_cleans_matching_acceptor_state() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::new(&path).unwrap();

        storage
            .save_acceptor_state(1, Some(make_pn(1)), Some((make_pn(1), "one".to_string())))
            .await
            .unwrap();
        storage
            .save_acceptor_state(2, Some(make_pn(2)), Some((make_pn(2), "two".to_string())))
            .await
            .unwrap();

        storage.save_decision(1, "one".to_string()).await.unwrap();

        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 2);
        assert_eq!(states[0].accepted, Some((make_pn(2), "two".to_string())));

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
    async fn duckdb_save_acceptor_state_upsert_clears_accepted_columns() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();

        storage
            .save_acceptor_state(1, Some(make_pn(2)), Some((make_pn(1), "old".to_string())))
            .await
            .unwrap();
        storage
            .save_acceptor_state(1, Some(make_pn(3)), None)
            .await
            .unwrap();

        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].highest_promised, Some(make_pn(3)));
        assert!(states[0].accepted.is_none());

        let conn = duckdb::Connection::open(&path).unwrap();
        let row = conn
            .query_row(
                "SELECT
                    accepted_round,
                    accepted_node_name,
                    accepted_node_incarnation,
                    accepted_value
                FROM acceptor_state
                WHERE slot = ?",
                params![1u64],
                |row| {
                    Ok((
                        row.get::<_, Option<u64>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<u64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, (None, None, None, None));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_persistence_survives_reopen() {
        let path = temp_db_path();

        {
            let mut storage = DuckDbStorage::new(&path).unwrap();
            storage.save_decision(0, "hello".to_string()).await.unwrap();
            storage
                .save_acceptor_state(5, Some(make_pn(3)), Some((make_pn(2), "world".to_string())))
                .await
                .unwrap();
        }

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

    #[tokio::test]
    async fn duckdb_load_decisions_skips_bad_json_rows() {
        let path = temp_db_path();
        {
            let mut storage = DuckDbStorage::new(&path).unwrap();
            storage.save_decision(0, "valid".to_string()).await.unwrap();
        }

        let conn = duckdb::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO decisions (slot, value) VALUES (?, ?)",
            params![1u64, "{not-json"],
        )
        .unwrap();
        drop(conn);

        let storage = DuckDbStorage::<String>::new(&path).unwrap();
        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions, vec![(0, "valid".to_string())]);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duckdb_load_acceptor_states_skips_invalid_rows() {
        let path = temp_db_path();

        {
            let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
            storage
                .save_acceptor_state(
                    5,
                    Some(make_pn(3)),
                    Some((make_pn(2), "accepted".to_string())),
                )
                .await
                .unwrap();
        }

        let conn = duckdb::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO acceptor_state (
                slot,
                highest_promised_round,
                highest_promised_node_name,
                highest_promised_node_incarnation,
                accepted_round,
                accepted_node_name,
                accepted_node_incarnation,
                accepted_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                6u64,
                4u64,
                Option::<String>::None,
                1000u64,
                Option::<u64>::None,
                Option::<String>::None,
                Option::<u64>::None,
                Option::<String>::None,
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO acceptor_state (
                slot,
                highest_promised_round,
                highest_promised_node_name,
                highest_promised_node_incarnation,
                accepted_round,
                accepted_node_name,
                accepted_node_incarnation,
                accepted_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                7u64,
                3u64,
                "node-1",
                1000u64,
                2u64,
                Option::<String>::None,
                1000u64,
                serde_json::to_string(&"accepted".to_string()).unwrap(),
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO acceptor_state (
                slot,
                highest_promised_round,
                highest_promised_node_name,
                highest_promised_node_incarnation,
                accepted_round,
                accepted_node_name,
                accepted_node_incarnation,
                accepted_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                8u64,
                3u64,
                "node-1",
                1000u64,
                2u64,
                "node-1",
                1000u64,
                "{bad-accepted",
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO acceptor_state (
                slot,
                highest_promised_round,
                highest_promised_node_name,
                highest_promised_node_incarnation,
                accepted_round,
                accepted_node_name,
                accepted_node_incarnation,
                accepted_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                9u64,
                3u64,
                "node-1",
                1000u64,
                Option::<u64>::None,
                Option::<String>::None,
                Option::<u64>::None,
                serde_json::to_string(&"accepted-without-proposal".to_string()).unwrap(),
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO acceptor_state (
                slot,
                highest_promised_round,
                highest_promised_node_name,
                highest_promised_node_incarnation,
                accepted_round,
                accepted_node_name,
                accepted_node_incarnation,
                accepted_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                10u64,
                3u64,
                "node-1",
                1000u64,
                2u64,
                "node-1",
                1000u64,
                Option::<String>::None,
            ],
        )
        .unwrap();
        drop(conn);

        let storage = DuckDbStorage::<String>::new(&path).unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 5);
        assert_eq!(states[0].highest_promised, Some(make_pn(3)));
        assert_eq!(
            states[0].accepted,
            Some((make_pn(2), "accepted".to_string()))
        );

        let _ = std::fs::remove_file(&path);
    }
}
