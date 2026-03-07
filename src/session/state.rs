use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use dashmap::DashMap;
use datafusion::arrow::datatypes::Schema;
use demofusion::gotv::{GotvError, QueryHandle, SpectateSession, StreamingStats};
use parking_lot::Mutex as SyncMutex;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{DemoflightError, Result};
use crate::source::SourceInfo;

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: Arc<str>,
    pub arrow_schema: Arc<Schema>,
    pub ipc_bytes: Vec<u8>,
}

pub struct StreamingSession {
    pub id: Uuid,
    pub source: SourceInfo,
    pub table_schemas: HashMap<Arc<str>, TableSchema>,
    pub created_at: Instant,

    spectate: Mutex<Option<SpectateSession>>,
    query_handles: DashMap<Uuid, QueryHandle>,
    cancel_token: OnceCell<CancellationToken>,
    parser_handle: OnceCell<JoinHandle<std::result::Result<(), GotvError>>>,
    streaming_started_at: OnceCell<Instant>,
    streaming_stats: OnceCell<Arc<StreamingStats>>,

    /// Tracks when queries were last registered OR when a handle was last claimed.
    last_activity_at: SyncMutex<Instant>,

    /// Number of query handles that have been claimed via take_query_handle.
    claimed_queries: AtomicUsize,

    /// Currently streaming (DoGet in progress)
    active_streams: AtomicUsize,

    /// Finished streaming
    completed_streams: AtomicUsize,

    /// Prevents struct literal construction outside this module.
    _private: (),
}

impl StreamingSession {
    pub fn new(
        id: Uuid,
        source: SourceInfo,
        table_schemas: Vec<TableSchema>,
        spectate: SpectateSession,
    ) -> Self {
        let schema_map: HashMap<Arc<str>, TableSchema> = table_schemas
            .into_iter()
            .map(|s| (Arc::clone(&s.name), s))
            .collect();

        Self {
            id,
            source,
            table_schemas: schema_map,
            created_at: Instant::now(),
            spectate: Mutex::new(Some(spectate)),
            query_handles: DashMap::new(),
            cancel_token: OnceCell::new(),
            parser_handle: OnceCell::new(),
            streaming_started_at: OnceCell::new(),
            streaming_stats: OnceCell::new(),
            last_activity_at: SyncMutex::new(Instant::now()),
            claimed_queries: AtomicUsize::new(0),
            active_streams: AtomicUsize::new(0),
            completed_streams: AtomicUsize::new(0),
            _private: (),
        }
    }

    pub fn table_names(&self) -> Vec<Arc<str>> {
        let mut names: Vec<_> = self.table_schemas.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn cancel_token(&self) -> Option<&CancellationToken> {
        self.cancel_token.get()
    }

    /// Abort the session due to an error or client disconnect.
    /// Triggers immediate shutdown of the GOTV connection.
    pub fn cancel(&self) {
        if let Some(token) = self.cancel_token.get() {
            tracing::warn!(session_id = %self.id, "Session cancelled");
            token.cancel();
        }
    }

    /// Signal that all queries have completed successfully.
    /// Triggers graceful shutdown of the GOTV connection.
    pub fn complete(&self) {
        if let Some(token) = self.cancel_token.get() {
            tracing::info!(session_id = %self.id, "Session completed, closing GOTV connection");
            token.cancel();
        }
    }

    pub fn is_streaming(&self) -> bool {
        self.streaming_started_at.get().is_some()
    }

    pub fn streaming_started_at(&self) -> Option<Instant> {
        self.streaming_started_at.get().copied()
    }

    pub fn streaming_stats(&self) -> Option<&Arc<StreamingStats>> {
        self.streaming_stats.get()
    }

    pub fn unclaimed_handle_count(&self) -> usize {
        self.query_handles.len()
    }

    pub fn has_unclaimed_handles(&self) -> bool {
        !self.query_handles.is_empty()
    }

    pub fn active_stream_count(&self) -> usize {
        self.active_streams.load(Ordering::Relaxed)
    }

    pub fn completed_stream_count(&self) -> usize {
        self.completed_streams.load(Ordering::Relaxed)
    }

    pub fn total_query_count(&self) -> usize {
        self.query_handles.len() + self.claimed_queries.load(Ordering::Relaxed)
    }

    pub fn on_stream_start(&self, query_id: &Uuid) {
        let active = self.active_streams.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!(
            session_id = %self.id,
            query_id = %query_id,
            active_streams = active,
            "Query stream started"
        );
    }

    pub fn on_stream_complete(&self, query_id: &Uuid, rows_streamed: u64, had_error: bool) {
        let active = self.active_streams.fetch_sub(1, Ordering::Relaxed) - 1;
        let completed = self.completed_streams.fetch_add(1, Ordering::Relaxed) + 1;
        let total = self.total_query_count();

        if had_error {
            tracing::warn!(
                session_id = %self.id,
                query_id = %query_id,
                rows_streamed = rows_streamed,
                active_streams = active,
                completed_streams = completed,
                total_queries = total,
                "Query stream completed with error"
            );
        } else {
            tracing::info!(
                session_id = %self.id,
                query_id = %query_id,
                rows_streamed = rows_streamed,
                active_streams = active,
                completed_streams = completed,
                total_queries = total,
                "Query stream completed"
            );
        }

        // All streams completed when: no active streams, no unclaimed handles,
        // and we've completed at least one query
        if active == 0 && !self.has_unclaimed_handles() && completed > 0 {
            tracing::info!(
                session_id = %self.id,
                total_queries = total,
                "All query streams completed"
            );
            self.complete();
        }
    }

    pub async fn add_query(&self, sql: &str) -> Result<Uuid> {
        let mut spectate_guard = self.spectate.lock().await;

        let spectate = spectate_guard
            .as_mut()
            .ok_or(DemoflightError::SessionLocked)?;

        let query_id = Uuid::new_v4();

        let handle = spectate
            .add_query(sql)
            .await
            .map_err(|e| DemoflightError::SqlValidation(e.to_string()))?;

        self.query_handles.insert(query_id, handle);
        *self.last_activity_at.lock() = Instant::now();

        tracing::debug!(
            session_id = %self.id,
            query_id = %query_id,
            total_queries = self.total_query_count(),
            "Query added to session"
        );

        Ok(query_id)
    }

    pub async fn ensure_streaming(&self) -> Result<()> {
        let mut spectate_guard = self.spectate.lock().await;

        let Some(spectate) = spectate_guard.take() else {
            tracing::debug!("ensure_streaming: already streaming");
            return Ok(());
        };

        if self.query_handles.is_empty() {
            *spectate_guard = Some(spectate);
            return Err(DemoflightError::InvalidState {
                expected: "at least one query".into(),
                actual: "no queries registered".into(),
            });
        }

        tracing::info!(
            session_id = %self.id,
            query_count = self.query_handles.len(),
            "Starting streaming session"
        );

        let cancel_token = CancellationToken::new();

        let result = spectate
            .start_with_stats(cancel_token.clone())
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to start streaming");
                DemoflightError::GotvConnection(e.to_string())
            })?;

        tracing::info!(session_id = %self.id, "Parser task spawned");

        let _ = self.cancel_token.set(cancel_token);
        let _ = self.parser_handle.set(result.parser_handle);
        let _ = self.streaming_started_at.set(Instant::now());
        if let Some(stats) = result.stats {
            let _ = self.streaming_stats.set(stats);
        }

        Ok(())
    }

    pub async fn take_query_handle(&self, query_id: &Uuid) -> Result<QueryHandle> {
        tracing::debug!(query_id = %query_id, "take_query_handle called");
        self.ensure_streaming().await?;

        let result = self
            .query_handles
            .remove(query_id)
            .map(|(_, handle)| {
                // Track that this query has been claimed
                self.claimed_queries.fetch_add(1, Ordering::Relaxed);
                // Update last activity when a handle is claimed
                *self.last_activity_at.lock() = Instant::now();
                handle
            })
            .ok_or_else(|| {
                if self.is_streaming() {
                    DemoflightError::QueryAlreadyConsumed(query_id.to_string())
                } else {
                    DemoflightError::QueryNotFound(query_id.to_string())
                }
            });

        tracing::debug!(
            query_id = %query_id,
            found = result.is_ok(),
            remaining_handles = self.query_handles.len(),
            "take_query_handle result"
        );

        result
    }

    pub fn is_expired(&self, max_duration_secs: u64, inactivity_timeout_secs: u64) -> bool {
        let now = Instant::now();
        let age = now.duration_since(self.created_at);

        // Absolute timeout - session has been alive too long
        if age.as_secs() > max_duration_secs {
            return true;
        }

        let completed = self.completed_streams.load(Ordering::Relaxed);
        let active = self.active_streams.load(Ordering::Relaxed);
        let claimed = self.claimed_queries.load(Ordering::Relaxed);

        // Session is complete - all claimed queries finished streaming,
        // no unclaimed handles remain
        // Mark as immediately expired for cleanup
        if claimed > 0 && completed == claimed && active == 0 && !self.has_unclaimed_handles() {
            return true;
        }

        // Check inactivity based on last activity (query add or handle claim)
        // This catches:
        // 1. Client registered queries but never called DoGet
        // 2. Client called DoGet on some queries but not all
        if self.has_unclaimed_handles() {
            let last_activity = *self.last_activity_at.lock();
            let inactivity = now.duration_since(last_activity);
            if inactivity.as_secs() > inactivity_timeout_secs {
                return true;
            }
        }

        false
    }
}

impl std::fmt::Debug for StreamingSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingSession")
            .field("id", &self.id)
            .field("source", &self.source.url)
            .field("table_count", &self.table_schemas.len())
            .field("is_streaming", &self.is_streaming())
            .field("unclaimed_handles", &self.unclaimed_handle_count())
            .field("created_at", &self.created_at)
            .finish()
    }
}

#[cfg(test)]
impl StreamingSession {
    pub fn new_for_test(table_schemas: Vec<TableSchema>) -> Self {
        use crate::source::SourceInfo;

        let schema_map: HashMap<Arc<str>, TableSchema> = table_schemas
            .into_iter()
            .map(|s| (Arc::clone(&s.name), s))
            .collect();

        Self {
            id: Uuid::new_v4(),
            source: SourceInfo::new("http://example.com"),
            table_schemas: schema_map,
            created_at: Instant::now(),
            spectate: Mutex::new(None),
            query_handles: DashMap::new(),
            cancel_token: OnceCell::new(),
            parser_handle: OnceCell::new(),
            streaming_started_at: OnceCell::new(),
            streaming_stats: OnceCell::new(),
            last_activity_at: SyncMutex::new(Instant::now()),
            claimed_queries: AtomicUsize::new(0),
            active_streams: AtomicUsize::new(0),
            completed_streams: AtomicUsize::new(0),
            _private: (),
        }
    }

    /// Simulate the passage of time by backdating created_at and last_activity_at
    pub fn with_age(mut self, age: std::time::Duration) -> Self {
        let past = Instant::now() - age;
        self.created_at = past;
        *self.last_activity_at.lock() = past;
        self
    }

    /// Simulate queries that were registered but not yet claimed (DoGet not called).
    ///
    /// NOTE: Currently a no-op because we can't create real QueryHandles without
    /// demofusion infrastructure. For expiration tests, use with_active_streams()
    /// or with_completed_streams() which set the claimed_queries counter.
    pub fn with_unclaimed_queries(self, _count: usize) -> Self {
        self
    }

    /// Simulate queries that have been claimed and are actively streaming
    pub fn with_active_streams(self, count: usize) -> Self {
        self.claimed_queries.store(count, Ordering::Relaxed);
        self.active_streams.store(count, Ordering::Relaxed);
        self
    }

    /// Simulate queries that have completed streaming
    pub fn with_completed_streams(self, count: usize) -> Self {
        self.claimed_queries.store(count, Ordering::Relaxed);
        self.completed_streams.store(count, Ordering::Relaxed);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table_schema(name: &str) -> TableSchema {
        TableSchema {
            name: name.into(),
            arrow_schema: Arc::new(Schema::empty()),
            ipc_bytes: vec![],
        }
    }

    #[test]
    fn test_table_names_sorted() {
        let schemas = vec![
            make_table_schema("Zebra"),
            make_table_schema("Apple"),
            make_table_schema("Mango"),
        ];

        let session = StreamingSession::new_for_test(schemas);
        let names = session.table_names();
        assert_eq!(
            names,
            vec![Arc::from("Apple"), Arc::from("Mango"), Arc::from("Zebra")]
        );
    }

    #[test]
    fn test_is_expired_by_absolute_timeout() {
        let session =
            StreamingSession::new_for_test(vec![]).with_age(std::time::Duration::from_secs(7200));

        assert!(session.is_expired(3600, 300));
    }

    #[test]
    fn test_not_expired_when_fresh() {
        let session = StreamingSession::new_for_test(vec![]);

        assert!(!session.is_expired(3600, 300));
    }

    #[test]
    fn test_expired_when_all_streams_completed() {
        // Session where all claimed queries have completed streaming
        let session = StreamingSession::new_for_test(vec![]).with_completed_streams(2);

        assert!(session.is_expired(3600, 300));
    }

    #[test]
    fn test_not_expired_with_active_streams() {
        // Session with active streams should not be expired
        let session = StreamingSession::new_for_test(vec![]).with_active_streams(1);

        assert!(!session.is_expired(3600, 300));
    }

    #[test]
    fn test_total_query_count_derived_from_state() {
        let session = StreamingSession::new_for_test(vec![]);
        assert_eq!(session.total_query_count(), 0);

        // Simulate claiming 3 queries
        session.claimed_queries.store(3, Ordering::Relaxed);
        assert_eq!(session.total_query_count(), 3);
    }
}
