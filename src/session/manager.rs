use std::sync::Arc;

use dashmap::DashMap;
use uuid::Uuid;

use crate::config::DemoflightConfig;
use crate::error::{DemoflightError, Result};

use super::state::StreamingSession;

pub struct SessionManager {
    sessions: DashMap<Uuid, Arc<StreamingSession>>,
    config: Arc<DemoflightConfig>,
}

impl SessionManager {
    pub fn new(config: Arc<DemoflightConfig>) -> Self {
        Self {
            sessions: DashMap::new(),
            config,
        }
    }

    pub fn insert(&self, session: StreamingSession) -> Result<()> {
        if self.sessions.len() >= self.config.max_sessions {
            return Err(DemoflightError::MaxSessionsReached);
        }

        self.sessions.insert(session.id, Arc::new(session));
        Ok(())
    }

    pub fn get(&self, id: &Uuid) -> Option<Arc<StreamingSession>> {
        self.sessions.get(id).map(|r| Arc::clone(&r))
    }

    pub fn get_or_err(&self, id: &Uuid) -> Result<Arc<StreamingSession>> {
        self.get(id)
            .ok_or_else(|| DemoflightError::SessionNotFound(id.to_string()))
    }

    pub fn remove(&self, id: &Uuid) -> Option<Arc<StreamingSession>> {
        self.sessions.remove(id).map(|(_, v)| v)
    }

    pub fn cleanup_expired(&self) -> Vec<Uuid> {
        let max_duration = self.config.max_session_duration_secs as u64;
        let inactivity_timeout = self.config.inactivity_timeout_secs as u64;

        let expired: Vec<Uuid> = self
            .sessions
            .iter()
            .filter(
                |entry: &dashmap::mapref::multiple::RefMulti<'_, Uuid, Arc<StreamingSession>>| {
                    entry.value().is_expired(max_duration, inactivity_timeout)
                },
            )
            .map(|entry| *entry.key())
            .collect();

        for id in &expired {
            if let Some((_, session)) = self.sessions.remove(id) {
                session.cancel();
            }
        }

        expired
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn max_queries_per_session(&self) -> usize {
        self.config.max_queries_per_session
    }

    pub fn for_each_session<F>(&self, mut f: F)
    where
        F: FnMut(&Arc<StreamingSession>),
    {
        for entry in self.sessions.iter() {
            f(entry.value());
        }
    }
}
