use demofusion::session::SessionError;
use thiserror::Error;
use tonic::Status;

#[derive(Debug, Error)]
pub enum DemoflightError {
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Query not found: {0}")]
    QueryNotFound(String),

    #[error("Session is locked, cannot add queries after streaming started")]
    SessionLocked,

    #[error("Invalid session state: expected {expected}, got {actual}")]
    InvalidState { expected: String, actual: String },

    #[error("Invalid source URL: {0}")]
    InvalidSourceUrl(String),

    #[error("SQL validation failed: {0}")]
    SqlValidation(String),

    #[error("JWT validation failed: {0}")]
    JwtValidation(String),

    #[error("Query already consumed: {0}")]
    QueryAlreadyConsumed(String),

    #[error("Invalid ticket format")]
    InvalidTicket,

    #[error("Maximum sessions reached")]
    MaxSessionsReached,

    #[error("Maximum queries per session reached")]
    MaxQueriesReached,

    #[error("GOTV connection failed: {0}")]
    GotvConnection(String),

    #[error("Schema discovery failed: {0}")]
    SchemaDiscovery(String),

    #[error("Query execution failed: {0}")]
    QueryExecution(String),

    #[error("Protobuf decode error: {0}")]
    ProtoDecode(#[from] prost::DecodeError),

    #[error("Streaming session error: {0}")]
    Session(#[from] SessionError),

    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<DemoflightError> for Status {
    fn from(e: DemoflightError) -> Self {
        use DemoflightError::*;

        let msg = e.to_string();

        match e {
            SessionNotFound(_) | QueryNotFound(_) => Status::not_found(msg),

            InvalidSourceUrl(_) | SqlValidation(_) | ProtoDecode(_) | InvalidTicket => {
                Status::invalid_argument(msg)
            }

            SessionLocked | InvalidState { .. } | QueryAlreadyConsumed(_) => {
                Status::failed_precondition(msg)
            }

            MaxSessionsReached | MaxQueriesReached => Status::resource_exhausted(msg),

            JwtValidation(_) => Status::unauthenticated(msg),

            GotvConnection(_) => Status::unavailable(msg),

            Session(ref inner) => match inner {
                SessionError::UnknownTable(_) | SessionError::Sql(_) => {
                    Status::invalid_argument(msg)
                }
                SessionError::AlreadyStarted | SessionError::NoQueries => {
                    Status::failed_precondition(msg)
                }
                SessionError::PipelineBreaker(_) => Status::invalid_argument(msg),
                _ => Status::internal(msg),
            },

            SchemaDiscovery(_) | QueryExecution(_) | DataFusion(_) | Internal(_) => {
                Status::internal(msg)
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, DemoflightError>;
