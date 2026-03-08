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

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn session_unknown_table_maps_to_invalid_argument() {
        let err = DemoflightError::Session(SessionError::UnknownTable("players".into()));
        let status: Status = err.into();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("players"));
    }

    #[test]
    fn session_sql_error_maps_to_invalid_argument() {
        let err = DemoflightError::Session(SessionError::Sql("syntax error".into()));
        let status: Status = err.into();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn session_pipeline_breaker_maps_to_invalid_argument() {
        let err = DemoflightError::Session(SessionError::PipelineBreaker(
            "SortExec, HashAggregateExec".into(),
        ));
        let status: Status = err.into();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("SortExec"));
    }

    #[test]
    fn session_already_started_maps_to_failed_precondition() {
        let err = DemoflightError::Session(SessionError::AlreadyStarted);
        let status: Status = err.into();
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[test]
    fn session_no_queries_maps_to_failed_precondition() {
        let err = DemoflightError::Session(SessionError::NoQueries);
        let status: Status = err.into();
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[test]
    fn session_internal_error_maps_to_internal() {
        let err = DemoflightError::Session(SessionError::Internal("unexpected state".into()));
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn session_parser_error_maps_to_internal() {
        let err = DemoflightError::Session(SessionError::Parser("corrupt packet".into()));
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn session_datafusion_error_maps_to_internal() {
        let err = DemoflightError::Session(SessionError::DataFusion("plan failed".into()));
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn gotv_connection_maps_to_unavailable() {
        let err = DemoflightError::GotvConnection("connection refused".into());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Unavailable);
    }

    #[test]
    fn max_sessions_maps_to_resource_exhausted() {
        let err = DemoflightError::MaxSessionsReached;
        let status: Status = err.into();
        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[test]
    fn jwt_validation_maps_to_unauthenticated() {
        let err = DemoflightError::JwtValidation("expired token".into());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Unauthenticated);
    }
}
