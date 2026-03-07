use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::{Any, Command};
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult,
    SchemaResult, Ticket,
};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use demofusion::gotv::QueryHandle;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use tracing::instrument;
use uuid::Uuid;

use crate::actions::handle_register_source;
use crate::commands::{route_command, RouteContext};
use crate::config::DemoflightConfig;
use crate::flight_sql::{build_get_tables_response, build_sql_info_data, get_table_schema};
use crate::proto::RegisterSourceRequest;
use crate::session::jwt::JwtHandler;
use crate::session::state::StreamingSession;
use crate::session::SessionManager;
use crate::tickets::decode_ticket;

fn describe_descriptor(descriptor: &FlightDescriptor) -> String {
    if !descriptor.cmd.is_empty() {
        if let Ok(any) = Any::decode(&descriptor.cmd[..]) {
            if let Ok(command) = Command::try_from(any) {
                return command.type_url().to_string();
            }
        }
        return "unknown_command".to_string();
    }
    if !descriptor.path.is_empty() {
        return format!("path:{}", descriptor.path.join("/"));
    }
    "empty".to_string()
}

pub struct DemoflightService {
    config: Arc<DemoflightConfig>,
    session_manager: Arc<SessionManager>,
    jwt_handler: Arc<JwtHandler>,
}

impl DemoflightService {
    pub fn new(config: DemoflightConfig) -> Self {
        let jwt_handler = Arc::new(JwtHandler::new(&config.jwt_secret));
        let config = Arc::new(config);
        let session_manager = Arc::new(SessionManager::new(config.clone()));

        Self {
            config,
            session_manager,
            jwt_handler,
        }
    }

    pub fn session_manager(&self) -> &Arc<SessionManager> {
        &self.session_manager
    }

    pub fn jwt_handler(&self) -> &Arc<JwtHandler> {
        &self.jwt_handler
    }

    pub fn spawn_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let session_manager = self.session_manager.clone();
        let interval_secs = self.config.cleanup_interval_secs;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                let expired_ids = session_manager.cleanup_expired();
                if !expired_ids.is_empty() {
                    tracing::info!(expired_count = expired_ids.len(), "Cleaned up expired sessions");
                }
            }
        })
    }

    fn decode_action_body<T: Message + Default>(&self, body: &[u8]) -> Result<T, Status> {
        T::decode(body).map_err(|e| Status::invalid_argument(format!("Invalid request body: {e}")))
    }

    fn extract_session_from_metadata<T>(&self, request: &Request<T>) -> Result<Uuid, Status> {
        let auth_header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("Missing authorization header"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("Invalid authorization header"))?;

        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("Invalid authorization format, expected 'Bearer <token>'"))?;

        let claims = self.jwt_handler.decode(token).map_err(Status::from)?;

        claims
            .sub
            .parse()
            .map_err(|_| Status::unauthenticated("Invalid session ID in token"))
    }

    fn try_extract_session_from_metadata<T>(&self, request: &Request<T>) -> Option<Uuid> {
        let auth_header = request.metadata().get("authorization")?;
        let auth_str = auth_header.to_str().ok()?;
        let token = auth_str.strip_prefix("Bearer ")?;
        let claims = self.jwt_handler.decode(token).ok()?;
        claims.sub.parse().ok()
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

struct StreamLifecycleTracker {
    session: Arc<StreamingSession>,
    query_id: Uuid,
    rows_streamed: u64,
    had_error: bool,
}

impl StreamLifecycleTracker {
    fn new(session: Arc<StreamingSession>, query_id: Uuid) -> Self {
        session.on_stream_start(&query_id);
        Self {
            session,
            query_id,
            rows_streamed: 0,
            had_error: false,
        }
    }

    fn record_batch(&mut self, num_rows: usize) {
        self.rows_streamed += num_rows as u64;
    }

    fn record_error(&mut self) {
        self.had_error = true;
    }
}

impl Drop for StreamLifecycleTracker {
    fn drop(&mut self) {
        self.session.on_stream_complete(&self.query_id, self.rows_streamed, self.had_error);
    }
}

fn query_handle_to_flight_stream(
    handle: QueryHandle,
    session: Arc<StreamingSession>,
    query_id: Uuid,
) -> BoxedFlightStream<FlightData> {
    let schema = handle.schema().clone();
    let tracker = StreamLifecycleTracker::new(session, query_id);

    let batch_stream = futures::stream::unfold(
        (handle, tracker),
        |(mut h, mut tracker)| async move {
            use futures::StreamExt;
            match h.next().await {
                Some(Ok(batch)) => {
                    tracker.record_batch(batch.num_rows());
                    Some((Ok(batch), (h, tracker)))
                }
                Some(Err(e)) => {
                    tracker.record_error();
                    Some((Err(e), (h, tracker)))
                }
                None => None,
            }
        },
    );

    let record_batch_stream = batch_stream.map(|r| {
        r.map_err(|e| FlightError::ExternalError(Box::new(e)))
    });

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(IpcWriteOptions::default())
        .build(record_batch_stream)
        .map_err(|e| Status::internal(format!("Flight encoding error: {e}")));

    Box::pin(flight_data_stream)
}

#[tonic::async_trait]
impl FlightService for DemoflightService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "Handshake not implemented - use DoAction for auth",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights not yet implemented"))
    }

    #[instrument(skip(self, request), fields(session_id, command))]
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let session_id = self.try_extract_session_from_metadata(&request);
        let descriptor = request.into_inner();

        let command_desc = describe_descriptor(&descriptor);
        tracing::Span::current().record("session_id", tracing::field::debug(&session_id));
        tracing::Span::current().record("command", &command_desc);

        let ctx = RouteContext {
            session_manager: &self.session_manager,
            jwt_handler: &self.jwt_handler,
            session_id,
        };

        let flight_info = route_command(descriptor, ctx).await?;
        Ok(Response::new(flight_info))
    }

    #[instrument(skip(self, request), fields(session_id, table))]
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let session_id = self.extract_session_from_metadata(&request)?;
        let descriptor = request.into_inner();

        let table_name = match descriptor.path.first() {
            Some(name) => name.clone(),
            None => {
                return Err(Status::invalid_argument(
                    "Table name required in FlightDescriptor.path",
                ));
            }
        };

        tracing::Span::current().record("session_id", session_id.to_string());
        tracing::Span::current().record("table", &table_name);

        let session = self
            .session_manager
            .get_or_err(&session_id)
            .map_err(Status::from)?;

        let schema_result = get_table_schema(&session, &table_name).map_err(Status::from)?;

        Ok(Response::new(schema_result))
    }

    #[instrument(skip(self, request), fields(session_id, query_id, command))]
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket_bytes = &request.get_ref().ticket;

        if let Ok(any) = Any::decode(&ticket_bytes[..]) {
            if let Ok(command) = Command::try_from(any) {
                if !matches!(command, Command::Unknown(_)) {
                    tracing::Span::current().record("command", command.type_url());
                    return self.handle_flight_sql_do_get(command, &request).await;
                }
            }
        }

        let ticket = request.into_inner();
        let (session_id, query_id) = decode_ticket(&ticket.ticket).map_err(Status::from)?;

        tracing::Span::current().record("session_id", session_id.to_string());
        tracing::Span::current().record("query_id", query_id.to_string());

        let session = self
            .session_manager
            .get_or_err(&session_id)
            .map_err(Status::from)?;

        let handle = session
            .take_query_handle(&query_id)
            .await
            .map_err(Status::from)?;

        let stream = query_handle_to_flight_stream(handle, session, query_id);

        Ok(Response::new(stream))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "DoPut not supported - this is a read-only server",
        ))
    }

    #[instrument(skip(self, request), fields(action_type, source_url, table_count))]
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        tracing::Span::current().record("action_type", &action.r#type);

        match action.r#type.as_str() {
            "demoflight.register_source" => {
                let req: RegisterSourceRequest = self.decode_action_body(&action.body)?;

                tracing::Span::current().record("source_url", &req.source_url);

                let response = handle_register_source(
                    req,
                    &self.session_manager,
                    &self.jwt_handler,
                    &self.config.allowed_source_patterns,
                    self.config.max_session_duration_secs,
                )
                .await
                .map_err(Status::from)?;

                let table_names: Vec<_> = response.tables.iter().map(|t| t.name.as_str()).collect();
                tracing::Span::current().record("table_count", response.tables.len());
                tracing::info!(tables = ?table_names, "source registered");

                let body = response.encode_to_vec();
                let result = arrow_flight::Result { body: body.into() };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }

            "demoflight.get_session_status" => {
                Err(Status::unimplemented("get_session_status not yet implemented"))
            }

            _ => Err(Status::unimplemented(format!(
                "Unknown action type: {}",
                action.r#type
            ))),
        }
    }

    #[instrument(skip(self, _request))]
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "demoflight.register_source".into(),
                description: "Register a GOTV broadcast source and create a session".into(),
            },
            ActionType {
                r#type: "demoflight.close_session".into(),
                description: "Close and cleanup a session".into(),
            },
            ActionType {
                r#type: "demoflight.get_session_status".into(),
                description: "Get current session status and query states".into(),
            },
        ];

        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange not supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "PollFlightInfo not supported - use DoGet for streaming results",
        ))
    }
}

impl DemoflightService {
    async fn handle_flight_sql_do_get(
        &self,
        command: Command,
        request: &Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        match command {
            Command::CommandGetSqlInfo(cmd) => {
                let sql_info = build_sql_info_data()
                    .map_err(|e| Status::internal(format!("Failed to build SqlInfo: {e}")))?;
                let batch = sql_info
                    .record_batch(cmd.info)
                    .map_err(|e| Status::internal(format!("Failed to filter SqlInfo: {e}")))?;

                let schema = batch.schema();
                let batch_stream = futures::stream::once(async move { Ok(batch) });
                let record_batch_stream =
                    batch_stream.map(|r| r.map_err(|e: datafusion::arrow::error::ArrowError| FlightError::Arrow(e)));

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .with_options(IpcWriteOptions::default())
                    .build(record_batch_stream)
                    .map_err(|e| Status::internal(format!("Flight encoding error: {e}")));

                Ok(Response::new(Box::pin(flight_data_stream)))
            }

            Command::CommandGetTables(cmd) => {
                let session_id = self.extract_session_from_metadata(request)?;

                let session = self
                    .session_manager
                    .get_or_err(&session_id)
                    .map_err(Status::from)?;

                let builder = build_get_tables_response(&cmd, &session).map_err(Status::from)?;

                let batch = builder
                    .build()
                    .map_err(|e| Status::internal(format!("Failed to build GetTables response: {e}")))?;

                let schema = batch.schema();
                let batch_stream = futures::stream::once(async move { Ok(batch) });
                let record_batch_stream =
                    batch_stream.map(|r| r.map_err(|e: datafusion::arrow::error::ArrowError| FlightError::Arrow(e)));

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .with_options(IpcWriteOptions::default())
                    .build(record_batch_stream)
                    .map_err(|e| Status::internal(format!("Flight encoding error: {e}")));

                Ok(Response::new(Box::pin(flight_data_stream)))
            }

            _ => Err(Status::unimplemented(format!(
                "Flight SQL command not implemented: {}",
                command.type_url()
            ))),
        }
    }
}
