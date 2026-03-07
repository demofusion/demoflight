use std::sync::Arc;

use arrow_flight::sql::{Any, Command, CommandGetSqlInfo, CommandGetTables, CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use prost::Message;
use tonic::Status;
use tracing::instrument;
use uuid::Uuid;

use crate::session::jwt::JwtHandler;
use crate::session::{SessionManager, StreamingSession};
use crate::tickets::encode_ticket;

pub struct RouteContext<'a> {
    pub session_manager: &'a SessionManager,
    pub jwt_handler: &'a JwtHandler,
    pub session_id: Option<Uuid>,
}

pub async fn route_command(
    descriptor: FlightDescriptor,
    ctx: RouteContext<'_>,
) -> Result<FlightInfo, Status> {
    if descriptor.cmd.is_empty() {
        return Err(Status::invalid_argument("Empty command in FlightDescriptor"));
    }

    let any = Any::decode(&descriptor.cmd[..])
        .map_err(|e| Status::invalid_argument(format!("Invalid command encoding: {}", e)))?;

    let command = Command::try_from(any)
        .map_err(|e| Status::invalid_argument(format!("Unknown command type: {}", e)))?;

    match command {
        Command::CommandStatementQuery(cmd) => {
            handle_statement_query(cmd, descriptor, &ctx).await
        }
        Command::CommandGetTables(cmd) => {
            let session = require_session(&ctx)?;
            handle_get_tables(cmd, descriptor, &session)
        }
        Command::CommandGetSqlInfo(cmd) => {
            handle_get_sql_info(cmd, descriptor)
        }
        Command::CommandGetCatalogs(_) => {
            Err(Status::unimplemented("GetCatalogs not implemented"))
        }
        Command::CommandGetDbSchemas(_) => {
            Err(Status::unimplemented("GetDbSchemas not implemented"))
        }
        Command::CommandGetTableTypes(_) => {
            Err(Status::unimplemented("GetTableTypes not implemented"))
        }
        _ => Err(Status::unimplemented(format!(
            "Command not implemented: {}",
            command.type_url()
        ))),
    }
}

fn require_session(ctx: &RouteContext<'_>) -> Result<Arc<StreamingSession>, Status> {
    let session_id = ctx.session_id.ok_or_else(|| {
        Status::unauthenticated("Session required. Include session token in Authorization header.")
    })?;

    ctx.session_manager
        .get_or_err(&session_id)
        .map_err(Status::from)
}

#[instrument(skip(descriptor, ctx), fields(session_id, sql, query_id))]
async fn handle_statement_query(
    cmd: CommandStatementQuery,
    descriptor: FlightDescriptor,
    ctx: &RouteContext<'_>,
) -> Result<FlightInfo, Status> {
    let session = require_session(ctx)?;

    tracing::Span::current().record("session_id", session.id.to_string());
    tracing::Span::current().record("sql", &cmd.query);

    if cmd.query.is_empty() {
        return Err(Status::invalid_argument("Query cannot be empty"));
    }

    let query_id = session
        .add_query(&cmd.query)
        .await
        .map_err(Status::from)?;

    tracing::Span::current().record("query_id", query_id.to_string());

    let ticket_bytes = encode_ticket(&session.id, &query_id);

    let endpoint = FlightEndpoint {
        ticket: Some(Ticket {
            ticket: ticket_bytes.into(),
        }),
        location: vec![],
        ..Default::default()
    };

    Ok(FlightInfo {
        schema: bytes::Bytes::new(),
        flight_descriptor: Some(descriptor),
        endpoint: vec![endpoint],
        total_records: -1,
        total_bytes: -1,
        ordered: false,
        app_metadata: bytes::Bytes::new(),
    })
}

fn handle_get_tables(cmd: CommandGetTables, descriptor: FlightDescriptor, _session: &StreamingSession) -> Result<FlightInfo, Status> {
    let ticket = Ticket {
        ticket: cmd.as_any().encode_to_vec().into(),
    };

    Ok(FlightInfo {
        schema: bytes::Bytes::new(),
        flight_descriptor: Some(descriptor),
        endpoint: vec![FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            ..Default::default()
        }],
        total_records: -1,
        total_bytes: -1,
        ordered: false,
        app_metadata: bytes::Bytes::new(),
    })
}

fn handle_get_sql_info(cmd: CommandGetSqlInfo, descriptor: FlightDescriptor) -> Result<FlightInfo, Status> {
    let ticket = Ticket {
        ticket: cmd.as_any().encode_to_vec().into(),
    };

    Ok(FlightInfo {
        schema: bytes::Bytes::new(),
        flight_descriptor: Some(descriptor),
        endpoint: vec![FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            ..Default::default()
        }],
        total_records: -1,
        total_bytes: -1,
        ordered: false,
        app_metadata: bytes::Bytes::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::{CommandGetCatalogs, CommandGetDbSchemas, CommandGetTableTypes};

    fn encode_as_any<T: ProstMessageExt>(cmd: &T) -> Vec<u8> {
        cmd.as_any().encode_to_vec()
    }

    mod flight_sql_command_decoding {
        use super::*;

        #[test]
        fn decodes_command_statement_query() {
            let cmd = CommandStatementQuery {
                query: "SELECT * FROM players".to_string(),
                transaction_id: None,
            };
            let encoded = encode_as_any(&cmd);

            let any = Any::decode(&encoded[..]).expect("Any decode should succeed");
            let command = Command::try_from(any).expect("Command conversion should succeed");

            assert!(matches!(command, Command::CommandStatementQuery(_)));
            if let Command::CommandStatementQuery(decoded) = command {
                assert_eq!(decoded.query, "SELECT * FROM players");
            }
        }

        #[test]
        fn decodes_command_get_sql_info() {
            let cmd = CommandGetSqlInfo { info: vec![1, 2, 3] };
            let encoded = encode_as_any(&cmd);

            let any = Any::decode(&encoded[..]).expect("Any decode should succeed");
            let command = Command::try_from(any).expect("Command conversion should succeed");

            assert!(matches!(command, Command::CommandGetSqlInfo(_)));
            if let Command::CommandGetSqlInfo(decoded) = command {
                assert_eq!(decoded.info, vec![1, 2, 3]);
            }
        }

        #[test]
        fn decodes_command_get_tables() {
            let cmd = CommandGetTables {
                catalog: Some("test_catalog".to_string()),
                db_schema_filter_pattern: None,
                table_name_filter_pattern: Some("test%".to_string()),
                table_types: vec!["TABLE".to_string()],
                include_schema: true,
            };
            let encoded = encode_as_any(&cmd);

            let any = Any::decode(&encoded[..]).expect("Any decode should succeed");
            let command = Command::try_from(any).expect("Command conversion should succeed");

            assert!(matches!(command, Command::CommandGetTables(_)));
            if let Command::CommandGetTables(decoded) = command {
                assert_eq!(decoded.catalog, Some("test_catalog".to_string()));
                assert!(decoded.include_schema);
            }
        }

        #[test]
        fn decodes_command_get_catalogs() {
            let cmd = CommandGetCatalogs {};
            let encoded = encode_as_any(&cmd);

            let any = Any::decode(&encoded[..]).expect("Any decode should succeed");
            let command = Command::try_from(any).expect("Command conversion should succeed");

            assert!(matches!(command, Command::CommandGetCatalogs(_)));
        }

        #[test]
        fn decodes_command_get_db_schemas() {
            let cmd = CommandGetDbSchemas {
                catalog: Some("test".to_string()),
                db_schema_filter_pattern: None,
            };
            let encoded = encode_as_any(&cmd);

            let any = Any::decode(&encoded[..]).expect("Any decode should succeed");
            let command = Command::try_from(any).expect("Command conversion should succeed");

            assert!(matches!(command, Command::CommandGetDbSchemas(_)));
        }

        #[test]
        fn decodes_command_get_table_types() {
            let cmd = CommandGetTableTypes {};
            let encoded = encode_as_any(&cmd);

            let any = Any::decode(&encoded[..]).expect("Any decode should succeed");
            let command = Command::try_from(any).expect("Command conversion should succeed");

            assert!(matches!(command, Command::CommandGetTableTypes(_)));
        }
    }

    mod ticket_encoding {
        use super::*;

        #[test]
        fn get_sql_info_ticket_roundtrips() {
            let original = CommandGetSqlInfo { info: vec![1, 2, 3] };
            let descriptor = FlightDescriptor::new_cmd(original.as_any().encode_to_vec());
            let flight_info = handle_get_sql_info(original.clone(), descriptor).unwrap();

            let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
            let any = Any::decode(&ticket.ticket[..]).expect("Ticket should decode as Any");
            let command = Command::try_from(any).expect("Should convert to Command");

            if let Command::CommandGetSqlInfo(decoded) = command {
                assert_eq!(decoded.info, original.info);
            } else {
                panic!("Expected CommandGetSqlInfo, got {:?}", command);
            }
        }
    }

    mod edge_cases {
        use super::*;

        #[test]
        fn empty_bytes_produces_unknown_command() {
            let empty: Vec<u8> = vec![];

            let any = Any::decode(&empty[..]).expect("Empty bytes decode as empty Any");
            let command_result = Command::try_from(any);

            assert!(
                command_result.is_err() || matches!(command_result, Ok(Command::Unknown(_))),
                "Empty Any should fail or produce Unknown command"
            );
        }

        #[test]
        fn garbage_bytes_fails_any_decode() {
            let garbage = vec![0xFF, 0xFE, 0xFD, 0xFC];

            let result = Any::decode(&garbage[..]);
            
            // Garbage may or may not decode as Any, but should not be a valid command
            if let Ok(any) = result {
                let command_result = Command::try_from(any);
                assert!(
                    command_result.is_err() || matches!(command_result, Ok(Command::Unknown(_))),
                    "Garbage should not decode as known command"
                );
            }
        }
    }
}
