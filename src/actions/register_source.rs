use std::sync::Arc;

use arrow_flight::{IpcMessage, SchemaAsIpc};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use demofusion::gotv::GotvSource;
use demofusion::session::{IntoStreamingSession, Schemas};
use uuid::Uuid;

use crate::error::{DemoflightError, Result};
use crate::proto::{RegisterSourceRequest, RegisterSourceResponse, TableInfo};
use crate::session::jwt::{now_unix, sha256_prefix, JwtHandler, SessionClaims};
use crate::session::{SessionManager, StreamingSession, TableSchema};
use crate::source::{validate_source_url, SourceInfo};

pub async fn handle_register_source(
    req: RegisterSourceRequest,
    session_manager: &SessionManager,
    jwt_handler: &JwtHandler,
    allowed_patterns: &[String],
    session_timeout_secs: i64,
    batch_size: usize,
    reject_pipeline_breakers: bool,
) -> Result<RegisterSourceResponse> {
    validate_source_url(&req.source_url, allowed_patterns)?;

    let source = GotvSource::connect(&req.source_url)
        .await
        .map_err(|e| DemoflightError::GotvConnection(e.to_string()))?;

    let (demofusion_session, schemas) = source
        .into_session()
        .await
        .map_err(|e| DemoflightError::GotvConnection(e.to_string()))?;

    let demofusion_session = demofusion_session
        .with_batch_size(batch_size)
        .with_reject_pipeline_breakers(reject_pipeline_breakers);

    let (table_infos, table_schemas) = extract_schemas(&schemas)?;

    let session_id = Uuid::new_v4();
    let source_info = SourceInfo::new(req.source_url.clone());

    let session = StreamingSession::new(session_id, source_info, table_schemas, demofusion_session);
    session_manager.insert(session)?;

    let token = jwt_handler.encode(SessionClaims {
        sub: session_id.to_string(),
        iss: "demoflight".into(),
        iat: now_unix(),
        exp: now_unix() + session_timeout_secs,
        source_type: "gotv_broadcast".into(),
        source_hash: sha256_prefix(&req.source_url, 8),
    })?;

    Ok(RegisterSourceResponse {
        session_token: token,
        tables: table_infos,
    })
}

fn extract_schemas(schemas: &Schemas) -> Result<(Vec<TableInfo>, Vec<TableSchema>)> {
    let ipc_options = IpcWriteOptions::default();
    let mut table_infos = Vec::new();
    let mut table_schemas = Vec::new();

    let mut entity_names: Vec<&str> = schemas.keys().map(|s| s.as_ref()).collect();
    entity_names.sort();

    for name in entity_names {
        if let Some(schema) = schemas.get(name) {
            let schema_as_ipc = SchemaAsIpc::new(&schema.arrow_schema, &ipc_options);
            let ipc_message: IpcMessage =
                schema_as_ipc
                    .try_into()
                    .map_err(|e: datafusion::arrow::error::ArrowError| {
                        DemoflightError::SchemaDiscovery(e.to_string())
                    })?;

            let ipc_bytes = ipc_message.0.to_vec();

            let name: Arc<str> = name.into();

            table_infos.push(TableInfo {
                name: name.to_string(),
                arrow_schema: ipc_bytes.clone(),
            });

            table_schemas.push(TableSchema {
                name,
                arrow_schema: Arc::clone(&schema.arrow_schema),
                ipc_bytes,
            });
        }
    }

    Ok((table_infos, table_schemas))
}
