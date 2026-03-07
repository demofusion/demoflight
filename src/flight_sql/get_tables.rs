use arrow_flight::sql::metadata::GetTablesBuilder;
use arrow_flight::sql::CommandGetTables;

use crate::error::Result;
use crate::session::StreamingSession;

const CATALOG_NAME: &str = "demoflight";
const SCHEMA_NAME: &str = "gotv";
const TABLE_TYPE: &str = "TABLE";

pub fn build_get_tables_response(
    cmd: &CommandGetTables,
    session: &StreamingSession,
) -> Result<GetTablesBuilder> {
    let mut builder: GetTablesBuilder = CommandGetTables {
        catalog: cmd.catalog.clone(),
        db_schema_filter_pattern: cmd.db_schema_filter_pattern.clone(),
        table_name_filter_pattern: cmd.table_name_filter_pattern.clone(),
        table_types: cmd.table_types.clone(),
        include_schema: cmd.include_schema,
    }
    .into_builder();

    for (name, table_schema) in &session.table_schemas {
        builder
            .append(
                CATALOG_NAME,
                SCHEMA_NAME,
                name,
                TABLE_TYPE,
                &table_schema.arrow_schema,
            )
            .map_err(|e| crate::error::DemoflightError::Internal(e.to_string()))?;
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{StreamingSession, TableSchema};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_session() -> StreamingSession {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int64, false),
            Field::new("entity_index", DataType::Int32, false),
        ]));

        let table_schemas = vec![
            TableSchema {
                name: "CCitadelPlayerPawn".into(),
                arrow_schema: Arc::clone(&schema),
                ipc_bytes: vec![],
            },
            TableSchema {
                name: "CNPC_Trooper".into(),
                arrow_schema: Arc::clone(&schema),
                ipc_bytes: vec![],
            },
        ];

        StreamingSession::new_for_test(table_schemas)
    }

    #[test]
    fn test_get_tables_no_filter() {
        let session = make_test_session();
        let cmd = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        };

        let builder = build_get_tables_response(&cmd, &session).unwrap();
        let batch = builder.build().unwrap();

        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_get_tables_with_name_filter() {
        let session = make_test_session();
        let cmd = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: Some("CCitadel%".to_string()),
            table_types: vec![],
            include_schema: false,
        };

        let builder = build_get_tables_response(&cmd, &session).unwrap();
        let batch = builder.build().unwrap();

        assert_eq!(batch.num_rows(), 1);
    }
}
