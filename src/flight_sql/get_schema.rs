use arrow_flight::SchemaResult;

use crate::error::{DemoflightError, Result};
use crate::session::StreamingSession;

pub fn get_table_schema(session: &StreamingSession, table_name: &str) -> Result<SchemaResult> {
    let table_schema = session.table_schemas.get(table_name).ok_or_else(|| {
        DemoflightError::QueryNotFound(format!("Table not found: {}", table_name))
    })?;

    Ok(SchemaResult {
        schema: table_schema.ipc_bytes.clone().into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{StreamingSession, TableSchema};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_get_table_schema_found() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tick",
            DataType::Int64,
            false,
        )]));

        let table_schemas = vec![TableSchema {
            name: "TestTable".into(),
            arrow_schema: Arc::clone(&schema),
            ipc_bytes: vec![1, 2, 3, 4],
        }];

        let session = StreamingSession::new_for_test(table_schemas);

        let result = get_table_schema(&session, "TestTable");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().schema.as_ref(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_get_table_schema_not_found() {
        let session = StreamingSession::new_for_test(vec![]);

        let result = get_table_schema(&session, "NonExistent");
        assert!(result.is_err());
    }
}
