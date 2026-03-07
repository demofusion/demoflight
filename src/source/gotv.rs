use datafusion::arrow::record_batch::RecordBatch;
use demofusion::gotv::QueryHandle;
use futures::StreamExt;

use crate::error::DemoflightError;

pub async fn poll_next_batch(
    handle: &mut QueryHandle,
) -> Option<Result<RecordBatch, DemoflightError>> {
    handle
        .next()
        .await
        .map(|r| r.map_err(|e| DemoflightError::QueryExecution(e.to_string())))
}
