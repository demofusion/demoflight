use uuid::Uuid;

use crate::error::{DemoflightError, Result};

pub fn encode_ticket(session_id: &Uuid, query_id: &Uuid) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(session_id.as_bytes());
    bytes.extend_from_slice(query_id.as_bytes());
    bytes
}

pub fn decode_ticket(bytes: &[u8]) -> Result<(Uuid, Uuid)> {
    if bytes.len() != 32 {
        return Err(DemoflightError::InvalidTicket);
    }
    let session_id = Uuid::from_slice(&bytes[0..16]).map_err(|_| DemoflightError::InvalidTicket)?;
    let query_id = Uuid::from_slice(&bytes[16..32]).map_err(|_| DemoflightError::InvalidTicket)?;
    Ok((session_id, query_id))
}
