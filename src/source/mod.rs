pub mod gotv;
pub mod validation;

use std::sync::Arc;

pub use gotv::poll_next_batch;
pub use validation::validate_source_url;

#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub url: Arc<str>,
}

impl SourceInfo {
    pub fn new(url: impl Into<Arc<str>>) -> Self {
        Self { url: url.into() }
    }
}
