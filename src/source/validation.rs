use regex::Regex;

use crate::error::{DemoflightError, Result};

pub fn validate_source_url(url: &str, allowed_patterns: &[String]) -> Result<()> {
    if allowed_patterns.is_empty() {
        return Ok(());
    }

    for pattern in allowed_patterns {
        if let Ok(re) = Regex::new(pattern) {
            if re.is_match(url) {
                return Ok(());
            }
        }
    }

    Err(DemoflightError::InvalidSourceUrl(format!(
        "URL does not match any allowed patterns: {}",
        url
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_steam_url() {
        let patterns = vec![r"^https?://[^/]*\.steamcontent\.com/.*$".to_string()];

        assert!(
            validate_source_url("http://dist1-ord1.steamcontent.com/tv/18895867", &patterns)
                .is_ok()
        );

        assert!(
            validate_source_url("https://dist2-iad1.steamcontent.com/tv/12345678", &patterns)
                .is_ok()
        );
    }

    #[test]
    fn test_invalid_url() {
        let patterns = vec![r"^https?://[^/]*\.steamcontent\.com/.*$".to_string()];

        assert!(validate_source_url("http://evil.com/malicious", &patterns).is_err());

        assert!(validate_source_url("http://localhost:8080/internal", &patterns).is_err());
    }

    #[test]
    fn test_empty_patterns_allows_all() {
        let patterns: Vec<String> = vec![];

        assert!(validate_source_url("http://anything.com", &patterns).is_ok());
    }
}
