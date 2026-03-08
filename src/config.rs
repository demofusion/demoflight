use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DemoflightConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,

    pub jwt_secret: String,

    #[serde(default = "default_max_session_duration")]
    pub max_session_duration_secs: i64,

    #[serde(default = "default_inactivity_timeout")]
    pub inactivity_timeout_secs: i64,

    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_secs: u64,

    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,

    #[serde(default = "default_max_queries")]
    pub max_queries_per_session: usize,

    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_secs: u64,

    #[serde(default = "default_allowed_patterns")]
    pub allowed_source_patterns: Vec<String>,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default)]
    pub reject_pipeline_breakers: bool,
}

fn default_listen_addr() -> String {
    "[::]:50051".into()
}

fn default_metrics_addr() -> String {
    "[::]:9090".into()
}

fn default_max_session_duration() -> i64 {
    3600
}

fn default_inactivity_timeout() -> i64 {
    300
}

fn default_connection_timeout() -> u64 {
    60
}

fn default_max_sessions() -> usize {
    100
}

fn default_max_queries() -> usize {
    10
}

fn default_cleanup_interval() -> u64 {
    60
}

fn default_allowed_patterns() -> Vec<String> {
    vec![r"^https?://[^/]*\.steamcontent\.com/.*$".into()]
}

fn default_batch_size() -> usize {
    8192
}

impl DemoflightConfig {
    #[allow(clippy::result_large_err)]
    pub fn load() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file("demoflight.toml"))
            .merge(Env::prefixed("DEMOFLIGHT_"))
            .extract()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::providers::Serialized;

    fn minimal_config() -> Figment {
        Figment::new().merge(Serialized::defaults(serde_json::json!({
            "jwt_secret": "test-secret"
        })))
    }

    #[test]
    fn batch_size_defaults_to_8192() {
        let config: DemoflightConfig = minimal_config().extract().unwrap();
        assert_eq!(config.batch_size, 8192);
    }

    #[test]
    fn reject_pipeline_breakers_defaults_to_false() {
        let config: DemoflightConfig = minimal_config().extract().unwrap();
        assert!(!config.reject_pipeline_breakers);
    }

    #[test]
    fn batch_size_can_be_overridden() {
        let config: DemoflightConfig = minimal_config()
            .merge(Serialized::defaults(serde_json::json!({
                "batch_size": 256
            })))
            .extract()
            .unwrap();
        assert_eq!(config.batch_size, 256);
    }

    #[test]
    fn reject_pipeline_breakers_can_be_enabled() {
        let config: DemoflightConfig = minimal_config()
            .merge(Serialized::defaults(serde_json::json!({
                "reject_pipeline_breakers": true
            })))
            .extract()
            .unwrap();
        assert!(config.reject_pipeline_breakers);
    }
}
