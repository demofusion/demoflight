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

impl DemoflightConfig {
    pub fn load() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file("demoflight.toml"))
            .merge(Env::prefixed("DEMOFLIGHT_"))
            .extract()
    }
}
