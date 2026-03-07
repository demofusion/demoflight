pub mod actions;
pub mod commands;
pub mod config;
pub mod error;
pub mod flight_sql;
pub mod metrics;
pub mod server;
pub mod session;
pub mod source;
pub mod tickets;

pub mod proto {
    include!("proto/demoflight.v1.rs");
}

pub use config::DemoflightConfig;
pub use error::DemoflightError;
pub use metrics::MetricsHandle;
pub use server::DemoflightService;
