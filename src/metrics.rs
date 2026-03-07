use std::sync::Arc;

use metrics::{counter, gauge, describe_counter, describe_gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::session::SessionManager;

pub struct MetricsHandle {
    prometheus: PrometheusHandle,
}

impl MetricsHandle {
    pub fn render(&self) -> String {
        self.prometheus.render()
    }
}

pub fn init_metrics() -> MetricsHandle {
    describe_counter!(
        "demoflight_sessions_created_total",
        "Total number of sessions created"
    );
    describe_counter!(
        "demoflight_sessions_closed_total",
        "Total number of sessions closed"
    );
    describe_counter!(
        "demoflight_sessions_expired_total",
        "Total number of sessions expired by cleanup"
    );
    describe_gauge!(
        "demoflight_sessions_active",
        "Current number of active sessions"
    );

    describe_counter!(
        "demoflight_queries_submitted_total",
        "Total number of queries submitted"
    );
    describe_counter!(
        "demoflight_streams_started_total",
        "Total number of DoGet streams started"
    );
    describe_counter!(
        "demoflight_streams_completed_total",
        "Total number of DoGet streams completed"
    );
    describe_counter!(
        "demoflight_streams_errored_total",
        "Total number of DoGet streams that ended with errors"
    );
    describe_gauge!(
        "demoflight_streams_active",
        "Current number of active DoGet streams"
    );

    describe_counter!(
        "demoflight_rows_streamed_total",
        "Total number of rows streamed to clients"
    );
    describe_counter!(
        "demoflight_batches_streamed_total",
        "Total number of Arrow batches streamed to clients"
    );

    describe_counter!(
        "demoflight_grpc_requests_total",
        "Total gRPC requests by method"
    );
    describe_counter!(
        "demoflight_grpc_errors_total",
        "Total gRPC errors by method and status"
    );

    describe_gauge!(
        "demoflight_streaming_rows_produced",
        "Rows produced by parser (per session)"
    );
    describe_gauge!(
        "demoflight_streaming_rows_sent",
        "Rows sent to clients (per session)"
    );
    describe_gauge!(
        "demoflight_streaming_rows_buffered",
        "Rows buffered awaiting send (per session)"
    );
    describe_counter!(
        "demoflight_streaming_gate_blocked_total",
        "Number of times distribution gate blocked (backpressure indicator)"
    );

    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    MetricsHandle { prometheus }
}

pub fn record_session_created() {
    counter!("demoflight_sessions_created_total").increment(1);
}

pub fn record_session_closed() {
    counter!("demoflight_sessions_closed_total").increment(1);
}

pub fn record_sessions_expired(count: u64) {
    counter!("demoflight_sessions_expired_total").increment(count);
}

pub fn set_active_sessions(count: u64) {
    gauge!("demoflight_sessions_active").set(count as f64);
}

pub fn record_query_submitted() {
    counter!("demoflight_queries_submitted_total").increment(1);
}

pub fn record_stream_started() {
    counter!("demoflight_streams_started_total").increment(1);
}

pub fn record_stream_completed(had_error: bool) {
    if had_error {
        counter!("demoflight_streams_errored_total").increment(1);
    } else {
        counter!("demoflight_streams_completed_total").increment(1);
    }
}

pub fn set_active_streams(count: u64) {
    gauge!("demoflight_streams_active").set(count as f64);
}

pub fn record_rows_streamed(count: u64) {
    counter!("demoflight_rows_streamed_total").increment(count);
}

pub fn record_batches_streamed(count: u64) {
    counter!("demoflight_batches_streamed_total").increment(count);
}

pub fn record_grpc_request(method: &str) {
    counter!("demoflight_grpc_requests_total", "method" => method.to_string()).increment(1);
}

pub fn record_grpc_error(method: &str, status: &str) {
    counter!(
        "demoflight_grpc_errors_total",
        "method" => method.to_string(),
        "status" => status.to_string()
    ).increment(1);
}

pub fn update_streaming_stats(session_id: &str, rows_produced: u64, rows_sent: u64, rows_buffered: u64) {
    gauge!(
        "demoflight_streaming_rows_produced",
        "session_id" => session_id.to_string()
    ).set(rows_produced as f64);
    gauge!(
        "demoflight_streaming_rows_sent",
        "session_id" => session_id.to_string()
    ).set(rows_sent as f64);
    gauge!(
        "demoflight_streaming_rows_buffered",
        "session_id" => session_id.to_string()
    ).set(rows_buffered as f64);
}

pub fn record_gate_blocked(session_id: &str) {
    counter!(
        "demoflight_streaming_gate_blocked_total",
        "session_id" => session_id.to_string()
    ).increment(1);
}

pub fn spawn_metrics_collector(session_manager: Arc<SessionManager>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            set_active_sessions(session_manager.session_count() as u64);
            
            // Collect streaming stats from all active sessions
            session_manager.for_each_session(|session| {
                if let Some(stats) = session.streaming_stats() {
                    let snapshot = stats.snapshot();
                    let session_id = session.id.to_string();
                    
                    update_streaming_stats(
                        &session_id,
                        snapshot.rows_produced,
                        snapshot.rows_sent,
                        snapshot.rows_buffered(),
                    );
                    
                    // Gate blocked is a counter - we need to track the delta
                    // For now, just report the total (Prometheus will handle rate calculation)
                    gauge!(
                        "demoflight_streaming_gate_blocked_total",
                        "session_id" => session_id
                    ).set(snapshot.gate_blocked_count as f64);
                }
            });
        }
    })
}
