use std::{net::SocketAddr, sync::Arc, time::Duration};

use demoflight::{DemoflightConfig, DemoflightService, MetricsHandle};
use tokio::net::TcpListener;
use tonic::transport::Server;
use tower_http::{classify::GrpcFailureClass, trace::TraceLayer};
use tracing::Span;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,demoflight=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = DemoflightConfig::load()?;
    let grpc_addr: SocketAddr = config.listen_addr.parse()?;
    let metrics_addr: SocketAddr = config.metrics_addr.parse()?;

    let metrics_handle = Arc::new(demoflight::metrics::init_metrics());

    tracing::info!("Starting Demoflight server on {}", grpc_addr);
    tracing::info!("Metrics available at http://{}/metrics", metrics_addr);

    let service = DemoflightService::new(config);
    let _cleanup_handle = service.spawn_cleanup_task();
    let _metrics_collector =
        demoflight::metrics::spawn_metrics_collector(Arc::clone(service.session_manager()));

    let flight_svc = arrow_flight::flight_service_server::FlightServiceServer::new(service);

    let trace_layer = TraceLayer::new_for_grpc()
        .make_span_with(|request: &http::Request<_>| {
            let client_ip = extract_client_ip(request);
            let method = request.uri().path();
            tracing::info_span!(
                "grpc_request",
                client_ip = %client_ip,
                method = %method,
                otel.kind = "server"
            )
        })
        .on_request(|_request: &http::Request<_>, _span: &Span| {
            tracing::debug!("started processing request");
        })
        .on_response(|response: &http::Response<_>, latency: Duration, _span: &Span| {
            let grpc_status = response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("0");
            tracing::info!(latency_ms = %latency.as_millis(), grpc_status = %grpc_status, "finished");
        })
        .on_failure(|error: GrpcFailureClass, latency: Duration, _span: &Span| {
            tracing::error!(latency_ms = %latency.as_millis(), error = %error, "request failed");
        });

    let metrics_server = spawn_metrics_server(metrics_addr, metrics_handle);

    let grpc_server = Server::builder()
        .layer(trace_layer)
        .add_service(flight_svc)
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    tokio::select! {
        result = grpc_server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "gRPC server error");
            }
        }
        result = metrics_server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Metrics server error");
            }
        }
    }

    tracing::info!("Server shutdown complete");
    Ok(())
}

async fn spawn_metrics_server(
    addr: SocketAddr,
    metrics_handle: Arc<MetricsHandle>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let metrics = Arc::clone(&metrics_handle);

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            if stream.readable().await.is_ok() {
                let _ = stream.try_read(&mut buf);
            }

            let body = metrics.render();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );

            if stream.writable().await.is_ok() {
                let _ = stream.try_write(response.as_bytes());
            }
        });
    }
}

fn extract_client_ip<B>(request: &http::Request<B>) -> String {
    if let Some(forwarded_for) = request.headers().get("x-forwarded-for") {
        if let Ok(value) = forwarded_for.to_str() {
            if let Some(first_ip) = value.split(',').next() {
                return first_ip.trim().to_string();
            }
        }
    }

    if let Some(real_ip) = request.headers().get("x-real-ip") {
        if let Ok(value) = real_ip.to_str() {
            return value.trim().to_string();
        }
    }

    request
        .extensions()
        .get::<tonic::transport::server::TcpConnectInfo>()
        .and_then(|info| info.remote_addr())
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received Ctrl+C, shutting down"),
        _ = terminate => tracing::info!("Received SIGTERM, shutting down"),
    }
}
