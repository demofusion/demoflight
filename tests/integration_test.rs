use std::net::SocketAddr;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::{Any, CommandGetSqlInfo};
use arrow_flight::{FlightClient, FlightDescriptor};
use demoflight::{DemoflightConfig, DemoflightService};
use prost::Message;
use tonic::transport::{Channel, Server};

async fn start_test_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let config = DemoflightConfig {
        listen_addr: "[::1]:0".to_string(),
        metrics_addr: "[::1]:0".to_string(),
        jwt_secret: "test-secret-key-for-integration-tests".to_string(),
        max_session_duration_secs: 3600,
        inactivity_timeout_secs: 300,
        connection_timeout_secs: 60,
        cleanup_interval_secs: 60,
        max_sessions: 100,
        max_queries_per_session: 10,
        allowed_source_patterns: vec![".*".to_string()],
    };

    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let service = DemoflightService::new(config);
    let flight_svc = FlightServiceServer::new(service);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(flight_svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    (addr, handle)
}

async fn create_client(addr: SocketAddr) -> FlightClient {
    let channel = Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();

    FlightClient::new(channel)
}

#[tokio::test]
async fn test_list_actions() {
    let (addr, handle) = start_test_server().await;
    let mut client = create_client(addr).await;

    let actions: Vec<_> = client
        .list_actions()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(actions.len() >= 3);

    let action_types: Vec<_> = actions.iter().map(|a| a.r#type.as_str()).collect();
    assert!(action_types.contains(&"demoflight.register_source"));
    assert!(action_types.contains(&"demoflight.close_session"));
    assert!(action_types.contains(&"demoflight.get_session_status"));

    handle.abort();
}

#[tokio::test]
async fn test_get_sql_info() {
    let (addr, handle) = start_test_server().await;
    let mut client = create_client(addr).await;

    let cmd = CommandGetSqlInfo { info: vec![] };
    let any = Any::pack(&cmd).unwrap();
    let descriptor = FlightDescriptor::new_cmd(any.encode_to_vec());

    let flight_info = client.get_flight_info(descriptor).await.unwrap();

    assert_eq!(flight_info.endpoint.len(), 1);
    let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();

    let stream = client.do_get(ticket.clone()).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    assert!(!batches.is_empty());

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Expected at least some SQL info rows");

    handle.abort();
}

#[tokio::test]
async fn test_handshake_not_implemented() {
    let (addr, handle) = start_test_server().await;
    let mut client = create_client(addr).await;

    let result = client.handshake("user").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("unimplemented") || err_str.contains("not implemented"),
        "Expected 'unimplemented' error, got: {}",
        err
    );

    handle.abort();
}

#[tokio::test]
async fn test_invalid_command_rejected() {
    let (addr, handle) = start_test_server().await;
    let mut client = create_client(addr).await;

    let descriptor = FlightDescriptor::new_cmd(vec![0, 1, 2, 3]);

    let result = client.get_flight_info(descriptor).await;

    assert!(result.is_err());

    handle.abort();
}

use futures::TryStreamExt;
