use pubky_test::{run_aggregator, run_subs};
use redis::AsyncCommands;
use testcontainers::{
    GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

// test struct to avoid deriving Deserialize on the domain struct
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TestAggregatedMessage {
    pub key: String,
    pub value: i64,
    pub count: u64,
}

#[tokio::test]
async fn test_redis() {
    let container = GenericImage::new("redis", "7.2.4")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(6379).await.unwrap();

    let redis_url = format!("redis://{host}:{host_port}");

    let (tx, rx) = tokio::sync::mpsc::channel(32);
    let cancel_tkn = CancellationToken::new();

    let subs_handle = tokio::spawn(run_subs(
        tx,
        redis_url.clone(),
        vec!["inputA".to_string(), "inputB".to_string()],
        cancel_tkn.clone(),
    ));

    let aggregator_handle = tokio::spawn(run_aggregator(
        rx,
        redis_url.clone(),
        "outputChannel".to_string(),
        cancel_tkn.clone(),
    ));

    let client = redis::Client::open(redis_url.clone()).unwrap();

    let mut output_pubsub = client.get_async_pubsub().await.unwrap();
    output_pubsub.subscribe("outputChannel").await.unwrap();
    let mut output_stream = output_pubsub.on_message();

    let mut publisher = client.get_multiplexed_async_connection().await.unwrap();

    // small pause for for sub
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    publisher
        .publish::<_, _, usize>("inputA", r#"{"key":"cpu","value":10}"#)
        .await
        .unwrap();

    publisher
        .publish::<_, _, usize>("inputB", r#"{"key":"cpu","value":5}"#)
        .await
        .unwrap();

    let first_msg = tokio::time::timeout(std::time::Duration::from_secs(2), output_stream.next())
        .await
        .expect("timed out waiting for first output")
        .expect("output stream closed");

    let first_payload: String = first_msg.get_payload().unwrap();
    let first: TestAggregatedMessage = serde_json::from_str(&first_payload).unwrap();

    assert_eq!(first.key, "cpu");
    assert_eq!(first.value, 10);
    assert_eq!(first.count, 1);

    let second_msg = tokio::time::timeout(std::time::Duration::from_secs(2), output_stream.next())
        .await
        .expect("timed out waiting for second output")
        .expect("output stream closed");

    let second_payload: String = second_msg.get_payload().unwrap();
    let second: TestAggregatedMessage = serde_json::from_str(&second_payload).unwrap();

    assert_eq!(second.key, "cpu");
    assert_eq!(second.value, 15);
    assert_eq!(second.count, 2);

    cancel_tkn.cancel();

    subs_handle.await.unwrap().unwrap();
    aggregator_handle.await.unwrap().unwrap();
}
