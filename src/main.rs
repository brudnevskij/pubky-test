use crate::aggregator::{Aggregator, Message};
use clap::Parser;
use redis::AsyncCommands;
use tokio::sync::mpsc;
mod aggregator;
mod error;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "redis://127.0.0.1/")]
    redis_url: String,

    #[arg(long, default_value = "inputA,inputB,inputC")]
    inputs: String,

    #[arg(long, default_value = "outputChannel")]
    output: String,
}

async fn run_aggregator(
    mut rx: tokio::sync::mpsc::Receiver<Message>,
    redis_url: String,
    output_channel: String,
) {
    let client = redis::Client::open(redis_url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let mut aggregator = Aggregator::default();

    while let Some(msg) = rx.recv().await {
        let aggregates_msg = aggregator.process(msg);
        let payload = serde_json::to_string(&aggregates_msg).unwrap();

        let _: redis::RedisResult<()> = conn.publish(&output_channel, payload).await;
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let (_, rx) = mpsc::channel(32);

    let aggregator_handle = tokio::spawn(run_aggregator(rx, args.redis_url, args.output));
    aggregator_handle.await.unwrap();
}
