use crate::{
    aggregator::{Aggregator, Message},
    error::AppError,
};
use clap::Parser;
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
mod aggregator;
mod error;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "redis://127.0.0.1/")]
    redis_url: String,

    #[arg(long, value_delimiter = ',', default_value = "inputA,inputB,inputC")]
    inputs: Vec<String>,

    #[arg(long, default_value = "outputChannel")]
    output: String,
}

async fn run_subs(
    tx: tokio::sync::mpsc::Sender<Message>,
    redis_url: String,
    input_channels: Vec<String>,
) -> Result<(), AppError> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;

    for channel_name in input_channels {
        pubsub.subscribe(channel_name).await?;
    }

    let mut stream = pubsub.on_message();

    while let Some(redis_msg) = stream.next().await {
        let payload: String = match redis_msg.get_payload() {
            Ok(payload) => payload,
            Err(_) => {
                // failed to parse
                // TODO: log err
                continue;
            }
        };

        let msg: Message = match serde_json::from_str(&payload) {
            Ok(msg) => msg,
            Err(_) => {
                // TODO: log
                continue;
            }
        };

        if tx.send(msg).await.is_err() {
            // channel closed
            break;
        }
    }

    Ok(())
}

async fn run_aggregator(
    mut rx: tokio::sync::mpsc::Receiver<Message>,
    redis_url: String,
    output_channel: String,
) -> Result<(), AppError> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut aggregator = Aggregator::default();

    while let Some(msg) = rx.recv().await {
        let aggregates_msg = aggregator.process(msg);
        let payload = serde_json::to_string(&aggregates_msg)?;

        let _: redis::RedisResult<()> = conn.publish(&output_channel, payload).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let args = Args::parse();

    let (tx, rx) = mpsc::channel(32);

    let subs_handler = tokio::spawn(run_subs(tx, args.redis_url.clone(), args.inputs));
    let aggregator_handle = tokio::spawn(run_aggregator(rx, args.redis_url, args.output));

    let _ = subs_handler.await?;
    let _ = aggregator_handle.await?;

    Ok(())
}
