use crate::{
    aggregator::{Aggregator, Message},
    error::AppResult,
};
use clap::Parser;
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
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
    cancel_tkn: CancellationToken,
) -> AppResult<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;

    for channel_name in input_channels {
        pubsub.subscribe(channel_name).await?;
    }

    let mut stream = pubsub.on_message();

    loop {
        tokio::select! {
            _ = cancel_tkn.cancelled() => {
                // shutdown requested
               break;
            }

            msg_redis = stream.next() =>{
                let Some(msg) = msg_redis else {
                    // redis stream closed
                    break;
                };

                let payload: String = match msg.get_payload(){
                    Ok(payload)=> payload,
                    Err(_) => {
                        //todo: log
                        continue;
                    }
                };

                let msg: Message = match serde_json::from_str(&payload){
                    Ok(msg) => msg,
                    Err(_) => continue,
                };

                if tx.send(msg).await.is_err(){
                    // closed
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn run_aggregator(
    mut rx: tokio::sync::mpsc::Receiver<Message>,
    redis_url: String,
    output_channel: String,
    cancel_tkn: CancellationToken,
) -> AppResult<()> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut aggregator = Aggregator::default();
    loop {
        tokio::select! {
            _ = cancel_tkn.cancelled() =>{
                break;
            }
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    break;
                };

                let aggregated_msg = aggregator.process(msg);
                let payload = serde_json::to_string(&aggregated_msg)?;
                match conn.publish::<_,_,usize>(&output_channel, payload).await {
                    Ok(_) => (),
                    Err(_) => {
                        // log error
                        continue;
                    },
                };
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let args = Args::parse();

    let (tx, rx) = mpsc::channel(32);
    let cancel_tkn = CancellationToken::new();

    let subs_handler = tokio::spawn(run_subs(
        tx,
        args.redis_url.clone(),
        args.inputs,
        cancel_tkn.clone(),
    ));
    let aggregator_handle = tokio::spawn(run_aggregator(
        rx,
        args.redis_url,
        args.output,
        cancel_tkn.clone(),
    ));

    tokio::signal::ctrl_c().await?;
    cancel_tkn.cancel();

    subs_handler.await??;
    aggregator_handle.await??;

    Ok(())
}
