pub mod aggregator;
pub mod error;

use crate::{
    aggregator::{Aggregator, Message},
    error::AppResult,
};
use redis::AsyncCommands;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

pub async fn run_subs(
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

pub async fn run_aggregator(
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
