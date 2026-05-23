pub mod aggregator;
pub mod error;

use crate::{
    aggregator::{Aggregator, Message},
    error::AppResult,
};
use redis::AsyncCommands;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[tracing::instrument(
    name = "redis_subscriber",
    skip(tx, client, cancel_tkn),
    fields(input_channels = ?input_channels)
)]
pub async fn run_subs(
    tx: tokio::sync::mpsc::Sender<Message>,
    client: redis::Client,
    input_channels: Vec<String>,
    cancel_tkn: CancellationToken,
) -> AppResult<()> {
    tracing::info!("starting subscriber task");

    let mut pubsub = client.get_async_pubsub().await?;
    tracing::info!("connected to redis pubsub");

    for channel_name in input_channels {
        tracing::info!(channel = %channel_name,"subscribing to the channel");
        pubsub.subscribe(channel_name).await?;
    }

    let mut stream = pubsub.on_message();

    loop {
        tokio::select! {
            _ = cancel_tkn.cancelled() => {
               tracing::info!("subscriber received shutdown signal");
               break;
            }

            msg_redis = stream.next() =>{
                let Some(msg) = msg_redis else {
                    tracing::warn!("redis pubsub stream closed");
                    break;
                };

                let channel = msg.get_channel_name().to_string();
                let payload: String = match msg.get_payload(){
                    Ok(payload)=> payload,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            channel = %channel,
                            "failed to read message payload"
                            );
                        continue;
                    }
                };

                let msg: Message = match serde_json::from_str(&payload){
                    Ok(msg) => msg,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            channel = %channel,
                            payload = %payload,
                            "failed to deserialize redis message"
                            );
                        continue;
                    },
                };

                if tx.send(msg).await.is_err(){
                    tracing::warn!("receiver is closed; stopping subscriber");
                    break;
                }
            }
        }
    }

    tracing::info!("subscriber task stopped");
    Ok(())
}

#[tracing::instrument(
    name = "aggregator",
    skip(rx, client, cancel_tkn),
    fields(output_channel = %output_channel)
)]
pub async fn run_aggregator(
    mut rx: tokio::sync::mpsc::Receiver<Message>,
    client: redis::Client,
    output_channel: String,
    cancel_tkn: CancellationToken,
) -> AppResult<()> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    tracing::info!("connected to redis publisher");

    let mut aggregator = Aggregator::default();
    loop {
        tokio::select! {
            _ = cancel_tkn.cancelled() =>{
               tracing::info!("aggregator received shutdown signal");
                break;
            }
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    tracing::warn!("subscriber channel closed; stopping aggregator");
                    break;
                };

                let aggregated_msg = aggregator.process(msg);
                let payload = serde_json::to_string(&aggregated_msg)?;
                let payload_len = payload.len();
                match conn.publish::<_,_,usize>(&output_channel, payload).await {
                    Ok(receivers) => {
                        tracing::debug!(
                            output_channel = %output_channel,
                            receivers,
                            payload_len = %payload_len,
                            "published aggregated message"
                        );
                    },
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            output_channel = %output_channel,
                            payload_len = %payload_len,
                            "failed to publish aggregated message"
                        );
                        continue;
                    },
                };
            }
        }
    }

    tracing::info!("aggregator task stopped");
    Ok(())
}
