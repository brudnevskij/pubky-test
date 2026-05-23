use clap::Parser;
use pubky_test::{error::AppResult, run_aggregator, run_subs};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_names(false)
        .with_thread_ids(false)
        .init();
}

#[tokio::main]
async fn main() -> AppResult<()> {
    init_tracing();
    let args = Args::parse();

    tracing::info!(
        redis_url = %args.redis_url,
        output_channel =%args.output,
        input_channels = ?args.inputs,
        "starting redis aggregatror"
    );

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
    tracing::info!("shutdown signal received");
    cancel_tkn.cancel();

    subs_handler.await??;
    aggregator_handle.await??;
    tracing::info!("shutdo completed");

    Ok(())
}
