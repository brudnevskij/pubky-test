use clap::Parser;

mod aggregator;

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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let client = redis::Client::open(args.redis_url.clone()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let pong: String = redis::cmd("PING").query_async(&mut conn).await.unwrap();
    println!("{pong}");
}
