use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    redis_url: String,

    #[arg(long, default_value = "inputA,inputB,inputC")]
    inputs: String,

    #[arg(long, default_value = "outputChannel")]
    output: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", args);
}
