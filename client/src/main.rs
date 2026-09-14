use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct TcpClientCli {
    #[arg(short, long)]
    verbose: bool,
    /// the port to connect to for tcp connections
    #[arg(long, default_value_t = discord_common::DEFAULT_TCP_PORT)]
    tcp_port: u16,
    /// the host to connect to
    #[arg(long, default_value_t = discord_common::DEFAULT_HOST.to_string())]
    host: String,
}

#[tokio::main]
async fn main() {
    let cli = TcpClientCli::parse();
    println!(
        "verbose: {} tcp_port: {} host: {}",
        cli.verbose, cli.tcp_port, cli.host
    );
}
