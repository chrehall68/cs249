use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct ServerCli {
    #[arg(short, long)]
    verbose: bool,
    /// the port to listen for http connections on
    #[arg(long, default_value_t = 8000)]
    http_port: u16,
    /// the port to listen for regular tcp connections on
    #[arg(long, default_value_t = 8001)]
    tcp_port: u16,
    /// the host to listen on
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
}

fn main() {
    let cli = ServerCli::parse();
    println!(
        "verbose: {} http port: {} tcp_port: {} host: {}",
        cli.verbose, cli.http_port, cli.tcp_port, cli.host
    );
}
