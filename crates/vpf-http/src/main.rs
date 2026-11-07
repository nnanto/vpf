use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use vpf::store::Store;

#[derive(Parser)]
#[command(about = "Serve immutable VPF snapshots over HTTP with byte-range reads")]
struct Args {
    #[arg(long)]
    store: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let store = Arc::new(Store::open(args.store)?);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!("VPF HTTP listening on {}", listener.local_addr()?);
    axum::serve(listener, vpf_http::router(store))
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("failed to listen for shutdown: {error}");
            }
        })
        .await?;
    Ok(())
}
