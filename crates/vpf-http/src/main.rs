use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use vpf::catalog::Catalog;

#[derive(Parser)]
#[command(about = "Serve immutable VPF snapshots over HTTP with byte-range reads")]
struct Args {
    /// Directory of `.vpf` files; `<name>.vpf` is served as `/files/<name>/...`.
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !args.dir.is_dir() {
        return Err(format!("{} is not a directory", args.dir.display()).into());
    }
    let catalog = Arc::new(Catalog::new(args.dir));
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!("VPF HTTP listening on {}", listener.local_addr()?);
    axum::serve(listener, vpf_http::router(catalog))
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("failed to listen for shutdown: {error}");
            }
        })
        .await?;
    Ok(())
}
