#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;

use clap::Parser;
use server::{Result, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Abort the whole process on any thread panic. Without this, a panic in a
    // tokio worker (e.g. inside the listener task or the notify watcher
    // callback) only kills that single task: the runtime stays up, the HTTP
    // port stays bound, and systemd sees the service as healthy while the
    // listener is silently dead. Forcing an abort lets systemd's Restart=always
    // recover the service. We call the default hook first so we still get the
    // panic message + backtrace in the logs before exiting.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);
    run_websocket_server(&full_address, true, compression_level).await?;

    Ok(())
}
