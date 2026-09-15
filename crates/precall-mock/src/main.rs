// SPDX-License-Identifier: GPL-2.0-only
//! `precall-mock` — headless capture client for end-to-end testing.

use clap::{Parser, Subcommand};
use precall_mock::{pinned_client_config, sample_meta, synthesize_jpeg, Session, SyntheticFrame};
use precall_proto::AuthMethod;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "precall-mock", version, about = "Precall headless test client")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Stream N synthetic captures to a broker.
    Stream(StreamArgs),
}

#[derive(clap::Args)]
struct StreamArgs {
    /// Broker address (host:port).
    #[arg(long, default_value = "127.0.0.1:3390")]
    server: SocketAddr,
    /// TLS server name (must match cert SAN).
    #[arg(long, default_value = "localhost")]
    server_name: String,
    /// PSK token presented in the handshake.
    #[arg(long, env = "PRECALL_PSK")]
    psk: String,
    /// Pinned server cert fingerprint (hex SHA-256).
    #[arg(long, env = "PRECALL_SERVER_FINGERPRINT")]
    pin: String,
    /// Client UUID (generated once if omitted; persist for a stable identity).
    #[arg(long)]
    client_id: Option<Uuid>,
    #[arg(long, default_value = "MOCKBOX")]
    hostname: String,
    /// Number of captures to send.
    #[arg(long, default_value_t = 5)]
    count: u64,
    /// Delay between captures.
    #[arg(long, default_value_t = 250)]
    interval_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Cmd::Stream(args) => stream(args).await,
    }
}

async fn stream(args: StreamArgs) -> anyhow::Result<()> {
    let tls = pinned_client_config(&args.pin).map_err(anyhow::Error::msg)?;
    let client_id = args.client_id.unwrap_or_else(Uuid::new_v4);
    let mut session = Session::connect(
        args.server,
        &args.server_name,
        tls,
        AuthMethod::Psk { token: args.psk.clone() },
        client_id,
        &args.hostname,
    )
    .await?;
    println!("session {}", session.hello.session_id);

    for i in 0..args.count {
        let token = format!("mock-img-{i:04}");
        let frame = SyntheticFrame {
            jpeg: synthesize_jpeg(1280, 720, i),
            meta: sample_meta(i, &token),
        };
        session.send_capture(&frame).await?;
        session.heartbeat(i + 1, 0).await?;
        println!("sent capture {i} token={token}");
        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
    }
    session.close().await?;
    Ok(())
}
