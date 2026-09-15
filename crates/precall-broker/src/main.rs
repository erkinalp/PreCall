// SPDX-License-Identifier: GPL-2.0-only
//! `precall-broker` — reverse-capture TLS listener + ingestion daemon.

use clap::{Parser, Subcommand};
use precall_broker::{tls, Broker, BrokerConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "precall-broker", version, about = "Precall connection broker")]
struct Cli {
    /// TOML config file (CLI flags override it).
    #[arg(long, env = "PRECALL_BROKER_CONFIG")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the broker.
    Serve(ServeArgs),
    /// Generate a self-signed TLS cert for development.
    GenCert {
        /// Hostname the cert is issued to.
        #[arg(long, default_value = "localhost")]
        hostname: String,
        #[arg(long, default_value = "cert.pem")]
        cert: PathBuf,
        #[arg(long, default_value = "key.pem")]
        key: PathBuf,
    },
}

#[derive(clap::Args)]
struct ServeArgs {
    #[arg(long, env = "PRECALL_LISTEN", default_value = "0.0.0.0:3390")]
    listen: SocketAddr,
    #[arg(long, env = "PRECALL_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    #[arg(long, env = "PRECALL_TLS_KEY")]
    tls_key: Option<PathBuf>,
    /// Generate a self-signed dev cert under --data-root if none is given.
    #[arg(long)]
    self_signed: bool,
    #[arg(long, env = "PRECALL_DATA_ROOT", default_value = "data/broker")]
    data_root: PathBuf,
    /// Pre-shared token (repeatable). Prefer the config file for real deploys.
    #[arg(long = "psk", env = "PRECALL_PSK", value_delimiter = ',')]
    psk_tokens: Vec<String>,
    /// Enrolled client-cert fingerprint, hex (repeatable).
    #[arg(long = "enroll-cert")]
    enrolled_certs: Vec<String>,
    #[arg(long)]
    allow_ntlm: bool,
    /// Reject client_ids not previously seen (strict tenancy).
    #[arg(long)]
    strict_tenancy: bool,
    /// Store objects unencrypted at rest (NOT for production).
    #[arg(long)]
    no_encryption_at_rest: bool,
    #[arg(long, env = "PRECALL_MAX_CLIENTS", default_value_t = 100)]
    max_clients: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();
    let cli = Cli::parse();
    match cli.command {
        Cmd::GenCert { hostname, cert, key } => {
            let s = tls::write_self_signed(&hostname, &cert, &key)?;
            println!("wrote {} and {}", cert.display(), key.display());
            println!("fingerprint (pin this in clients): {}", s.fingerprint);
            Ok(())
        }
        Cmd::Serve(args) => serve(cli.config, args).await,
    }
}

async fn serve(config_path: Option<PathBuf>, args: ServeArgs) -> anyhow::Result<()> {
    let mut cfg = if let Some(path) = &config_path {
        BrokerConfig::load(Some(path))?
    } else {
        BrokerConfig {
            listen: args.listen,
            tls_cert: args.tls_cert.clone().unwrap_or_default(),
            tls_key: args.tls_key.clone().unwrap_or_default(),
            data_root: args.data_root.clone(),
            psk_tokens: args.psk_tokens.clone(),
            enrolled_certs: args.enrolled_certs.clone(),
            allow_ntlm: args.allow_ntlm,
            allow_any_client: !args.strict_tenancy,
            require_sealed_payloads: false,
            heartbeat_interval_ms: 30_000,
            max_clients: args.max_clients,
            encrypt_at_rest: !args.no_encryption_at_rest,
            embedding_dim: 384,
        }
    };
    if !args.psk_tokens.is_empty() {
        cfg.psk_tokens = args.psk_tokens.clone();
    }
    if !args.enrolled_certs.is_empty() {
        cfg.enrolled_certs = args.enrolled_certs.clone();
    }

    let (cert_path, key_path) = if args.self_signed && !cfg.tls_cert.exists() {
        let cert = cfg.data_root.join("dev-cert.pem");
        let key = cfg.data_root.join("dev-key.pem");
        std::fs::create_dir_all(&cfg.data_root)?;
        if !cert.exists() {
            let s = tls::write_self_signed("localhost", &cert, &key)?;
            tracing::info!(fingerprint = %s.fingerprint, "generated dev cert");
            eprintln!("dev fingerprint (pin in clients): {}", s.fingerprint);
        }
        (cert, key)
    } else {
        (cfg.tls_cert.clone(), cfg.tls_key.clone())
    };
    let tls_cfg = tls::server_config(&cert_path, &key_path)?;
    let broker = Broker::new(cfg, tls_cfg)?;
    Arc::new(broker).run().await?;
    Ok(())
}
