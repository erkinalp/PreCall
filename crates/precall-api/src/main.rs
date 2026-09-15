// SPDX-License-Identifier: GPL-2.0-only
//! `precall-api` — HTTP server binary.

use clap::Parser;
use precall_api::{build_router, ApiState, Embedder};
use precall_store::objects::load_or_create_key;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "precall-api", about = "Precall query API (Recall-compatible REST + WS)")]
struct Cli {
    /// Listen address, e.g. 127.0.0.1:8080
    #[arg(long, env = "PRECALL_API_LISTEN", default_value = "127.0.0.1:8080")]
    listen: SocketAddr,

    /// Data root shared with the broker (contains {client}/ukg.db etc.)
    #[arg(long, env = "PRECALL_DATA_ROOT")]
    data_root: PathBuf,

    /// Bearer token(s) for the API. Repeatable. Empty = loopback-open.
    #[arg(long = "token", env = "PRECALL_API_TOKEN", value_delimiter = ',')]
    tokens: Vec<String>,

    /// AI pipeline base URL; empty/absent = built-in local embedder.
    #[arg(long, env = "PRECALL_AI_URL")]
    ai_url: Option<String>,

    /// Embedding dimension for the si_* stores.
    #[arg(long, env = "PRECALL_EMBEDDING_DIM", default_value_t = 384)]
    embedding_dim: usize,

    /// Disable the background embedding/enrichment worker.
    #[arg(long, env = "PRECALL_DISABLE_ENRICHER")]
    no_enricher: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let embedder = match cli.ai_url {
        Some(url) if !url.is_empty() => Embedder::Http {
            base: url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        },
        _ => Embedder::Local,
    };
    let embedder = Arc::new(embedder);

    let object_key = load_or_create_key(&cli.data_root)?;

    let state = ApiState {
        data_root: cli.data_root.clone(),
        embedder: embedder.clone(),
        object_key,
        auth: precall_api::auth::ApiAuth::new(&cli.tokens),
        embedding_dim: cli.embedding_dim,
    };
    state.auth.note_open_mode();

    if !cli.no_enricher {
        precall_api::enrich::spawn_enricher(
            cli.data_root.clone(),
            embedder,
            cli.embedding_dim,
            std::time::Duration::from_secs(5),
        );
    }

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!("precall-api listening on {}", cli.listen);
    axum::serve(listener, app).await?;
    Ok(())
}
