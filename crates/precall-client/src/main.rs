// SPDX-License-Identifier: GPL-2.0-only
//! `precall-client` — capture agent CLI.
//!
//!   run                 capture + stream (console mode)
//!   run --as-service    SCM-hosted mode (invoked by the service manager)
//!   install-service     register with the Service Control Manager (admin)
//!   uninstall-service   remove the service
//!   enroll              verify connectivity + persist config to HKCU\Software\Precall

use clap::{Parser, Subcommand};
use precall_client::config::ClientConfig;
use precall_client::{run, service};

#[derive(Parser)]
#[command(name = "precall-client", about = "Precall capture client for Windows")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Run(RunArgs),
    InstallService,
    UninstallService,
    Enroll(RunArgs),
}

#[derive(Parser, Clone)]
struct RunArgs {
    /// Broker host:port.
    #[arg(long, env = "PRECALL_SERVER")]
    server: Option<String>,
    /// TLS SNI / verification name.
    #[arg(long, env = "PRECALL_SERVER_NAME")]
    server_name: Option<String>,
    /// SHA-256 fingerprint (hex) of broker cert — cert pinning.
    #[arg(long, env = "PRECALL_SERVER_FINGERPRINT")]
    fingerprint: Option<String>,
    /// PSK token (prefer env; a command-line token shows in process lists).
    #[arg(long, env = "PRECALL_PSK")]
    psk: Option<String>,
    /// Capture cadence in ms.
    #[arg(long, env = "PRECALL_INTERVAL_MS")]
    capture_interval_ms: Option<u64>,
    /// Enable WASAPI loopback audio.
    #[arg(long, env = "PRECALL_AUDIO")]
    audio: bool,
    /// Disable on-device OCR.
    #[arg(long, env = "PRECALL_NO_OCR")]
    no_ocr: bool,
    /// Never capture these processes (comma-separated exe names).
    #[arg(long, env = "PRECALL_EXCLUDE_PROCESSES", value_delimiter = ',')]
    exclude_processes: Vec<String>,
    /// Never capture these domains (comma-separated suffixes).
    #[arg(long, env = "PRECALL_EXCLUDE_DOMAINS", value_delimiter = ',')]
    exclude_domains: Vec<String>,
    /// Internal: invoked by SCM.
    #[arg(long, hide = true)]
    as_service: bool,
}

impl RunArgs {
    fn config(&self) -> anyhow::Result<ClientConfig> {
        let mut c = ClientConfig::default();
        if let Some(s) = &self.server {
            c.server = s.clone();
        }
        if let Some(s) = &self.server_name {
            c.server_name = s.clone();
        }
        c.fingerprint = self.fingerprint.clone();
        c.psk = self.psk.clone();
        if let Some(ms) = self.capture_interval_ms {
            c.capture_interval_ms = ms;
        }
        c.enable_audio = self.audio;
        c.enable_ocr = !self.no_ocr;
        if !self.exclude_processes.is_empty() {
            c.excluded_processes = self.exclude_processes.clone();
        }
        if !self.exclude_domains.is_empty() {
            c.excluded_domains = self.exclude_domains.clone();
        }
        Ok(c.resolve()?)
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "precall_client=info,precall_proto=info".into()),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Cmd::Run(args) => {
            let cfg = args.config()?;
            if args.as_service {
                service::run_as_service(cfg)?;
                return Ok(());
            }
            let (_tx, rx) = tokio::sync::watch::channel(false);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(run::run(cfg, rx))?;
        }
        Cmd::Enroll(args) => {
            let cfg = args.config()?;
            cfg.persist()?;
            println!("Enrolled: server={} client_id={}", cfg.server, cfg.client_id.unwrap());
            println!("Verify the broker fingerprint printed above matches the broker's own output.");
        }
        Cmd::InstallService => {
            let exe = std::env::current_exe()?;
            let abs = exe.canonicalize()?;
            service::install(&abs)?;
            println!("Service 'Precall' installed (start it with: sc start Precall)");
        }
        Cmd::UninstallService => {
            service::uninstall()?;
            println!("Service 'Precall' removed");
        }
    }
    Ok(())
}
