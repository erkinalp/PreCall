// SPDX-License-Identifier: GPL-2.0-only
//! Windows service host. `precall-client install-service` registers with SCM;
//! the service entrypoint is `ffi_service_main` below, which delegates to the
//! same `run()` used by console mode.

use crate::config::ClientConfig;
use crate::run;
use std::ffi::OsString;
use windows_service::define_windows_service;
use windows_service::service::{
    ServiceAccess, ServiceControl, ServiceControlAccept, ServiceErrorControl, ServiceExitCode,
    ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::service_dispatcher;
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

pub const SERVICE_NAME: &str = "Precall";

pub fn run_as_service(cfg: ClientConfig) -> windows_service::Result<()> {
    // The config has to cross the extern boundary — stash it.
    SERVICE_CONFIG.with(|c| *c.borrow_mut() = Some(cfg));
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
}

thread_local! {
    static SERVICE_CONFIG: std::cell::RefCell<Option<ClientConfig>> = const { std::cell::RefCell::new(None) };
}

define_windows_service!(ffi_service_main, service_main);

fn service_main(_args: Vec<OsString>) {
    if let Err(e) = run_service_inner() {
        tracing::error!("service failed: {e}");
    }
}

fn run_service_inner() -> windows_service::Result<()> {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let status_handle = service_control_handler::register(SERVICE_NAME, move |event| {
        match event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                let _ = stop_tx.send(true);
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    })?;

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: std::time::Duration::default(),
        process_id: None,
    })?;

    let cfg = SERVICE_CONFIG.with(|c| c.borrow_mut().take()).unwrap_or_default();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .map_err(|e| windows_service::Error::Winapi(std::io::Error::other(e)))?;
    let _ = rt.block_on(run::run(cfg, stop_rx));

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: std::time::Duration::default(),
        process_id: None,
    })?;
    Ok(())
}

/// `sc create Precall` — registers this exe as a service (needs admin).
pub fn install(exe_path: &std::path::Path) -> windows_service::Result<()> {
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )?;
    let info = ServiceInfo {
        name: OsString::from(SERVICE_NAME),
        display_name: OsString::from("Precall Capture Agent"),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        // `run --as-service` makes the same binary run under SCM.
        executable_path: exe_path.to_path_buf(),
        launch_arguments: vec![OsString::from("run"), OsString::from("--as-service")],
        account_name: None, // Local System — see README re: interactive capture.
        account_password: None,
        dependencies: vec![],
    };
    manager.create_service(&info, ServiceAccess::START)?;
    Ok(())
}

pub fn uninstall() -> windows_service::Result<()> {
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT,
    )?;
    let svc = manager.open_service(
        SERVICE_NAME,
        ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
    )?;
    let _ = svc.stop();
    svc.delete()
}
