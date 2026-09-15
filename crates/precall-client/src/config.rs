// SPDX-License-Identifier: GPL-2.0-only
//! Client configuration — CLI/env first, persistent values under
//! `HKCU\Software\Precall` (enrollment writes once, service reads every boot).

use crate::ClientError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// `host:port` of the broker.
    pub server: String,
    /// TLS SNI/verification name (self-signed setups use a stable hostname).
    pub server_name: String,
    /// SHA-256 hex fingerprint of the broker certificate (pinning).
    pub fingerprint: Option<String>,
    /// Pre-shared auth token — stored only under HKCU, never in configs
    /// that could be committed.
    pub psk: Option<String>,
    /// Stable client identity; generated on first run.
    pub client_id: Option<Uuid>,
    pub capture_interval_ms: u64,
    pub enable_audio: bool,
    pub enable_ocr: bool,
    /// Processes never captured (exact or suffix match on exe name).
    pub excluded_processes: Vec<String>,
    /// Domains never captured (suffix match on browser URL host).
    pub excluded_domains: Vec<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server: "127.0.0.1:8443".into(),
            server_name: "precall-server".into(),
            fingerprint: None,
            psk: None,
            client_id: None,
            capture_interval_ms: 5000,
            enable_audio: false,
            enable_ocr: true,
            excluded_processes: vec![
                "credential manager".into(),
                "lockapp.exe".into(),
            ],
            excluded_domains: Vec::new(),
        }
    }
}

const REG_PATH: &str = "Software\\Precall";

/// Registry-backed persistence (HKCU so no elevation is required for
/// enrollment; the service runs as the interactive user anyway).
#[cfg(windows)]
mod reg {
    use super::*;
    use windows::core::{w, PCWSTR};
    use windows::Win32::System::Registry::{
        RegCloseKey, RegCreateKeyExW, RegGetValueW, RegSetValueExW,
        HKEY_CURRENT_USER, KEY_READ, KEY_WRITE, REG_SZ, REG_VALUE_TYPE, RRF_RT_REG_SZ,
    };

    pub fn get(name: &str) -> Option<String> {
        unsafe {
            let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
            let mut ty = REG_VALUE_TYPE(0);
            let mut size = 0u32;
            let path: Vec<u16> = REG_PATH.encode_utf16().chain(std::iter::once(0)).collect();
            if RegGetValueW(
                HKEY_CURRENT_USER,
                PCWSTR(path.as_ptr()),
                PCWSTR(wide.as_ptr()),
                RRF_RT_REG_SZ,
                Some(&mut ty),
                None,
                Some(&mut size),
            )
            .0 != 0 || size == 0
            {
                return None;
            }
            let mut buf = vec![0u16; (size / 2) as usize];
            if RegGetValueW(
                HKEY_CURRENT_USER,
                PCWSTR(path.as_ptr()),
                PCWSTR(wide.as_ptr()),
                RRF_RT_REG_SZ,
                Some(&mut ty),
                Some(buf.as_mut_ptr() as *mut _),
                Some(&mut size),
            )
            .0 != 0
            {
                return None;
            }
            buf.truncate(buf.iter().position(|&c| c == 0).unwrap_or(buf.len()));
            Some(String::from_utf16_lossy(&buf))
        }
    }

    pub fn set(name: &str, value: &str) -> Result<(), ClientError> {
        unsafe {
            let mut key = windows::Win32::System::Registry::HKEY::default();
            let path: Vec<u16> = REG_PATH.encode_utf16().chain(std::iter::once(0)).collect();
            let rc = RegCreateKeyExW(
                HKEY_CURRENT_USER,
                PCWSTR(path.as_ptr()),
                Some(0),
                w!(""),
                windows::Win32::System::Registry::REG_OPTION_NON_VOLATILE,
                KEY_READ | KEY_WRITE,
                None,
                &mut key,
                None,
            );
            if rc.0 != 0 {
                return Err(ClientError::Config(format!("RegCreateKeyExW: {rc:?}")));
            }
            let nwide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
            let vwide: Vec<u16> = value.encode_utf16().chain(std::iter::once(0)).collect();
            let r = RegSetValueExW(
                key,
                PCWSTR(nwide.as_ptr()),
                Some(0),
                REG_SZ,
                Some(std::slice::from_raw_parts(
                    vwide.as_ptr() as *const u8,
                    vwide.len() * 2,
                )),
            );
            let _ = RegCloseKey(key);
            if r.0 != 0 {
                Err(ClientError::Config(format!("RegSetValueExW: {r:?}")))
            } else {
                Ok(())
            }
        }
    }

}

#[cfg(not(windows))]
mod reg {
    use super::*;
    pub fn get(_name: &str) -> Option<String> {
        None
    }
    pub fn set(_name: &str, _v: &str) -> Result<(), ClientError> {
        Err(ClientError::Config("registry is Windows-only".into()))
    }
}

impl ClientConfig {
    /// Merge: explicit args > registry > defaults.
    pub fn resolve(mut self) -> Result<Self, ClientError> {
        if self.server == "127.0.0.1:8443" {
            if let Some(v) = reg::get("Server") {
                self.server = v;
            }
        }
        if self.server_name == "precall-server" {
            if let Some(v) = reg::get("ServerName") {
                self.server_name = v;
            }
        }
        if self.fingerprint.is_none() {
            self.fingerprint = reg::get("Fingerprint");
        }
        if self.psk.is_none() {
            self.psk = reg::get("Psk").or_else(|| std::env::var("PRECALL_PSK").ok());
        }
        if self.client_id.is_none() {
            self.client_id = reg::get("ClientId").and_then(|s| Uuid::parse_str(&s).ok());
        }
        if self.client_id.is_none() {
            self.client_id = Some(Uuid::new_v4());
            reg::set("ClientId", &self.client_id.unwrap().to_string())?;
        }
        Ok(self)
    }

    /// Persist enrollment values (called by `enroll` subcommand).
    pub fn persist(&self) -> Result<(), ClientError> {
        reg::set("Server", &self.server)?;
        reg::set("ServerName", &self.server_name)?;
        if let Some(f) = &self.fingerprint {
            reg::set("Fingerprint", f)?;
        }
        if let Some(p) = &self.psk {
            reg::set("Psk", p)?;
        }
        if let Some(c) = &self.client_id {
            reg::set("ClientId", &c.to_string())?;
        }
        Ok(())
    }
}
