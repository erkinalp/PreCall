// SPDX-License-Identifier: GPL-2.0-only
//! Privacy filtering — decisions happen BEFORE encoding so excluded
//! content never reaches memory, let alone the wire.
//!
//! Sources of exclusion:
//! * `WDA_MONITOR` / `WDA_EXCLUDEFROMCAPTURE` display affinity (the app's
//!   own request — password managers, DRM, bank apps all set this).
//! * User-configured process list (exact or suffix match on exe name).
//! * User-configured domain list (suffix match on browser URL host).
//! * Global pause flag pushed over PCCTRL (`ControlMessage::Pause`).

use crate::meta::WindowMeta;
use std::sync::atomic::{AtomicBool, Ordering};

static PAUSED: AtomicBool = AtomicBool::new(false);

pub fn set_paused(p: bool) {
    PAUSED.store(p, Ordering::Relaxed);
}
pub fn paused() -> bool {
    PAUSED.load(Ordering::Relaxed)
}

#[derive(Debug)]
pub struct Exclusions {
    pub processes: Vec<String>,
    pub domains: Vec<String>,
}

impl Exclusions {
    pub fn new(processes: &[String], domains: &[String]) -> Self {
        Self {
            processes: processes.iter().map(|s| s.to_lowercase()).collect(),
            domains: domains.iter().map(|s| s.to_lowercase()).collect(),
        }
    }

    fn process_excluded(&self, name: &str) -> bool {
        let n = name.to_lowercase();
        self.processes
            .iter()
            .any(|p| n == *p || n.ends_with(&format!("\\{p}")) || n.ends_with(&format!("/{p}")))
    }

    fn domain_excluded(&self, host: &str) -> bool {
        let h = host.to_lowercase();
        self.domains
            .iter()
            .any(|d| h == *d || h.ends_with(&format!(".{d}")))
    }
}

/// Why a capture was dropped — surfaced in logs and the heartbeat counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    Paused,
    ProtectedWindow,
    ExcludedProcess,
    ExcludedDomain,
    NoWindow,
}

/// Should this capture be dropped? `meta` may be absent (no foreground
/// window — record nothing, matching Recall's behavior on idle desktop).
pub fn check(meta: Option<&WindowMeta>, ex: &Exclusions, protected_hwnd: bool) -> Option<DropReason> {
    if paused() {
        return Some(DropReason::Paused);
    }
    let Some(m) = meta else {
        return Some(DropReason::NoWindow);
    };
    if protected_hwnd {
        return Some(DropReason::ProtectedWindow);
    }
    if ex.process_excluded(&m.process_name) {
        return Some(DropReason::ExcludedProcess);
    }
    for w in &m.urls {
        if ex.domain_excluded(&w.domain) {
            return Some(DropReason::ExcludedDomain);
        }
    }
    None
}
