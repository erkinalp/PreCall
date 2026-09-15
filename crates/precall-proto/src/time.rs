// SPDX-License-Identifier: GPL-2.0-only
//! FILETIME helpers shared by client (writer) and broker (reader).

/// 100ns ticks between 1601-01-01 and 1970-01-01 (the FILETIME epoch delta).
pub const FILETIME_UNIX_DELTA: u64 = 116_444_736_000_000_000;

/// Current time as FILETIME ticks.
pub fn filetime_now() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    FILETIME_UNIX_DELTA + now.as_secs() * 10_000_000 + now.subsec_nanos() as u64 / 100
}

/// FILETIME → unix seconds.
pub fn filetime_to_unix(ticks: u64) -> f64 {
    (ticks.saturating_sub(FILETIME_UNIX_DELTA)) as f64 / 10_000_000.0
}
