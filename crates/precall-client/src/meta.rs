// SPDX-License-Identifier: GPL-2.0-only
//! Foreground-window metadata — the ukg.db-side of a capture.
//!
//! Window identity comes from the Win32 windowing APIs; the process/AUMID
//! side feeds `ActivationUri` (Recall relaunches via
//! `IApplicationActivationManager::ActivateApplication(aumid)`).

use precall_proto::metadata::{AppRecord, FileRecord, WebRecord};
use windows::core::{Interface, PWSTR};
use windows::Win32::Foundation::{CloseHandle, HWND, MAX_PATH};
use windows::Win32::Storage::Packaging::Appx::GetApplicationUserModelId;
use windows::Win32::System::Com::{CoCreateInstance, CoInitializeEx, CoTaskMemFree, CLSCTX_ALL, COINIT_APARTMENTTHREADED};
use windows::Win32::System::Com::StructuredStorage::PropVariantToStringAlloc;
use windows::Win32::System::Threading::{
    GetCurrentProcessId, OpenProcess, QueryFullProcessImageNameW, PROCESS_NAME_WIN32,
    PROCESS_QUERY_LIMITED_INFORMATION,
};
use windows::Win32::UI::Shell::PropertiesSystem::{
    IPropertyStore, SHGetPropertyStoreForWindow,
};
use windows::Win32::UI::Shell::{IShellWindows, ShellWindows};
use windows::Win32::UI::WindowsAndMessaging::{
    GetForegroundWindow, GetWindowDisplayAffinity, GetWindowRect, GetWindowTextW,
    GetWindowThreadProcessId,
};

/// Desktop-space window bounds (i32 — proto's `Rect` is i16 PDU-side).
#[derive(Debug, Default, Clone, Copy)]
pub struct WindowBounds {
    pub left: i32,
    pub top: i32,
    pub right: i32,
    pub bottom: i32,
}

/// Everything Recall records for the window under capture.
#[derive(Debug, Default)]
pub struct WindowMeta {
    pub hwnd: isize,
    pub title: String,
    pub bounds: WindowBounds,
    pub is_foreground: bool,
    pub process_name: String,
    pub process_path: String,
    pub pid: u32,
    /// AUMID for packaged apps; drives `ActivationUri`.
    pub aumid: Option<String>,
    pub urls: Vec<WebRecord>,
    pub files: Vec<FileRecord>,
}

impl WindowMeta {
    /// `Name` column: `{process_name} ({pid})` — Recall's convention.
    pub fn capture_name(&self) -> String {
        format!("{} ({})", self.process_name, self.pid)
    }

    pub fn activation_uri(&self) -> Option<String> {
        self.aumid
            .as_ref()
            .map(|a| format!("shell:AppsFolder\\{a}"))
    }

    pub fn fallback_uri(&self) -> Option<String> {
        if self.process_path.is_empty() {
            None
        } else {
            Some(format!("file:///{}", self.process_path.replace('\\', "/")))
        }
    }

    pub fn app_record(&self) -> Option<AppRecord> {
        if self.process_name.is_empty() {
            return None;
        }
        Some(AppRecord {
            windows_app_id: self.aumid.clone(),
            icon_uri: None,
            name: self.process_name.clone(),
            path: if self.process_path.is_empty() {
                None
            } else {
                Some(self.process_path.clone())
            },
            properties: None,
        })
    }
}

/// Snapshot the foreground window.
pub fn foreground_meta() -> WindowMeta {
    let mut m = WindowMeta::default();
    unsafe {
        let hwnd = GetForegroundWindow();
        m.hwnd = hwnd.0 as isize;
        m.is_foreground = true;
        let mut buf = [0u16; 512];
        let n = GetWindowTextW(hwnd, &mut buf);
        m.title = String::from_utf16_lossy(&buf[..n as usize]);
        let mut r = windows::Win32::Foundation::RECT::default();
        let _ = GetWindowRect(hwnd, &mut r);
        m.bounds = WindowBounds {
            left: r.left,
            top: r.top,
            right: r.right,
            bottom: r.bottom,
        };
        let mut pid = 0u32;
        GetWindowThreadProcessId(hwnd, Some(&mut pid));
        m.pid = pid;
        if let Ok(h) = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid) {
            let mut name = [0u16; MAX_PATH as usize];
            let mut len = MAX_PATH;
            if QueryFullProcessImageNameW(h, PROCESS_NAME_WIN32, PWSTR(name.as_mut_ptr()), &mut len).is_ok() {
                m.process_path = String::from_utf16_lossy(&name[..len as usize]);
                m.process_name = m
                    .process_path
                    .rsplit(['\\', '/'])
                    .next()
                    .unwrap_or("")
                    .to_lowercase();
            }
            // AUMID (store apps) — fails with APPMODEL_ERROR for unpackaged.
            m.aumid = aumid_for(h);
            let _ = CloseHandle(h);
        }
        // Some hosts expose the AUMID as a window property even when the
        // process API can't see it.
        if m.aumid.is_none() {
            m.aumid = aumid_for_window(hwnd);
        }
    }
    m
}

unsafe fn aumid_for(h: windows::Win32::Foundation::HANDLE) -> Option<String> {
    unsafe {
        let mut len = 0u32;
        let _ = GetApplicationUserModelId(h, &mut len, None);
        if len == 0 || len > 4096 {
            return None;
        }
        let mut buf = vec![0u16; len as usize];
        if GetApplicationUserModelId(h, &mut len, Some(PWSTR(buf.as_mut_ptr()))).is_err() {
            return None;
        }
        buf.truncate(buf.iter().position(|&c| c == 0).unwrap_or(buf.len()));
        if buf.is_empty() {
            None
        } else {
            Some(String::from_utf16_lossy(&buf))
        }
    }
}

unsafe fn aumid_for_window(hwnd: HWND) -> Option<String> {
    unsafe {
        let store: IPropertyStore = SHGetPropertyStoreForWindow(hwnd).ok()?;
        // PKEY_AppUserModel_ID {9F4C2855-9F79-4B39-A8D0-E1D42DE1D5F3}, pid 5
        let pkey = windows::Win32::Foundation::PROPERTYKEY {
            fmtid: windows::core::GUID::from_u128(0x9f4c2855_9f79_4b39_a8d0_e1d42de1d5f3),
            pid: 5,
        };
        let pv = store.GetValue(&pkey).ok()?;
        let s = PropVariantToStringAlloc(&pv).ok()?;
        let out = s.to_string().ok();
        CoTaskMemFree(Some(s.0 as *const _));
        out.filter(|v| !v.is_empty())
    }
}

/// URLs of the foreground browser window via UI Automation (address-bar
/// `Value` pattern). Best-effort — any failure yields an empty list.
#[cfg(windows)]
pub fn browser_url(hwnd: HWND) -> Vec<WebRecord> {
    use windows::Win32::System::Variant::{VARIANT, VT_I4};
    use windows::Win32::UI::Accessibility::{
        CUIAutomation, IUIAutomation, IUIAutomationValuePattern, TreeScope_Subtree,
        UIA_ControlTypePropertyId, UIA_EditControlTypeId, UIA_ValuePatternId,
    };

    unsafe {
        let _ = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        let uia: IUIAutomation = match CoCreateInstance(&CUIAutomation, None, CLSCTX_ALL) {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        let root = match uia.ElementFromHandle(hwnd) {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };
        let v = VARIANT {
            Anonymous: windows::Win32::System::Variant::VARIANT_0 {
                Anonymous: std::mem::ManuallyDrop::new(
                    windows::Win32::System::Variant::VARIANT_0_0 {
                        vt: VT_I4,
                        wReserved1: 0,
                        wReserved2: 0,
                        wReserved3: 0,
                        Anonymous: windows::Win32::System::Variant::VARIANT_0_0_0 {
                            lVal: UIA_EditControlTypeId.0,
                        },
                    },
                ),
            },
        };
        let cond = match uia.CreatePropertyCondition(UIA_ControlTypePropertyId, &v) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let edits = match root.FindAll(TreeScope_Subtree, &cond) {
            Ok(a) => a,
            Err(_) => return Vec::new(),
        };
        let len = edits.Length().unwrap_or(0);
        let mut out = Vec::new();
        for i in 0..len {
            let Ok(el) = edits.GetElement(i) else { continue };
            // Address bars are Edit controls named "Address" / "Search" / "URL".
            let name = el.CurrentName().map(|s| s.to_string()).unwrap_or_default();
            let lname = name.to_lowercase();
            if !(lname.contains("address") || lname.contains("url") || lname.contains("search")) {
                continue;
            }
            let Ok(vp) = el.GetCurrentPatternAs::<IUIAutomationValuePattern>(UIA_ValuePatternId) else {
                continue;
            };
            let Ok(val) = vp.CurrentValue() else { continue };
            let url = val.to_string();
            if url.starts_with("http://") || url.starts_with("https://") || url.contains('.') {
                let host = url_host(&url);
                if !host.is_empty() {
                    out.push(WebRecord {
                        domain: host,
                        uri: url.clone(),
                        icon_uri: None,
                        properties: None,
                    });
                }
            }
            if !out.is_empty() {
                break; // first hit is the address bar; others are search fields
            }
        }
        out
    }
}

#[cfg(not(windows))]
pub fn browser_url(_hwnd: ()) -> Vec<WebRecord> {
    Vec::new()
}

fn url_host(url: &str) -> String {
    let s = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    s.split(['/', '?', '#', ':']).next().unwrap_or("").to_lowercase()
}

/// Explorer windows open at file-system paths (Recall's `File` activity).
#[cfg(windows)]
pub fn explorer_paths() -> Vec<FileRecord> {
    use windows::Win32::System::Variant::{VARIANT, VT_I4};
    use windows::Win32::UI::Shell::IWebBrowserApp;

    unsafe {
        let _ = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        let shell: IShellWindows = match CoCreateInstance(&ShellWindows, None, CLSCTX_ALL) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let count = shell.Count().unwrap_or(0);
        let mut out = Vec::new();
        for i in 0..count {
            let v = VARIANT {
                Anonymous: windows::Win32::System::Variant::VARIANT_0 {
                    Anonymous: std::mem::ManuallyDrop::new(
                        windows::Win32::System::Variant::VARIANT_0_0 {
                            vt: VT_I4,
                            wReserved1: 0,
                            wReserved2: 0,
                            wReserved3: 0,
                            Anonymous: windows::Win32::System::Variant::VARIANT_0_0_0 { lVal: i },
                        },
                    ),
                },
            };
            let Ok(item) = shell.Item(&v) else { continue };
            // ShellWindows items also implement IWebBrowserApp (Explorer
            // folder windows and IE/frame hosts alike).
            let Ok(app) = item.cast::<IWebBrowserApp>() else { continue };
            let Ok(loc) = app.LocationURL() else { continue };
            let url = loc.to_string();
            if url.is_empty() {
                continue;
            }
            // Convert file:/// URL → filesystem path for the File row.
            if let Some(rest) = url.strip_prefix("file:///") {
                let path = rest.replace('/', "\\");
                let name = path
                    .rsplit('\\')
                    .next()
                    .unwrap_or(&path)
                    .to_string();
                let clean = path.trim_end_matches('\\').to_string();
                let ext = name
                    .rsplit_once('.')
                    .map(|(_, e)| e.to_string());
                out.push(FileRecord {
                    name,
                    path: clean,
                    extension: ext,
                    kind: Some("folder".into()),
                    r#type: None,
                    properties: None,
                    object_id: None,
                    volume_id: None,
                });
            }
        }
        out
    }
}

/// Protected-window check: any nonzero display affinity (WDA_MONITOR /
/// WDA_EXCLUDEFROMCAPTURE) means the app asked not to be captured — honor it.
pub fn is_protected_window(hwnd: HWND) -> bool {
    unsafe {
        let mut affinity = 0u32;
        if GetWindowDisplayAffinity(hwnd, &mut affinity).is_ok() {
            affinity != 0
        } else {
            false
        }
    }
}

/// Current process id (used by the service to tag self-captures).
pub fn self_pid() -> u32 {
    unsafe { GetCurrentProcessId() }
}
