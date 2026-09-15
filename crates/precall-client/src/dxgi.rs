// SPDX-License-Identifier: GPL-2.0-only
//! DXGI Desktop Duplication — the same capture mechanism Terminal Services
//! uses for session remoting. Runs on ONE dedicated thread (D3D11 objects
//! are !Send); frames go out over a std::mpsc channel.

use crate::ClientError;
use std::sync::mpsc::{channel, Receiver};
use windows::core::Interface;
use windows::Win32::Graphics::Direct3D11::{
    D3D11CreateDevice, D3D11_CPU_ACCESS_READ, D3D11_MAP_READ, D3D11_MAPPED_SUBRESOURCE,
    D3D11_SDK_VERSION, D3D11_TEXTURE2D_DESC, D3D11_USAGE_STAGING, ID3D11Device,
    ID3D11DeviceContext, ID3D11Texture2D,
};
use windows::Win32::Graphics::Dxgi::Common::{
    DXGI_FORMAT_B8G8R8A8_UNORM, DXGI_SAMPLE_DESC,
};
use windows::Win32::Graphics::Dxgi::{
    CreateDXGIFactory1, IDXGIAdapter1, IDXGIFactory1, IDXGIOutput1,
    IDXGIOutputDuplication, IDXGIResource, DXGI_ERROR_WAIT_TIMEOUT, DXGI_OUTDUPL_FRAME_INFO,
};
use windows::Win32::Graphics::Direct3D::{D3D_DRIVER_TYPE_HARDWARE, D3D_FEATURE_LEVEL_11_0};

/// A desktop frame straight off the GPU: tightly-packed BGRA + metadata.
pub struct CaptureFrame {
    pub bgra: Vec<u8>,
    pub width: u32,
    pub height: u32,
    /// Dirty rect count from the frame info — 0 = unchanged desktop.
    pub dirty_regions: u32,
}

/// Owns a duplication session for output `output_index` (0 = primary).
pub struct Duplicator {
    // Held for lifetime: the duplication interface dies with the device.
    _device: ID3D11Device,
    ctx: ID3D11DeviceContext,
    dup: IDXGIOutputDuplication,
    staging: ID3D11Texture2D,
    width: u32,
    height: u32,
}

impl Duplicator {
    pub fn new(output_index: u32) -> Result<Self, ClientError> {
        unsafe {
            let factory: IDXGIFactory1 = CreateDXGIFactory1()?;
            let mut adapter: Option<IDXGIAdapter1> = None;
            let mut output = None;
            for i in 0.. {
                match factory.EnumAdapters1(i) {
                    Ok(a) => {
                        if let Ok(o) = a.EnumOutputs(output_index) {
                            adapter = Some(a);
                            output = Some(o);
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let adapter = adapter.ok_or_else(|| {
                ClientError::Config("no DXGI adapter found".into())
            })?;
            let output = output.ok_or_else(|| {
                ClientError::Config(format!("no DXGI output #{output_index}"))
            })?;
            let desc = output.GetDesc()?;
            let (width, height) = (
                (desc.DesktopCoordinates.right - desc.DesktopCoordinates.left) as u32,
                (desc.DesktopCoordinates.bottom - desc.DesktopCoordinates.top) as u32,
            );

            let mut device = None;
            let mut ctx = None;
            D3D11CreateDevice(
                Some(&adapter.cast()?),
                D3D_DRIVER_TYPE_HARDWARE,
                windows::Win32::Foundation::HMODULE::default(),
                windows::Win32::Graphics::Direct3D11::D3D11_CREATE_DEVICE_FLAG(0),
                Some(&[D3D_FEATURE_LEVEL_11_0]),
                D3D11_SDK_VERSION,
                Some(&mut device),
                None,
                Some(&mut ctx),
            )?;
            let device: ID3D11Device = device.ok_or_else(|| {
                ClientError::Config("D3D11CreateDevice returned no device".into())
            })?;
            let ctx: ID3D11DeviceContext = ctx.ok_or_else(|| {
                ClientError::Config("D3D11CreateDevice returned no context".into())
            })?;

            let out1: IDXGIOutput1 = output.cast()?;
            let dup = out1.DuplicateOutput(&device)?;

            let staging_desc = D3D11_TEXTURE2D_DESC {
                Width: width,
                Height: height,
                MipLevels: 1,
                ArraySize: 1,
                Format: DXGI_FORMAT_B8G8R8A8_UNORM,
                SampleDesc: DXGI_SAMPLE_DESC { Count: 1, Quality: 0 },
                Usage: D3D11_USAGE_STAGING,
                BindFlags: 0,
                CPUAccessFlags: D3D11_CPU_ACCESS_READ.0 as u32,
                MiscFlags: 0,
            };
            let mut staging = None;
            device.CreateTexture2D(&staging_desc, None, Some(&mut staging))?;
            let staging = staging.ok_or_else(|| {
                ClientError::Config("staging texture allocation failed".into())
            })?;

            Ok(Self { _device: device, ctx, dup, staging, width, height })
        }
    }

    pub fn width(&self) -> u32 {
        self.width
    }
    pub fn height(&self) -> u32 {
        self.height
    }

    /// Wait up to `timeout_ms` for a new frame. `Ok(None)` = timeout (desktop
    /// unchanged). `Err` on access-lost/invalid-ownership → caller recreates.
    pub fn next_frame(&mut self, timeout_ms: u32) -> Result<Option<CaptureFrame>, ClientError> {
        unsafe {
            let mut info = DXGI_OUTDUPL_FRAME_INFO::default();
            let mut resource: Option<IDXGIResource> = None;
            match self
                .dup
                .AcquireNextFrame(timeout_ms, &mut info, &mut resource)
            {
                Ok(()) => {}
                Err(e) if e.code() == DXGI_ERROR_WAIT_TIMEOUT => return Ok(None),
                Err(e) => return Err(ClientError::Windows(e)),
            }
            let result = (|| -> Result<CaptureFrame, ClientError> {
                let tex: ID3D11Texture2D =
                    resource.ok_or_else(|| ClientError::Config("frame resource empty".into()))?.cast()?;
                self.ctx.CopyResource(&self.staging, &tex);
                let mut map = D3D11_MAPPED_SUBRESOURCE::default();
                self.ctx.Map(
                    &self.staging,
                    0,
                    D3D11_MAP_READ,
                    0,
                    Some(&mut map),
                )?;
                let mut bgra = vec![0u8; (self.width * self.height * 4) as usize];
                let row_bytes = (self.width * 4) as usize;
                for y in 0..self.height as usize {
                    let src = (map.pData as *const u8).add(y * map.RowPitch as usize);
                    let dst = bgra.as_mut_ptr().add(y * row_bytes);
                    std::ptr::copy_nonoverlapping(src, dst, row_bytes);
                }
                self.ctx.Unmap(&self.staging, 0);
                Ok(CaptureFrame {
                    bgra,
                    width: self.width,
                    height: self.height,
                    dirty_regions: info.TotalMetadataBufferSize,
                })
            })();
            let _ = self.dup.ReleaseFrame();
            result.map(Some)
        }
    }
}

/// Spawn the capture thread. Sends `CaptureFrame`s whenever the desktop
/// changes; reconnects duplication on AccessLost by recreating the session.
pub fn spawn_capture(output_index: u32) -> Receiver<Result<CaptureFrame, ClientError>> {
    let (tx, rx) = channel();
    std::thread::Builder::new()
        .name("precall-dxgi".into())
        .spawn(move || {
            'outer: loop {
                let mut dup = match Duplicator::new(output_index) {
                    Ok(d) => d,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                };
                loop {
                    match dup.next_frame(100) {
                        Ok(Some(f)) => {
                            if tx.send(Ok(f)).is_err() {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(_e) => {
                            // AccessLost / InvalidCall → recreate once; persistent
                            // failure propagates and kills the thread.
                            match Duplicator::new(output_index) {
                                Ok(d) => {
                                    dup = d;
                                    continue;
                                }
                                Err(e2) => {
                                    let _ = tx.send(Err(e2));
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }
        })
        .expect("spawn precall-dxgi");
    rx
}
