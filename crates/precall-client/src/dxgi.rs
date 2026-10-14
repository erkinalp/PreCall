// SPDX-License-Identifier: GPL-2.0-only
//! DXGI Desktop Duplication — the same capture mechanism Terminal Services
//! uses for session remoting. Runs on ONE dedicated thread (D3D11 objects
//! are !Send); frames go out over a std::mpsc channel.

use crate::ClientError;
use precall_frame::ChangeSlot;
use std::sync::Arc;
use windows::core::Interface;
use windows::Win32::Graphics::Direct3D11::{
    D3D11CreateDevice, D3D11_CPU_ACCESS_READ, D3D11_MAP_READ, D3D11_MAPPED_SUBRESOURCE,
    D3D11_SDK_VERSION, D3D11_TEXTURE2D_DESC, D3D11_USAGE_STAGING, ID3D11Device,
    ID3D11DeviceContext, ID3D11Texture2D,
};
use windows::Win32::Graphics::Dxgi::Common::{
    DXGI_FORMAT_B8G8R8A8_UNORM, DXGI_SAMPLE_DESC,
};
use windows::Win32::Foundation::RECT;
use windows::Win32::Graphics::Dxgi::{
    CreateDXGIFactory1, IDXGIAdapter1, IDXGIFactory1, IDXGIOutput, IDXGIOutput1,
    IDXGIOutputDuplication, IDXGIResource, DXGI_ERROR_WAIT_TIMEOUT, DXGI_OUTDUPL_FRAME_INFO,
    DXGI_OUTDUPL_MOVE_RECT,
};
use windows::Win32::Graphics::Direct3D::{D3D_DRIVER_TYPE_HARDWARE, D3D_FEATURE_LEVEL_11_0};

/// The change metadata of a captured frame, as [`ChangeSlot`] needs it.
///
/// Taking it out of the frame means a pending set can never be double-counted,
/// and an unreadable read (`None`) propagates as "send the whole frame".
impl precall_frame::ChangeMeta for CaptureFrame {
    fn take_changes(&mut self) -> (Option<Vec<precall_frame::Rect>>, bool) {
        (self.dirty.take(), std::mem::replace(&mut self.moved, false))
    }
}

/// `sizeof(RECT)` on the wire — the metadata buffers are arrays of these.
const RECT_SIZE: usize = std::mem::size_of::<RECT>();

/// A desktop frame straight off the GPU: tightly-packed BGRA + metadata.
pub struct CaptureFrame {
    pub bgra: Vec<u8>,
    pub width: u32,
    pub height: u32,
    /// Rectangles DXGI says changed for this frame, in desktop coordinates.
    ///
    /// `None` means the metadata could not be read; the caller must treat that
    /// as "send the whole frame", never as "nothing changed" — silently
    /// treating it as unchanged would stop capture.
    pub dirty: Option<Vec<precall_frame::Rect>>,
    /// DXGI reported *move* rectangles: the desktop scrolled. The wire has no
    /// surface-copy operation, so the caller sends a keyframe instead.
    pub moved: bool,
    /// Size of the frame's metadata buffer, for diagnostics.
    pub metadata_bytes: u32,
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

/// The adapter and output the capture path would pick.
///
/// Adapter 0 is *not* assumed to have an output — a machine whose primary
/// adapter drives no display is ordinary — so this walks adapters exactly as
/// capture does. Anything that needs the desktop size must use this too, or it
/// can disagree with the duplicator we actually capture from.
unsafe fn select_output(
    factory: &IDXGIFactory1,
    output_index: u32,
) -> Option<(IDXGIAdapter1, IDXGIOutput)> {
    for i in 0.. {
        match factory.EnumAdapters1(i) {
            Ok(a) => {
                if let Ok(o) = a.EnumOutputs(output_index) {
                    return Some((a, o));
                }
            }
            Err(_) => break,
        }
    }
    None
}

impl Duplicator {
    pub fn new(output_index: u32) -> Result<Self, ClientError> {
        unsafe {
            let factory: IDXGIFactory1 = CreateDXGIFactory1()?;
            let (adapter, output) = select_output(&factory, output_index).ok_or_else(|| {
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

    /// Read the frame's dirty and move rectangles.
    ///
    /// Returns `(None, _)` when the metadata cannot be read, which the caller
    /// must not confuse with "the desktop did not change".
    #[allow(clippy::borrowed_box)]
    fn read_metadata(
        dup: &IDXGIOutputDuplication,
        info: &DXGI_OUTDUPL_FRAME_INFO,
    ) -> (Option<Vec<precall_frame::Rect>>, bool) {
        let capacity = info.TotalMetadataBufferSize.max(RECT_SIZE as u32 * 4) as usize;
        unsafe {
            // Dirty rectangles: the changed pixels, which are what we send.
            let mut dirty_buf = vec![0u8; capacity];
            let mut dirty_bytes = 0u32;
            let dirty_ok = dup
                .GetFrameDirtyRects(
                    dirty_buf.len() as u32,
                    dirty_buf.as_mut_ptr() as *mut RECT,
                    &mut dirty_bytes,
                )
                .is_ok();
            if !dirty_ok {
                return (None, false);
            }
            let count = (dirty_bytes as usize) / RECT_SIZE;
            let mut rects = Vec::with_capacity(count);
            for r in std::slice::from_raw_parts(dirty_buf.as_ptr() as *const RECT, count) {
                rects.push(precall_frame::Rect::new(
                    r.left as i16,
                    r.top as i16,
                    r.right as i16,
                    r.bottom as i16,
                ));
            }

            // Move rectangles: content copied within the surface. The protocol
            // has no copy operation, so any move makes this a keyframe.
            let mut move_buf = vec![0u8; capacity];
            let mut move_bytes = 0u32;
            let moved = match dup.GetFrameMoveRects(
                move_buf.len() as u32,
                move_buf.as_mut_ptr() as *mut DXGI_OUTDUPL_MOVE_RECT,
                &mut move_bytes,
            ) {
                Ok(()) => move_bytes > 0,
                // Unreadable move metadata is treated as "there was a move":
                // a spurious keyframe costs bandwidth, a missed one costs
                // correctness.
                Err(_) => true,
            };
            (Some(rects), moved)
        }
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
                let (dirty, moved) = Self::read_metadata(&self.dup, &info);
                Ok(CaptureFrame {
                    bgra,
                    width: self.width,
                    height: self.height,
                    dirty,
                    moved,
                    metadata_bytes: info.TotalMetadataBufferSize,
                })
            })();
            let _ = self.dup.ReleaseFrame();
            result.map(Some)
        }
    }
}

/// Size of the output the capture path will use, for the hello.
///
/// The broker needs it to tell a full-frame keyframe from a single-rectangle
/// delta; without it a small edit looks like a keyframe and would be archived as
/// the capture. It walks adapters rather than assuming adapter 0 has the display.
pub fn primary_output_size() -> Result<(u32, u32), ClientError> {
    unsafe {
        let factory: IDXGIFactory1 = CreateDXGIFactory1()?;
        let (_, output) = select_output(&factory, 0)
            .ok_or_else(|| ClientError::Config("no DXGI output".into()))?;
        let desc = output.GetDesc()?;
        let w = (desc.DesktopCoordinates.right as i64 - desc.DesktopCoordinates.left as i64).max(0);
        let h = (desc.DesktopCoordinates.bottom as i64 - desc.DesktopCoordinates.top as i64).max(0);
        Ok((w as u32, h as u32))
    }
}

/// What the capture thread sees right now: the desktop size, and a generation
/// counter that advances whenever the duplicator is created or re-created.
///
/// A display mode change destroys and re-creates the duplicator; the broker's
/// surface geometry came from the hello, so it must be told (a reconnect) or its
/// canvas keeps the old size and misclassifies frames. The generation also tells
/// the send loop that the desktop restarted, which is worth a keyframe.
#[derive(Clone, Copy, Debug, Default)]
pub struct CaptureGeometry {
    pub width: u32,
    pub height: u32,
    pub generation: u64,
}

pub type SharedGeometry = Arc<std::sync::Mutex<CaptureGeometry>>;

/// Spawn the capture thread.
///
/// Frames go into a [`ChangeSlot`], **not** a queue: at 1920x1080x60 the display
/// produces ~480 MB/s of BGRA, so an unbounded handoff grows without limit and
/// the stream shows a desktop from minutes ago. The slot holds one frame and
/// overwrites it, which is how a real-time agent drops frames; the send loop
/// reads [`ChangeSlot::dropped`] into the heartbeat so link pressure is visible.
///
/// `on_error` is called once if duplication cannot be (re)established.
pub fn spawn_capture(
    output_index: u32,
    on_error: impl Fn(ClientError) + Send + 'static,
) -> (Arc<ChangeSlot<CaptureFrame>>, SharedGeometry) {
    let slot = Arc::new(ChangeSlot::new());
    let producer = Arc::clone(&slot);
    let geometry: SharedGeometry = Arc::new(std::sync::Mutex::new(CaptureGeometry::default()));
    let geom = Arc::clone(&geometry);
    std::thread::Builder::new()
        .name("precall-dxgi".into())
        .spawn(move || {
            let publish_geometry = |dup: &Duplicator| {
                let mut g = geom.lock().unwrap();
                g.width = dup.width;
                g.height = dup.height;
                g.generation += 1;
            };
            let mut dup = match Duplicator::new(output_index) {
                Ok(d) => d,
                Err(e) => {
                    on_error(e);
                    producer.close();
                    return;
                }
            };
            publish_geometry(&dup);
            loop {
                match dup.next_frame(100) {
                    Ok(Some(f)) => producer.publish(f),
                    Ok(None) => {}
                    Err(_e) => {
                        // AccessLost / InvalidCall → recreate once; persistent
                        // failure propagates and kills the thread.
                        match Duplicator::new(output_index) {
                            Ok(d) => {
                                dup = d;
                                // New duplicator: the desktop may have moved to a
                                // different size (mode change) and its contents
                                // are not comparable with the previous surface.
                                publish_geometry(&dup);
                            }
                            Err(e2) => {
                                on_error(e2);
                                // Close before returning: otherwise the send
                                // loop blocks forever on a slot nobody fills,
                                // still heartbeating, capturing nothing.
                                producer.close();
                                return;
                            }
                        }
                    }
                }
            }
        })
        .expect("spawn precall-dxgi");
    (slot, geometry)
}
