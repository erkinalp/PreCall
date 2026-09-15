// SPDX-License-Identifier: GPL-2.0-only
//! WASAPI loopback capture → interleaved PCM16. Runs on a dedicated thread;
//! chunks (1s) flow out over std::mpsc. The wire header declares codec
//! `pcm16` (Opus encoding is a future codec upgrade — the channel layout
//! already carries an explicit codec tag).

use crate::ClientError;
use std::sync::mpsc::{channel, Receiver};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use windows::Win32::Media::Audio::{
    eConsole, eRender, IAudioCaptureClient, IAudioClient, IMMDeviceEnumerator,
    MMDeviceEnumerator, AUDCLNT_SHAREMODE_SHARED, AUDCLNT_STREAMFLAGS_LOOPBACK,
    WAVEFORMATEX, WAVEFORMATEXTENSIBLE,
};
use windows::Win32::System::Com::{CoCreateInstance, CoInitializeEx, CLSCTX_ALL, COINIT_MULTITHREADED};

/// A captured audio chunk.
pub struct AudioChunk {
    pub pcm16: Vec<i16>,
    pub channels: u16,
    pub sample_rate: u32,
}

struct Loopback {
    client: IAudioClient,
    capture: IAudioCaptureClient,
    block_align: u16,
    channels: u16,
    sample_rate: u32,
    bits: u16,
    float: bool,
}

impl Loopback {
    unsafe fn new() -> Result<Self, ClientError> {
        unsafe {
            CoInitializeEx(None, COINIT_MULTITHREADED).ok()?;
            let enumerator: IMMDeviceEnumerator =
                CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL)?;
            let device = enumerator.GetDefaultAudioEndpoint(eRender, eConsole)?;
            let client: IAudioClient = device.Activate(
                CLSCTX_ALL,
                None,
            )?;
            let fmt_ptr = client.GetMixFormat()?;
            let fmt = &*(fmt_ptr as *const WAVEFORMATEX);
            let channels = fmt.nChannels;
            let sample_rate = fmt.nSamplesPerSec;
            let bits = fmt.wBitsPerSample;
            // wFormatTag: 1=PCM, 3=FLOAT, 0xFFFE=EXTENSIBLE (sub-format inside)
            let float = if fmt.wFormatTag == 3 {
                true
            } else if fmt.wFormatTag == 0xFFFE {
                let ext = &*(fmt_ptr as *const WAVEFORMATEXTENSIBLE);
                // KSDATAFORMAT_SUBTYPE_IEEE_FLOAT
                // {00000003-0000-0010-8000-00aa00389b71}
                let sub: windows::core::GUID = ext.SubFormat;
                sub == windows::core::GUID::from_u128(0x00000003_0000_0010_8000_00aa00389b71)
            } else {
                false
            };
            client.Initialize(
                AUDCLNT_SHAREMODE_SHARED,
                AUDCLNT_STREAMFLAGS_LOOPBACK,
                0,
                0,
                fmt_ptr,
                None,
            )?;
            let capture: IAudioCaptureClient = client.GetService()?;
            client.Start()?;
            windows::Win32::System::Com::CoTaskMemFree(Some(fmt_ptr as *const _));
            Ok(Self {
                client,
                capture,
                block_align: fmt.nBlockAlign,
                channels,
                sample_rate,
                bits,
                float,
            })
        }
    }

    unsafe fn pump(&mut self) -> Result<Vec<i16>, ClientError> {
        unsafe {
            let mut out = Vec::new();
            loop {
                let frames = self.capture.GetNextPacketSize()?;
                if frames == 0 {
                    break;
                }
                let mut data = std::ptr::null_mut();
                let mut nframes = 0u32;
                let mut flags = 0u32;
                let mut pos = 0u64;
                let mut qpc = 0u64;
                self.capture
                    .GetBuffer(&mut data, &mut nframes, &mut flags, Some(&mut pos), Some(&mut qpc))?;
                let byte_len = nframes as usize * self.block_align as usize;
                if flags == 0 && !data.is_null() && byte_len > 0 {
                    let src = std::slice::from_raw_parts(data as *const u8, byte_len);
                    if self.float && self.bits == 32 {
                        for s in src.chunks_exact(4) {
                            let f = f32::from_le_bytes(s.try_into().unwrap());
                            out.push((f.clamp(-1.0, 1.0) * 32767.0) as i16);
                        }
                    } else if self.bits == 16 {
                        for s in src.chunks_exact(2) {
                            out.push(i16::from_le_bytes(s.try_into().unwrap()));
                        }
                    } else if self.bits == 32 && !self.float {
                        for s in src.chunks_exact(4) {
                            let v = i32::from_le_bytes(s.try_into().unwrap());
                            out.push((v >> 16) as i16);
                        }
                    }
                }
                self.capture.ReleaseBuffer(nframes)?;
            }
            Ok(out)
        }
    }
}

/// Spawn the loopback thread. `stop` is checked between packets.
pub fn spawn_loopback(stop: Arc<AtomicBool>) -> Receiver<Result<AudioChunk, ClientError>> {
    let (tx, rx) = channel();
    std::thread::Builder::new()
        .name("precall-wasapi".into())
        .spawn(move || unsafe {
            let mut lb = match Loopback::new() {
                Ok(l) => l,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(500));
                match lb.pump() {
                    Ok(pcm) if !pcm.is_empty() => {
                        if tx
                            .send(Ok(AudioChunk {
                                pcm16: pcm,
                                channels: lb.channels,
                                sample_rate: lb.sample_rate,
                            }))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                }
            }
            let _ = lb.client.Stop();
        })
        .expect("spawn precall-wasapi");
    rx
}
