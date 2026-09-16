// SPDX-License-Identifier: GPL-2.0-only
use bytes::BytesMut;
use precall_proto::*;
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn gfx_start_end_round_trip() {
    let pdus = [
        GfxPdu::StartFrame { frame_id: 7, timestamp: 0xDEAD_BEEF },
        GfxPdu::WireToSurface1 {
            surface_id: 0,
            codec_id: RDPGFX_CODECID_JPEG,
            pixel_format: RDPGFX_PIXEL_FORMAT_XRGB_8888,
            dest_rect: Rect::new(0, 0, 1920, 1080),
            bitmap_data: bytes::Bytes::from_static(&[0xFF, 0xD8, 0xFF, 0xE0, 1, 2, 3]),
        },
        GfxPdu::EndFrame { frame_id: 7 },
        GfxPdu::FrameAcknowledge { queue_depth: 0, frame_id: 7, total_frames_decoded: 6 },
    ];
    let mut buf = BytesMut::new();
    for p in &pdus {
        p.encode(&mut buf);
    }
    let decoded: Vec<GfxPdu> = GfxPduIter::new(&buf)
        .collect::<Result<_, _>>()
        .expect("decode");
    assert_eq!(decoded.as_slice(), &pdus[..]);
}

#[test]
fn gfx_wire_to_surface_layout_matches_spec() {
    // Byte-for-byte check of RDPGFX_WIRE_TO_SURFACE_PDU_1 layout:
    // cmdId(2) flags(2) pduLength(4) surfaceId(2) codecId(2) pixelFormat(1)
    // destRect(8) bitmapDataLength(4) bitmapData
    let pdu = GfxPdu::WireToSurface1 {
        surface_id: 2,
        codec_id: RDPGFX_CODECID_AVC420,
        pixel_format: RDPGFX_PIXEL_FORMAT_XRGB_8888,
        dest_rect: Rect::new(10, 20, 110, 220),
        bitmap_data: bytes::Bytes::from_static(&[0xAA, 0xBB]),
    };
    let mut buf = BytesMut::new();
    pdu.encode(&mut buf);
    let b = &buf[..];
    assert_eq!(&b[0..2], &[0x01, 0x00]); // cmdId WIRETOSURFACE_1
    assert_eq!(u32::from_le_bytes(b[4..8].try_into().unwrap()), b.len() as u32);
    assert_eq!(&b[8..10], &[0x02, 0x00]); // surfaceId
    assert_eq!(&b[10..12], &[0x0E, 0x00]); // codecId AVC420
    assert_eq!(b[12], 0x20); // XRGB_8888
    assert_eq!(&b[13..21], &[10, 0, 20, 0, 110, 0, 220, 0][..]); // rect
    assert_eq!(u32::from_le_bytes(b[21..25].try_into().unwrap()), 2);
    assert_eq!(&b[25..27], &[0xAA, 0xBB]);
}

#[test]
fn mux_codec_round_trip() {
    let frames = [
        MuxFrame::new(ChannelId::GfxRdp, vec![1, 2, 3]),
        MuxFrame::new(ChannelId::PcMeta, br#"{"a":1}"#.to_vec()),
        MuxFrame::new(ChannelId::PcCtrl, Vec::<u8>::new()),
    ];
    let mut codec = MuxCodec;
    let mut buf = BytesMut::new();
    for f in &frames {
        codec.encode(f.clone(), &mut buf).unwrap();
    }
    let mut out = Vec::new();
    while let Some(f) = codec.decode(&mut buf).unwrap() {
        out.push(f);
    }
    assert_eq!(out.as_slice(), &frames[..]);
}

#[test]
fn mux_codec_partial_reads() {
    let frame = MuxFrame::new(ChannelId::PcAudio, vec![9u8; 100]);
    let mut codec = MuxCodec;
    let mut buf = BytesMut::new();
    codec.encode(frame.clone(), &mut buf).unwrap();
    // Feed one byte at a time — must reassemble exactly.
    let mut stream = BytesMut::new();
    let mut decoded = None;
    for byte in buf.iter() {
        stream.extend_from_slice(&[*byte]);
        if let Some(f) = codec.decode(&mut stream).unwrap() {
            decoded = Some(f);
        }
    }
    assert_eq!(decoded, Some(frame));
}

#[test]
fn channel_name_mapping() {
    for c in ChannelId::all() {
        assert_eq!(ChannelId::from_name(c.name()), Some(*c));
        assert_eq!(ChannelId::from_u8((*c).into()), Some(*c));
    }
    assert_eq!(ChannelId::from_name("pcmeta"), Some(ChannelId::PcMeta));
}

#[tokio::test]
async fn handshake_json_round_trip() {
    let hello = ClientHello {
        protocol_version: PROTOCOL_VERSION,
        client_id: uuid::Uuid::new_v4(),
        hostname: "DESKTOP-TEST".into(),
        auth: AuthMethod::Psk { token: "s3cret".into() },
        capabilities: Capabilities {
            displays: vec![DisplayInfo { surface_id: 0, width: 2560, height: 1440, origin_x: 0, origin_y: 0 }],
            channels: ChannelId::all().to_vec(),
            codec_ids: vec![RDPGFX_CODECID_JPEG, RDPGFX_CODECID_AVC420],
            capture_interval_ms: 5000,
            client_build: "precall-mock/0.1.0".into(),
        },
    };
    let (mut tx, mut rx) = tokio::io::duplex(4096);
    write_limited_json(&mut tx, &hello).await.unwrap();
    let back: ClientHello = read_limited_json(&mut rx).await.unwrap();
    assert_eq!(back.protocol_version, PROTOCOL_VERSION);
    assert_eq!(back.client_id, hello.client_id);
    assert_eq!(back.capabilities.displays[0].width, 2560);
}
