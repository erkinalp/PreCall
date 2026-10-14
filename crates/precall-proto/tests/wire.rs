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

/// The client's capture framing must be produced by the codec that parses it.
///
/// This is a regression test with teeth: the client hand-rolled these bytes and
/// emitted a START_FRAME whose flags/length/timestamp fields were mis-sized
/// (`pdu_len` decoded as 0x000C0000) and 4-byte rectangle coordinates where the
/// PDU has `i16`. Every end-to-end test drives `precall-mock`, which uses the
/// codec, so the malformed client framing was invisible until it was read.
#[test]
fn capture_framing_round_trips_through_the_codec() {
    use precall_proto::{encode_capture_frame, GfxPduIter, Rect};

    let regions = vec![
        (Rect::new(0, 0, 1280, 720), bytes::Bytes::from_static(b"\xff\xd8full\xff\xd9")),
    ];
    let framed = encode_capture_frame(7, 0xDEAD_BEEF, 1, &regions);
    let pdus: Vec<_> = GfxPduIter::new(&framed)
        .collect::<Result<Vec<_>, _>>()
        .expect("the client framing must decode");
    assert_eq!(pdus.len(), 3, "START + one region + END");
    match &pdus[0] {
        precall_proto::GfxPdu::StartFrame { frame_id, timestamp } => {
            assert_eq!(*frame_id, 7);
            assert_eq!(*timestamp, 0xDEAD_BEEF);
        }
        other => panic!("expected START, got {other:?}"),
    }
    match &pdus[1] {
        precall_proto::GfxPdu::WireToSurface1 { dest_rect, bitmap_data, codec_id, .. } => {
            assert_eq!(*dest_rect, Rect::new(0, 0, 1280, 720));
            assert_eq!(*codec_id, precall_proto::RDPGFX_CODECID_JPEG);
            assert_eq!(&bitmap_data[..2], &[0xFF, 0xD8]);
        }
        other => panic!("expected a region, got {other:?}"),
    }
    assert!(matches!(pdus[2], precall_proto::GfxPdu::EndFrame { frame_id: 7 }));

    // Several rectangles in one frame: one PDU each, exact i16 coordinates.
    let many = vec![
        (Rect::new(16, 16, 48, 40), bytes::Bytes::from_static(b"a")),
        (Rect::new(-4, 8, 12, 30), bytes::Bytes::from_static(b"bb")),
        (Rect::new(100, 100, 101, 101), bytes::Bytes::from_static(b"ccc")),
    ];
    let framed = encode_capture_frame(9, 1, 1, &many);
    let pdus: Vec<_> = GfxPduIter::new(&framed).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(pdus.len(), 5, "START + 3 regions + END");
    for (i, (rect, data)) in many.iter().enumerate() {
        match &pdus[i + 1] {
            precall_proto::GfxPdu::WireToSurface1 { dest_rect, bitmap_data, .. } => {
                assert_eq!(dest_rect, rect, "rectangle {i} (negative origins included)");
                assert_eq!(bitmap_data.as_ref(), data.as_ref());
            }
            other => panic!("expected region {i}, got {other:?}"),
        }
    }
}
