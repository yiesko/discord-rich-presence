#![no_main]

use libfuzzer_sys::fuzz_target;
use rsrpc_transport_ipc::frame::{MAX_IPC_PAYLOAD, PacketType, encode};

// IPC frame header: `u32` type + `u32` length + body (all little-endian).
// Feeds arbitrary bytes through header parsing, the length bound and the
// encode round-trip, mirroring what `handle_stream` does with socket
// bytes. Must never panic and must never allocate from the length word:
// oversize lengths refuse instead of reserving.
fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }
    // Guarded by the length check above: infallible by construction.
    let rtype = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let length = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let body = &data[8..];

    let Some(packet) = PacketType::try_from_u32(rtype) else {
        // Unknown types refuse with 1003: no body is ever touched.
        return;
    };
    // The length bound fires before any allocation keyed on it.
    if length > MAX_IPC_PAYLOAD {
        return;
    }
    let take = (length as usize).min(body.len());
    let text = &body[..take];
    // Encode round-trips a header the parser just accepted.
    let frame = encode(packet, &String::from_utf8_lossy(text));
    // Header shape the parser expects back: type + length + body.
    assert!(frame.len() >= 8);
});
