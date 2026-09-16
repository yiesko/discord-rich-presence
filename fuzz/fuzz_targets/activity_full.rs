#![no_main]

use libfuzzer_sys::fuzz_target;

// Full IPC pipeline: parse an activity command, apply fixes, encode both
// bridge protocols. Must never panic on any input: games and tools send
// whatever bytes they want over the socket.
fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(mut cmd) = serde_json::from_str::<rsrpc_types::cmd::ActivityCmd>(text) else {
        return;
    };
    cmd.fix();
    let _ = rsrpc_protocol::commands::cached_activity(&mut cmd);
});
