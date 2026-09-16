#![no_main]

use libfuzzer_sys::fuzz_target;

// Database trimming: arbitrary bodies (full, truncated, garbage) must
// never panic. Parse errors are fine (Result); panics are not.
fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = rsrpc_detect::db::trim_detectable(text);
});
