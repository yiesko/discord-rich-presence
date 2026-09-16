#![no_main]

use libfuzzer_sys::fuzz_target;

// Database struct parse (what the refresh path does first): any input
// must parse or fail cleanly, never panic.
fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ =
        serde_json::from_str::<Vec<rsrpc_detect::db::DetectableActivity>>(text);
});
