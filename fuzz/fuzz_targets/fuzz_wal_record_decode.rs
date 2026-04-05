#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Should never panic regardless of input
    let _ = iondb_wal::record::deserialize_from(data);
    let _ = iondb_wal::record::read_header(data);
});
