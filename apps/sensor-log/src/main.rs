//! # sensor-log — Tier 1 Dogfood Application
//!
//! A bare-metal `no_std` firmware skeleton that logs timestamped sensor
//! readings to `IonDB` on a Cortex-M0 target.
//!
//! ## Phase 0 status
//!
//! Host-native binary demonstrating `StorageEngine::put()` / `get()` with the
//! sorted-array B+ tree placeholder and in-memory I/O backend. Will be
//! converted to a `no_std` entry point when the QEMU runner is activated.

use iondb_core::StorageEngine;
use iondb_storage::bptree::BTreeEngine;

fn main() {
    let mut storage_buf = [0u8; 4096];
    let Some(mut engine) = BTreeEngine::new(&mut storage_buf) else {
        return;
    };

    // Simulate sensor readings: timestamp-keyed temperature values
    let readings: &[(&[u8], &[u8])] = &[
        (b"ts:0001", b"temp:23.5"),
        (b"ts:0002", b"temp:24.1"),
        (b"ts:0003", b"temp:22.8"),
        (b"ts:0004", b"temp:25.0"),
    ];

    for &(ts, val) in readings {
        if engine.put(ts, val).is_err() {
            return;
        }
    }

    // Verify retrieval
    if let Ok(Some(_val)) = engine.get(b"ts:0001") {
        // Successfully retrieved first reading
    }

    let stats = engine.stats();
    assert_eq!(stats.key_count, 4);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn sensor_log_round_trip() {
        let mut buf = [0u8; 4096];
        // unwrap OK: test-only, buffer is large enough
        let mut engine = BTreeEngine::new(&mut buf).unwrap();

        assert_eq!(engine.put(b"ts:0001", b"temp:23.5"), Ok(()));
        assert_eq!(engine.put(b"ts:0002", b"temp:24.1"), Ok(()));
        assert_eq!(engine.put(b"ts:0003", b"temp:22.8"), Ok(()));

        assert_eq!(engine.get(b"ts:0001"), Ok(Some(b"temp:23.5".as_slice())));
        assert_eq!(engine.get(b"ts:0003"), Ok(Some(b"temp:22.8".as_slice())));

        assert_eq!(engine.stats().key_count, 3);
    }

    #[test]
    fn sensor_log_overwrite() {
        let mut buf = [0u8; 4096];
        let mut engine = BTreeEngine::new(&mut buf).unwrap();

        assert_eq!(engine.put(b"ts:0001", b"temp:23.5"), Ok(()));
        assert_eq!(engine.put(b"ts:0001", b"temp:24.0"), Ok(()));

        assert_eq!(engine.get(b"ts:0001"), Ok(Some(b"temp:24.0".as_slice())));
        assert_eq!(engine.stats().key_count, 1);
    }
}
