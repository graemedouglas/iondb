//! # sensor-log — Tier 1 Dogfood Application
//!
//! A bare-metal `no_std` firmware skeleton that logs timestamped sensor
//! readings to `IonDB` on a Cortex-M0 target.
//!
//! ## Phase 1 status
//!
//! Host-native binary demonstrating `StorageEngine` put/get and B+ tree
//! range queries by timestamp. Uses the page-based B+ tree engine.

use iondb_core::StorageEngine;
use iondb_storage::bptree::BTreeEngine;

fn main() {
    let mut storage_buf = [0u8; 8192];
    let Some(mut engine) = BTreeEngine::new(&mut storage_buf, 256) else {
        return;
    };

    // Simulate sensor readings: timestamp-keyed temperature values
    let readings: &[(&[u8], &[u8])] = &[
        (b"ts:0001", b"temp:23.5"),
        (b"ts:0002", b"temp:24.1"),
        (b"ts:0003", b"temp:22.8"),
        (b"ts:0004", b"temp:25.0"),
        (b"ts:0005", b"temp:23.2"),
        (b"ts:0006", b"temp:24.7"),
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

    // Range query: readings from ts:0002 to ts:0005
    let mut range_count = 0u32;
    let _ = engine.range(b"ts:0002", b"ts:0005", |_k, _v| {
        range_count += 1;
        true
    });
    assert_eq!(range_count, 3); // ts:0002, ts:0003, ts:0004

    let stats = engine.stats();
    assert_eq!(stats.key_count, 6);
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn sensor_log_round_trip() {
        let mut buf = [0u8; 8192];
        let mut engine = BTreeEngine::new(&mut buf, 256).unwrap();

        assert_eq!(engine.put(b"ts:0001", b"temp:23.5"), Ok(()));
        assert_eq!(engine.put(b"ts:0002", b"temp:24.1"), Ok(()));
        assert_eq!(engine.put(b"ts:0003", b"temp:22.8"), Ok(()));

        assert_eq!(engine.get(b"ts:0001"), Ok(Some(b"temp:23.5".as_slice())));
        assert_eq!(engine.get(b"ts:0003"), Ok(Some(b"temp:22.8".as_slice())));
        assert_eq!(engine.stats().key_count, 3);
    }

    #[test]
    fn sensor_log_overwrite() {
        let mut buf = [0u8; 8192];
        let mut engine = BTreeEngine::new(&mut buf, 256).unwrap();

        assert_eq!(engine.put(b"ts:0001", b"temp:23.5"), Ok(()));
        assert_eq!(engine.put(b"ts:0001", b"temp:24.0"), Ok(()));

        assert_eq!(engine.get(b"ts:0001"), Ok(Some(b"temp:24.0".as_slice())));
        assert_eq!(engine.stats().key_count, 1);
    }

    #[test]
    fn sensor_log_range_query() {
        let mut buf = [0u8; 8192];
        let mut engine = BTreeEngine::new(&mut buf, 256).unwrap();

        for i in 0u8..10 {
            let k = [b't', b's', b':', b'0', b'0', i / 10 + b'0', i % 10 + b'0'];
            let v = [b'v', i + b'0'];
            assert_eq!(engine.put(&k, &v), Ok(()));
        }

        // Range query: ts:0003 to ts:0007
        let mut results = Vec::new();
        engine
            .range(b"ts:0003", b"ts:0007", |k, v| {
                results.push((k.to_vec(), v.to_vec()));
                true
            })
            .unwrap();

        assert_eq!(results.len(), 4); // ts:0003, ts:0004, ts:0005, ts:0006
    }
}
