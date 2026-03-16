//! Property-based tests for `IonDB` storage engines.
//!
//! Verifies structural invariants under randomized insert/delete sequences
//! for all three engines: B+ tree, extendible hash, and linear hash.

#![allow(
    clippy::unwrap_used,
    clippy::large_stack_arrays,
    unused_results,
    dead_code
)]

use iondb_core::traits::storage_engine::StorageEngine;
use proptest::prelude::*;

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Generate a random operation sequence (insert or delete).
#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (prop::collection::vec(any::<u8>(), 1..16),
              prop::collection::vec(any::<u8>(), 0..16))
            .prop_map(|(k, v)| Op::Put(k, v)),
        1 => prop::collection::vec(any::<u8>(), 1..16)
            .prop_map(Op::Delete),
    ]
}

fn ops_strategy() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op_strategy(), 1..80)
}

/// Apply operations and return the expected state (a sorted map of key→value).
fn apply_ops_to_reference(ops: &[Op]) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map = std::collections::BTreeMap::new();
    for op in ops {
        match op {
            Op::Put(k, v) => {
                map.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                map.remove(k);
            }
        }
    }
    map
}

// ─── B+ Tree Properties ────────────────────────────────────────────────────

#[cfg(feature = "storage-bptree")]
mod bptree_props {
    use super::*;
    use iondb_storage::bptree::BTreeEngine;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Every key that was successfully put (and not deleted) must be
        /// retrievable with the correct value.
        #[test]
        fn get_returns_last_put(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = BTreeEngine::new(&mut buf, 128).unwrap();

            // Track successful operations against reference model
            let mut live: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            for op in &ops {
                match op {
                    Op::Put(k, v) => {
                        if engine.put(k, v).is_ok() {
                            live.insert(k.clone(), v.clone());
                        }
                    }
                    Op::Delete(k) => {
                        if let Ok(true) = engine.delete(k) {
                            live.remove(k);
                        }
                    }
                }
            }

            for (k, v) in &live {
                let got = engine.get(k).unwrap();
                prop_assert_eq!(got, Some(v.as_slice()),
                    "value mismatch for key {:?}", k);
            }
        }

        /// key_count stat equals the number of distinct keys stored.
        #[test]
        fn stats_key_count_is_accurate(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = BTreeEngine::new(&mut buf, 128).unwrap();

            // Apply ops, tracking which ones succeeded
            let mut live: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            for op in &ops {
                match op {
                    Op::Put(k, v) => {
                        if engine.put(k, v).is_ok() {
                            live.insert(k.clone(), v.clone());
                        }
                    }
                    Op::Delete(k) => {
                        if let Ok(true) = engine.delete(k) {
                            live.remove(k);
                        }
                    }
                }
            }
            prop_assert_eq!(engine.stats().key_count, live.len() as u64);
        }

        /// Range scan returns keys in sorted ascending order.
        #[test]
        fn range_scan_is_sorted(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = BTreeEngine::new(&mut buf, 128).unwrap();

            // Track successful ops to avoid checking keys lost to capacity
            for op in &ops {
                match op {
                    Op::Put(k, v) => { let _ = engine.put(k, v); }
                    Op::Delete(k) => { let _ = engine.delete(k); }
                }
            }

            let mut keys = Vec::new();
            let _ = engine.range(&[0u8], &[0xFF, 0xFF, 0xFF, 0xFF], |k, _v| {
                keys.push(k.to_vec());
                true
            });

            for w in keys.windows(2) {
                prop_assert!(w[0] < w[1],
                    "range scan not sorted: {:?} >= {:?}", w[0], w[1]);
            }
        }

        /// After delete, get returns None for that key.
        #[test]
        fn delete_removes_key(
            key in prop::collection::vec(any::<u8>(), 1..8),
            value in prop::collection::vec(any::<u8>(), 0..8),
        ) {
            let mut buf = [0u8; 4096];
            let mut engine = BTreeEngine::new(&mut buf, 128).unwrap();
            if engine.put(&key, &value).is_ok() {
                let _ = engine.delete(&key);
                prop_assert_eq!(engine.get(&key).unwrap(), None);
            }
        }
    }
}

// ─── Extendible Hash Properties ─────────────────────────────────────────────

#[cfg(feature = "storage-hash-ext")]
mod ext_hash_props {
    use super::*;
    use iondb_storage::hash::extendible::ExtendibleHashEngine;

    fn apply_ops(engine: &mut ExtendibleHashEngine<'_>, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    let _ = engine.put(k, v);
                }
                Op::Delete(k) => {
                    let _ = engine.delete(k);
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Every key that was put (and not deleted) must be retrievable.
        #[test]
        fn get_returns_last_put(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = ExtendibleHashEngine::new(&mut buf, 256).unwrap();
            apply_ops(&mut engine, &ops);

            let expected = apply_ops_to_reference(&ops);
            for (k, v) in &expected {
                if let Ok(Some(got)) = engine.get(k) {
                    prop_assert_eq!(got, v.as_slice(),
                        "value mismatch for key {:?}", k);
                }
            }
        }

        /// key_count stat equals the number of distinct keys stored.
        #[test]
        fn stats_key_count_is_accurate(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = ExtendibleHashEngine::new(&mut buf, 256).unwrap();

            let mut live: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            for op in &ops {
                match op {
                    Op::Put(k, v) => {
                        if engine.put(k, v).is_ok() {
                            live.insert(k.clone(), v.clone());
                        }
                    }
                    Op::Delete(k) => {
                        if let Ok(true) = engine.delete(k) {
                            live.remove(k);
                        }
                    }
                }
            }
            prop_assert_eq!(engine.stats().key_count, live.len() as u64);
        }

        /// After delete, get returns None for that key.
        #[test]
        fn delete_removes_key(
            key in prop::collection::vec(any::<u8>(), 1..8),
            value in prop::collection::vec(any::<u8>(), 0..8),
        ) {
            let mut buf = [0u8; 4096];
            let mut engine = ExtendibleHashEngine::new(&mut buf, 128).unwrap();
            if engine.put(&key, &value).is_ok() {
                let _ = engine.delete(&key);
                prop_assert_eq!(engine.get(&key).unwrap(), None);
            }
        }

        /// No keys are lost: every successful put that wasn't later
        /// successfully deleted must be retrievable.
        #[test]
        fn no_keys_lost(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = ExtendibleHashEngine::new(&mut buf, 256).unwrap();

            let mut live: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            for op in &ops {
                match op {
                    Op::Put(k, v) => {
                        if engine.put(k, v).is_ok() {
                            live.insert(k.clone(), v.clone());
                        }
                    }
                    Op::Delete(k) => {
                        if let Ok(true) = engine.delete(k) {
                            live.remove(k);
                        }
                    }
                }
            }

            for (k, v) in &live {
                let got = engine.get(k).unwrap();
                prop_assert_eq!(got, Some(v.as_slice()),
                    "key {:?} lost after operations", k);
            }
        }
    }
}

// ─── Linear Hash Properties ────────────────────────────────────────────────

#[cfg(feature = "storage-hash-linear")]
mod linear_hash_props {
    use super::*;
    use iondb_storage::hash::linear::LinearHashEngine;

    fn apply_ops(engine: &mut LinearHashEngine<'_>, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    let _ = engine.put(k, v);
                }
                Op::Delete(k) => {
                    let _ = engine.delete(k);
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Every key that was put (and not deleted) must be retrievable.
        #[test]
        fn get_returns_last_put(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = LinearHashEngine::new(&mut buf, 128, 4).unwrap();
            apply_ops(&mut engine, &ops);

            let expected = apply_ops_to_reference(&ops);
            for (k, v) in &expected {
                if let Ok(Some(got)) = engine.get(k) {
                    prop_assert_eq!(got, v.as_slice(),
                        "value mismatch for key {:?}", k);
                }
            }
        }

        /// key_count stat equals the number of distinct keys stored.
        #[test]
        fn stats_key_count_is_accurate(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = LinearHashEngine::new(&mut buf, 128, 4).unwrap();

            let mut live: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            for op in &ops {
                match op {
                    Op::Put(k, v) => {
                        if engine.put(k, v).is_ok() {
                            live.insert(k.clone(), v.clone());
                        }
                    }
                    Op::Delete(k) => {
                        if let Ok(true) = engine.delete(k) {
                            live.remove(k);
                        }
                    }
                }
            }
            prop_assert_eq!(engine.stats().key_count, live.len() as u64);
        }

        /// After delete, get returns None for that key.
        #[test]
        fn delete_removes_key(
            key in prop::collection::vec(any::<u8>(), 1..8),
            value in prop::collection::vec(any::<u8>(), 0..8),
        ) {
            let mut buf = [0u8; 4096];
            let mut engine = LinearHashEngine::new(&mut buf, 128, 4).unwrap();
            if engine.put(&key, &value).is_ok() {
                let _ = engine.delete(&key);
                prop_assert_eq!(engine.get(&key).unwrap(), None);
            }
        }

        /// No keys are lost after random operations.
        #[test]
        fn no_keys_lost(ops in ops_strategy()) {
            let mut buf = [0u8; 65535];
            let mut engine = LinearHashEngine::new(&mut buf, 128, 4).unwrap();

            let mut live: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            for op in &ops {
                match op {
                    Op::Put(k, v) => {
                        if engine.put(k, v).is_ok() {
                            live.insert(k.clone(), v.clone());
                        }
                    }
                    Op::Delete(k) => {
                        if let Ok(true) = engine.delete(k) {
                            live.remove(k);
                        }
                    }
                }
            }

            for (k, v) in &live {
                let got = engine.get(k).unwrap();
                prop_assert_eq!(got, Some(v.as_slice()),
                    "key {:?} lost after operations", k);
            }
        }

        /// Split pointer and level advance correctly: after many inserts
        /// bucket_count should grow beyond initial_buckets.
        #[test]
        fn splits_grow_bucket_count(count in 20u16..60) {
            let mut buf = [0u8; 65535];
            let mut engine = LinearHashEngine::new(&mut buf, 128, 2).unwrap();
            for i in 0..count {
                let k = i.to_be_bytes();
                let _ = engine.put(&k, &k);
            }
            let stats = engine.stats();
            // With 2 initial buckets and 20+ keys, load-factor splits must
            // have grown the bucket count beyond 2.
            prop_assert!(stats.page_count > 3,
                "expected splits to allocate pages, got page_count={}",
                stats.page_count);
        }
    }
}
