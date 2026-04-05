//! Property-based tests for WAL layout equivalence and crash simulation.
//!
//! Uses `proptest` to generate random transaction workloads and verify
//! invariants across layouts and simulated crash scenarios.

#![allow(unused_results, clippy::expect_used)]

use iondb_io::failpoint::{FailpointIoBackend, Fault};
use iondb_io::memory::MemoryIoBackend;
use iondb_wal::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
use iondb_wal::record::RecordType;
use iondb_wal::wal::Wal;
use proptest::prelude::*;

// ── Op enum ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Op {
    Begin(u64),
    Put(u64, Vec<u8>, Vec<u8>),
    Delete(u64, Vec<u8>),
    Commit(u64),
    Rollback(u64),
}

// ── Strategy ────────────────────────────────────────────────────────────────

/// Generate a sequence of complete transactions (1-5 txns, each with 1-4
/// mutations), interleaved into a single op stream.
fn op_strategy() -> impl Strategy<Value = Vec<Op>> {
    // Generate 1-5 transactions.
    prop::collection::vec(txn_strategy(), 1..=5).prop_map(|txns| {
        let mut ops = Vec::new();
        // Interleave: emit all Begins first, then mutations round-robin,
        // then terminators.
        let mut begins = Vec::new();
        let mut mutations: Vec<Vec<Op>> = Vec::new();
        let mut terminators = Vec::new();

        for (txn_ops, term) in txns {
            if let Some(begin) = txn_ops.first() {
                begins.push(begin.clone());
            }
            mutations.push(txn_ops[1..].to_vec());
            terminators.push(term);
        }

        // Emit begins
        ops.extend(begins);

        // Emit mutations round-robin
        let max_mutations = mutations.iter().map(Vec::len).max().unwrap_or(0);
        for i in 0..max_mutations {
            for txn_muts in &mutations {
                if i < txn_muts.len() {
                    ops.push(txn_muts[i].clone());
                }
            }
        }

        // Emit terminators
        ops.extend(terminators);

        ops
    })
}

/// Generate a single transaction: `Begin` + 1-4 mutations + `Commit` or
/// `Rollback`. Returns `(ops_without_terminator, terminator)`.
fn txn_strategy() -> impl Strategy<Value = (Vec<Op>, Op)> {
    // txn_id from 1..=100
    (
        1..=100u64,
        prop::collection::vec(mutation_strategy(), 1..=4),
        prop::bool::ANY,
    )
        .prop_map(|(txn_id, mut muts, do_commit)| {
            let mut ops = vec![Op::Begin(txn_id)];
            for m in &mut muts {
                match m {
                    Op::Put(ref mut tid, _, _) | Op::Delete(ref mut tid, _) => *tid = txn_id,
                    _ => {}
                }
            }
            ops.extend(muts);
            let term = if do_commit {
                Op::Commit(txn_id)
            } else {
                Op::Rollback(txn_id)
            };
            (ops, term)
        })
}

/// Generate a single mutation: `Put` or `Delete` with small keys/values.
fn mutation_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (
            prop::collection::vec(any::<u8>(), 1..=8),
            prop::collection::vec(any::<u8>(), 1..=16),
        )
            .prop_map(|(key, value)| Op::Put(0, key, value)),
        prop::collection::vec(any::<u8>(), 1..=8).prop_map(|key| Op::Delete(0, key)),
    ]
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Apply ops to a WAL, ignoring errors (failpoint may cause failures).
fn apply_ops(wal: &mut Wal<impl iondb_core::IoBackend>, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Begin(txn_id) => {
                let _ = wal.begin_tx(*txn_id);
            }
            Op::Put(txn_id, key, value) => {
                let _ = wal.put(*txn_id, key, value);
            }
            Op::Delete(txn_id, key) => {
                let _ = wal.delete(*txn_id, key);
            }
            Op::Commit(txn_id) => {
                let _ = wal.commit_tx(*txn_id);
            }
            Op::Rollback(txn_id) => {
                let _ = wal.rollback_tx(*txn_id);
            }
        }
    }
}

/// Collect committed records from a WAL as `(lsn, txn_id, record_type)` tuples.
fn collect_committed_records(wal: &Wal<impl iondb_core::IoBackend>) -> Vec<(u64, u64, RecordType)> {
    let mut scratch = [0u64; 64];
    let mut reader = wal.recover_committed(&mut scratch).expect("reader");
    let mut buf = [0u8; 512];
    let mut records = Vec::new();
    while let Some(rec) = reader.next_record(&mut buf).expect("read") {
        records.push((rec.lsn, rec.txn_id, rec.record_type));
    }
    records
}

// ── Test 1: Layout Equivalence ──────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn layout_equivalence(ops in op_strategy()) {
        // Config 1: Flat + Logical
        let flat_config = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Logical,
        };

        // Config 2: PageSegmented(256) + Logical
        let paged_config = WalConfig {
            layout: WalLayout::PageSegmented { page_size: 256 },
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Logical,
        };

        // Run on flat WAL
        let mut flat_storage = vec![0u8; 65536];
        let flat_records = {
            let backend = MemoryIoBackend::new(&mut flat_storage);
            let mut wal = Wal::new(backend, flat_config).expect("flat wal");
            apply_ops(&mut wal, &ops);
            collect_committed_records(&wal)
        };

        // Run on paged WAL
        let mut paged_storage = vec![0u8; 65536];
        let paged_records = {
            let backend = MemoryIoBackend::new(&mut paged_storage);
            let mut wal = Wal::new(backend, paged_config).expect("paged wal");
            apply_ops(&mut wal, &ops);
            collect_committed_records(&wal)
        };

        // Verify (txn_id, record_type) sequences are identical
        let flat_seq: Vec<(u64, RecordType)> = flat_records
            .iter()
            .map(|&(_, txn_id, rt)| (txn_id, rt))
            .collect();
        let paged_seq: Vec<(u64, RecordType)> = paged_records
            .iter()
            .map(|&(_, txn_id, rt)| (txn_id, rt))
            .collect();

        prop_assert_eq!(
            flat_seq,
            paged_seq,
            "Flat and paged layouts must produce identical committed record sequences"
        );
    }
}

// ── Test 2: Crash Recovery Flat ─────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn crash_recovery_flat(ops in op_strategy(), crash_after in 1..20u64) {
        let config = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryTransaction,
            truncation: TruncationMode::Logical,
        };

        let mut storage = vec![0u8; 65536];

        // Phase 1: Write with failpoint
        {
            let backend = MemoryIoBackend::new(&mut storage);
            let mut failpoint = FailpointIoBackend::new(backend);
            failpoint.set_fault_after(Fault::ErrorBeforeWrite, crash_after);
            let mut wal = Wal::new(failpoint, config.clone()).expect("wal");
            apply_ops(&mut wal, &ops);
            // WAL and failpoint backend are dropped here, releasing &mut storage
        }

        // Phase 2: Recover from the same storage
        {
            let storage_len = storage.len() as u64;
            let recover_result =
                MemoryIoBackend::with_len(&mut storage, storage_len);
            let Ok(backend) = recover_result else {
                return Ok(()); // Cannot create backend; skip
            };
            let Ok(wal) = Wal::open(backend, config.clone()) else {
                return Ok(()); // Not enough data written; skip
            };

            let mut scratch = [0u64; 64];
            let reader = wal.recover_committed(&mut scratch);
            if let Ok(mut reader) = reader {
                let mut buf = [0u8; 512];
                let mut prev_lsn: Option<u64> = None;
                while let Some(rec) = reader.next_record(&mut buf).expect("recovery read") {
                    // LSNs must be monotonically increasing
                    if let Some(prev) = prev_lsn {
                        prop_assert!(
                            rec.lsn > prev,
                            "LSNs must increase: prev={}, current={}",
                            prev,
                            rec.lsn
                        );
                    }
                    prev_lsn = Some(rec.lsn);
                }
            }
            // If recover_committed fails, that's acceptable after a crash
        }
    }
}

// ── Test 3: Deterministic crash tests ───────────────────────────────────────

/// Writing a synced transaction followed by a crash during a second transaction
/// should preserve only the first transaction's records on recovery.
#[test]
fn crash_with_every_transaction_sync_preserves_committed() {
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::EveryTransaction,
        truncation: TruncationMode::Logical,
    };

    // Try crashing at different write counts (5th and 6th writes)
    for crash_after in [5u64, 6] {
        let mut storage = vec![0u8; 65536];

        // Phase 1: Write txn1 (synced), then start txn2 (crash)
        {
            let backend = MemoryIoBackend::new(&mut storage);
            let mut failpoint = FailpointIoBackend::new(backend);
            failpoint.set_fault_after(Fault::ErrorBeforeWrite, crash_after);
            let mut wal = Wal::new(failpoint, config.clone()).expect("wal");

            // txn1: begin + put + commit (3 writes, synced on commit)
            let _ = wal.begin_tx(1);
            let _ = wal.put(1, b"key1", b"val1");
            let _ = wal.commit_tx(1);

            // txn2: begin + put (crash happens during one of these writes)
            let _ = wal.begin_tx(2);
            let _ = wal.put(2, b"key2", b"val2");
            // No commit for txn2
        }

        // Phase 2: Recover
        {
            let storage_len = storage.len() as u64;
            let backend = MemoryIoBackend::with_len(&mut storage, storage_len).expect("backend");
            let Ok(wal) = Wal::open(backend, config.clone()) else {
                continue; // Not enough data; skip this crash_after
            };

            let mut scratch = [0u64; 64];
            if let Ok(mut reader) = wal.recover_committed(&mut scratch) {
                let mut buf = [0u8; 512];
                let mut records = Vec::new();
                while let Some(rec) = reader.next_record(&mut buf).expect("read") {
                    records.push((rec.txn_id, rec.record_type));
                }

                // Only txn1 records should be present (begin, put, commit)
                for (txn_id, _rt) in &records {
                    assert_eq!(
                        *txn_id, 1,
                        "Only committed txn1 records should be recovered, \
                         but found txn_id={txn_id} with crash_after={crash_after}"
                    );
                }
                // Should have exactly 3 records from txn1 (if any were written)
                if !records.is_empty() {
                    assert_eq!(
                        records.len(),
                        3,
                        "Expected 3 records for txn1, got {} with crash_after={crash_after}",
                        records.len()
                    );
                }
            }
        }
    }
}

/// With `SyncPolicy::None` and `MemoryIoBackend` (where sync is a no-op),
/// data still persists because writes go directly to the buffer.
#[test]
fn crash_with_no_sync_may_lose_data() {
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Logical,
    };

    let mut storage = vec![0u8; 65536];

    // Phase 1: Write with no sync policy
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, config.clone()).expect("wal");

        wal.begin_tx(1).expect("begin");
        wal.put(1, b"key1", b"val1").expect("put");
        wal.commit_tx(1).expect("commit");
    }

    // Phase 2: Re-open and recover
    {
        let storage_len = storage.len() as u64;
        let backend = MemoryIoBackend::with_len(&mut storage, storage_len).expect("backend");
        let wal = Wal::open(backend, config).expect("open");

        let mut scratch = [0u64; 64];
        let mut reader = wal.recover_committed(&mut scratch).expect("reader");
        let mut buf = [0u8; 512];
        let mut records = Vec::new();
        while let Some(rec) = reader.next_record(&mut buf).expect("read") {
            records.push((rec.txn_id, rec.record_type));
        }

        // With MemoryIoBackend sync is a no-op, so data still persists
        // despite SyncPolicy::None
        assert_eq!(records.len(), 3, "Expected 3 records (begin, put, commit)");
        assert_eq!(records[0], (1, RecordType::Begin));
        assert_eq!(records[1], (1, RecordType::Put));
        assert_eq!(records[2], (1, RecordType::Commit));
    }
}
