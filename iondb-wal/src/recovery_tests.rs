//! Unit tests for recovery readers (raw and committed).

// Tests use unwrap and discard LSN return values for brevity; panics are
// acceptable in test code and unused LSNs are not meaningful here.
#![allow(unused_results, clippy::unwrap_used)]

use crate::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
use crate::wal::Wal;
use iondb_core::error::Error;
use iondb_io::memory::MemoryIoBackend;

/// Helper: create a flat WAL config with no sync and logical truncation.
fn flat_config() -> WalConfig {
    WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Logical,
    }
}

// ── raw_recovery_empty_wal ──────────────────────────────────────────────

/// An empty WAL yields no records via the raw recovery reader.
#[test]
fn raw_recovery_empty_wal() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    // unwrap acceptable in tests
    let wal = Wal::new(backend, flat_config()).unwrap();

    let mut reader = wal.recover().unwrap();
    let mut buf = [0u8; 512];
    assert!(reader.next_record(&mut buf).unwrap().is_none());
}

// ── raw_recovery_reads_all_records ──────────────────────────────────────

/// Raw reader returns ALL records including uncommitted ones.
#[test]
fn raw_recovery_reads_all_records() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_config()).unwrap();

    // txn1: begin, put, commit
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

    // txn2: begin, put (no commit)
    wal.begin_tx(2).unwrap();
    wal.put(2, b"k2", b"v2").unwrap();

    let mut reader = wal.recover().unwrap();
    let mut buf = [0u8; 512];
    let mut count = 0;
    while reader.next_record(&mut buf).unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 5);
}

// ── committed_recovery_filters_uncommitted ──────────────────────────────

/// Committed reader returns only records from committed transactions.
#[test]
fn committed_recovery_filters_uncommitted() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_config()).unwrap();

    // txn1: begin, put, commit
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

    // txn2: begin, put (no commit)
    wal.begin_tx(2).unwrap();
    wal.put(2, b"k2", b"v2").unwrap();

    let mut scratch = [0u64; 16];
    let mut reader = wal.recover_committed(&mut scratch).unwrap();
    let mut buf = [0u8; 512];
    let mut count = 0;
    let mut txn_ids = [0u64; 16];
    while let Some(rec) = reader.next_record(&mut buf).unwrap() {
        txn_ids[count] = rec.txn_id;
        count += 1;
    }
    // Only txn1's 3 records (begin, put, commit).
    assert_eq!(count, 3);
    for txn_id in txn_ids.iter().take(count) {
        assert_eq!(*txn_id, 1);
    }
}

// ── committed_recovery_interleaved_transactions ─────────────────────────

/// Interleaved transactions: only the committed one's records are returned.
#[test]
fn committed_recovery_interleaved_transactions() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_config()).unwrap();

    // Interleave txn1 and txn2.
    wal.begin_tx(1).unwrap();
    wal.begin_tx(2).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.put(2, b"k2", b"v2").unwrap();
    wal.commit_tx(1).unwrap();
    wal.rollback_tx(2).unwrap();

    let mut scratch = [0u64; 16];
    let mut reader = wal.recover_committed(&mut scratch).unwrap();
    let mut buf = [0u8; 512];
    let mut count = 0;
    while let Some(rec) = reader.next_record(&mut buf).unwrap() {
        assert_eq!(rec.txn_id, 1);
        count += 1;
    }
    // txn1: begin, put, commit = 3 records.
    assert_eq!(count, 3);
}

// ── scratch_buffer_exhaustion ───────────────────────────────────────────

/// Providing a scratch buffer that is too small returns [`Error::WalError`].
#[test]
fn scratch_buffer_exhaustion() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_config()).unwrap();

    // Commit 3 different transactions.
    for txn_id in 1..=3u64 {
        wal.begin_tx(txn_id).unwrap();
        wal.put(txn_id, b"k", b"v").unwrap();
        wal.commit_tx(txn_id).unwrap();
    }

    // Provide scratch of size 2 (but 3 committed txns).
    let mut scratch = [0u64; 2];
    let result = wal.recover_committed(&mut scratch);
    assert_eq!(result.unwrap_err(), Error::WalError);
}

// ── recovery_lsn_order ──────────────────────────────────────────────────

/// Recovered records have monotonically increasing LSNs.
#[test]
fn recovery_lsn_order() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_config()).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.put(1, b"k2", b"v2").unwrap();
    wal.commit_tx(1).unwrap();

    let mut reader = wal.recover().unwrap();
    let mut buf = [0u8; 512];
    let mut prev_lsn: Option<iondb_core::types::Lsn> = None;
    while let Some(rec) = reader.next_record(&mut buf).unwrap() {
        if let Some(prev) = prev_lsn {
            assert!(rec.lsn > prev, "LSNs must be monotonically increasing");
        }
        prev_lsn = Some(rec.lsn);
    }
    // Ensure we actually read some records.
    assert!(prev_lsn.is_some());
}
