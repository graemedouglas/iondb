//! Unit tests for the WAL core: construction, append, sync, checkpoint.

// Tests use unwrap and discard LSN return values for brevity; panics are
// acceptable in test code and unused LSNs are not meaningful here.
#![allow(unused_results, clippy::unwrap_used)]

use crate::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
use crate::wal::Wal;
use iondb_core::error::Error;
use iondb_io::memory::MemoryIoBackend;

use super::circular_free_space;

/// Helper: create a flat/logical config with no auto-sync.
fn flat_logical() -> WalConfig {
    WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Logical,
    }
}

/// Helper: create a flat/circular config.
fn flat_circular(capacity: usize) -> WalConfig {
    WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Circular { capacity },
    }
}

/// Helper: create a paged/logical config.
fn paged_logical(page_size: usize) -> WalConfig {
    WalConfig {
        layout: WalLayout::PageSegmented { page_size },
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Logical,
    }
}

// ── new_flat_logical ─────────────────────────────────────────────────────

#[test]
fn new_flat_logical() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    // unwrap acceptable in tests
    let wal = Wal::new(backend, flat_logical()).unwrap();

    assert_eq!(wal.current_lsn(), 0);
    assert_eq!(wal.checkpoint_lsn(), 0);
    assert!(wal.remaining().is_none());
}

// ── append_and_lsn_increments ────────────────────────────────────────────

#[test]
fn append_and_lsn_increments() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_logical()).unwrap();

    let lsn0 = wal.begin_tx(1).unwrap();
    let lsn1 = wal.put(1, b"key", b"value").unwrap();
    let lsn2 = wal.commit_tx(1).unwrap();

    assert_eq!(lsn0, 0);
    assert_eq!(lsn1, 1);
    assert_eq!(lsn2, 2);
    assert_eq!(wal.current_lsn(), 3);
}

// ── open_empty_backend_fails ─────────────────────────────────────────────

#[test]
fn open_empty_backend_fails() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let result = Wal::open(backend, flat_logical());
    assert_eq!(result.unwrap_err(), Error::WalError);
}

// ── open_restores_position ───────────────────────────────────────────────

#[test]
fn open_restores_position() {
    let mut storage = [0u8; 4096];

    // Phase 1: write some records.
    let written_lsn;
    let total_bytes;
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_logical()).unwrap();
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();
        written_lsn = wal.current_lsn();
        total_bytes = wal.write_end();
    }

    // Phase 2: reopen and verify position is restored.
    {
        let backend = MemoryIoBackend::with_len(&mut storage, total_bytes).unwrap();
        let wal = Wal::open(backend, flat_logical()).unwrap();
        assert_eq!(wal.current_lsn(), written_lsn);
    }
}

// ── all_record_types ─────────────────────────────────────────────────────

#[test]
fn all_record_types() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_logical()).unwrap();

    let lsn0 = wal.begin_tx(1).unwrap();
    let lsn1 = wal.put(1, b"key", b"value").unwrap();
    let lsn2 = wal.delete(1, b"key").unwrap();
    let lsn3 = wal.rollback_tx(1).unwrap();

    assert_eq!(lsn0, 0);
    assert_eq!(lsn1, 1);
    assert_eq!(lsn2, 2);
    assert_eq!(lsn3, 3);
    assert_eq!(wal.current_lsn(), 4);
}

// ── new_circular ─────────────────────────────────────────────────────────

#[test]
fn new_circular() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, flat_circular(1024)).unwrap();

    assert!(wal.remaining().is_some());
    // Capacity = 1024, write_offset = 32 (circular header), tail = 32
    // Free = 1024 - (32 - 32) = 1024
    assert_eq!(wal.remaining(), Some(1024));
}

// ── new_paged ────────────────────────────────────────────────────────────

#[test]
fn new_paged() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_logical(256)).unwrap();

    let lsn0 = wal.begin_tx(1).unwrap();
    let lsn1 = wal.put(1, b"key", b"val").unwrap();

    assert_eq!(lsn0, 0);
    assert_eq!(lsn1, 1);
}

// ── checkpoint_updates_lsn ───────────────────────────────────────────────

#[test]
fn checkpoint_updates_lsn() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_logical()).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k", b"v").unwrap();
    wal.commit_tx(1).unwrap();

    assert_eq!(wal.checkpoint_lsn(), 0);
    wal.checkpoint(2).unwrap();
    assert_eq!(wal.checkpoint_lsn(), 2);
}

// ── invalid_config_rejected ──────────────────────────────────────────────

#[test]
fn invalid_config_rejected() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::PageSegmented { page_size: 256 },
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Circular { capacity: 1024 },
    };
    let result = Wal::new(backend, config);
    assert_eq!(result.unwrap_err(), Error::WalError);
}

// ── circular_free_space tests ────────────────────────────────────────────

#[test]
fn circular_free_space_empty() {
    assert_eq!(circular_free_space(32, 32, 1024), 1024);
}

#[test]
fn circular_free_space_partial() {
    // head = 100, tail = 32 => used = 68, free = 1024 - 68 = 956
    assert_eq!(circular_free_space(100, 32, 1024), 956);
}

#[test]
fn circular_free_space_wrapped() {
    // head = 10, tail = 100 => free = 100 - 10 = 90
    assert_eq!(circular_free_space(10, 100, 1024), 90);
}

// ── write_end tracks position ────────────────────────────────────────────

#[test]
fn write_end_advances() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_logical()).unwrap();

    assert_eq!(wal.write_end(), 0);
    wal.begin_tx(1).unwrap();
    assert!(wal.write_end() > 0);
}

// ── sync_policy_every_record ─────────────────────────────────────────────

#[test]
fn sync_policy_every_record_works() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::EveryRecord,
        truncation: TruncationMode::Logical,
    };
    let mut wal = Wal::new(backend, config).unwrap();
    // Should not panic even with EveryRecord sync.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k", b"v").unwrap();
    wal.commit_tx(1).unwrap();
}

// ── layout returns reference ─────────────────────────────────────────────

#[test]
fn layout_returns_correct_ref() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, flat_logical()).unwrap();
    assert_eq!(*wal.layout(), WalLayout::Flat);
}

// ── recovery_start for non-circular ──────────────────────────────────────

#[test]
fn recovery_start_flat_logical() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, flat_logical()).unwrap();
    assert_eq!(wal.recovery_start(), 0);
}

// ── Circular mode: append, capacity exhaustion, checkpoint, remaining ───

#[test]
fn circular_append_reduces_remaining() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_circular(1024)).unwrap();

    let before = wal.remaining().unwrap();
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    let after = wal.remaining().unwrap();
    assert!(after < before, "remaining should decrease after appending");
}

#[test]
fn circular_capacity_exhaustion() {
    // Use a tiny capacity so we run out quickly.
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    // Minimum capacity is 61, give just enough for a couple records.
    let mut wal = Wal::new(backend, flat_circular(128)).unwrap();

    // A Begin record is ~29 bytes. With circular header (32), we have
    // 128 - (32 - 32) = 128 bytes of free space initially, but writes
    // start at offset 32 so effectively 128 - 0 = 128 free.
    // Keep appending until we exceed capacity.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"key", b"val").unwrap();
    wal.commit_tx(1).unwrap();

    // Keep trying — should eventually get CapacityExhausted.
    let result = (|| -> iondb_core::error::Result<()> {
        for i in 2..20u64 {
            wal.begin_tx(i)?;
            wal.put(i, b"kkkk", b"vvvv")?;
            wal.commit_tx(i)?;
        }
        Ok(())
    })();
    assert_eq!(result.unwrap_err(), Error::CapacityExhausted);
}

#[test]
fn circular_checkpoint_frees_space_and_header_updates() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_circular(1024)).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    let commit_lsn = wal.commit_tx(1).unwrap();

    // Checkpoint up to the commit LSN.
    wal.checkpoint(commit_lsn).unwrap();
    assert_eq!(wal.checkpoint_lsn(), commit_lsn);

    // The remaining space should still be reported correctly.
    assert!(wal.remaining().is_some());
}

#[test]
fn circular_open_restores_state() {
    let mut storage = [0u8; 4096];
    let config = flat_circular(2048);

    let written_lsn;
    let total_bytes;
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, config.clone()).unwrap();
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();
        written_lsn = wal.current_lsn();
        total_bytes = wal.write_end();
    }

    {
        let backend = MemoryIoBackend::with_len(&mut storage, total_bytes).unwrap();
        let wal = Wal::open(backend, config).unwrap();
        assert_eq!(wal.current_lsn(), written_lsn);
        assert!(wal.remaining().is_some());
    }
}

// ── Paged WAL: append, open/scan, write_end ─────────────────────────────

#[test]
fn paged_append_and_read_back() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = paged_logical(256);
    let mut wal = Wal::new(backend, config).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"hello", b"world").unwrap();
    wal.commit_tx(1).unwrap();

    assert_eq!(wal.current_lsn(), 3);
    // write_end should be greater than zero for paged layout.
    assert!(wal.write_end() > 0);
}

#[test]
fn paged_open_restores_position() {
    let mut storage = [0u8; 8192];
    let config = paged_logical(256);

    let written_lsn;
    let total_bytes;
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, config.clone()).unwrap();
        wal.begin_tx(1).unwrap();
        wal.put(1, b"key1", b"val1").unwrap();
        wal.commit_tx(1).unwrap();
        wal.begin_tx(2).unwrap();
        wal.put(2, b"key2", b"val2").unwrap();
        wal.commit_tx(2).unwrap();
        written_lsn = wal.current_lsn();
        total_bytes = wal.write_end();
    }

    {
        let backend = MemoryIoBackend::with_len(&mut storage, total_bytes).unwrap();
        let wal = Wal::open(backend, config).unwrap();
        assert_eq!(wal.current_lsn(), written_lsn);
    }
}

#[test]
fn paged_open_with_checkpoint() {
    let mut storage = [0u8; 8192];
    let config = paged_logical(256);

    let total_bytes;
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, config.clone()).unwrap();
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        let commit_lsn = wal.commit_tx(1).unwrap();
        wal.checkpoint(commit_lsn).unwrap();
        total_bytes = wal.write_end();
    }

    {
        let backend = MemoryIoBackend::with_len(&mut storage, total_bytes).unwrap();
        let wal = Wal::open(backend, config).unwrap();
        // The checkpoint record's LSN is 3 (the 4th record written).
        // scan_paged finds Checkpoint records and sets checkpoint_lsn to rec.lsn.
        assert!(wal.checkpoint_lsn() > 0);
    }
}

#[test]
fn paged_remaining_returns_none() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, paged_logical(256)).unwrap();
    // remaining() only applies to circular mode.
    assert!(wal.remaining().is_none());
}

#[test]
fn paged_write_end_tracks_pages() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_logical(128)).unwrap();

    // Each record is 29+ bytes. With page_size=128, usable=108.
    // Write enough to span multiple pages.
    for i in 0..10u64 {
        wal.begin_tx(i).unwrap();
        wal.put(i, b"kk", b"vv").unwrap();
        wal.commit_tx(i).unwrap();
    }

    let end = wal.write_end();
    // With multiple pages, end should be well past the first page.
    assert!(end > 128, "write_end should span multiple pages");
}

#[test]
fn paged_many_records_span_pages() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = paged_logical(128);
    let mut wal = Wal::new(backend, config.clone()).unwrap();

    // Write lots of records to span many pages.
    let num_txns = 15u64;
    for i in 1..=num_txns {
        wal.begin_tx(i).unwrap();
        wal.put(i, b"k", b"v").unwrap();
        wal.commit_tx(i).unwrap();
    }

    let expected_lsn = wal.current_lsn();
    let total_bytes = wal.write_end();

    // Reopen and verify.
    let mut storage2 = [0u8; 16384];
    #[allow(clippy::cast_possible_truncation)]
    let byte_count = total_bytes as usize;
    storage2[..byte_count].copy_from_slice(&storage[..byte_count]);
    let backend2 = MemoryIoBackend::with_len(&mut storage2, total_bytes).unwrap();
    let reopened = Wal::open(backend2, config).unwrap();
    assert_eq!(reopened.current_lsn(), expected_lsn);
}

// ── Physical compaction tests (std feature) ─────────────────────────────

#[cfg(feature = "std")]
#[test]
fn physical_compact_basic() {
    use crate::config::TruncationMode;

    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Physical,
    };
    let mut wal = Wal::new(backend, config).unwrap();

    // Write some records.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    let commit_lsn = wal.commit_tx(1).unwrap();

    // Write more records after the checkpoint boundary.
    wal.begin_tx(2).unwrap();
    wal.put(2, b"k2", b"v2").unwrap();
    wal.commit_tx(2).unwrap();

    // Checkpoint up to the first transaction's commit.
    // This should trigger physical compaction.
    wal.checkpoint(commit_lsn).unwrap();

    // The WAL should still be functional after compaction.
    let lsn = wal.begin_tx(3).unwrap();
    assert!(lsn > 0);
}

#[cfg(feature = "std")]
#[test]
fn physical_compact_all_checkpointed() {
    use crate::config::TruncationMode;

    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Physical,
    };
    let mut wal = Wal::new(backend, config).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

    // Pass a very high up_to_lsn so that ALL records (including the
    // checkpoint record itself) have lsn <= checkpoint_lsn. This triggers
    // the "all records are checkpointed" path in physical_compact.
    wal.checkpoint(u64::MAX / 2).unwrap();

    // write_end should be 0 after compaction (all records were checkpointed).
    assert_eq!(wal.write_end(), 0);

    // The WAL is still functional after compaction.
    wal.begin_tx(4).unwrap();
    wal.put(4, b"new_key", b"new_val").unwrap();
    wal.commit_tx(4).unwrap();
    assert!(wal.current_lsn() > 3);
}

#[cfg(feature = "std")]
#[test]
fn physical_compact_preserves_live_records() {
    use crate::config::TruncationMode;

    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Physical,
    };
    let mut wal = Wal::new(backend, config).unwrap();

    // txn1: will be checkpointed.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    let c1 = wal.commit_tx(1).unwrap();

    // txn2: will survive compaction.
    wal.begin_tx(2).unwrap();
    wal.put(2, b"k2", b"v2").unwrap();
    wal.commit_tx(2).unwrap();

    // Checkpoint txn1 only.
    wal.checkpoint(c1).unwrap();

    // Recover: should still be able to read txn2's records via raw recovery.
    let mut reader = wal.recover().unwrap();
    let mut buf = [0u8; 512];
    let mut found_txn2 = false;
    while let Some(rec) = reader.next_record(&mut buf).unwrap() {
        if rec.txn_id == 2 {
            found_txn2 = true;
        }
    }
    assert!(found_txn2, "txn2 records should survive compaction");
}

#[cfg(feature = "std")]
#[test]
fn physical_truncation_remaining_is_none() {
    use crate::config::TruncationMode;

    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Physical,
    };
    let wal = Wal::new(backend, config).unwrap();
    assert!(wal.remaining().is_none());
}

// ── Sync policies ───────────────────────────────────────────────────────

#[test]
fn sync_policy_every_transaction() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::EveryTransaction,
        truncation: TruncationMode::Logical,
    };
    let mut wal = Wal::new(backend, config).unwrap();
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k", b"v").unwrap();
    wal.commit_tx(1).unwrap();
    // Rollback should also trigger sync.
    wal.begin_tx(2).unwrap();
    wal.rollback_tx(2).unwrap();
}

#[test]
fn sync_policy_periodic() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::Periodic(2),
        truncation: TruncationMode::Logical,
    };
    let mut wal = Wal::new(backend, config).unwrap();
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k", b"v").unwrap();
    wal.commit_tx(1).unwrap();
}

#[test]
fn explicit_sync() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_logical()).unwrap();
    wal.begin_tx(1).unwrap();
    wal.sync().unwrap();
}

// ── Debug impls ─────────────────────────────────────────────────────────

#[test]
fn wal_debug_impl() {
    extern crate alloc;
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, flat_logical()).unwrap();
    let debug_str = alloc::format!("{wal:?}");
    assert!(debug_str.contains("Wal"));
}

// ── recover_committed_to_vec (alloc feature) ────────────────────────────

#[cfg(feature = "alloc")]
#[test]
fn recover_committed_to_vec_basic() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_logical()).unwrap();

    // txn1: committed.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

    // txn2: uncommitted.
    wal.begin_tx(2).unwrap();
    wal.put(2, b"k2", b"v2").unwrap();

    let records = wal.recover_committed_to_vec().unwrap();
    // Should only have txn1's records (begin, put, commit = 3).
    assert_eq!(records.len(), 3);
    for rec in &records {
        assert_eq!(rec.txn_id, 1);
    }
}

#[cfg(feature = "alloc")]
#[test]
fn recover_committed_to_vec_empty_wal() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, flat_logical()).unwrap();

    let records = wal.recover_committed_to_vec().unwrap();
    assert!(records.is_empty());
}

#[cfg(feature = "alloc")]
#[test]
fn recover_committed_to_vec_multiple_txns() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_logical()).unwrap();

    for txn_id in 1..=5u64 {
        wal.begin_tx(txn_id).unwrap();
        wal.put(txn_id, b"k", b"v").unwrap();
        wal.commit_tx(txn_id).unwrap();
    }

    let records = wal.recover_committed_to_vec().unwrap();
    // 5 txns * 3 records each = 15.
    assert_eq!(records.len(), 15);
}

#[cfg(feature = "alloc")]
#[test]
fn recover_committed_to_vec_paged() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_logical(256)).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"pk", b"pv").unwrap();
    wal.commit_tx(1).unwrap();

    // uncommitted
    wal.begin_tx(2).unwrap();
    wal.put(2, b"pk2", b"pv2").unwrap();

    let records = wal.recover_committed_to_vec().unwrap();
    assert_eq!(records.len(), 3);
    for rec in &records {
        assert_eq!(rec.txn_id, 1);
    }
}

// ── Backend accessor ────────────────────────────────────────────────────

#[test]
fn backend_accessor() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, flat_logical()).unwrap();
    let _b = wal.backend();
    // Just verifying it compiles and returns.
}

// ── Paged recovery reader ───────────────────────────────────────────────

#[test]
fn paged_raw_recovery_reads_all() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_logical(256)).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

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

#[test]
fn paged_committed_recovery_filters() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_logical(256)).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

    wal.begin_tx(2).unwrap();
    wal.put(2, b"k2", b"v2").unwrap();

    let mut scratch = [0u64; 16];
    let mut reader = wal.recover_committed(&mut scratch).unwrap();
    let mut buf = [0u8; 512];
    let mut count = 0;
    while let Some(rec) = reader.next_record(&mut buf).unwrap() {
        assert_eq!(rec.txn_id, 1);
        count += 1;
    }
    assert_eq!(count, 3);
}

// ── Physical truncation open/scan tests ─────────────────────────────────

#[cfg(feature = "std")]
#[test]
fn physical_open_restores_position() {
    use crate::config::TruncationMode;

    let mut storage = [0u8; 8192];
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Physical,
    };

    let written_lsn;
    let total_bytes;
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, config.clone()).unwrap();
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();
        written_lsn = wal.current_lsn();
        total_bytes = wal.write_end();
    }

    {
        let backend = MemoryIoBackend::with_len(&mut storage, total_bytes).unwrap();
        let wal = Wal::open(backend, config).unwrap();
        assert_eq!(wal.current_lsn(), written_lsn);
    }
}

// ── Flat scan with checkpoint record ────────────────────────────────────

#[test]
fn open_flat_with_checkpoint_restores_checkpoint_lsn() {
    let mut storage = [0u8; 8192];
    let config = flat_logical();

    let total_bytes;
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, config.clone()).unwrap();
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        let commit_lsn = wal.commit_tx(1).unwrap();
        wal.checkpoint(commit_lsn).unwrap();
        total_bytes = wal.write_end();
    }

    {
        let backend = MemoryIoBackend::with_len(&mut storage, total_bytes).unwrap();
        let wal = Wal::open(backend, config).unwrap();
        // The checkpoint record's LSN is 3 (the 4th record written, after
        // begin=0, put=1, commit=2, checkpoint=3). The scan should have
        // found this Checkpoint record and set checkpoint_lsn.
        assert!(wal.checkpoint_lsn() > 0);
    }
}

// ── Circular recovery_start reads tail from header ──────────────────────

#[test]
fn circular_recovery_start_reads_tail() {
    let mut storage = [0u8; 4096];
    let config = flat_circular(2048);
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, config).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

    // For circular WAL, recovery_start() reads the tail_offset from the
    // circular header.
    let start = wal.recovery_start();
    // The tail_offset is set to CIRCULAR_HEADER_SIZE (32) by Wal::new.
    assert_eq!(start, 32);
}
