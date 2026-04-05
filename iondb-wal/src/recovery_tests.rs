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

// ── Paged recovery ──────────────────────────────────────────────────────

/// Helper: create a paged WAL config with no sync and logical truncation.
fn paged_config(page_size: usize) -> WalConfig {
    WalConfig {
        layout: WalLayout::PageSegmented { page_size },
        sync_policy: SyncPolicy::None,
        truncation: TruncationMode::Logical,
    }
}

/// Raw recovery reader works with paged layout and reads all records.
#[test]
fn paged_raw_recovery_reads_all() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_config(256)).unwrap();

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

/// Committed recovery filters out uncommitted transactions in paged layout.
#[test]
fn paged_committed_recovery_filters_uncommitted() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_config(256)).unwrap();

    // txn1: committed.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    wal.commit_tx(1).unwrap();

    // txn2: uncommitted.
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

/// Interleaved transactions in paged layout: only committed records returned.
#[test]
fn paged_committed_interleaved() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_config(256)).unwrap();

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
    assert_eq!(count, 3);
}

/// Paged layout with records spanning multiple pages.
#[test]
fn paged_recovery_multi_page() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    // Small pages to force page transitions.
    let mut wal = Wal::new(backend, paged_config(128)).unwrap();

    for i in 1..=5u64 {
        wal.begin_tx(i).unwrap();
        wal.put(i, b"kk", b"vv").unwrap();
        wal.commit_tx(i).unwrap();
    }

    let mut reader = wal.recover().unwrap();
    let mut buf = [0u8; 512];
    let mut count = 0;
    while reader.next_record(&mut buf).unwrap().is_some() {
        count += 1;
    }
    // 5 txns * 3 records = 15.
    assert_eq!(count, 15);
}

/// Paged committed recovery with multiple pages.
#[test]
fn paged_committed_recovery_multi_page() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_config(128)).unwrap();

    // Committed txns.
    for i in 1..=3u64 {
        wal.begin_tx(i).unwrap();
        wal.put(i, b"kk", b"vv").unwrap();
        wal.commit_tx(i).unwrap();
    }

    // Uncommitted txn.
    wal.begin_tx(99).unwrap();
    wal.put(99, b"kk", b"vv").unwrap();

    let mut scratch = [0u64; 16];
    let mut reader = wal.recover_committed(&mut scratch).unwrap();
    let mut buf = [0u8; 512];
    let mut count = 0;
    while let Some(rec) = reader.next_record(&mut buf).unwrap() {
        assert!(rec.txn_id >= 1 && rec.txn_id <= 3);
        count += 1;
    }
    // 3 committed txns * 3 records = 9.
    assert_eq!(count, 9);
}

/// Corruption in a paged layout: corrupt the CRC of a record within a page,
/// verify recovery skips it and continues from the next page.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn paged_corruption_skips_to_next_page() {
    use iondb_core::page::PAGE_HEADER_SIZE;

    let mut storage = [0u8; 16384];
    let page_size = 256;
    let write_end;

    // Phase 1: write records.
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, paged_config(page_size)).unwrap();

        // txn1: write to first page.
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();

        // txn2: may start on a different page.
        wal.begin_tx(2).unwrap();
        wal.put(2, b"k2", b"v2").unwrap();
        wal.commit_tx(2).unwrap();

        write_end = wal.write_end();
    }

    // Phase 2: corrupt the CRC of the first record in page 0.
    let corrupt_idx = PAGE_HEADER_SIZE + 2;
    storage[corrupt_idx] = 0xFF;
    storage[corrupt_idx + 1] = 0xFF;

    // Phase 3: read back and verify corruption is handled.
    let backend = MemoryIoBackend::with_len(&mut storage, write_end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };
    let mut reader = crate::recovery::RawRecoveryReader::new(&backend, &layout, 0, write_end);
    let mut buf = [0u8; 512];

    let mut count = 0;
    while reader.next_record(&mut buf).unwrap().is_some() {
        count += 1;
    }
    // We corrupted page 0's first record -- the reader should skip that
    // page and recover records from subsequent pages.
    assert!(count < 6, "some records should be lost due to corruption");
}

/// Corruption in flat layout: corrupt a record's CRC, verify recovery
/// skips to the next record via magic scan.
#[test]
fn flat_corruption_scans_for_next_magic() {
    let mut storage = [0u8; 4096];
    let write_end;

    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        // Write 3 records.
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();

        write_end = wal.write_end();
    }

    // Corrupt the CRC of the first record (bytes 2..6).
    storage[2] = 0xFF;
    storage[3] = 0xFF;

    let backend = MemoryIoBackend::with_len(&mut storage, write_end).unwrap();
    let layout = WalLayout::Flat;
    let mut reader = crate::recovery::RawRecoveryReader::new(&backend, &layout, 0, write_end);
    let mut buf = [0u8; 512];

    let mut count = 0;
    while reader.next_record(&mut buf).unwrap().is_some() {
        count += 1;
    }
    // First record corrupted, scanner should find records 2 and 3.
    assert_eq!(count, 2);
}

/// Debug impls for recovery readers.
#[test]
fn recovery_reader_debug_impls() {
    extern crate alloc;
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let wal = Wal::new(backend, flat_config()).unwrap();

    let reader = wal.recover().unwrap();
    let debug_str = alloc::format!("{reader:?}");
    assert!(debug_str.contains("RawRecoveryReader"));

    let mut scratch = [0u64; 16];
    let committed = wal.recover_committed(&mut scratch).unwrap();
    let debug_str = alloc::format!("{committed:?}");
    assert!(debug_str.contains("CommittedRecoveryReader"));
}

/// Paged scratch buffer exhaustion.
#[test]
fn paged_scratch_buffer_exhaustion() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_config(256)).unwrap();

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

/// Flat corruption where the corrupted record is the LAST record --
/// `scan_for_magic` finds nothing, and `advance_flat` terminates with Done.
#[test]
fn flat_corruption_last_record_no_next_magic() {
    let mut storage = [0u8; 4096];
    let write_end;

    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        // Write only one record.
        wal.begin_tx(1).unwrap();
        write_end = wal.write_end();
    }

    // Corrupt the CRC of the only record (bytes 2..6).
    storage[2] = 0xFF;
    storage[3] = 0xFF;

    let backend = MemoryIoBackend::with_len(&mut storage, write_end).unwrap();
    let layout = WalLayout::Flat;
    let mut reader = crate::recovery::RawRecoveryReader::new(&backend, &layout, 0, write_end);
    let mut buf = [0u8; 512];

    // The only record is corrupted. scan_for_magic won't find another.
    // advance_flat should return Done.
    let result = reader.next_record(&mut buf).unwrap();
    assert!(result.is_none());
}

/// Flat recovery at end-of-data (no magic at offset) hits the
/// "no magic -- end of written data" path in `advance_flat`.
#[test]
fn flat_recovery_no_magic_at_offset() {
    // Write some zeros (no WAL records) and try to recover.
    let mut storage = [0u8; 256];
    let backend = MemoryIoBackend::with_len(&mut storage, 256).unwrap();
    let layout = WalLayout::Flat;
    let mut reader = crate::recovery::RawRecoveryReader::new(&backend, &layout, 0, 256);
    let mut buf = [0u8; 512];

    // No records written; the first bytes are zeros (no magic).
    let result = reader.next_record(&mut buf).unwrap();
    assert!(result.is_none());
}

/// Paged cursor: record claims to extend beyond usable area.
/// Craft a header with large `key_len` at a position where the full record
/// would exceed the remaining usable space, forcing a page skip.
#[test]
fn paged_cursor_record_exceeds_usable_area() {
    use iondb_core::page::PAGE_CHECKSUM_SIZE;

    let page_size = 256usize;
    let usable_end = page_size - PAGE_CHECKSUM_SIZE;

    let mut storage = [0u8; 4096];

    // Write a valid record header with magic but claiming a big key.
    let mut header = [0u8; 29];
    header[0] = 0x57; // magic
    header[1] = 0x4C;
    // key_len at 23..25 (le16): set to 200
    header[23] = 200;
    header[24] = 0;
    // val_len at 25..29 (le32): set to 0
    // Total record = 29 + 200 = 229 bytes.
    // Place at pos_in_page = usable_end - 40 = 252 - 40 = 212.
    // Remaining = 252 - 212 = 40 bytes. 40 >= 29 (header fits) but 40 < 229 (record won't fit).
    let pip = usable_end - 40;
    storage[pip..pip + 29].copy_from_slice(&header);

    let end = (page_size * 2) as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, end).unwrap();

    let mut buf = [0u8; 512];
    let result =
        crate::paged::read_record_paged(&backend, 0, pip, page_size, end, &mut buf).unwrap();
    // Should advance to next page (record doesn't fit).
    // Since page 1 is empty, returns None.
    assert!(result.is_none());
}

/// Committed flat recovery with magic-byte corruption: the first-pass scan
/// should skip corrupted records using `scan_for_magic`.
#[test]
fn committed_flat_recovery_with_magic_corruption() {
    let mut storage = [0u8; 8192];
    let write_end;

    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        // txn1: committed.
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();

        // txn2: committed.
        wal.begin_tx(2).unwrap();
        wal.put(2, b"k2", b"v2").unwrap();
        wal.commit_tx(2).unwrap();

        write_end = wal.write_end();
    }

    // Corrupt the MAGIC bytes of the first record (bytes 0..2).
    // read_header will return Err because magic doesn't match.
    // scan_committed_flat should then call scan_for_magic to find the next record.
    storage[0] = 0xFF;
    storage[1] = 0xFF;

    let backend = MemoryIoBackend::with_len(&mut storage, write_end).unwrap();
    let layout = WalLayout::Flat;

    let mut scratch = [0u64; 16];
    let result = crate::recovery::CommittedRecoveryReader::new(
        &backend, &layout, 0, write_end, &mut scratch,
    );
    // Should succeed -- scan_committed_flat handles corruption via scan_for_magic.
    assert!(result.is_ok());
}

/// Committed flat recovery where ALL records have corrupted magic (no magic found).
#[test]
fn committed_flat_recovery_all_corrupted() {
    let mut storage = [0u8; 4096];
    let write_end;

    {
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();
        wal.begin_tx(1).unwrap();
        wal.commit_tx(1).unwrap();
        write_end = wal.write_end();
    }

    // Corrupt magic of the first record.
    storage[0] = 0xFF;
    // Also corrupt the second record's magic (at offset 29).
    storage[29] = 0xFF;

    let backend = MemoryIoBackend::with_len(&mut storage, write_end).unwrap();
    let layout = WalLayout::Flat;
    let mut scratch = [0u64; 16];
    let result = crate::recovery::CommittedRecoveryReader::new(
        &backend, &layout, 0, write_end, &mut scratch,
    );
    // Should succeed with 0 committed transactions found.
    assert!(result.is_ok());
}

/// Paged committed scan encounters insufficient header space near page end.
#[test]
fn paged_committed_scan_insufficient_header_space() {
    use iondb_core::page::PAGE_CHECKSUM_SIZE;

    let page_size = 256usize;
    let usable_end = page_size - PAGE_CHECKSUM_SIZE;

    let mut storage = [0u8; 4096];

    // Write magic bytes at a position near the end of a page where there's
    // not enough space for a full record header (29 bytes).
    // Position: usable_end - 10 = 242 (only 10 bytes before checksum).
    let pip = usable_end - 10;
    storage[pip] = 0x57; // WAL magic byte 1
    storage[pip + 1] = 0x4C; // WAL magic byte 2

    let end = (page_size * 2) as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };

    // Use CommittedRecoveryReader which exercises scan_committed_paged.
    // Start reading from pip where there's not enough space for a header.
    // The scan should skip to the next page.
    let mut scratch = [0u64; 16];
    let result = crate::recovery::CommittedRecoveryReader::new(
        &backend, &layout, 0, end, &mut scratch,
    );
    assert!(result.is_ok());
}

/// Paged committed scan with a record that exceeds usable space.
#[test]
fn paged_committed_scan_record_exceeds_usable() {
    use iondb_core::page::PAGE_CHECKSUM_SIZE;

    let page_size = 256usize;
    let usable_end = page_size - PAGE_CHECKSUM_SIZE;

    let mut storage = [0u8; 4096];

    // Craft a valid-looking record header with a large key_len at a position
    // where the total record size exceeds the remaining usable space.
    let pip = usable_end - 40; // 40 bytes remaining, but record claims > 40.
    // Write a WAL header with magic + valid record_type + big key_len.
    storage[pip] = 0x57; // magic
    storage[pip + 1] = 0x4C;
    // record_type at offset 22: Put=1
    storage[pip + 22] = 1;
    // key_len at 23..25 (le16): 200 bytes.
    storage[pip + 23] = 200;
    storage[pip + 24] = 0;
    // val_len at 25..29 (le32): 0
    // Total = 29 + 200 = 229 > 40 remaining. scan_committed_paged should
    // skip to next page.

    let end = (page_size * 2) as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };

    let mut scratch = [0u64; 16];
    let result = crate::recovery::CommittedRecoveryReader::new(
        &backend, &layout, 0, end, &mut scratch,
    );
    assert!(result.is_ok());
}

/// Paged committed scan where header parsing fails (corrupted `record_type`).
#[test]
fn paged_committed_scan_header_parse_error() {
    use iondb_core::page::PAGE_HEADER_SIZE;

    let page_size = 256usize;

    let mut storage = [0u8; 4096];

    // Write magic bytes at PAGE_HEADER_SIZE with an invalid record_type.
    storage[PAGE_HEADER_SIZE] = 0x57; // magic
    storage[PAGE_HEADER_SIZE + 1] = 0x4C;
    // record_type at offset 22: use invalid value 0xFF.
    storage[PAGE_HEADER_SIZE + 22] = 0xFF;
    // key_len and val_len at 23..29: zeros (small record).
    // The record_type=0xFF is invalid, so read_header will return Err.
    // scan_committed_paged should handle this by advancing to next page.

    let end = (page_size * 2) as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };

    let mut scratch = [0u64; 16];
    let result = crate::recovery::CommittedRecoveryReader::new(
        &backend, &layout, 0, end, &mut scratch,
    );
    assert!(result.is_ok());
}

/// Flat recovery with small buffer: the buffer is too small for the record
/// body (but large enough for the header), triggering the "buf too small for
/// total" path in `advance_flat`.
#[test]
fn flat_recovery_small_buffer_for_body() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_config()).unwrap();

    // Write a record with a large key+value.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"a_long_key_value", b"a_long_value_data").unwrap();

    let write_end = wal.write_end();
    let layout = WalLayout::Flat;

    let mut reader = crate::recovery::RawRecoveryReader::new(
        wal.backend(),
        &layout,
        0,
        write_end,
    );
    // Use a buffer large enough for the Begin record (29 bytes), but too
    // small for the Put record (29 + 16 + 17 = 62 bytes).
    let mut small_buf = [0u8; 40];
    // First record is Begin (29 bytes) -- should succeed.
    let first = reader.next_record(&mut small_buf).unwrap();
    assert!(first.is_some());
    // Second record is Put (62 bytes) -- buffer too small, should return None.
    let second = reader.next_record(&mut small_buf).unwrap();
    assert!(second.is_none());
}

/// Paged recovery with checksum-boundary position exercises the
/// "pos >= `usable_end`" path in `advance_paged`.
#[test]
fn paged_cursor_at_checksum_boundary() {

    let page_size = 128usize;

    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = paged_config(page_size);
    let mut wal = Wal::new(backend, config).unwrap();

    // Write records to fill pages. With page_size=128, usable=108.
    // Record is ~31 bytes. 3 records per page (93 <= 108). The 4th
    // record won't fit and triggers a new page.
    for i in 0..4u64 {
        wal.begin_tx(i).unwrap();
    }

    // Now recover using raw recovery reader. The cursor will traverse
    // page boundaries exercising the advance_paged logic.
    let write_end = wal.write_end();
    let layout = WalLayout::PageSegmented { page_size };
    let mut reader = crate::recovery::RawRecoveryReader::new(
        wal.backend(),
        &layout,
        0,
        write_end,
    );
    let mut buf = [0u8; 512];
    let mut count = 0;
    while reader.next_record(&mut buf).unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 4);
}

/// Paged recovery with corruption: CRC mismatch in paged cursor triggers
/// skip to next page via `advance_paged`.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn paged_cursor_crc_mismatch_skips_page() {
    use iondb_core::page::PAGE_HEADER_SIZE;

    let page_size = 256usize;

    let mut storage = [0u8; 8192];
    let write_end;

    {
        let backend = MemoryIoBackend::new(&mut storage);
        let config = paged_config(page_size);
        let mut wal = Wal::new(backend, config).unwrap();

        // Write records spanning 2+ pages.
        for i in 1..=4u64 {
            wal.begin_tx(i).unwrap();
            wal.put(i, b"kk", b"vv").unwrap();
            wal.commit_tx(i).unwrap();
        }
        write_end = wal.write_end();
    }

    // Corrupt only the CRC of a record in the first page (not the magic).
    // Record starts at PAGE_HEADER_SIZE. CRC is at bytes 2..6 of the record.
    let crc_offset = PAGE_HEADER_SIZE + 2;
    storage[crc_offset] ^= 0xFF;

    let backend = MemoryIoBackend::with_len(&mut storage, write_end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };
    let mut reader = crate::recovery::RawRecoveryReader::new(
        &backend, &layout, 0, write_end,
    );
    let mut buf = [0u8; 512];
    let mut count = 0;
    while reader.next_record(&mut buf).unwrap().is_some() {
        count += 1;
    }
    // First page is corrupted (CRC mismatch), so all records on page 0
    // are skipped. Records on subsequent pages should be recovered.
    assert!(count > 0, "should recover records from later pages");
    assert!(count < 12, "should have lost page 0 records");
}

/// Paged recovery with record that would overflow usable space exercises
/// the `record fits in page` check in `advance_paged` (line 179).
#[test]
fn paged_cursor_record_overflow_usable() {
    use iondb_core::page::PAGE_CHECKSUM_SIZE;

    let page_size = 256usize;
    let usable_end = page_size - PAGE_CHECKSUM_SIZE;

    let mut storage = [0u8; 4096];

    // Craft a record header at a position where the claimed record size
    // exceeds the remaining usable space. The cursor should skip to next page.
    let pip = usable_end - 35; // 35 bytes remaining, header fits (29), but total won't.
    storage[pip] = 0x57; // magic
    storage[pip + 1] = 0x4C;
    // Zero CRC (will be checked after size check fails).
    // key_len at 23..25: 10 bytes -> total = 29 + 10 = 39 > 35.
    storage[pip + 23] = 10;
    storage[pip + 24] = 0;
    // val_len: 0

    let end = (page_size * 2) as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };
    let mut reader = crate::recovery::RawRecoveryReader::new(
        &backend, &layout, 0, end,
    );
    let mut buf = [0u8; 512];
    let result = reader.next_record(&mut buf).unwrap();
    // Should skip to next page (empty) and return None.
    assert!(result.is_none());
}

/// Flat recovery with a buffer smaller than `RECORD_HEADER_SIZE`.
/// Should return `WalError` from `advance_flat`.
#[test]
fn flat_recovery_tiny_buffer_error() {
    let mut storage = [0u8; 4096];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, flat_config()).unwrap();
    wal.begin_tx(1).unwrap();

    let write_end = wal.write_end();
    let layout = WalLayout::Flat;
    let mut reader = crate::recovery::RawRecoveryReader::new(
        wal.backend(), &layout, 0, write_end,
    );
    // Buffer smaller than RECORD_HEADER_SIZE (29 bytes).
    let mut tiny_buf = [0u8; 10];
    let result = reader.next_record(&mut tiny_buf);
    assert_eq!(result.unwrap_err(), Error::WalError);
}

/// Paged recovery with a buffer smaller than `RECORD_HEADER_SIZE`.
#[test]
fn paged_recovery_tiny_buffer_error() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_config(256)).unwrap();
    wal.begin_tx(1).unwrap();

    let write_end = wal.write_end();
    let layout = WalLayout::PageSegmented { page_size: 256 };
    let mut reader = crate::recovery::RawRecoveryReader::new(
        wal.backend(), &layout, 0, write_end,
    );
    let mut tiny_buf = [0u8; 10];
    let result = reader.next_record(&mut tiny_buf);
    assert_eq!(result.unwrap_err(), Error::WalError);
}

// NOTE: "Short read" paths in cursor.rs (n < RECORD_HEADER_SIZE, n < total)
// are not triggerable with MemoryIoBackend because its read() always returns
// from the full underlying buffer regardless of the logical size. These paths
// are only reachable with a real file-system backend that has been truncated.

/// Paged recovery where record size exceeds remaining usable space in cursor
/// (different from `read_record_paged` test -- exercises `advance_paged` line 179).
#[test]
fn paged_cursor_advance_record_exceeds_usable() {
    use iondb_core::page::PAGE_CHECKSUM_SIZE;

    let page_size = 256usize;
    let usable_end = page_size - PAGE_CHECKSUM_SIZE;

    let mut storage = [0u8; 4096];

    // Craft a record header with magic at a position where total > remaining.
    let pip = usable_end - 35;
    storage[pip] = 0x57;
    storage[pip + 1] = 0x4C;
    storage[pip + 23] = 10; // key_len=10 => total=39 > 35
    storage[pip + 24] = 0;

    let end = (page_size * 2) as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };
    let mut reader = crate::recovery::RawRecoveryReader::new(
        &backend, &layout, 0, end,
    );
    let mut buf = [0u8; 512];
    // Cursor starts at page_offset=0, pos_in_page=16.
    // At pip, it finds magic but record doesn't fit. Should skip to page 1.
    // Page 1 is empty, returns None.
    let result = reader.next_record(&mut buf).unwrap();
    assert!(result.is_none());
}

/// Paged recovery with buffer too small for record body in cursor path.
#[test]
fn paged_cursor_small_buffer_for_body() {
    let mut storage = [0u8; 8192];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = paged_config(256);
    let mut wal = Wal::new(backend, config).unwrap();

    // Write a Begin (29 bytes) and then a Put with large key+value.
    wal.begin_tx(1).unwrap();
    wal.put(1, b"a_long_key_val", b"a_long_val_data").unwrap();

    let write_end = wal.write_end();
    let layout = WalLayout::PageSegmented { page_size: 256 };
    let mut reader = crate::recovery::RawRecoveryReader::new(
        wal.backend(), &layout, 0, write_end,
    );
    // Buffer large enough for Begin (29) but not for Put (29+14+15=58).
    let mut small_buf = [0u8; 40];
    let first = reader.next_record(&mut small_buf).unwrap();
    assert!(first.is_some());
    // Second record doesn't fit in buffer.
    let second = reader.next_record(&mut small_buf);
    assert_eq!(second.unwrap_err(), Error::WalError);
}

// NOTE: Truncated-backend committed scan tests are not possible with
// MemoryIoBackend since its read() always succeeds on the full buffer.

/// Flat: `read_record` with small buffer returns None.
#[test]
fn flat_read_record_small_buffer() {
    use crate::flat;
    use crate::record::{serialize_into, RecordType};

    let mut storage = [0u8; 512];
    let mut backend = MemoryIoBackend::new(&mut storage);
    let mut tmp = [0u8; 512];
    let n = serialize_into(&mut tmp, 0, 1, RecordType::Put, b"key", b"val").unwrap();
    flat::write_record(&mut backend, 0, &tmp[..n]).unwrap();

    // Buffer too small for header.
    let mut tiny = [0u8; 10];
    let result = flat::read_record(&backend, 0, n as u64, &mut tiny);
    assert_eq!(result.unwrap_err(), Error::WalError);

    // Buffer large enough for header (29) but not for full record.
    let mut small = [0u8; 29];
    let result = flat::read_record(&backend, 0, n as u64, &mut small).unwrap();
    assert!(result.is_none());
}

// NOTE: Truncated-backend tests for read_record are not possible with
// MemoryIoBackend since read() returns from the full underlying buffer.

/// Flat: `CircularHeader` encode with small buffer returns `WalError`.
#[test]
fn circular_header_encode_small_buffer() {
    use crate::flat::CircularHeader;

    let hdr = CircularHeader {
        head_offset: 0,
        tail_offset: 0,
        checkpoint_lsn: 0,
    };
    let mut tiny = [0u8; 10];
    assert_eq!(hdr.encode(&mut tiny).unwrap_err(), Error::WalError);
}

/// Flat: `CircularHeader` decode with small buffer returns `WalError`.
#[test]
fn circular_header_decode_small_buffer() {
    use crate::flat::CircularHeader;

    let tiny = [0u8; 10];
    assert_eq!(CircularHeader::decode(&tiny).unwrap_err(), Error::WalError);
}

/// Flat: `CircularHeader` decode with wrong magic returns `WalError`.
#[test]
fn circular_header_decode_wrong_magic() {
    use crate::flat::CircularHeader;

    let mut buf = [0u8; 32];
    buf[0] = 0xFF; // wrong magic
    assert_eq!(CircularHeader::decode(&buf).unwrap_err(), Error::WalError);
}

/// Record: `deserialize_from` with buffer too small for body (header says
/// more key+value than buffer contains).
#[test]
fn record_deserialize_truncated_body() {
    use crate::record;

    let mut buf = [0u8; 64];
    let _n = record::serialize_into(&mut buf, 0, 1, record::RecordType::Put, b"key", b"val")
        .unwrap();
    // Truncate to just the header.
    let result = record::deserialize_from(&buf[..29]);
    assert_eq!(result.unwrap_err(), Error::WalError);
}

/// Record: `read_header` with buffer too small returns `WalError`.
#[test]
fn record_read_header_small_buffer() {
    use crate::record;

    let tiny = [0u8; 10];
    assert_eq!(record::read_header(&tiny).unwrap_err(), Error::WalError);
}

/// Record: `read_header` with wrong magic returns `WalError`.
#[test]
fn record_read_header_wrong_magic() {
    use crate::record;

    let mut buf = [0u8; 29];
    buf[0] = 0xFF;
    assert_eq!(record::read_header(&buf).unwrap_err(), Error::WalError);
}

/// `scan_for_magic` where no magic is found at all.
#[test]
fn scan_for_magic_not_found() {
    use crate::flat;

    let mut storage = [0u8; 256];
    let backend = MemoryIoBackend::with_len(&mut storage, 256).unwrap();
    let result = flat::scan_for_magic(&backend, 0, 256).unwrap();
    assert!(result.is_none());
}

/// Paged: `read_record_paged` with buffer too small for header.
#[test]
fn paged_read_record_small_buffer_header() {
    use iondb_core::page::PAGE_HEADER_SIZE;

    let mut storage = [0u8; 4096];
    let mut backend = MemoryIoBackend::new(&mut storage);
    // Write a record in paged format first.
    let mut writer = crate::paged::PagedWriter::new(256, 0);
    let mut tmp = [0u8; 512];
    let n = crate::record::serialize_into(
        &mut tmp, 0, 1, crate::record::RecordType::Begin, b"", b"",
    )
    .unwrap();
    writer.write_record(&mut backend, &tmp[..n]).unwrap();

    // Try reading with tiny buffer.
    let mut tiny = [0u8; 10];
    let result = crate::paged::read_record_paged(
        &backend, 0, PAGE_HEADER_SIZE, 256, 256, &mut tiny,
    );
    assert_eq!(result.unwrap_err(), Error::WalError);
}

/// Paged: `read_record_paged` with buffer too small for full record body.
#[test]
fn paged_read_record_small_buffer_body() {
    use iondb_core::page::PAGE_HEADER_SIZE;

    let mut storage = [0u8; 4096];
    let mut backend = MemoryIoBackend::new(&mut storage);
    let mut writer = crate::paged::PagedWriter::new(256, 0);
    let mut tmp = [0u8; 512];
    let n = crate::record::serialize_into(
        &mut tmp, 0, 1, crate::record::RecordType::Put, b"key_data", b"val_data",
    )
    .unwrap();
    writer.write_record(&mut backend, &tmp[..n]).unwrap();

    // Buffer large enough for header (29) but not for full record.
    let mut small = [0u8; 29];
    let result = crate::paged::read_record_paged(
        &backend, 0, PAGE_HEADER_SIZE, 256, 256, &mut small,
    );
    assert_eq!(result.unwrap_err(), Error::WalError);
}

// NOTE: Truncated-body tests for read_record_paged are not triggerable
// with MemoryIoBackend since read() returns from the full underlying buffer.

/// Paged: `verify_page` with truncated page (can't read checksum).
#[test]
fn paged_verify_page_truncated() {
    let mut storage = [0u8; 128];
    // Backend is smaller than page_size, so reading the checksum fails.
    let backend = MemoryIoBackend::with_len(&mut storage, 128).unwrap();
    let result = crate::paged::verify_page(&backend, 0, 256);
    assert_eq!(result.unwrap_err(), Error::WalError);
}

/// Paged recovery with committed scan where a record at near-boundary
/// claims to exceed the remaining usable area (exercises line 332-335
/// in `scan_committed_paged` in recovery.rs).
#[test]
fn paged_committed_scan_record_overflow_in_scan() {
    // Use a small page to make it easier to fill to near-boundary.
    let page_size = 128usize;
    // usable = 128 - 4 = 124 bytes per page. After header: 124 - 16 = 108 usable.

    let mut storage = [0u8; 8192];

    // Write real records using the paged writer that fill a page to near-end,
    // then manually craft a fake record header that claims a large key.
    {
        let mut backend = MemoryIoBackend::new(&mut storage);
        let mut writer = crate::paged::PagedWriter::new(page_size, 0);

        // Each Begin record is 29 bytes. Write 2 of them (58 bytes).
        // pos_in_page starts at 16, then 16+29=45, 45+29=74.
        // Remaining after 2: 124 - 74 = 50 bytes. >= RECORD_HEADER_SIZE (29).
        for i in 0..2u64 {
            let mut tmp = [0u8; 512];
            let n = crate::record::serialize_into(
                &mut tmp, i, i + 1, crate::record::RecordType::Begin, b"", b"",
            )
            .unwrap();
            writer.write_record(&mut backend, &tmp[..n]).unwrap();
        }
        // pos_in_page is now 74 (= 16 + 2*29).
        // Remaining = 124 - 74 = 50 bytes. Header fits (29 <= 50).
        // Write a fake header with key_len=50 => total=79 > 50 remaining.
        let pos = 74usize;
        storage[pos] = 0x57; // magic
        storage[pos + 1] = 0x4C;
        // record_type at 22: Begin=0 (valid)
        storage[pos + 22] = 0;
        // key_len at 23..25: 50 -> total = 29 + 50 = 79 > 50 remaining
        storage[pos + 23] = 50;
        storage[pos + 24] = 0;
    }

    let end = (page_size * 2) as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, end).unwrap();
    let layout = WalLayout::PageSegmented { page_size };

    let mut scratch = [0u64; 16];
    let result = crate::recovery::CommittedRecoveryReader::new(
        &backend, &layout, 0, end, &mut scratch,
    );
    assert!(result.is_ok());
}

/// Paged recovery with checkpoint record.
#[test]
fn paged_recovery_with_checkpoint() {
    let mut storage = [0u8; 16384];
    let backend = MemoryIoBackend::new(&mut storage);
    let mut wal = Wal::new(backend, paged_config(256)).unwrap();

    wal.begin_tx(1).unwrap();
    wal.put(1, b"k1", b"v1").unwrap();
    let commit_lsn = wal.commit_tx(1).unwrap();
    wal.checkpoint(commit_lsn).unwrap();

    wal.begin_tx(2).unwrap();
    wal.put(2, b"k2", b"v2").unwrap();
    wal.commit_tx(2).unwrap();

    let mut reader = wal.recover().unwrap();
    let mut buf = [0u8; 512];
    let mut count = 0;
    let mut found_checkpoint = false;
    while let Some(rec) = reader.next_record(&mut buf).unwrap() {
        if rec.record_type == crate::record::RecordType::Checkpoint {
            found_checkpoint = true;
        }
        count += 1;
    }
    assert!(count >= 4);
    assert!(found_checkpoint);
}
