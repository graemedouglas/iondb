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
