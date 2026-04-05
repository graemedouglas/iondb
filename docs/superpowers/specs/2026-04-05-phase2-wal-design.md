# Phase 2 Design: WAL, Recovery, and Checkpointing

## Overview

The write-ahead log (WAL) lives in `iondb-wal`, depending only on `iondb-core`. It provides durable, crash-recoverable logging of all mutations. The WAL is generic over `IoBackend` and works across all three hardware tiers.

Key design decisions:

- **Two storage layouts**: flat sequential and page-segmented (user chooses)
- **Four sync policies**: `EveryRecord`, `EveryTransaction`, `Periodic(n)`, `None`
- **Three truncation modes**: logical-only, logical+physical (`std` only), circular buffer
- **Iterator-based recovery**: raw iterator (zero-alloc, single-pass) and committed-only filter (needs scratch buffer)
- **WAL does not own transaction lifecycle** — `TxnId` is provided by the caller

---

## Record Format

29-byte fixed header followed by variable-length key and value:

```
Offset  Size  Field
------  ----  -----
 0      2     magic (0x57 0x4C, "WL")
 2      4     crc32 (IEEE, over bytes 6..end)
 6      8     lsn (little-endian u64)
14      8     txn_id (little-endian u64)
22      1     record_type (u8)
23      2     key_len (little-endian u16)
25      4     val_len (little-endian u32)
29      var   key (key_len bytes)
29+K    var   value (val_len bytes)
```

Total record size: `29 + key_len + val_len`

The CRC covers everything from byte 6 to the end of the record (LSN through value). The magic bytes enable forward-scanning to find the next valid record after corruption.

The LSN is stored explicitly in the record (not derived from offset) so that it remains monotonic across all layout and truncation mode combinations, including circular wrap-around.

---

## Core Types

```rust
/// WAL record types
#[repr(u8)]
pub enum RecordType {
    Begin = 0,
    Put = 1,
    Delete = 2,
    Commit = 3,
    Rollback = 4,
    Checkpoint = 5,
}

/// Sync policy configuration
pub enum SyncPolicy {
    /// Sync after every record append
    EveryRecord,
    /// Sync after Commit/Rollback records
    EveryTransaction,
    /// Sync every N records
    Periodic(u32),
    /// Caller controls sync manually
    None,
}

/// Storage layout
pub enum WalLayout {
    /// Records appended back-to-back as a raw byte stream.
    /// Minimal overhead. Recovery after corruption requires
    /// magic-byte scanning to find next valid record.
    Flat,
    /// Records written into fixed-size WalSegment pages.
    /// Uses page headers/checksums from iondb-core page format.
    /// Natural recovery boundary per page. Records that don't
    /// fit in the current page start a new page (no spanning).
    PageSegmented { page_size: usize },
}

/// Truncation mode
pub enum TruncationMode {
    /// Track checkpoint_lsn. Recovery skips records before it.
    /// Dead space is not reclaimed. Works on all tiers.
    Logical,
    /// Logical + physically reclaim space by compacting or
    /// truncating the backing storage. Only available with std.
    #[cfg(feature = "std")]
    Physical,
    /// Fixed-size ring buffer. Head/tail pointers wrap around.
    /// Caller must checkpoint before tail overtakes unrecovered data.
    /// Only valid with WalLayout::Flat.
    Circular { capacity: usize },
}
```

---

## Supported Layout x Truncation Combinations

| | Logical | Physical (`std`) | Circular |
|---|---|---|---|
| **Flat** | Yes | Yes | Yes |
| **PageSegmented** | Yes | Yes | **No** |

Circular + PageSegmented is rejected at `Wal::new()` with `Error::WalError`. Rationale: pages spanning the circular wrap boundary adds unjustified complexity with no clear use case.

---

## WAL API

```rust
/// WAL configuration — validated at construction time.
pub struct WalConfig {
    pub layout: WalLayout,
    pub sync_policy: SyncPolicy,
    pub truncation: TruncationMode,
}

/// The write-ahead log, generic over I/O backend.
pub struct Wal<I: IoBackend> {
    backend: I,
    config: WalConfig,
    next_lsn: Lsn,
    checkpoint_lsn: Lsn,
    write_offset: u64,
    // Circular mode: head_offset, tail_offset
    // PageSegmented mode: current_page_offset, position_in_page
}

impl<I: IoBackend> Wal<I> {
    /// Create a new, empty WAL.
    /// Rejects invalid config combinations (e.g., Circular + PageSegmented).
    pub fn new(backend: I, config: WalConfig) -> Result<Self>;

    /// Open an existing WAL. Scans to find current write position
    /// and checkpoint_lsn. Does NOT replay records.
    /// Returns WalError if the backend is empty (use `new()` instead).
    pub fn open(backend: I, config: WalConfig) -> Result<Self>;

    // --- Record append (caller provides TxnId) ---

    /// Write a Begin record for the given transaction.
    pub fn begin_tx(&mut self, txn_id: TxnId) -> Result<Lsn>;

    /// Append a Put record.
    pub fn put(&mut self, txn_id: TxnId, key: &[u8], value: &[u8]) -> Result<Lsn>;

    /// Append a Delete record.
    pub fn delete(&mut self, txn_id: TxnId, key: &[u8]) -> Result<Lsn>;

    /// Write a Commit record. Syncs per policy.
    pub fn commit_tx(&mut self, txn_id: TxnId) -> Result<Lsn>;

    /// Write a Rollback record.
    pub fn rollback_tx(&mut self, txn_id: TxnId) -> Result<Lsn>;

    // --- Sync and checkpoint ---

    /// Force sync to backend regardless of sync policy.
    pub fn sync(&mut self) -> Result<()>;

    /// Write a Checkpoint record at the given LSN and truncate
    /// per the configured truncation mode. The caller is responsible
    /// for ensuring all dirty pages up to `up_to_lsn` have been
    /// flushed before calling this.
    pub fn checkpoint(&mut self, up_to_lsn: Lsn) -> Result<()>;

    // --- Recovery ---

    /// Create a raw recovery reader. Reads ALL valid records from
    /// checkpoint_lsn forward, including uncommitted transactions.
    /// Single-pass, zero-alloc. Does NOT implement Iterator (see
    /// Recovery Readers section for rationale).
    pub fn recover(&self) -> Result<RawRecoveryReader<'_, I>>;

    /// Create a filtered recovery reader. Reads only records from
    /// committed transactions. Uses caller-provided scratch buffer
    /// to track in-flight transaction IDs. Returns WalError if
    /// more concurrent transactions are encountered than scratch
    /// buffer slots.
    pub fn recover_committed<'a>(
        &'a self,
        scratch: &'a mut [TxnId],
    ) -> Result<CommittedRecoveryReader<'a, I>>;

    /// Convenience: collect all committed records into a Vec.
    /// Only available with `alloc` feature.
    #[cfg(feature = "alloc")]
    pub fn recover_committed_to_vec(&self) -> Result<alloc::vec::Vec<OwnedWalRecord>>;

    // --- Queries ---

    /// Next LSN that will be assigned.
    pub fn current_lsn(&self) -> Lsn;

    /// LSN of the last checkpoint.
    pub fn checkpoint_lsn(&self) -> Lsn;

    /// Remaining capacity in bytes. None if unbounded (Logical
    /// truncation with a growable backend).
    pub fn remaining(&self) -> Option<usize>;
}
```

### Recovery Readers

Recovery readers use a `next_record()` method instead of the standard `Iterator`
trait. This avoids the lending/streaming iterator problem: `WalRecord` borrows
key/value data from an internal read buffer, and each `next_record()` call
overwrites the previous record's data. The standard `Iterator` trait requires
independently-owned items, which would force either heap allocation or data
copying — neither acceptable for `no_std` zero-alloc operation.

Callers process one record at a time:

```rust
let mut reader = wal.recover()?;
let mut buf = [0u8; 512]; // caller-provided read buffer
while let Some(record) = reader.next_record(&mut buf)? {
    // process record — must finish before next call
}
```

```rust
/// Reads all valid records from checkpoint_lsn forward.
/// Skips corrupted records (CRC mismatch) and incomplete
/// records (truncated writes). For flat layout, uses magic-byte
/// scanning to find the next valid record after corruption.
pub struct RawRecoveryReader<'a, I: IoBackend> {
    backend: &'a I,
    offset: u64,
    end: u64,
    layout: &'a WalLayout,
}

impl<'a, I: IoBackend> RawRecoveryReader<'a, I> {
    /// Read the next valid record into `buf`. Returns None at end-of-log.
    /// `buf` must be large enough for the largest expected record
    /// (29 + key_len + val_len). WalRecord borrows from `buf`.
    pub fn next_record<'buf>(
        &mut self,
        buf: &'buf mut [u8],
    ) -> Result<Option<WalRecord<'buf>>>;
}

/// Filters to committed transactions only. Two-pass:
///   Pass 1 (at construction): scan record headers (29-byte fixed
///          stack buffer, no caller buffer needed) to collect
///          committed TxnIds into the caller-provided scratch buffer.
///   Pass 2 (next_record calls): yield records whose TxnId is in
///          the committed set, read into caller-provided buffer.
/// Returns WalError if scratch buffer is exhausted during pass 1.
pub struct CommittedRecoveryReader<'a, I: IoBackend> {
    backend: &'a I,
    scratch: &'a [TxnId],   // filled during pass 1
    committed_count: usize,  // number of valid entries in scratch
    offset: u64,
    end: u64,
    layout: &'a WalLayout,
}

impl<'a, I: IoBackend> CommittedRecoveryReader<'a, I> {
    /// Read the next committed record into `buf`. Returns None at end-of-log.
    /// Skips records from uncommitted/rolled-back transactions.
    pub fn next_record<'buf>(
        &mut self,
        buf: &'buf mut [u8],
    ) -> Result<Option<WalRecord<'buf>>>;
}

/// Owned version of WalRecord for use with `alloc`.
/// Key and value are heap-allocated Vec<u8>.
#[cfg(feature = "alloc")]
pub struct OwnedWalRecord {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub record_type: RecordType,
    pub key: alloc::vec::Vec<u8>,
    pub value: alloc::vec::Vec<u8>,
}
```

---

## Storage Layouts

### Flat Layout

Records are appended sequentially to the `IoBackend`:

```
[Record 0][Record 1][Record 2]...[Record N]
                                           ^ write_offset
```

- Minimal overhead (no page headers).
- Recovery after corruption: scan forward from the corrupted record, looking for magic bytes `0x57 0x4C` followed by a valid CRC. False positives in payload data are rejected by CRC check.
- Truncation (logical): `checkpoint_lsn` advances, recovery starts from there.
- Truncation (circular): write wraps to offset 0 when capacity is reached. A header at offset 0 stores the current head/tail offsets and checkpoint LSN.

### Page-Segmented Layout

Records are written into fixed-size `WalSegment` pages:

```
[Page Header (16B)][Record][Record]...[Padding][Checksum (4B)]
[Page Header (16B)][Record][Record]...[Padding][Checksum (4B)]
```

- Uses `PageType::WalSegment` and the existing page header/checksum format.
- Records do NOT span page boundaries. If a record doesn't fit in the remaining space of the current page, the page is padded and a new page is started.
- Recovery boundary: corruption in one page does not affect subsequent pages. Recovery skips the corrupted page and continues from the next page boundary.
- Overhead: 20 bytes per page (16-byte header + 4-byte checksum) plus padding waste.
- Page size must be large enough to hold the 29-byte record header plus at least 1 byte of key. Validated at construction.

---

## Truncation Modes

### Logical

- Checkpoint LSN is discovered by scanning for Checkpoint records during `Wal::open()`. The most recent Checkpoint record's LSN is the checkpoint LSN. If no Checkpoint record exists, recovery starts from the beginning.
- For page-segmented layout, the first page's header `lsn` field is updated to the checkpoint LSN as an optimization (avoids full scan on open).
- Recovery starts from `checkpoint_lsn`, ignoring all prior records.
- No space is reclaimed. The WAL grows monotonically.
- Suitable for all tiers. Simplest mode.

### Physical (`std` only)

- After writing the checkpoint record, compacts the backing storage using a crash-safe two-phase approach:
  1. Write and sync the new checkpoint pointer (checkpoint LSN + offset of live data). This is the commit point — if we crash after this, recovery knows where valid data starts regardless of compaction state.
  2. Copy live data (from checkpoint forward) to the beginning of the backend.
  3. Truncate the backend to the new size.
  4. Update the checkpoint pointer to reflect the new offsets. Sync.
- If a crash occurs during step 2 or 3, recovery reads the checkpoint pointer from step 1 and finds valid data at the original (pre-compaction) offsets. The incomplete compaction is detected and either retried or abandoned.
- Reclaims disk space on Tier 3 systems.
- Only available when the `std` feature is enabled (compile-time gated).

### Circular

- The WAL occupies a fixed-size buffer that wraps around.
- A 32-byte header block at offset 0 (separate from records):

```
Offset  Size  Field
------  ----  -----
 0      4     magic (0x57 0x4C 0x43 0x52, "WLCR")
 4      4     crc32 (IEEE, over bytes 8..32)
 8      8     head_offset (little-endian u64, next write position)
16      8     tail_offset (little-endian u64, oldest live record)
24      8     checkpoint_lsn (little-endian u64)
```

- Records start at offset 32 (after the header block).
- Writes advance the head. When head reaches capacity, it wraps to offset 32.
- `checkpoint()` advances the tail to the checkpoint LSN's offset, freeing space. Updates the header block atomically (write + sync).
- If head would overtake tail (WAL full), returns `Error::CapacityExhausted`.
- `remaining()` returns the free space between head and tail.
- Caller is responsible for checkpointing frequently enough to avoid exhaustion.
- Only valid with `WalLayout::Flat`.

---

## Sync Policy Behavior

| Policy | When `sync()` is called on backend |
|---|---|
| `EveryRecord` | After every `put`, `delete`, `begin_tx`, `commit_tx`, `rollback_tx` |
| `EveryTransaction` | After `commit_tx` and `rollback_tx` only |
| `Periodic(n)` | After every N record appends (count resets after sync) |
| `None` | Only when caller explicitly calls `wal.sync()` |

The write-ahead invariant (**no dirty page is flushed before its log record is synced**) is the caller's responsibility to enforce. The WAL provides the `sync()` method and the sync policies; the buffer pool / facade coordinates the ordering.

---

## Checkpoint Flow

Checkpointing is a coordinated operation between the WAL and the caller (buffer pool / facade):

1. **Caller** flushes all dirty pages with LSN <= target LSN to storage.
2. **Caller** calls `wal.checkpoint(target_lsn)`.
3. **WAL** writes a Checkpoint record.
4. **WAL** updates stored `checkpoint_lsn`.
5. **WAL** truncates per configured mode:
   - Logical: updates the stored checkpoint pointer.
   - Physical: compacts and truncates.
   - Circular: advances tail offset.

---

## Recovery Flow

On startup after a crash:

1. Call `Wal::open(backend, config)` — scans to find write position and checkpoint LSN.
2. Call `wal.recover()` or `wal.recover_committed(&mut scratch)`.
3. Iterate records. Apply committed mutations to storage engine.
4. WAL is now ready for new appends at the correct position.

### Corruption Handling

- **CRC mismatch**: Record is skipped. In flat layout, magic-byte scan finds the next record. In page-segmented layout, skip to next page boundary.
- **Truncated record** (incomplete write): Detected when record header indicates more bytes than remain. Treated as end-of-log.
- **Corrupted length fields**: In flat layout, magic-byte scan recovers. In page-segmented layout, page checksum catches the corruption and recovery skips to next page.

---

## Feature Flags

| Flag | Effect |
|---|---|
| `std` | Enables `TruncationMode::Physical`. Pulls in `iondb-core/std`. |
| `alloc` | Enables `Vec`-backed internal buffers for recovery. Pulls in `iondb-core/alloc`. |

Without `alloc`, all operations use caller-provided buffers. The API is fully functional in `no_std` without heap.

---

## Testing Strategy

### Unit Tests (in `iondb-wal/src/`)

**Record format:**
- Serialization/deserialization round-trip for all `RecordType` variants
- CRC computation and validation — correct CRC passes, flipped bit detected
- Magic bytes — present at start of every serialized record
- Records at max key/value sizes (`MAX_KEY_LEN`, `MAX_VALUE_LEN`)
- Zero-length key and value

**Layout — Flat:**
- Sequential append and read-back
- Magic-byte scanning finds next valid record after injected corruption
- Circular wrap-around: write past capacity, verify wrap and correct read-back
- Circular header block: head/tail/checkpoint persisted correctly

**Layout — Page-Segmented:**
- Records packed into pages respecting page boundaries
- Record that doesn't fit starts a new page (padding is correct)
- Page checksum validated on read
- Corruption in page N does not affect recovery of page N+1
- Minimum page size validation at construction

**Truncation:**
- Logical: checkpoint_lsn advances, recovery skips old records
- Physical (`std` only): backing storage is compacted, offsets reset
- Circular: tail advances on checkpoint, remaining() reflects freed space
- Circular exhaustion: returns `CapacityExhausted` when full

**Sync policy:**
- `EveryRecord`: sync called after each append
- `EveryTransaction`: sync called only after commit/rollback
- `Periodic(n)`: sync called every N records
- `None`: sync never called implicitly

**Config validation:**
- Circular + PageSegmented rejected at construction
- Page size too small for minimum record rejected

**Queries:**
- `current_lsn()` increments monotonically
- `checkpoint_lsn()` reflects last checkpoint
- `remaining()` correct for circular; `None` for logical

### Recovery Tests (using `MemoryIoBackend`)

- Clean log: all records recovered in LSN order
- Incomplete final record (truncated write): treated as end-of-log
- Corrupted CRC mid-log (flat): record skipped, magic-byte scan finds next
- Corrupted page (page-segmented): page skipped, next page recovered intact
- Multiple interleaved transactions: `recover_committed()` yields only committed
- Incomplete transaction (Begin + Put, no Commit): filtered by `recover_committed()`
- Checkpoint recovery: only records after `checkpoint_lsn` yielded
- Empty WAL: iterator yields nothing
- Open-append-recover cycle: crash -> open -> new writes -> recover -> all committed data present
- Scratch buffer exhaustion: more in-flight txns than slots -> `WalError`

### Layout Equivalence (proptest)

- Generate random operation sequences (begin, put, delete, commit, rollback)
- Execute on both flat and page-segmented layouts
- Recover from both, assert identical record sequences

### Sync Policy x Crash Matrix (using `FailpointIoBackend`)

For each sync policy:
- Crash at various write counts
- Verify: with `EveryTransaction`, committed data survives; with `None`, unsynced data may be lost
- Verify: no partial/corrupted records visible after recovery

### Crash Simulation Property Tests (50+ scenarios, proptest + `FailpointIoBackend`)

- Random operation sequences with fault injection at random write counts
- After fault: re-open WAL, run `recover_committed()`, verify:
  - Only committed transactions are returned
  - Records within each transaction are in correct order
  - No duplicate records
  - LSNs are monotonically increasing
- Partial writes: incomplete records detected and skipped
- Multiple crash/recover cycles: WAL state remains consistent
- Circular mode: crash during wrap-around, verify recovery handles boundary
- Deterministic seeds for CI reproducibility

### Fuzz Targets (`tests/fuzz/`)

- `fuzz_wal_record_decode`: Arbitrary bytes to record deserialization. No panics.
- `fuzz_wal_recovery`: Write valid records, inject random byte corruption, run recovery. No panics, all returned records have valid CRCs.
- `fuzz_wal_operations`: Random sequence of WAL operations (append, checkpoint, recover) with random configs. No panics, invariants hold.

### Structural Tests

- `iondb-wal` compiles for `thumbv6m-none-eabi` with no features
- `iondb-wal` does not depend on any implementation crate other than `iondb-core`
- Zero warnings with `RUSTFLAGS="-D warnings"`

---

## Files to Create/Modify

### New files in `iondb-wal/src/`:

| File | Contents |
|---|---|
| `record.rs` | `RecordType`, `WalRecord`, serialization/deserialization, CRC |
| `config.rs` | `WalConfig`, `SyncPolicy`, `WalLayout`, `TruncationMode`, validation |
| `wal.rs` | `Wal<I>` struct, append methods, sync, checkpoint |
| `recovery.rs` | `RawRecoveryIter`, `CommittedRecoveryIter` |
| `flat.rs` | Flat layout internals: sequential I/O, magic-byte scanning, circular header |
| `paged.rs` | Page-segmented layout internals: page packing, page-boundary recovery |

### Modified files:

| File | Change |
|---|---|
| `iondb-wal/src/lib.rs` | Module declarations, public re-exports |
| `iondb-wal/Cargo.toml` | Add dev-dependencies (proptest, iondb-io for FailpointIoBackend) |

### New test/fuzz files:

| File | Contents |
|---|---|
| `tests/fuzz/fuzz_wal_record_decode.rs` | Fuzz target for record deserialization |
| `tests/fuzz/fuzz_wal_recovery.rs` | Fuzz target for recovery with corruption |
| `tests/fuzz/fuzz_wal_operations.rs` | Fuzz target for operation sequences |
