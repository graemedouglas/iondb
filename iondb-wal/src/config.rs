//! WAL configuration types: sync policy, storage layout, truncation mode.
//!
//! Use [`WalConfig`] to describe how a WAL instance should behave and call
//! [`WalConfig::validate`] before opening the log to detect invalid
//! combinations early.

use iondb_core::error::{Error, Result};
use iondb_core::page::PAGE_OVERHEAD;

use crate::record::RECORD_HEADER_SIZE;

/// Minimum circular-buffer capacity: enough to hold the WAL header (32 bytes)
/// plus one record header.
const MIN_CIRCULAR_CAPACITY: usize = 32 + RECORD_HEADER_SIZE;

// ── SyncPolicy ──────────────────────────────────────────────────────────────

/// Controls when the WAL is flushed to durable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Sync after every individual record is appended.
    ///
    /// Provides the strongest durability guarantee at the cost of throughput.
    EveryRecord,

    /// Sync on every transaction commit or rollback.
    ///
    /// A good balance between durability and write amplification for
    /// workloads that batch multiple records per transaction.
    EveryTransaction,

    /// Sync every `N` records.
    ///
    /// A value of `0` is invalid and will be rejected by [`WalConfig::validate`].
    Periodic(u32),

    /// Never sync automatically; the caller is responsible for flushing.
    ///
    /// Suitable for in-memory layouts or testing.
    None,
}

// ── WalLayout ────────────────────────────────────────────────────────────────

/// Storage layout used by the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalLayout {
    /// Records are appended back-to-back without any page framing.
    ///
    /// Minimal per-record overhead; suitable for flash or raw block devices
    /// where seeking is expensive.
    Flat,

    /// Records are packed into fixed-size pages, each with its own CRC-32
    /// checksum.
    ///
    /// Corruption is isolated to individual pages, making recovery more
    /// precise at the cost of some space overhead.
    PageSegmented {
        /// Size of each page in bytes.
        ///
        /// Must be at least `PAGE_OVERHEAD + RECORD_HEADER_SIZE`
        /// (`PAGE_OVERHEAD` = 20, `RECORD_HEADER_SIZE` = 29, so ≥ 49).
        page_size: usize,
    },
}

// ── TruncationMode ───────────────────────────────────────────────────────────

/// Strategy used to reclaim space in the WAL after checkpointing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncationMode {
    /// Remove committed records from the logical head of the log without
    /// immediately reclaiming the underlying storage.
    ///
    /// Portable across all targets including `no_std`.
    Logical,

    /// Physically truncate the underlying file to reclaim disk space.
    ///
    /// Only available when the `std` feature is enabled (requires a file
    /// system with `ftruncate` semantics).
    #[cfg(feature = "std")]
    Physical,

    /// Wrap around within a fixed-capacity buffer, overwriting the oldest
    /// committed records.
    ///
    /// Ideal for microcontroller environments where flash size is known at
    /// compile time.
    Circular {
        /// Total byte capacity of the circular buffer.
        ///
        /// Must be at least `32 + RECORD_HEADER_SIZE` (= 61) to hold the WAL
        /// header plus one record.
        capacity: usize,
    },
}

// ── WalConfig ────────────────────────────────────────────────────────────────

/// Complete configuration for a WAL instance.
///
/// Construct a value directly, then call [`WalConfig::validate`] before
/// opening the log.
///
/// # Example
///
/// ```rust
/// use iondb_wal::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
///
/// let cfg = WalConfig {
///     layout: WalLayout::Flat,
///     sync_policy: SyncPolicy::EveryTransaction,
///     truncation: TruncationMode::Logical,
/// };
/// cfg.validate().expect("valid config");
/// ```
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// How records are stored on the underlying medium.
    pub layout: WalLayout,

    /// When the WAL is synced to durable storage.
    pub sync_policy: SyncPolicy,

    /// How space is reclaimed after checkpointing.
    pub truncation: TruncationMode,
}

impl WalConfig {
    /// Validate the configuration, returning [`Error::WalError`] for any
    /// invalid combination.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WalError`] when any of the following conditions hold:
    ///
    /// | Condition | Reason |
    /// |---|---|
    /// | `Circular` + `PageSegmented` | Circular wrapping requires a contiguous flat address space. |
    /// | `PageSegmented` with `page_size < PAGE_OVERHEAD + RECORD_HEADER_SIZE` | Page too small to hold even one record. |
    /// | `Circular` with `capacity < 32 + RECORD_HEADER_SIZE` | Buffer too small to hold the WAL header plus one record. |
    /// | `Periodic(0)` | A period of zero would sync on every operation, use `EveryRecord` instead. |
    pub fn validate(&self) -> Result<()> {
        // Circular truncation is incompatible with page-segmented layout.
        if let TruncationMode::Circular { .. } = self.truncation {
            if let WalLayout::PageSegmented { .. } = self.layout {
                return Err(Error::WalError);
            }
        }

        // PageSegmented: page must be large enough to hold one record.
        if let WalLayout::PageSegmented { page_size } = self.layout {
            if page_size < PAGE_OVERHEAD + RECORD_HEADER_SIZE {
                return Err(Error::WalError);
            }
        }

        // Circular: buffer must hold WAL header + at least one record header.
        if let TruncationMode::Circular { capacity } = self.truncation {
            if capacity < MIN_CIRCULAR_CAPACITY {
                return Err(Error::WalError);
            }
        }

        // Periodic(0) is meaningless.
        if let SyncPolicy::Periodic(0) = self.sync_policy {
            return Err(Error::WalError);
        }

        Ok(())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    extern crate alloc;
    use super::*;
    use alloc::format;

    // ── helpers ─────────────────────────────────────────────────────────────

    fn flat_logical() -> WalConfig {
        WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Logical,
        }
    }

    fn min_page_size() -> usize {
        PAGE_OVERHEAD + RECORD_HEADER_SIZE
    }

    // ── valid configs ────────────────────────────────────────────────────────

    #[test]
    fn valid_flat_logical() {
        assert!(flat_logical().validate().is_ok());
    }

    #[test]
    fn valid_flat_circular() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryTransaction,
            truncation: TruncationMode::Circular {
                capacity: MIN_CIRCULAR_CAPACITY,
            },
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn valid_paged_logical() {
        let cfg = WalConfig {
            layout: WalLayout::PageSegmented {
                page_size: min_page_size(),
            },
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Logical,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn valid_paged_logical_large_page() {
        let cfg = WalConfig {
            layout: WalLayout::PageSegmented { page_size: 4096 },
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Logical,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn valid_circular_large_capacity() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Circular { capacity: 1024 },
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn valid_periodic_nonzero() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::Periodic(10),
            truncation: TruncationMode::Logical,
        };
        assert!(cfg.validate().is_ok());
    }

    // ── all sync policies construct without error ────────────────────────────

    #[test]
    fn sync_policy_every_record() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Logical,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn sync_policy_every_transaction() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryTransaction,
            truncation: TruncationMode::Logical,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn sync_policy_periodic_nonzero() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::Periodic(1),
            truncation: TruncationMode::Logical,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn sync_policy_none() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Logical,
        };
        assert!(cfg.validate().is_ok());
    }

    // ── rejected configs ─────────────────────────────────────────────────────

    #[test]
    fn reject_circular_plus_paged() {
        let cfg = WalConfig {
            layout: WalLayout::PageSegmented { page_size: 4096 },
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Circular { capacity: 1024 },
        };
        assert_eq!(cfg.validate(), Err(Error::WalError));
    }

    #[test]
    fn reject_paged_too_small() {
        let cfg = WalConfig {
            layout: WalLayout::PageSegmented {
                page_size: min_page_size() - 1,
            },
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Logical,
        };
        assert_eq!(cfg.validate(), Err(Error::WalError));
    }

    #[test]
    fn reject_paged_zero_page_size() {
        let cfg = WalConfig {
            layout: WalLayout::PageSegmented { page_size: 0 },
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Logical,
        };
        assert_eq!(cfg.validate(), Err(Error::WalError));
    }

    #[test]
    fn reject_circular_too_small() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Circular {
                capacity: MIN_CIRCULAR_CAPACITY - 1,
            },
        };
        assert_eq!(cfg.validate(), Err(Error::WalError));
    }

    #[test]
    fn reject_circular_zero_capacity() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Circular { capacity: 0 },
        };
        assert_eq!(cfg.validate(), Err(Error::WalError));
    }

    #[test]
    fn reject_periodic_zero() {
        let cfg = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::Periodic(0),
            truncation: TruncationMode::Logical,
        };
        assert_eq!(cfg.validate(), Err(Error::WalError));
    }

    // ── derive traits ────────────────────────────────────────────────────────

    #[test]
    fn sync_policy_derives() {
        let a = SyncPolicy::Periodic(5);
        let b = a;
        assert_eq!(a, b);
        let _ = format!("{a:?}");
    }

    #[test]
    fn wal_layout_derives() {
        let a = WalLayout::PageSegmented { page_size: 512 };
        let b = a;
        assert_eq!(a, b);
        let _ = format!("{a:?}");
    }

    #[test]
    fn truncation_mode_derives() {
        let a = TruncationMode::Circular { capacity: 256 };
        let b = a;
        assert_eq!(a, b);
        let _ = format!("{a:?}");
    }

    #[test]
    fn wal_config_derives() {
        let cfg = flat_logical();
        let cloned = cfg.clone();
        let _ = format!("{cfg:?}");
        // Validate both to confirm the clone is independent.
        assert!(cfg.validate().is_ok());
        assert!(cloned.validate().is_ok());
    }
}
