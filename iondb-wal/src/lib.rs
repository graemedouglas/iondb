//! # iondb-wal
//!
//! Write-ahead log for `IonDB`.
//!
//! Provides sequential log append, CRC validation, crash recovery,
//! and checkpoint support. `no_std` compatible.
//!
//! ## Storage Layouts
//!
//! - **Flat**: Records appended back-to-back. Minimal overhead.
//! - **`PageSegmented`**: Records in fixed-size pages with per-page checksums.
//!   Corruption isolation per page.
//!
//! ## Sync Policies
//!
//! - **`EveryRecord`**: Maximum durability.
//! - **`EveryTransaction`**: Sync on commit/rollback.
//! - **`Periodic`**: Sync every N records.
//! - **`None`**: Caller controls sync.

#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

/// Re-export core dependency.
pub use iondb_core;

pub mod config;
pub mod flat;
pub mod paged;
pub mod record;
pub mod recovery;
pub mod wal;

// Re-exports will be added as modules are implemented:
// pub use config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
// pub use record::{RecordType, WalRecord, MAGIC, RECORD_HEADER_SIZE};
// pub use recovery::{CommittedRecoveryReader, RawRecoveryReader};
// #[cfg(feature = "alloc")]
// pub use recovery::OwnedWalRecord;
// pub use wal::Wal;
