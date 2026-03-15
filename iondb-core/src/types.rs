//! Shared types used across all `IonDB` crates.

/// Transaction identifier.
pub type TxnId = u64;

/// Log sequence number for WAL ordering.
pub type Lsn = u64;

/// Page identifier within a storage file.
pub type PageId = u32;

/// Maximum key length in bytes.
pub const MAX_KEY_LEN: usize = 256;

/// Maximum value length in bytes.
pub const MAX_VALUE_LEN: usize = 65536;

/// Default page size in bytes (4 KiB).
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Minimum supported page size in bytes.
pub const MIN_PAGE_SIZE: usize = 64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_sizes() {
        assert_eq!(core::mem::size_of::<TxnId>(), 8);
        assert_eq!(core::mem::size_of::<Lsn>(), 8);
        assert_eq!(core::mem::size_of::<PageId>(), 4);
    }

    #[test]
    fn page_size_constraints() {
        const { assert!(DEFAULT_PAGE_SIZE >= MIN_PAGE_SIZE) };
        const { assert!(DEFAULT_PAGE_SIZE.is_power_of_two()) };
        const { assert!(MIN_PAGE_SIZE.is_power_of_two()) };
    }

    #[test]
    fn key_value_limits() {
        const { assert!(MAX_KEY_LEN > 0) };
        const { assert!(MAX_VALUE_LEN > 0) };
        const { assert!(MAX_VALUE_LEN >= MAX_KEY_LEN) };
    }
}
