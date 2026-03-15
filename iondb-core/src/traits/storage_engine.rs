//! The `StorageEngine` trait — pluggable storage backend interface.

use crate::error;

/// Statistics about a storage engine instance.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    /// Total number of keys stored.
    pub key_count: u64,
    /// Total bytes used by keys and values.
    pub data_bytes: u64,
    /// Number of pages allocated.
    pub page_count: u32,
}

/// Pluggable storage backend interface.
///
/// All storage data structures (B+ tree, hash table, etc.) implement this
/// trait. Users may also implement it for custom storage structures.
pub trait StorageEngine {
    /// Retrieve the value associated with `key`, if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails.
    fn get(&self, key: &[u8]) -> error::Result<Option<&[u8]>>;

    /// Insert or update a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails (e.g., capacity exhausted).
    fn put(&mut self, key: &[u8], value: &[u8]) -> error::Result<()>;

    /// Delete a key. Returns `true` if the key existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete operation fails.
    fn delete(&mut self, key: &[u8]) -> error::Result<bool>;

    /// Flush any buffered writes to the underlying storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush/sync operation fails.
    fn flush(&mut self) -> error::Result<()>;

    /// Return statistics about this engine instance.
    fn stats(&self) -> EngineStats;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_stats_default() {
        let stats = EngineStats::default();
        assert_eq!(stats.key_count, 0);
        assert_eq!(stats.data_bytes, 0);
        assert_eq!(stats.page_count, 0);
    }

    #[test]
    fn engine_stats_clone() {
        let stats = EngineStats {
            key_count: 10,
            data_bytes: 1024,
            page_count: 4,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.key_count, 10);
        assert_eq!(cloned.data_bytes, 1024);
        assert_eq!(cloned.page_count, 4);
    }
}
