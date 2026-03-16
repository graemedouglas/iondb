//! # edge-config — Tier 2 Dogfood Application
//!
//! A `no_std + alloc` application skeleton for an ESP32-class target that
//! stores and retrieves device configuration key-value pairs with
//! transactional updates.
//!
//! ## Phase 1 status
//!
//! Hash-table config store skeleton using extendible hashing for O(1) lookups.
//! Demonstrates `get`/`put`/`delete` on the `ExtendibleHashEngine`.

use iondb_core::StorageEngine;
use iondb_storage::hash::extendible::ExtendibleHashEngine;

fn main() {
    let mut buf = [0u8; 8192];
    let Some(mut store) = ExtendibleHashEngine::new(&mut buf, 256) else {
        return;
    };

    // Store device configuration
    if store.put(b"wifi.ssid", b"MyNetwork").is_err() {
        return;
    }
    if store.put(b"wifi.pass", b"secret123").is_err() {
        return;
    }
    if store.put(b"device.name", b"sensor-01").is_err() {
        return;
    }
    if store.put(b"calibration.offset", b"0.5").is_err() {
        return;
    }

    // Verify retrieval
    if let Ok(Some(ssid)) = store.get(b"wifi.ssid") {
        assert_eq!(ssid, b"MyNetwork");
    }

    // Update a config value
    if store.put(b"device.name", b"sensor-02").is_err() {
        return;
    }
    if let Ok(Some(name)) = store.get(b"device.name") {
        assert_eq!(name, b"sensor-02");
    }

    // Delete a config entry
    let _ = store.delete(b"calibration.offset");

    let stats = store.stats();
    assert_eq!(stats.key_count, 3);
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn config_store_round_trip() {
        let mut buf = [0u8; 8192];
        let mut store = ExtendibleHashEngine::new(&mut buf, 256).unwrap();

        assert_eq!(store.put(b"wifi.ssid", b"TestNet"), Ok(()));
        assert_eq!(store.put(b"wifi.pass", b"pass123"), Ok(()));
        assert_eq!(store.get(b"wifi.ssid"), Ok(Some(b"TestNet".as_slice())));
        assert_eq!(store.get(b"wifi.pass"), Ok(Some(b"pass123".as_slice())));
        assert_eq!(store.stats().key_count, 2);
    }

    #[test]
    fn config_store_update() {
        let mut buf = [0u8; 8192];
        let mut store = ExtendibleHashEngine::new(&mut buf, 256).unwrap();

        assert_eq!(store.put(b"key", b"old"), Ok(()));
        assert_eq!(store.put(b"key", b"new"), Ok(()));
        assert_eq!(store.get(b"key"), Ok(Some(b"new".as_slice())));
        assert_eq!(store.stats().key_count, 1);
    }

    #[test]
    fn config_store_delete() {
        let mut buf = [0u8; 8192];
        let mut store = ExtendibleHashEngine::new(&mut buf, 256).unwrap();

        assert_eq!(store.put(b"key", b"val"), Ok(()));
        assert_eq!(store.delete(b"key"), Ok(true));
        assert_eq!(store.get(b"key"), Ok(None));
        assert_eq!(store.delete(b"key"), Ok(false));
    }
}
