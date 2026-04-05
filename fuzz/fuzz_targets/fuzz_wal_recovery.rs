#![no_main]

use libfuzzer_sys::fuzz_target;
use iondb_io::memory::MemoryIoBackend;
use iondb_wal::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
use iondb_wal::recovery::RawRecoveryReader;
use iondb_wal::wal::Wal;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 { return; }
    let mut storage = vec![0u8; 65536];

    // Write valid records
    {
        let backend = MemoryIoBackend::new(&mut storage);
        let config = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Logical,
        };
        if let Ok(mut wal) = Wal::new(backend, config) {
            let _ = wal.begin_tx(1);
            let _ = wal.put(1, b"key", b"value");
            let _ = wal.commit_tx(1);
        }
    }

    // Inject corruption from fuzzer data
    let corrupt_offset = (data[0] as usize) % 512;
    let corrupt_len = data.len().min(128).min(storage.len() - corrupt_offset);
    if corrupt_len > 1 {
        storage[corrupt_offset..corrupt_offset + corrupt_len - 1]
            .copy_from_slice(&data[1..corrupt_len]);
    }

    // Recovery must not panic
    let storage_len = storage.len() as u64;
    let backend = MemoryIoBackend::with_len(&mut storage, storage_len);
    if let Ok(backend) = backend {
        let mut reader = RawRecoveryReader::new(&backend, &WalLayout::Flat, 0, 512);
        let mut buf = [0u8; 512];
        while let Ok(Some(_)) = reader.next_record(&mut buf) {}
    }
});
