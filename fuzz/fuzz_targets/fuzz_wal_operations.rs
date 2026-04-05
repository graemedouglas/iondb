#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use iondb_io::memory::MemoryIoBackend;
use iondb_wal::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
use iondb_wal::wal::Wal;

#[derive(Arbitrary, Debug)]
enum FuzzOp {
    Begin(u8),
    Put(u8, Vec<u8>, Vec<u8>),
    Delete(u8, Vec<u8>),
    Commit(u8),
    Rollback(u8),
    Checkpoint,
    Recover,
}

fuzz_target!(|ops: Vec<FuzzOp>| {
    if ops.len() > 100 { return; } // limit to avoid timeouts
    let mut storage = vec![0u8; 65536];
    let backend = MemoryIoBackend::new(&mut storage);
    let config = WalConfig {
        layout: WalLayout::Flat,
        sync_policy: SyncPolicy::EveryTransaction,
        truncation: TruncationMode::Logical,
    };
    if let Ok(mut wal) = Wal::new(backend, config) {
        for op in &ops {
            match op {
                FuzzOp::Begin(t) => { let _ = wal.begin_tx(*t as u64); }
                FuzzOp::Put(t, k, v) => {
                    let k = &k[..k.len().min(32)];
                    let v = &v[..v.len().min(64)];
                    let _ = wal.put(*t as u64, k, v);
                }
                FuzzOp::Delete(t, k) => {
                    let k = &k[..k.len().min(32)];
                    let _ = wal.delete(*t as u64, k);
                }
                FuzzOp::Commit(t) => { let _ = wal.commit_tx(*t as u64); }
                FuzzOp::Rollback(t) => { let _ = wal.rollback_tx(*t as u64); }
                FuzzOp::Checkpoint => {
                    let lsn = wal.current_lsn().saturating_sub(1);
                    let _ = wal.checkpoint(lsn);
                }
                FuzzOp::Recover => {
                    let mut scratch = [0u64; 256];
                    if let Ok(mut reader) = wal.recover_committed(&mut scratch) {
                        let mut buf = [0u8; 512];
                        while let Ok(Some(_)) = reader.next_record(&mut buf) {}
                    }
                }
            }
        }
    }
});
