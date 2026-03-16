use super::*;

fn make(buf: &mut [u8]) -> ExtendibleHashEngine<'_> {
    ExtendibleHashEngine::new(buf, 128).unwrap()
}

#[test]
fn new_valid() {
    let mut buf = [0u8; 2048];
    assert!(ExtendibleHashEngine::new(&mut buf, 128).is_some());
}

#[test]
fn new_invalid() {
    let mut buf = [0u8; 128];
    assert!(ExtendibleHashEngine::new(&mut buf, 128).is_none()); // need 4 pages
}

#[test]
fn put_and_get() {
    let mut buf = [0u8; 4096];
    let mut e = make(&mut buf);
    assert_eq!(e.put(b"hello", b"world"), Ok(()));
    assert_eq!(e.get(b"hello"), Ok(Some(b"world".as_slice())));
}

#[test]
fn get_missing() {
    let mut buf = [0u8; 2048];
    let e = make(&mut buf);
    assert_eq!(e.get(b"nope"), Ok(None));
}

#[test]
fn put_overwrite() {
    let mut buf = [0u8; 4096];
    let mut e = make(&mut buf);
    assert_eq!(e.put(b"k", b"v1"), Ok(()));
    assert_eq!(e.put(b"k", b"v2"), Ok(()));
    assert_eq!(e.get(b"k"), Ok(Some(b"v2".as_slice())));
    assert_eq!(e.stats().key_count, 1);
}

#[test]
fn delete_existing() {
    let mut buf = [0u8; 4096];
    let mut e = make(&mut buf);
    assert_eq!(e.put(b"k", b"v"), Ok(()));
    assert_eq!(e.delete(b"k"), Ok(true));
    assert_eq!(e.get(b"k"), Ok(None));
}

#[test]
fn delete_missing() {
    let mut buf = [0u8; 2048];
    let mut e = make(&mut buf);
    assert_eq!(e.delete(b"nope"), Ok(false));
}

#[test]
fn many_keys_with_splits() {
    // Large buffer needed to exercise multiple directory doublings.
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 65535];
    let mut e = ExtendibleHashEngine::new(&mut buf, 256).unwrap();
    for i in 0u16..50 {
        let k = i.to_be_bytes();
        assert_eq!(e.put(&k, &k), Ok(()), "insert {i} failed");
    }
    for i in 0u16..50 {
        let k = i.to_be_bytes();
        assert_eq!(e.get(&k), Ok(Some(k.as_slice())), "get {i} failed");
    }
    assert_eq!(e.stats().key_count, 50);
}

#[test]
fn stats_accuracy() {
    let mut buf = [0u8; 4096];
    let mut e = make(&mut buf);
    assert_eq!(e.stats().key_count, 0);
    assert_eq!(e.put(b"ab", b"cd"), Ok(()));
    assert_eq!(e.stats().key_count, 1);
    assert_eq!(e.stats().data_bytes, 4);
}

#[test]
fn flush_is_noop() {
    let mut buf = [0u8; 2048];
    let mut e = make(&mut buf);
    assert_eq!(e.flush(), Ok(()));
}

#[test]
fn capacity_exhaustion() {
    // Tiny buffer: 4 pages * 64 bytes = 256 bytes
    let mut buf = [0u8; 256];
    let mut e = ExtendibleHashEngine::new(&mut buf, 64).unwrap();
    let mut i = 0u16;
    loop {
        let k = i.to_le_bytes();
        if e.put(&k, b"val").is_err() {
            break;
        }
        i += 1;
        if i > 200 {
            break; // safety net
        }
    }
    assert!(i > 0); // at least one key was inserted
}

#[test]
fn update_forces_split() {
    // 256-byte pages, 2048-byte buffer (8 pages)
    let mut buf = [0u8; 2048];
    let mut e = ExtendibleHashEngine::new(&mut buf, 256).unwrap();
    // Insert several keys to fill a bucket
    for i in 0u8..6 {
        let k = [b'k', i + b'a'];
        assert_eq!(e.put(&k, &[i]), Ok(()), "insert {i} failed");
    }
    // Update one key with a much larger value to force a bucket split
    let big_val = [0xABu8; 80];
    assert_eq!(e.put(b"ka", &big_val), Ok(()));
    assert_eq!(e.get(b"ka"), Ok(Some(big_val.as_slice())));
}

#[test]
fn update_with_larger_value_in_full_bucket() {
    // 64-byte pages give only ~36 usable bytes per bucket (header=24, CRC=4).
    // Each entry uses 8-byte slot + key_len + val_len, so a bucket fills fast.
    // We insert keys with small values, then update one with a value large
    // enough that it no longer fits after delete+reinsert, forcing the
    // split_bucket path inside put's update branch (lines 307-318).
    //
    // Use enough pages to allow multiple splits to succeed.
    // Stack array is large but bounded.
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 2048];
    let mut e = ExtendibleHashEngine::new(&mut buf, 64).unwrap();

    // Insert several small key-value pairs. With 64-byte pages the buckets
    // are tiny, so after a handful of inserts buckets will be near-full.
    let mut inserted = [false; 10];
    for i in 0u16..10 {
        let k = i.to_le_bytes();
        if e.put(&k, &[0xAA]).is_ok() {
            inserted[i as usize] = true;
        }
    }

    // Find a key that was successfully inserted and update it with a value
    // big enough to overflow the bucket after delete+reinsert, exercising
    // the split-on-update path. The value must be large enough to not fit
    // alongside other entries in the same bucket, but small enough to fit
    // in an empty bucket after splitting.
    let big_val = [0xBBu8; 16];
    let mut exercised = false;
    for i in 0u16..10 {
        if !inserted[i as usize] {
            continue;
        }
        let k = i.to_le_bytes();
        if e.put(&k, &big_val).is_ok() {
            // Verify the updated value is retrievable.
            let got = e.get(&k);
            assert_eq!(
                got,
                Ok(Some(big_val.as_slice())),
                "key {i} lost after update"
            );
            exercised = true;
            break;
        }
    }
    assert!(
        exercised,
        "expected at least one update to succeed via the split path"
    );
}

#[test]
fn directory_exhaustion() {
    // With 64-byte pages the directory page can hold at most
    // (64 - 16 - 4) / 4 = 11 entries. Starting at depth 1 (2 entries),
    // directory doubling goes 2 → 4 → 8 → 16 which exceeds 11, so the
    // third doubling must return CapacityExhausted (line 198).
    //
    // Use a 512-byte buffer (8 pages of 64 bytes) so we have a few
    // spare bucket pages but the directory page itself is the bottleneck.
    // Stack array is large but bounded.
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 4096];
    let mut e = ExtendibleHashEngine::new(&mut buf, 64).unwrap();

    let mut hit_capacity = false;
    for i in 0u16..200 {
        let k = i.to_le_bytes();
        match e.put(&k, &[0xCC]) {
            Ok(()) => {}
            Err(Error::CapacityExhausted) => {
                hit_capacity = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert!(
        hit_capacity,
        "should hit CapacityExhausted from directory exhaustion"
    );
}
