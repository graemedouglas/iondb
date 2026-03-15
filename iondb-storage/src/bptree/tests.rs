extern crate alloc;
use super::*;

fn make_engine(buf: &mut [u8]) -> BTreeEngine<'_> {
    BTreeEngine::new(buf, 128).unwrap()
}

#[test]
fn new_valid() {
    let mut buf = [0u8; 1024];
    assert!(BTreeEngine::new(&mut buf, 128).is_some());
}

#[test]
fn new_invalid() {
    let mut buf = [0u8; 64];
    assert!(BTreeEngine::new(&mut buf, 128).is_none()); // too small
    let mut buf2 = [0u8; 256];
    assert!(BTreeEngine::new(&mut buf2, 65).is_none()); // not power of 2
    assert!(BTreeEngine::new(&mut buf2, 32).is_none()); // < MIN_PAGE_SIZE
}

#[test]
fn put_and_get() {
    let mut buf = [0u8; 4096];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.put(b"hello", b"world"), Ok(()));
    assert_eq!(e.get(b"hello"), Ok(Some(b"world".as_slice())));
}

#[test]
fn get_missing() {
    let mut buf = [0u8; 1024];
    let e = make_engine(&mut buf);
    assert_eq!(e.get(b"nope"), Ok(None));
}

#[test]
fn put_overwrite() {
    let mut buf = [0u8; 4096];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.put(b"k", b"v1"), Ok(()));
    assert_eq!(e.put(b"k", b"v2"), Ok(()));
    assert_eq!(e.get(b"k"), Ok(Some(b"v2".as_slice())));
    assert_eq!(e.stats().key_count, 1);
}

#[test]
fn delete_existing() {
    let mut buf = [0u8; 4096];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.put(b"k", b"v"), Ok(()));
    assert_eq!(e.delete(b"k"), Ok(true));
    assert_eq!(e.get(b"k"), Ok(None));
    assert_eq!(e.stats().key_count, 0);
}

#[test]
fn delete_missing() {
    let mut buf = [0u8; 1024];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.delete(b"nope"), Ok(false));
}

#[test]
fn sorted_order_many_keys() {
    let mut buf = [0u8; 8192];
    let mut e = make_engine(&mut buf);
    for i in (0u8..20).rev() {
        let k = [b'k', i + b'a'];
        assert_eq!(e.put(&k, &[i]), Ok(()));
    }
    for i in 0u8..20 {
        let k = [b'k', i + b'a'];
        assert_eq!(e.get(&k), Ok(Some([i].as_slice())));
    }
    assert_eq!(e.stats().key_count, 20);
}

#[test]
fn split_occurs_and_keys_survive() {
    let mut buf = [0u8; 16384];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
    // 64-byte pages are very small, forcing splits quickly
    for i in 0u8..30 {
        let k = [b'k', i / 10 + b'0', i % 10 + b'0'];
        assert_eq!(e.put(&k, &[i]), Ok(()), "insert {i} failed");
    }
    for i in 0u8..30 {
        let k = [b'k', i / 10 + b'0', i % 10 + b'0'];
        assert_eq!(e.get(&k), Ok(Some([i].as_slice())), "get {i} failed");
    }
}

#[test]
fn range_scan() {
    let mut buf = [0u8; 8192];
    let mut e = make_engine(&mut buf);
    for i in 0u8..10 {
        let k = [b'k', i + b'0'];
        assert_eq!(e.put(&k, &[i]), Ok(()));
    }
    let mut results = alloc::vec::Vec::new();
    e.range(b"k3", b"k7", |k, v| {
        results.push((k.to_vec(), v.to_vec()));
        true
    })
    .unwrap();
    assert_eq!(results.len(), 4); // k3, k4, k5, k6
    assert_eq!(results[0].0, b"k3");
    assert_eq!(results[3].0, b"k6");
}

#[test]
fn stats_accuracy() {
    let mut buf = [0u8; 4096];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.stats().key_count, 0);
    assert_eq!(e.stats().data_bytes, 0);
    assert_eq!(e.put(b"ab", b"cd"), Ok(()));
    assert_eq!(e.stats().key_count, 1);
    assert_eq!(e.stats().data_bytes, 4);
}

#[test]
fn flush_is_noop() {
    let mut buf = [0u8; 1024];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.flush(), Ok(()));
}

#[test]
fn empty_key_and_value() {
    let mut buf = [0u8; 1024];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.put(b"", b""), Ok(()));
    assert_eq!(e.get(b""), Ok(Some(b"".as_slice())));
}

#[test]
fn multiple_deletes() {
    let mut buf = [0u8; 4096];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.put(b"a", b"1"), Ok(()));
    assert_eq!(e.put(b"b", b"2"), Ok(()));
    assert_eq!(e.put(b"c", b"3"), Ok(()));
    assert_eq!(e.delete(b"b"), Ok(true));
    assert_eq!(e.get(b"a"), Ok(Some(b"1".as_slice())));
    assert_eq!(e.get(b"b"), Ok(None));
    assert_eq!(e.get(b"c"), Ok(Some(b"3".as_slice())));
    assert_eq!(e.stats().key_count, 2);
}

#[test]
fn capacity_exhaustion() {
    let mut buf = [0u8; 512];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
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
    assert!(i > 0); // at least some inserts succeeded
}
