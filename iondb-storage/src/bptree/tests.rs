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
    }
    assert!(i > 0); // at least some inserts succeeded
}

#[test]
fn range_empty_tree() {
    let mut buf = [0u8; 1024];
    let e = make_engine(&mut buf);
    let mut count = 0usize;
    e.range(b"a", b"z", |_k, _v| {
        count += 1;
        true
    })
    .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn range_early_stop() {
    let mut buf = [0u8; 8192];
    let mut e = make_engine(&mut buf);
    for i in 0u8..5 {
        let k = [b'k', i + b'0'];
        assert_eq!(e.put(&k, &[i]), Ok(()));
    }
    let mut results = alloc::vec::Vec::new();
    e.range(b"k0", b"k5", |k, v| {
        results.push((k.to_vec(), v.to_vec()));
        results.len() < 2
    })
    .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn delete_missing_from_nonempty() {
    let mut buf = [0u8; 4096];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.put(b"a", b"1"), Ok(()));
    assert_eq!(e.put(b"c", b"3"), Ok(()));
    assert_eq!(e.delete(b"b"), Ok(false));
    assert_eq!(e.stats().key_count, 2);
}

#[test]
fn internal_node_split() {
    // 64-byte pages with large buffer to force 3+ level tree (internal node splits).
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 32768];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
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
fn range_single_leaf() {
    // All keys fit in one leaf, so range scan reaches leaf_next == NO_PAGE (line 370).
    let mut buf = [0u8; 1024];
    let mut e = make_engine(&mut buf);
    for i in 0u8..3 {
        let k = [b'k', i + b'0'];
        assert_eq!(e.put(&k, &[i]), Ok(()));
    }
    let mut results = alloc::vec::Vec::new();
    // Range that spans beyond all keys so we don't break on k >= end first.
    e.range(b"k0", b"z", |k, v| {
        results.push((k.to_vec(), v.to_vec()));
        true
    })
    .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b"k0");
    assert_eq!(results[2].0, b"k2");
}

#[test]
fn internal_node_split_large() {
    // 120 keys with 64-byte pages forces 3+ level tree with internal splits.
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 65536];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
    let count = 120u16;
    for i in 0..count {
        let k = i.to_be_bytes();
        assert_eq!(e.put(&k, &k), Ok(()), "insert {i} failed");
    }
    // Verify all keys survive
    for i in 0..count {
        let k = i.to_be_bytes();
        assert_eq!(e.get(&k), Ok(Some(k.as_slice())), "get {i} failed");
    }
    assert_eq!(e.stats().key_count, u64::from(count));

    let mut collected = alloc::vec::Vec::new();
    e.range(&0u16.to_be_bytes(), &count.to_be_bytes(), |k, _v| {
        collected.push(k.to_vec());
        true
    })
    .unwrap();
    assert_eq!(collected.len(), count as usize);
    for w in collected.windows(2) {
        assert!(w[0] < w[1], "keys not sorted: {:?} >= {:?}", w[0], w[1]);
    }

    let (first_leaf, _, _) = e.find_leaf(&0u16.to_be_bytes()).unwrap();
    let (mut cur, mut leaf_count) = (first_leaf, 1usize);
    loop {
        let pg = e.page(cur).unwrap();
        let next = node::leaf_next(pg).unwrap();
        if next == node::NO_PAGE {
            break;
        }
        let next_pg = e.page(next).unwrap();
        assert_eq!(node::leaf_prev(next_pg).unwrap(), cur);
        cur = next;
        leaf_count += 1;
    }
    assert!(leaf_count > 1, "expected multiple leaves");
    let first_pg = e.page(first_leaf).unwrap();
    assert_eq!(node::leaf_prev(first_pg).unwrap(), node::NO_PAGE);
}

#[test]
fn update_forces_leaf_split() {
    let mut buf = [0u8; 8192];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
    // Insert several small keys to nearly fill a leaf
    for i in 0u8..6 {
        let k = [b'k', i + b'a'];
        assert_eq!(e.put(&k, &[i]), Ok(()));
    }
    // Update one key with a much larger value to force a split
    let big_val = [0xABu8; 20];
    assert_eq!(e.put(b"kb", &big_val), Ok(()));
    assert_eq!(e.get(b"kb"), Ok(Some(big_val.as_slice())));
    assert_eq!(e.stats().key_count, 6);
}

#[test]
fn proptest_repro_key_lost_after_split() {
    // Regression: random key sequence that previously caused data corruption
    // in split_leaf_and_insert (slot/data area overlap).
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 65535];
    let mut e = BTreeEngine::new(&mut buf, 128).unwrap();
    let ops: &[(&[u8], &[u8])] = &[
        (b"\x00", b""),
        (b"\x00", b""),
        (b"\xc7", b""),
        (&[199, 0, 0, 0, 0, 0], &[0, 0, 0]),
        (
            &[0, 0, 0, 0, 0, 117, 199, 248, 120, 214, 78],
            &[52, 206, 159, 4, 76, 211, 251, 103],
        ),
        (
            &[
                190, 67, 32, 11, 91, 112, 126, 17, 115, 149, 36, 41, 217, 229, 13,
            ],
            &[116, 12, 215, 23, 117, 78, 104, 234, 219, 131, 217],
        ),
        (
            &[
                195, 103, 229, 220, 162, 119, 55, 134, 188, 8, 9, 238, 149, 145, 244,
            ],
            &[
                103, 244, 239, 79, 142, 38, 14, 223, 54, 107, 4, 179, 118, 253, 10,
            ],
        ),
    ];
    for (k, v) in ops {
        assert_eq!(e.put(k, v), Ok(()), "put({k:?}) failed");
    }
    for (k, _) in ops {
        assert!(e.get(k).unwrap().is_some(), "key {k:?} lost");
    }
}

#[test]
fn two_pages_mut_same_id() {
    // Corrupted state: same page ID for both pages.
    let mut buf = [0u8; 1024];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.two_pages_mut(1, 1), Err(Error::PageError));
}

#[test]
fn two_pages_mut_oob() {
    // Corrupted state: page ID beyond buffer bounds.
    let mut buf = [0u8; 1024];
    let mut e = make_engine(&mut buf);
    assert_eq!(e.two_pages_mut(1, 999), Err(Error::PageError));
}

#[test]
fn two_pages_mut_reversed() {
    // Exercise the a > b path directly (alloc_page never produces a > b).
    let mut buf = [0u8; 1024];
    let mut e = make_engine(&mut buf);
    let (pa, pb) = e.two_pages_mut(3, 1).unwrap();
    assert_eq!(pa.len(), 128); // page 3
    assert_eq!(pb.len(), 128); // page 1
}

#[test]
fn corrupted_tree_cycle_detection() {
    // Corrupt tree to create a cycle: internal node child → itself.
    // find_leaf should detect depth >= MAX_HEIGHT and return Corruption.
    let mut buf = [0u8; 16384];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
    for i in 0u8..20 {
        e.put(&[b'k', i / 10 + b'0', i % 10 + b'0'], &[i]).unwrap();
    }
    let root = e.root_id().unwrap();
    let pg = e.page(root).unwrap();
    let child = node::internal_left_child(pg).unwrap();
    // Corrupt child: change page type to Internal and point back to root
    {
        let p = e.page_mut(child).unwrap();
        p[0] = iondb_core::page::PageType::BTreeInternal.as_byte();
        endian::write_u16_le(&mut p[16..], 0).unwrap(); // count=0
        endian::write_u32_le(&mut p[20..], root).unwrap(); // left_child=root
    }
    assert_eq!(e.get(b"anything"), Err(Error::Corruption));
}

#[test]
fn split_adjustment_left_heavy() {
    // Force split point adjustment: large first entry makes naive midpoint
    // overshoot left-half capacity (cum[mid] > cap).
    let mut buf = [0u8; 4096];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
    // 3 small entries (cost 9 each = 27, fits in cap=32)
    e.put(&[0x01], &[]).unwrap();
    e.put(&[0x02], &[]).unwrap();
    e.put(&[0x03], &[]).unwrap();
    // Insert before all with large value (cost 24), triggers split.
    // Naive mid=2 gives cum[2]=33 > cap=32, so mid adjusts to 1.
    e.put(&[0x00], &[0xAA; 15]).unwrap();
    assert_eq!(e.get(&[0x00]), Ok(Some([0xAA; 15].as_slice())));
    assert_eq!(e.get(&[0x01]), Ok(Some([].as_slice())));
    assert_eq!(e.get(&[0x03]), Ok(Some([].as_slice())));
}

#[test]
fn proptest_repro2_key_lost_after_split() {
    // Regression from proptest: 12 puts with mixed key/value sizes.
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 65535];
    let mut engine = BTreeEngine::new(&mut buf, 128).unwrap();
    let ops: &[(&[u8], &[u8])] = &[
        (&[48, 32, 147, 53, 8, 218], &[0, 0, 0, 0, 0, 0, 0, 0]),
        (&[71], &[]),
        (&[48, 33, 0], &[0, 254]),
        (
            &[220, 30, 165, 128, 114, 173, 67, 184, 119],
            &[14, 223, 32, 12, 161, 47, 95, 32, 225, 136, 114, 146, 143],
        ),
        (
            &[
                239, 136, 163, 240, 218, 191, 81, 119, 119, 25, 27, 8, 58, 123,
            ],
            &[135, 209, 55, 173, 3, 145],
        ),
        (
            &[48, 32, 147, 53, 8, 217, 150, 32, 246, 16, 168, 125],
            &[74, 56, 130, 92, 123, 237, 82, 155, 71, 86, 32],
        ),
        (
            &[13, 37, 126, 177, 98, 236, 46, 164, 147, 44, 135],
            &[216, 12, 207, 115, 50, 205, 71, 206, 44, 252, 197, 26],
        ),
        (
            &[33, 234, 62, 125, 179, 96],
            &[167, 121, 152, 162, 224, 84, 69],
        ),
        (&[9, 52, 8, 122, 221, 216, 96, 142, 45, 249], &[]),
        (&[11, 234, 175, 4], &[]),
        (
            &[24, 65, 237, 26, 72, 218, 144, 103, 240, 111, 211, 114, 84],
            &[180, 105, 166, 246, 218, 168, 93, 252, 63, 142, 100, 102, 35],
        ),
        (
            &[
                20, 143, 142, 87, 101, 72, 189, 134, 49, 37, 69, 4, 63, 241, 248,
            ],
            &[75, 86, 27, 164, 75, 103, 201, 247, 174, 53, 116, 231],
        ),
    ];
    for (k, v) in ops {
        assert_eq!(engine.put(k, v), Ok(()), "put({k:?}) failed");
    }
    for (k, v) in ops {
        let got = engine.get(k).unwrap();
        assert_eq!(got, Some(*v), "key {k:?} lost");
    }
}

#[test]
fn internal_set_left_child_round_trip() {
    // Exercise the internal_set_left_child function (node.rs line 251).
    let mut page = [0u8; 128];
    node::internal_init(&mut page, 1, 42).unwrap();
    assert_eq!(node::internal_left_child(&page).unwrap(), 42);
    node::internal_set_left_child(&mut page, 99).unwrap();
    assert_eq!(node::internal_left_child(&page).unwrap(), 99);
}

#[test]
fn corrupted_internal_split_propagation() {
    // Corrupt internal node key slot so split_internal_and_insert fails
    // via propagate_split (mod.rs line 277 ? error path).
    #[allow(clippy::large_stack_arrays)]
    let mut buf = [0u8; 65536];
    let mut e = BTreeEngine::new(&mut buf, 64).unwrap();
    for i in 0u16..50 { e.put(&i.to_be_bytes(), &i.to_be_bytes()).unwrap(); }
    let search = 200u16.to_be_bytes();
    let (_leaf_id, stack, depth) = e.find_leaf(&search).unwrap();
    let parent_id = stack[depth - 1];
    // Corrupt first key's offset and shrink data_end so has_space is false.
    let p = e.page_mut(parent_id).unwrap();
    endian::write_u16_le(&mut p[node::INTL_HDR + 4..], 0xFFFF).unwrap();
    endian::write_u16_le(&mut p[18..], node::INTL_HDR as u16).unwrap();
    assert!(e.put(&search, &[0xBB; 30]).is_err());
}
