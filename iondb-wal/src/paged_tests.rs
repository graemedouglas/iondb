//! Unit tests for the page-segmented WAL layout.

// Tests use unwrap and discard some return values for brevity; panics are
// acceptable in test code.
#![allow(unused_results, clippy::unwrap_used)]

use crate::paged::{read_record_paged, verify_page, PagedWriter};
use crate::record::{serialize_into, RecordType};
use iondb_core::error::Error;
use iondb_core::page::PAGE_HEADER_SIZE;
use iondb_core::traits::io_backend::IoBackend;
use iondb_io::memory::MemoryIoBackend;

/// Serialise a Put record into a stack buffer and return the serialised bytes.
fn make_put_record(lsn: u64, key: &[u8], value: &[u8]) -> ([u8; 512], usize) {
    let mut buf = [0u8; 512];
    // unwrap acceptable in tests
    let n = serialize_into(&mut buf, lsn, 0, RecordType::Put, key, value).unwrap();
    (buf, n)
}

/// Write a single Put record, finalise the page, then read it back.
#[test]
fn write_and_read_single_record() {
    let mut storage = [0u8; 4096];
    let mut backend = MemoryIoBackend::new(&mut storage);

    let page_size = 256usize;
    let mut writer = PagedWriter::new(page_size, 0);

    let (buf, n) = make_put_record(1, b"hello", b"world");
    let record_offset = writer.write_record(&mut backend, &buf[..n]).unwrap();
    writer.finalize_page(&mut backend).unwrap();

    // The record should start after the page header.
    assert_eq!(record_offset, PAGE_HEADER_SIZE as u64);

    // Read it back.
    let end = writer.current_offset();
    let mut read_buf = [0u8; 512];
    let result =
        read_record_paged(&backend, 0, PAGE_HEADER_SIZE, page_size, end, &mut read_buf).unwrap();

    let (rec, _, _) = result.unwrap();
    assert_eq!(rec.lsn, 1);
    assert_eq!(rec.record_type, RecordType::Put);
    assert_eq!(rec.key, b"hello");
    assert_eq!(rec.value, b"world");
}

/// A record that exceeds the usable space per page is rejected with
/// [`Error::WalError`].
#[test]
fn record_too_large_for_page_rejected() {
    let mut storage = [0u8; 4096];
    let mut backend = MemoryIoBackend::new(&mut storage);

    // page_size = 64 → usable = 44 bytes.
    let page_size = 64usize;
    let mut writer = PagedWriter::new(page_size, 0);

    // Build a record that is definitely larger than 44 bytes.
    let key = [0u8; 30];
    let value = [0u8; 30];
    let (buf, n) = make_put_record(1, &key, &value);

    let result = writer.write_record(&mut backend, &buf[..n]);
    assert_eq!(result, Err(Error::WalError));
}

/// Writing records across multiple pages (auto page-flip) round-trips
/// correctly for all records.
#[test]
fn auto_new_page_when_full() {
    let mut storage = [0u8; 4096];
    let mut backend = MemoryIoBackend::new(&mut storage);

    let page_size = 128usize;
    let mut writer = PagedWriter::new(page_size, 0);

    // Each record: RECORD_HEADER_SIZE (29) + 1 + 1 = 31 bytes.
    // Usable per page: 128 - 20 = 108 bytes → fits 3 records per page
    // (3 * 31 = 93 ≤ 108). Ten records → 4 pages.
    for i in 0u8..10 {
        let key = [i; 1];
        let value = [i + 100; 1];
        let (buf, n) = make_put_record(u64::from(i), &key, &value);
        let _offset = writer.write_record(&mut backend, &buf[..n]).unwrap();
    }
    writer.finalize_page(&mut backend).unwrap();

    let end = writer.current_offset();

    // Read all records back.
    let mut count = 0u8;
    let mut cur_page: u64 = 0;
    let mut cur_pos: usize = PAGE_HEADER_SIZE;
    let mut read_buf = [0u8; 512];

    loop {
        match read_record_paged(&backend, cur_page, cur_pos, page_size, end, &mut read_buf).unwrap()
        {
            None => break,
            Some((rec, np, npos)) => {
                assert_eq!(rec.lsn, u64::from(count));
                assert_eq!(rec.key, &[count]);
                assert_eq!(rec.value, &[count + 100]);
                cur_page = np;
                cur_pos = npos;
                count += 1;
            }
        }
    }

    assert_eq!(count, 10);
}

/// `verify_page` returns [`Error::Corruption`] after a byte is corrupted.
#[test]
fn corruption_in_page_detected() {
    let mut storage = [0u8; 4096];
    let mut backend = MemoryIoBackend::new(&mut storage);

    let page_size = 256usize;
    let mut writer = PagedWriter::new(page_size, 0);

    let (buf, n) = make_put_record(99, b"key", b"val");
    let _record_offset = writer.write_record(&mut backend, &buf[..n]).unwrap();
    writer.finalize_page(&mut backend).unwrap();

    // Page should verify cleanly before corruption.
    verify_page(&backend, 0, page_size).unwrap();

    // Corrupt a byte in the payload area.
    let _written = backend.write(PAGE_HEADER_SIZE as u64, &[0xFF]).unwrap();

    // Verification must now report corruption.
    let result = verify_page(&backend, 0, page_size);
    assert_eq!(result, Err(Error::Corruption));
}
