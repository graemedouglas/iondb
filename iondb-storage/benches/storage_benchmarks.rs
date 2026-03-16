//! Criterion benchmarks for `IonDB` storage engines.
//!
//! Baselines point reads, point writes, and (for B+ tree) range scans
//! across different page sizes.

#![allow(
    clippy::unwrap_used,
    clippy::wildcard_imports,
    unused_results,
    missing_docs
)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use iondb_core::traits::storage_engine::StorageEngine;

// ─── B+ Tree Benchmarks ────────────────────────────────────────────────────

#[cfg(feature = "storage-bptree")]
mod bptree_bench {
    use super::*;
    use iondb_storage::bptree::BTreeEngine;

    /// Pre-populate a B+ tree with `n` sequential keys and return the engine.
    fn populated_btree(buf: &mut [u8], page_size: usize, n: u16) -> BTreeEngine<'_> {
        let mut e = BTreeEngine::new(buf, page_size).unwrap();
        for i in 0..n {
            let k = i.to_be_bytes();
            e.put(&k, &k).unwrap();
        }
        e
    }

    pub fn bench_btree_put(c: &mut Criterion) {
        let mut group = c.benchmark_group("btree/put");
        for &ps in &[128, 256, 512] {
            group.bench_with_input(BenchmarkId::from_parameter(ps), &ps, |b, &ps| {
                b.iter(|| {
                    let mut buf = vec![0u8; 65536];
                    let mut e = BTreeEngine::new(&mut buf, ps).unwrap();
                    for i in 0u16..100 {
                        let k = i.to_be_bytes();
                        e.put(black_box(&k), black_box(&k)).unwrap();
                    }
                });
            });
        }
        group.finish();
    }

    pub fn bench_btree_get(c: &mut Criterion) {
        let mut group = c.benchmark_group("btree/get");
        for &ps in &[128, 256, 512] {
            group.bench_with_input(BenchmarkId::from_parameter(ps), &ps, |b, &ps| {
                let mut buf = vec![0u8; 65536];
                let e = populated_btree(&mut buf, ps, 100);
                b.iter(|| {
                    for i in 0u16..100 {
                        let k = i.to_be_bytes();
                        black_box(e.get(black_box(&k)).unwrap());
                    }
                });
            });
        }
        group.finish();
    }

    pub fn bench_btree_range(c: &mut Criterion) {
        let mut group = c.benchmark_group("btree/range");
        for &ps in &[128, 256, 512] {
            group.bench_with_input(BenchmarkId::from_parameter(ps), &ps, |b, &ps| {
                let mut buf = vec![0u8; 65536];
                let e = populated_btree(&mut buf, ps, 100);
                b.iter(|| {
                    let mut count = 0u32;
                    e.range(
                        black_box(&10u16.to_be_bytes()),
                        black_box(&90u16.to_be_bytes()),
                        |_k, _v| {
                            count += 1;
                            true
                        },
                    )
                    .unwrap();
                    black_box(count);
                });
            });
        }
        group.finish();
    }
}

// ─── Extendible Hash Benchmarks ─────────────────────────────────────────────

#[cfg(feature = "storage-hash-ext")]
mod ext_hash_bench {
    use super::*;
    use iondb_storage::hash::extendible::ExtendibleHashEngine;

    fn populated_ext(buf: &mut [u8], page_size: usize, n: u16) -> ExtendibleHashEngine<'_> {
        let mut e = ExtendibleHashEngine::new(buf, page_size).unwrap();
        for i in 0..n {
            let k = i.to_be_bytes();
            e.put(&k, &k).unwrap();
        }
        e
    }

    pub fn bench_ext_put(c: &mut Criterion) {
        let mut group = c.benchmark_group("ext_hash/put");
        for &ps in &[128, 256] {
            group.bench_with_input(BenchmarkId::from_parameter(ps), &ps, |b, &ps| {
                b.iter(|| {
                    let mut buf = vec![0u8; 65536];
                    let mut e = ExtendibleHashEngine::new(&mut buf, ps).unwrap();
                    for i in 0u16..50 {
                        let k = i.to_be_bytes();
                        e.put(black_box(&k), black_box(&k)).unwrap();
                    }
                });
            });
        }
        group.finish();
    }

    pub fn bench_ext_get(c: &mut Criterion) {
        let mut group = c.benchmark_group("ext_hash/get");
        for &ps in &[128, 256] {
            group.bench_with_input(BenchmarkId::from_parameter(ps), &ps, |b, &ps| {
                let mut buf = vec![0u8; 65536];
                let e = populated_ext(&mut buf, ps, 50);
                b.iter(|| {
                    for i in 0u16..50 {
                        let k = i.to_be_bytes();
                        black_box(e.get(black_box(&k)).unwrap());
                    }
                });
            });
        }
        group.finish();
    }
}

// ─── Linear Hash Benchmarks ────────────────────────────────────────────────

#[cfg(feature = "storage-hash-linear")]
mod linear_hash_bench {
    use super::*;
    use iondb_storage::hash::linear::LinearHashEngine;

    fn populated_linear(
        buf: &mut [u8],
        page_size: usize,
        initial: u32,
        n: u16,
    ) -> LinearHashEngine<'_> {
        let mut e = LinearHashEngine::new(buf, page_size, initial).unwrap();
        for i in 0..n {
            let k = i.to_be_bytes();
            e.put(&k, &k).unwrap();
        }
        e
    }

    pub fn bench_linear_put(c: &mut Criterion) {
        let mut group = c.benchmark_group("linear_hash/put");
        for &ps in &[128, 256, 512] {
            group.bench_with_input(BenchmarkId::from_parameter(ps), &ps, |b, &ps| {
                b.iter(|| {
                    let mut buf = vec![0u8; 65536];
                    let mut e = LinearHashEngine::new(&mut buf, ps, 4).unwrap();
                    for i in 0u16..100 {
                        let k = i.to_be_bytes();
                        e.put(black_box(&k), black_box(&k)).unwrap();
                    }
                });
            });
        }
        group.finish();
    }

    pub fn bench_linear_get(c: &mut Criterion) {
        let mut group = c.benchmark_group("linear_hash/get");
        for &ps in &[128, 256, 512] {
            group.bench_with_input(BenchmarkId::from_parameter(ps), &ps, |b, &ps| {
                let mut buf = vec![0u8; 65536];
                let e = populated_linear(&mut buf, ps, 4, 100);
                b.iter(|| {
                    for i in 0u16..100 {
                        let k = i.to_be_bytes();
                        black_box(e.get(black_box(&k)).unwrap());
                    }
                });
            });
        }
        group.finish();
    }
}

// ─── Criterion Groups ──────────────────────────────────────────────────────

#[cfg(feature = "storage-bptree")]
criterion_group!(
    btree_benches,
    bptree_bench::bench_btree_put,
    bptree_bench::bench_btree_get,
    bptree_bench::bench_btree_range,
);

#[cfg(feature = "storage-hash-ext")]
criterion_group!(
    ext_hash_benches,
    ext_hash_bench::bench_ext_put,
    ext_hash_bench::bench_ext_get,
);

#[cfg(feature = "storage-hash-linear")]
criterion_group!(
    linear_hash_benches,
    linear_hash_bench::bench_linear_put,
    linear_hash_bench::bench_linear_get,
);

// Conditional main based on enabled features.
// Default feature is storage-bptree only.
#[cfg(all(
    feature = "storage-bptree",
    feature = "storage-hash-ext",
    feature = "storage-hash-linear"
))]
criterion_main!(btree_benches, ext_hash_benches, linear_hash_benches);

#[cfg(all(
    feature = "storage-bptree",
    feature = "storage-hash-ext",
    not(feature = "storage-hash-linear")
))]
criterion_main!(btree_benches, ext_hash_benches);

#[cfg(all(
    feature = "storage-bptree",
    not(feature = "storage-hash-ext"),
    feature = "storage-hash-linear"
))]
criterion_main!(btree_benches, linear_hash_benches);

#[cfg(all(
    feature = "storage-bptree",
    not(feature = "storage-hash-ext"),
    not(feature = "storage-hash-linear")
))]
criterion_main!(btree_benches);

#[cfg(all(
    not(feature = "storage-bptree"),
    feature = "storage-hash-ext",
    feature = "storage-hash-linear"
))]
criterion_main!(ext_hash_benches, linear_hash_benches);

#[cfg(all(
    not(feature = "storage-bptree"),
    feature = "storage-hash-ext",
    not(feature = "storage-hash-linear")
))]
criterion_main!(ext_hash_benches);

#[cfg(all(
    not(feature = "storage-bptree"),
    not(feature = "storage-hash-ext"),
    feature = "storage-hash-linear"
))]
criterion_main!(linear_hash_benches);
