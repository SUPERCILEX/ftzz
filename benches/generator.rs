use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main, Throughput};
use tempfile::tempdir;

use ftzz::generator::{generate, Generate};

fn simple_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_generate");

    for num_files in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_files));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_files),
            &num_files,
            |b, num_files| {
                b.iter_with_large_drop(|| {
                    let dir = tempdir().unwrap();

                    generate(Generate::new(
                        dir.path().to_path_buf(),
                        *num_files as usize,
                        5,
                        None,
                        0,
                    ))
                        .unwrap();

                    dir
                })
            },
        );
    }
}

fn huge_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("huge_generate");

    let num_files = 1_000_000;
    group
        .sample_size(10)
        .throughput(Throughput::Elements(num_files));
    group.bench_with_input(
        BenchmarkId::from_parameter(num_files),
        &num_files,
        |b, num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                generate(Generate::new(
                    dir.path().to_path_buf(),
                    *num_files as usize,
                    5,
                    None,
                    0,
                ))
                    .unwrap();

                dir
            })
        },
    );
}

fn deep_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("deep_generate");

    let num_files = 10_000;
    group.throughput(Throughput::Elements(num_files));
    group.bench_with_input(
        BenchmarkId::from_parameter(num_files),
        &num_files,
        |b, num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                generate(Generate::new(
                    dir.path().to_path_buf(),
                    *num_files as usize,
                    100,
                    None,
                    0,
                ))
                    .unwrap();

                dir
            })
        },
    );
}

fn shallow_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("shallow_generate");

    let num_files = 10_000;
    group.throughput(Throughput::Elements(num_files));
    group.bench_with_input(
        BenchmarkId::from_parameter(num_files),
        &num_files,
        |b, num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                generate(Generate::new(
                    dir.path().to_path_buf(),
                    *num_files as usize,
                    0,
                    None,
                    0,
                ))
                    .unwrap();

                dir
            })
        },
    );
}

fn sparse_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse_generate");

    let num_files = 10_000;
    group.throughput(Throughput::Elements(num_files));
    group.bench_with_input(
        BenchmarkId::from_parameter(num_files),
        &num_files,
        |b, num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                generate(Generate::new(
                    dir.path().to_path_buf(),
                    *num_files as usize,
                    5,
                    Some(1),
                    0,
                ))
                    .unwrap();

                dir
            })
        },
    );
}

fn dense_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("dense_generate");

    let num_files = 10_000;
    group.throughput(Throughput::Elements(num_files));
    group.bench_with_input(
        BenchmarkId::from_parameter(num_files),
        &num_files,
        |b, num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                let num_files = *num_files as usize;
                generate(Generate::new(
                    dir.path().to_path_buf(),
                    num_files,
                    5,
                    Some(num_files),
                    0,
                ))
                    .unwrap();

                dir
            })
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.005).warm_up_time(Duration::from_secs(1));
    targets =
    deep_generate,
    dense_generate,
    huge_generate,
    shallow_generate,
    simple_generate,
    sparse_generate,
}
criterion_main!(benches);
