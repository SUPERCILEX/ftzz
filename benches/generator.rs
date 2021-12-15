use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::tempdir;

use ftzz::generator::GeneratorBuilder;

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

                    GeneratorBuilder::default()
                        .root_dir(dir.path().to_path_buf())
                        .num_files(*num_files as usize)
                        .max_depth(5)
                        .file_to_dir_ratio(None)
                        .entropy(0)
                        .build()
                        .unwrap()
                        .generate()
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

                GeneratorBuilder::default()
                    .root_dir(dir.path().to_path_buf())
                    .num_files(*num_files as usize)
                    .max_depth(5)
                    .file_to_dir_ratio(None)
                    .entropy(0)
                    .build()
                    .unwrap()
                    .generate()
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

                GeneratorBuilder::default()
                    .root_dir(dir.path().to_path_buf())
                    .num_files(*num_files as usize)
                    .max_depth(100)
                    .file_to_dir_ratio(None)
                    .entropy(0)
                    .build()
                    .unwrap()
                    .generate()
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

                GeneratorBuilder::default()
                    .root_dir(dir.path().to_path_buf())
                    .num_files(*num_files as usize)
                    .max_depth(0)
                    .file_to_dir_ratio(None)
                    .entropy(0)
                    .build()
                    .unwrap()
                    .generate()
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

                GeneratorBuilder::default()
                    .root_dir(dir.path().to_path_buf())
                    .num_files(*num_files as usize)
                    .max_depth(5)
                    .file_to_dir_ratio(Some(1))
                    .entropy(0)
                    .build()
                    .unwrap()
                    .generate()
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
                GeneratorBuilder::default()
                    .root_dir(dir.path().to_path_buf())
                    .num_files(num_files)
                    .max_depth(5)
                    .file_to_dir_ratio(Some(num_files))
                    .entropy(0)
                    .build()
                    .unwrap()
                    .generate()
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
