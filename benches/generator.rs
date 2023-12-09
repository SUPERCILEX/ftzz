use std::{fmt, num::NonZeroU64, time::Duration};

use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion, PlotConfiguration,
    Throughput,
};
use ftzz::{Generator, NumFilesWithRatio};
use tempfile::tempdir;

struct Sink;

impl fmt::Write for Sink {
    fn write_str(&mut self, _: &str) -> fmt::Result {
        Ok(())
    }
}

fn simple_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_generate");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for num_files in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_files));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_files),
            &num_files,
            |b, &num_files| {
                b.iter_with_large_drop(|| {
                    let dir = tempdir().unwrap();

                    Generator::builder()
                        .root_dir(dir.path().to_path_buf())
                        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
                            NonZeroU64::new(num_files).unwrap(),
                        ))
                        .max_depth(5)
                        .build()
                        .generate(&mut Sink)
                        .unwrap();

                    dir
                });
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
        |b, &num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                Generator::builder()
                    .root_dir(dir.path().to_path_buf())
                    .num_files_with_ratio(NumFilesWithRatio::from_num_files(
                        NonZeroU64::new(num_files).unwrap(),
                    ))
                    .max_depth(5)
                    .build()
                    .generate(&mut Sink)
                    .unwrap();

                dir
            });
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
        |b, &num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                Generator::builder()
                    .root_dir(dir.path().to_path_buf())
                    .num_files_with_ratio(NumFilesWithRatio::from_num_files(
                        NonZeroU64::new(num_files).unwrap(),
                    ))
                    .max_depth(100)
                    .build()
                    .generate(&mut Sink)
                    .unwrap();

                dir
            });
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
        |b, &num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                Generator::builder()
                    .root_dir(dir.path().to_path_buf())
                    .num_files_with_ratio(NumFilesWithRatio::from_num_files(
                        NonZeroU64::new(num_files).unwrap(),
                    ))
                    .max_depth(0)
                    .build()
                    .generate(&mut Sink)
                    .unwrap();

                dir
            });
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
        |b, &num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                Generator::builder()
                    .root_dir(dir.path().to_path_buf())
                    .num_files_with_ratio(
                        NumFilesWithRatio::new(
                            NonZeroU64::new(num_files).unwrap(),
                            NonZeroU64::new(1).unwrap(),
                        )
                        .unwrap(),
                    )
                    .max_depth(5)
                    .build()
                    .generate(&mut Sink)
                    .unwrap();

                dir
            });
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
        |b, &num_files| {
            b.iter_with_large_drop(|| {
                let dir = tempdir().unwrap();

                Generator::builder()
                    .root_dir(dir.path().to_path_buf())
                    .num_files_with_ratio(
                        NumFilesWithRatio::new(
                            NonZeroU64::new(num_files).unwrap(),
                            NonZeroU64::new(num_files).unwrap(),
                        )
                        .unwrap(),
                    )
                    .max_depth(5)
                    .build()
                    .generate(&mut Sink)
                    .unwrap();

                dir
            });
        },
    );
}

fn bytes_generate(c: &mut Criterion) {
    let mut group = c.benchmark_group("bytes_generate");

    for num_bytes in [100_000, 1_000_000, 10_000_000] {
        group.throughput(Throughput::Bytes(num_bytes));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_bytes),
            &num_bytes,
            |b, &num_bytes| {
                b.iter_with_large_drop(|| {
                    let dir = tempdir().unwrap();

                    Generator::builder()
                        .root_dir(dir.path().to_path_buf())
                        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
                            NonZeroU64::new(10000).unwrap(),
                        ))
                        .max_depth(5)
                        .num_bytes(num_bytes)
                        .build()
                        .generate(&mut Sink)
                        .unwrap();

                    dir
                });
            },
        );
    }
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
    bytes_generate,
}
criterion_main!(benches);
