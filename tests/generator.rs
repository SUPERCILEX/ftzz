#![allow(clippy::items_after_test_module)]

use std::{
    cmp::{max, min},
    collections::VecDeque,
    fmt::Write,
    fs::{DirEntry, File, create_dir},
    hash::{DefaultHasher, Hasher},
    io,
    io::{BufReader, Read, stdout},
    num::NonZeroU64,
    path::Path,
};

use expect_test::expect_file;
use ftzz::{Generator, NumFilesWithRatio};
use io_adapters::WriteExtension;
use more_asserts::assert_le;
use rand::Rng;
use rstest::rstest;

use crate::inspect::InspectableTempDir;

mod inspect {
    use std::path::PathBuf;

    use tempfile::{TempDir, tempdir};

    pub struct InspectableTempDir {
        pub path: PathBuf,
        _guard: Option<TempDir>,
    }

    impl InspectableTempDir {
        pub fn new() -> Self {
            let dir = tempdir().unwrap();
            println!("Using dir {:?}", dir.path());

            if option_env!("INSPECT").is_some() {
                Self {
                    path: dir.into_path(),
                    _guard: None,
                }
            } else {
                Self {
                    path: dir.path().to_path_buf(),
                    _guard: Some(dir),
                }
            }
        }
    }
}

#[test]
fn gen_in_empty_existing_dir_is_allowed() {
    let dir = InspectableTempDir::new();
    let mut golden = String::new();

    let empty = dir.path.join("empty");
    create_dir(&empty).unwrap();

    Generator::builder()
        .root_dir(empty)
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(1).unwrap(),
        ))
        .build()
        .generate(&mut golden)
        .unwrap();
    print_and_hash_dir(&dir.path, &mut golden);

    expect_file!["../testdata/generator/gen_in_empty_existing_dir_is_allowed.stdout"]
        .assert_eq(&golden);
}

#[test]
fn gen_in_non_empty_existing_dir_is_disallowed() {
    let dir = InspectableTempDir::new();
    let mut golden = String::new();

    let non_empty = dir.path.join("nonempty");
    create_dir(&non_empty).unwrap();
    File::create(non_empty.join("file")).unwrap();

    let result = Generator::builder()
        .root_dir(non_empty)
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(1).unwrap(),
        ))
        .build()
        .generate(&mut golden);

    drop(result.unwrap_err());
    print_and_hash_dir(&dir.path, &mut golden);

    expect_file!["../testdata/generator/gen_in_non_empty_existing_dir_is_disallowed.stdout"]
        .assert_eq(&golden);
}

#[test]
fn gen_creates_new_dir_if_not_present() {
    let dir = InspectableTempDir::new();
    let mut golden = String::new();

    Generator::builder()
        .root_dir(dir.path.join("new"))
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(1).unwrap(),
        ))
        .build()
        .generate(&mut golden)
        .unwrap();

    assert!(dir.path.join("new").exists());
    print_and_hash_dir(&dir.path, &mut golden);

    expect_file!["../testdata/generator/gen_creates_new_dir_if_not_present.stdout"]
        .assert_eq(&golden);
}

#[rstest]
#[case(1_000)]
#[cfg_attr(not(miri), case(10_000))]
#[cfg_attr(not(miri), case(100_000))]
fn simple_create_files(#[case] num_files: u64) {
    let dir = InspectableTempDir::new();
    let mut golden = String::new();

    Generator::builder()
        .root_dir(dir.path.clone())
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(num_files).unwrap(),
        ))
        .build()
        .generate(&mut golden)
        .unwrap();

    print_and_hash_dir(&dir.path, &mut golden);

    expect_file![format!(
        "../testdata/generator/simple_create_files_{num_files}.stdout"
    )]
    .assert_eq(&golden);
}

#[rstest]
fn advanced_create_files(
    #[values(1, 1_000, 10_000)] num_files: u64,
    #[values((0, false), (1_000, false), (1_000, true), (100_000, false), (100_000, true))] bytes: (
        u64,
        bool,
    ),
    #[values(0, 1, 10)] max_depth: u32,
    #[values(1, 100, 1_000)] ftd_ratio: u64,
    #[values(false, true)] files_exact: bool,
) {
    #[cfg(miri)]
    if num_files > 100 || bytes.0 > 10_000 {
        return;
    }

    let dir = InspectableTempDir::new();
    let mut golden = String::new();

    Generator::builder()
        .root_dir(dir.path.clone())
        .num_files_with_ratio(
            NumFilesWithRatio::new(
                NonZeroU64::new(num_files).unwrap(),
                NonZeroU64::new(min(num_files, ftd_ratio)).unwrap(),
            )
            .unwrap(),
        )
        .num_bytes(bytes.0)
        .files_exact(files_exact)
        .bytes_exact(bytes.1)
        .max_depth(max_depth)
        .build()
        .generate(&mut golden)
        .unwrap();

    if files_exact {
        assert_eq!(count_num_files(&dir.path), num_files);
    }
    if bytes.1 {
        assert_eq!(count_num_bytes(&dir.path), bytes.0);
    }
    print_and_hash_dir(&dir.path, &mut golden);

    expect_file![format!(
        "../testdata/generator/advanced_create_files{}{}{}_{}_{}_{}.stdout",
        if files_exact { "_exact" } else { "" },
        if bytes.0 > 0 {
            format!("_bytes_{}", bytes.0)
        } else {
            String::new()
        },
        if bytes.1 { "_exact" } else { "" },
        num_files,
        max_depth,
        ftd_ratio,
    )]
    .assert_eq(&golden);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(10)]
#[case(50)]
#[cfg_attr(miri, ignore)] // Miri is way too slow unfortunately
fn max_depth_is_respected(#[case] max_depth: u32) {
    let dir = InspectableTempDir::new();
    let mut golden = String::new();

    Generator::builder()
        .root_dir(dir.path.clone())
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(10_000).unwrap(),
        ))
        .max_depth(max_depth)
        .build()
        .generate(&mut golden)
        .unwrap();

    assert_le!(find_max_depth(&dir.path), max_depth);
    print_and_hash_dir(&dir.path, &mut golden);

    expect_file![format!(
        "../testdata/generator/max_depth_is_respected_{max_depth}.stdout"
    )]
    .assert_eq(&golden);
}

#[rstest]
#[case(0)]
#[case(42)]
#[case(69)]
#[cfg_attr(miri, ignore)] // Miri is way too slow unfortunately
fn fill_byte_is_respected(#[case] fill_byte: u8) {
    let dir = InspectableTempDir::new();
    let mut golden = String::new();

    Generator::builder()
        .root_dir(dir.path.clone())
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(1_000).unwrap(),
        ))
        .num_bytes(100_000)
        .fill_byte(fill_byte)
        .build()
        .generate(&mut golden)
        .unwrap();

    let assert_fill_byte = || {
        let mut queue = VecDeque::from([dir.path.clone()]);
        while let Some(path) = queue.pop_front() {
            for entry in path.read_dir().unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    queue.push_back(entry.path());
                } else {
                    for byte in BufReader::new(File::open(entry.path()).unwrap()).bytes() {
                        assert_eq!(fill_byte, byte.unwrap());
                    }
                }
            }
        }
    };

    assert_fill_byte();
    print_and_hash_dir(&dir.path, &mut golden);

    expect_file![format!(
        "../testdata/generator/fill_byte_is_respected_{fill_byte}.stdout"
    )]
    .assert_eq(&golden);
}

#[test]
#[cfg_attr(miri, ignore)] // Miri is way too slow unfortunately
fn fuzz_test() {
    let dir = InspectableTempDir::new();

    let mut rng = rand::rng();
    let num_files = rng.random_range(1..25_000);
    let num_bytes = if rng.random() {
        rng.random_range(0..100_000)
    } else {
        0
    };
    let max_depth = rng.random_range(0..100);
    let ratio = rng.random_range(1..num_files);
    let files_exact = rng.random();
    let bytes_exact = rng.random();

    let g = Generator::builder()
        .root_dir(dir.path.clone())
        .num_files_with_ratio(
            NumFilesWithRatio::new(
                NonZeroU64::new(num_files).unwrap(),
                NonZeroU64::new(ratio).unwrap(),
            )
            .unwrap(),
        )
        .num_bytes(num_bytes)
        .max_depth(max_depth)
        .files_exact(files_exact)
        .bytes_exact(bytes_exact)
        .build();
    println!("Params: {g:?}");
    g.generate(&mut stdout().write_adapter()).unwrap();

    assert_le!(find_max_depth(&dir.path), max_depth);
    if files_exact {
        assert_eq!(count_num_files(&dir.path), num_files);
    }
    if bytes_exact {
        assert_eq!(count_num_bytes(&dir.path), num_bytes);
    }
}

/// Recursively hashes the file and directory names in dir
fn print_and_hash_dir(dir: &Path, output: &mut impl Write) {
    writeln!(output).unwrap();

    let mut hasher = DefaultHasher::new();

    let mut entries = Vec::new();
    let mut queue = VecDeque::from([dir.to_path_buf()]);
    while let Some(path) = queue.pop_front() {
        for entry in path.read_dir().unwrap() {
            entries.push(entry.unwrap());
        }

        entries.sort_by_key(DirEntry::file_name);
        for entry in &entries {
            if entry.file_type().unwrap().is_dir() {
                queue.push_back(entry.path());
            } else if entry.metadata().unwrap().len() > 0 {
                io::copy(
                    &mut File::open(entry.path()).unwrap(),
                    &mut (&mut hasher).write_adapter(),
                )
                .unwrap();
            }

            hasher.write(entry.file_name().to_str().unwrap().as_bytes());
            #[cfg(not(windows))]
            writeln!(
                output,
                "{}",
                &entry.path().to_str().unwrap()[dir.as_os_str().len()..]
            )
            .unwrap();
            #[cfg(windows)]
            writeln!(
                output,
                "{}",
                &entry.path().to_str().unwrap()[dir.as_os_str().len()..].replace('\\', "/")
            )
            .unwrap();
        }
        entries.clear();
    }

    writeln!(output, "\n0x{:x}", hasher.finish()).unwrap();
}

fn find_max_depth(dir: &Path) -> u32 {
    let mut depth = 0;
    for entry in dir.read_dir().unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            depth = max(depth, find_max_depth(&path) + 1);
        }
    }
    depth
}

fn count_num_files(dir: &Path) -> u64 {
    let mut num_files = 0;
    let mut queue = VecDeque::from([dir.to_path_buf()]);
    while let Some(path) = queue.pop_front() {
        for entry in path.read_dir().unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                queue.push_back(entry.path());
            } else {
                num_files += 1;
            }
        }
    }
    num_files
}

fn count_num_bytes(dir: &Path) -> u64 {
    let mut num_bytes = 0;
    let mut queue = VecDeque::from([dir.to_path_buf()]);
    while let Some(path) = queue.pop_front() {
        for entry in path.read_dir().unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                queue.push_back(entry.path());
            } else {
                num_bytes += entry.metadata().unwrap().len();
            }
        }
    }
    num_bytes
}
