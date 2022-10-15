use std::{
    cmp::{max, min},
    collections::VecDeque,
    fs::{create_dir, DirEntry, File},
    hash::Hasher,
    io::{stdout, BufReader, Read, Write},
    num::NonZeroU64,
    path::Path,
};

use goldenfile::Mint;
use more_asserts::assert_le;
use rand::Rng;
use rstest::rstest;
use seahash::SeaHasher;

use ftzz::generator::{Generator, NumFilesWithRatio};

use crate::inspect::InspectableTempDir;

mod api;

mod inspect {
    use std::path::PathBuf;

    use tempfile::{tempdir, TempDir};

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
    let empty = dir.path.join("empty");
    create_dir(&empty).unwrap();

    let mut mint = Mint::new("testdata/generator");
    let mut goldenfile = mint
        .new_goldenfile("gen_in_empty_existing_dir_is_allowed.stdout")
        .unwrap();

    Generator::builder()
        .root_dir(empty)
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(1).unwrap(),
        ))
        .build()
        .generate(&mut goldenfile)
        .unwrap();

    print_and_hash_dir(&dir.path, &mut goldenfile);
}

#[test]
fn gen_in_non_empty_existing_dir_is_disallowed() {
    let dir = InspectableTempDir::new();
    let non_empty = dir.path.join("nonempty");
    create_dir(&non_empty).unwrap();
    File::create(non_empty.join("file")).unwrap();

    let mut mint = Mint::new("testdata/generator");
    let mut goldenfile = mint
        .new_goldenfile("gen_in_non_empty_existing_dir_is_disallowed.stdout")
        .unwrap();

    let result = Generator::builder()
        .root_dir(non_empty)
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(1).unwrap(),
        ))
        .build()
        .generate(&mut goldenfile);

    drop(result.unwrap_err());
    print_and_hash_dir(&dir.path, &mut goldenfile);
}

#[test]
fn gen_creates_new_dir_if_not_present() {
    let dir = InspectableTempDir::new();

    let mut mint = Mint::new("testdata/generator");
    let mut goldenfile = mint
        .new_goldenfile("gen_creates_new_dir_if_not_present.stdout")
        .unwrap();

    Generator::builder()
        .root_dir(dir.path.join("new"))
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(1).unwrap(),
        ))
        .build()
        .generate(&mut goldenfile)
        .unwrap();

    assert!(dir.path.join("new").exists());
    print_and_hash_dir(&dir.path, &mut goldenfile);
}

#[rstest]
#[case(1_000)]
#[cfg_attr(not(miri), case(10_000))]
#[cfg_attr(not(miri), case(100_000))]
fn simple_create_files(#[case] num_files: u64) {
    let dir = InspectableTempDir::new();

    let mut mint = Mint::new("testdata/generator");
    let mut goldenfile = mint
        .new_goldenfile(format!("simple_create_files_{num_files}.stdout"))
        .unwrap();

    Generator::builder()
        .root_dir(dir.path.clone())
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(num_files).unwrap(),
        ))
        .build()
        .generate(&mut goldenfile)
        .unwrap();

    print_and_hash_dir(&dir.path, &mut goldenfile);
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

    let mut mint = Mint::new("testdata/generator");
    let mut goldenfile = mint
        .new_goldenfile(format!(
            "advanced_create_files{}{}{}_{}_{}_{}.stdout",
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
        ))
        .unwrap();

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
        .generate(&mut goldenfile)
        .unwrap();

    if files_exact {
        assert_eq!(count_num_files(&dir.path), num_files);
    }
    if bytes.1 {
        assert_eq!(count_num_bytes(&dir.path), bytes.0);
    }
    print_and_hash_dir(&dir.path, &mut goldenfile);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(10)]
#[case(100)]
#[cfg_attr(miri, ignore)] // Miri is way too slow unfortunately
fn max_depth_is_respected(#[case] max_depth: u32) {
    let dir = InspectableTempDir::new();

    let mut mint = Mint::new("testdata/generator");
    let mut goldenfile = mint
        .new_goldenfile(format!("max_depth_is_respected_{max_depth}.stdout"))
        .unwrap();

    Generator::builder()
        .root_dir(dir.path.clone())
        .num_files_with_ratio(NumFilesWithRatio::from_num_files(
            NonZeroU64::new(10_000).unwrap(),
        ))
        .max_depth(max_depth)
        .build()
        .generate(&mut goldenfile)
        .unwrap();

    assert_le!(find_max_depth(&dir.path), max_depth);
    print_and_hash_dir(&dir.path, &mut goldenfile);
}

#[test]
#[cfg_attr(miri, ignore)] // Miri is way too slow unfortunately
fn fuzz_test() {
    let dir = InspectableTempDir::new();

    let mut rng = rand::thread_rng();
    let num_files = rng.gen_range(1..25_000);
    let num_bytes = if rng.gen() {
        rng.gen_range(0..100_000)
    } else {
        0
    };
    let max_depth = rng.gen_range(0..100);
    let ratio = rng.gen_range(1..num_files);
    let files_exact = rng.gen();
    let bytes_exact = rng.gen();

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
    g.generate(&mut stdout()).unwrap();

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

    let mut hasher = SeaHasher::new();

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
                for byte in BufReader::new(File::open(entry.path()).unwrap()).bytes() {
                    hasher.write_u8(byte.unwrap());
                }
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
                &entry.path().to_str().unwrap()[dir.as_os_str().len()..].replace("\\", "/")
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
