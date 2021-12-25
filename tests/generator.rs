use std::{
    cmp::{max, min},
    collections::VecDeque,
    fs::{create_dir, File},
    hash::Hasher,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use more_asserts::assert_le;
use rand::Rng;
use rstest::rstest;
use seahash::SeaHasher;
use tempfile::tempdir;

use ftzz::generator::GeneratorBuilder;

macro_rules! allow_inspection {
    ($dir:ident) => {
        if option_env!("INSPECT").is_some() {
            $dir.into_path();
        }
    };
}

#[test]
fn gen_in_empty_existing_dir_is_allowed() {
    let dir = tempdir().unwrap();
    let empty = dir.path().join("empty");
    create_dir(&empty).unwrap();

    GeneratorBuilder::default()
        .root_dir(empty)
        .num_files(1)
        .build()
        .unwrap()
        .generate()
        .unwrap();
}

#[test]
fn gen_in_non_emtpy_existing_dir_is_disallowed() {
    let dir = tempdir().unwrap();
    let non_empty = dir.path().join("nonempty");
    create_dir(&non_empty).unwrap();
    File::create(non_empty.join("file")).unwrap();

    let result = GeneratorBuilder::default()
        .root_dir(non_empty)
        .num_files(1)
        .build()
        .unwrap()
        .generate();

    assert!(result.is_err());
}

#[test]
fn gen_creates_new_dir_if_not_present() {
    let dir = tempdir().unwrap();

    GeneratorBuilder::default()
        .root_dir(dir.path().join("new"))
        .num_files(1)
        .build()
        .unwrap()
        .generate()
        .unwrap();

    assert!(dir.path().join("new").exists());
}

#[rstest]
#[case(1_000)]
#[case(10_000)]
#[case(100_000)]
fn simple_create_files(#[case] num_files: usize) {
    let dir = tempdir().unwrap();
    println!("Using dir {:?}", dir.path());

    GeneratorBuilder::default()
        .root_dir(dir.path().to_path_buf())
        .num_files(num_files)
        .build()
        .unwrap()
        .generate()
        .unwrap();

    let hash = hash_dir(dir.path());
    #[cfg(bazel)]
    let hash_file: PathBuf = runfiles::Runfiles::create().unwrap().rlocation(format!(
        "__main__/ftzz/testdata/generator/simple_create_files_{}.hash",
        num_files
    ));
    #[cfg(not(bazel))]
    let hash_file = PathBuf::from(format!(
        "testdata/generator/simple_create_files_{}.hash",
        num_files
    ));

    allow_inspection!(dir);
    assert_matching_hashes(hash, &hash_file);
}

#[rstest]
fn advanced_create_files(
    #[values(1, 1_000, 10_000)] num_files: usize,
    #[values(0, 1, 10)] max_depth: u32,
    #[values(1, 100, 1_000)] ftd_ratio: usize,
    #[values(false, true)] files_exact: bool,
) {
    let dir = tempdir().unwrap();
    println!("Using dir {:?}", dir.path());

    GeneratorBuilder::default()
        .root_dir(dir.path().to_path_buf())
        .num_files(num_files)
        .files_exact(files_exact)
        .max_depth(max_depth)
        .file_to_dir_ratio(min(num_files, ftd_ratio))
        .build()
        .unwrap()
        .generate()
        .unwrap();

    let hash = hash_dir(dir.path());
    #[cfg(bazel)]
    let hash_file: PathBuf = runfiles::Runfiles::create().unwrap().rlocation(format!(
        "__main__/ftzz/testdata/generator/advanced_create_files{}_{}_{}_{}.hash",
        if files_exact { "_exact" } else { "" },
        num_files,
        max_depth,
        ftd_ratio,
    ));
    #[cfg(not(bazel))]
    let hash_file = PathBuf::from(format!(
        "testdata/generator/advanced_create_files{}_{}_{}_{}.hash",
        if files_exact { "_exact" } else { "" },
        num_files,
        max_depth,
        ftd_ratio,
    ));

    let actual_num_files = if files_exact {
        count_num_files(dir.path())
    } else {
        num_files
    };
    allow_inspection!(dir);
    assert_matching_hashes(hash, &hash_file);
    assert_eq!(actual_num_files, num_files);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
#[case(10)]
#[case(100)]
fn max_depth_is_respected(#[case] max_depth: u32) {
    let dir = tempdir().unwrap();
    println!("Using dir {:?}", dir.path());

    GeneratorBuilder::default()
        .root_dir(dir.path().to_path_buf())
        .num_files(10_000)
        .max_depth(max_depth)
        .build()
        .unwrap()
        .generate()
        .unwrap();

    let actual_max_depth = find_max_depth(dir.path());
    allow_inspection!(dir);
    assert_le!(actual_max_depth, max_depth);
}

#[test]
fn fuzz_test() {
    let dir = tempdir().unwrap();
    println!("Using dir {:?}", dir.path());

    let mut rng = rand::thread_rng();
    let num_files = rng.gen_range(1..25_000);
    let max_depth = rng.gen_range(0..100);
    let ratio = rng.gen_range(1..num_files);
    let files_exact = rng.gen();

    GeneratorBuilder::default()
        .root_dir(dir.path().to_path_buf())
        .num_files(num_files)
        .max_depth(max_depth)
        .file_to_dir_ratio(ratio)
        .files_exact(files_exact)
        .build()
        .unwrap()
        .generate()
        .unwrap();

    let actual_max_depth = find_max_depth(dir.path());
    let actual_num_files = if files_exact {
        count_num_files(dir.path())
    } else {
        num_files
    };

    allow_inspection!(dir);

    assert_le!(actual_max_depth, max_depth);
    assert_eq!(actual_num_files, num_files);
}

/// Recursively hashes the file and directory names in dir
fn hash_dir(dir: &Path) -> u64 {
    let mut hasher = SeaHasher::new();

    let mut entries = Vec::new();
    let mut queue = VecDeque::from([dir.to_path_buf()]);
    while let Some(path) = queue.pop_front() {
        for entry in path.read_dir().unwrap() {
            entries.push(entry.unwrap());
        }

        entries.sort_by_key(|e| e.file_name());
        for entry in &entries {
            if entry.file_type().unwrap().is_dir() {
                queue.push_back(entry.path())
            }
            hasher.write(entry.file_name().to_str().unwrap().as_bytes());
        }
        entries.clear();
    }

    hasher.finish()
}

fn assert_matching_hashes(hash: u64, hash_file: &Path) {
    if option_env!("REGEN").is_some() {
        File::create(hash_file)
            .unwrap()
            .write_all(&hash.to_be_bytes())
            .unwrap()
    } else {
        let mut expected_hash = Vec::new();
        File::open(&hash_file)
            .unwrap_or_else(|e| {
                panic!(
                    "Regenerate test files with `REGEN=true cargo test` \n{}: {:?}",
                    e, hash_file,
                )
            })
            .read_to_end(&mut expected_hash)
            .unwrap();

        assert_eq!(hash.to_be_bytes(), expected_hash.as_slice());
    }
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

fn count_num_files(dir: &Path) -> usize {
    let mut num_files = 0;
    let mut queue = VecDeque::from([dir.to_path_buf()]);
    while let Some(path) = queue.pop_front() {
        for entry in path.read_dir().unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                queue.push_back(path);
            } else {
                num_files += 1;
            }
        }
    }
    num_files
}
