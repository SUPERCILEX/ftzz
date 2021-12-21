use std::{
    cmp::{max, min},
    collections::VecDeque,
    fs::{create_dir, File},
    hash::Hasher,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use more_asserts::assert_le;
use rstest::rstest;
use seahash::SeaHasher;
use tempfile::tempdir;

use ftzz::generator::GeneratorBuilder;

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

    assert_matching_hashes(hash, &hash_file)
}

#[rstest]
fn advanced_create_files(
    #[values(1, 1_000, 10_000)] num_files: usize,
    #[values(0, 1, 10)] max_depth: u32,
    #[values(1, 100, 1_000)] ftd_ratio: usize,
) {
    let dir = tempdir().unwrap();
    println!("Using dir {:?}", dir.path());

    GeneratorBuilder::default()
        .root_dir(dir.path().to_path_buf())
        .num_files(num_files)
        .max_depth(max_depth)
        .file_to_dir_ratio(min(num_files, ftd_ratio))
        .build()
        .unwrap()
        .generate()
        .unwrap();

    let hash = hash_dir(dir.path());
    #[cfg(bazel)]
    let hash_file: PathBuf = runfiles::Runfiles::create().unwrap().rlocation(format!(
        "__main__/ftzz/testdata/generator/advanced_create_files_{}_{}_{}.hash",
        num_files, max_depth, ftd_ratio,
    ));
    #[cfg(not(bazel))]
    let hash_file = PathBuf::from(format!(
        "testdata/generator/advanced_create_files_{}_{}_{}.hash",
        num_files, max_depth, ftd_ratio,
    ));

    assert_matching_hashes(hash, &hash_file)
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

    assert_le!(find_max_depth(dir.path()), max_depth);
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
    if cfg!(regenerate_testdata) {
        File::create(hash_file)
            .unwrap()
            .write_all(&hash.to_be_bytes())
            .unwrap()
    } else {
        let mut expected_hash = Vec::new();
        File::open(&hash_file)
            .unwrap_or_else(|e| {
                panic!(
                    "Regenerate test files with \
                    `RUSTFLAGS=\"--cfg regenerate_testdata\" cargo test`\
                    \n{}: {:?}",
                    e, hash_file
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
