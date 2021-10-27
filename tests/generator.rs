use std::{
    collections::VecDeque,
    fs::File,
    hash::Hasher,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use rstest::rstest;
use seahash::SeaHasher;
use tempfile::tempdir;

use ftzz::generator::{generate, Generate};

#[rstest]
#[case(1_000)]
#[case(10_000)]
#[case(100_000)]
#[case(1_000_000)]
fn simple_create_files(#[case] num_files: usize) {
    let dir = tempdir().unwrap();
    println!("Using dir {:?}", dir.path());

    generate(Generate::new(
        dir.path().to_path_buf(),
        num_files,
        5,
        None,
        0,
    ))
        .unwrap();

    let hash = hash_dir(dir.path());
    let hash_file = PathBuf::from(format!(
        "testdata/generator/simple_create_files_{}.hash",
        num_files
    ));

    if cfg!(regenerate_testdata) {
        File::create(hash_file)
            .unwrap()
            .write_all(&hash.to_be_bytes())
            .unwrap()
    } else {
        let mut expected_hash = Vec::new();
        File::open(hash_file)
            .expect(
                "Regenerate test files with `RUSTFLAGS=\"--cfg regenerate_testdata\" cargo test`",
            )
            .read_to_end(&mut expected_hash)
            .unwrap();

        assert_eq!(hash.to_be_bytes(), expected_hash.as_slice());
    }
}

/// Recursively hashes the file and directory names in dir
fn hash_dir(dir: &Path) -> u64 {
    let mut hasher = SeaHasher::new();

    let mut queue = VecDeque::from([dir.to_path_buf()]);
    while let Some(path) = queue.pop_front() {
        for entry in path.read_dir().unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                queue.push_back(entry.path())
            }
            hasher.write(entry.file_name().to_str().unwrap().as_bytes());
        }
    }

    hasher.finish()
}
