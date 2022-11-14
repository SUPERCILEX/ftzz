use std::{
    fs::create_dir_all,
    io,
    io::ErrorKind::NotFound,
    os::unix::io::{AsFd, OwnedFd},
};

use error_stack::{IntoReport, Report, Result, ResultExt};
use tracing::{event, instrument, Level};

use crate::{
    core::file_contents::FileContentsGenerator,
    utils::{with_dir_name, with_file_name},
};

pub struct GeneratorTaskParams<G: FileContentsGenerator> {
    pub target_dir: OwnedFd,
    pub num_files: u64,
    pub num_dirs: usize,
    pub file_offset: u64,
    pub file_contents: G,
}

pub struct GeneratorTaskOutcome {
    pub files_generated: u64,
    pub dirs_generated: usize,
    pub bytes_generated: u64,

    pub pool_return_file: OwnedFd,
    pub pool_return_byte_counts: Option<Vec<u64>>,
}

#[instrument(level = "trace", skip(params))]
pub fn create_files_and_dirs(
    params: GeneratorTaskParams<impl FileContentsGenerator>,
) -> Result<GeneratorTaskOutcome, io::Error> {
    let mut file = params.target_dir;
    let mut file_contents = params.file_contents;

    create_files(
        params.num_files,
        params.file_offset,
        &mut file,
        &mut file_contents,
    )
    .map(|bytes_written| GeneratorTaskOutcome {
        files_generated: params.num_files,
        dirs_generated: params.num_dirs,
        bytes_generated: bytes_written,

        pool_return_file: file,
        pool_return_byte_counts: file_contents.byte_counts_pool_return(),
    })
}

#[instrument(level = "trace", skip(contents, dir))]
fn create_files(
    num_files: u64,
    offset: u64,
    dir: &impl AsFd,
    contents: &mut impl FileContentsGenerator,
) -> Result<u64, io::Error> {
    let mut bytes_written = 0;

    for i in 0..num_files {
        with_file_name(i + offset, |s| {
            bytes_written += contents
                .create_file(dir, s, i.try_into().unwrap_or(usize::MAX))
                .into_report()
                .attach_printable_lazy(|| format!("Failed to create file {s:?}"))?;
            Ok(())
        })?;
    }

    Ok(bytes_written)
}
