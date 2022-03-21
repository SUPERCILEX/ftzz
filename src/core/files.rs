use std::{fs::create_dir_all, io::ErrorKind::NotFound, mem::MaybeUninit, ptr, slice};

use anyhow::Context;
use cli_errors::{CliExitAnyhowWrapper, CliResult};
use tracing::{event, instrument, Level};

use crate::{core::file_contents::FileContentsGenerator, utils::FastPathBuf};

pub fn with_file_name<T>(i: usize, f: impl FnOnce(&str) -> T) -> T {
    f(itoa::Buffer::new().format(i))
}

pub fn with_dir_name<T>(i: usize, f: impl FnOnce(&str) -> T) -> T {
    const SUFFIX: &str = ".dir";
    with_file_name(i, |s| {
        let mut buf = [MaybeUninit::<u8>::uninit(); 39 + SUFFIX.len()]; // 39 to support u128
        unsafe {
            let buf_ptr = buf.as_mut_ptr() as *mut u8;
            ptr::copy_nonoverlapping(s.as_ptr(), buf_ptr, s.len());
            ptr::copy_nonoverlapping(SUFFIX.as_ptr(), buf_ptr.add(s.len()), SUFFIX.len());

            f(std::str::from_utf8_unchecked(slice::from_raw_parts(
                buf.as_ptr() as *const u8,
                s.len() + SUFFIX.len(),
            )))
        }
    })
}

pub struct GeneratorTaskParams<G: FileContentsGenerator> {
    pub target_dir: FastPathBuf,
    pub num_files: usize,
    pub num_dirs: usize,
    pub file_offset: usize,
    pub file_contents: G,
}

pub struct GeneratorTaskOutcome {
    pub files_generated: usize,
    pub dirs_generated: usize,
    pub bytes_generated: usize,

    pub pool_return_file: FastPathBuf,
    pub pool_return_byte_counts: Option<Vec<usize>>,
}

#[instrument(level = "trace", skip(params))]
pub fn create_files_and_dirs(
    params: GeneratorTaskParams<impl FileContentsGenerator>,
) -> CliResult<GeneratorTaskOutcome> {
    let mut file = params.target_dir;
    let mut file_contents = params.file_contents;

    create_dirs(params.num_dirs, &mut file)?;
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

#[instrument(level = "trace")]
fn create_dirs(num_dirs: usize, dir: &mut FastPathBuf) -> CliResult<()> {
    for i in 0..num_dirs {
        with_dir_name(i, |s| dir.push(s));

        create_dir_all(&dir)
            .with_context(|| format!("Failed to create directory {:?}", dir))
            .with_code(exitcode::IOERR)?;

        dir.pop();
    }
    Ok(())
}

#[instrument(level = "trace", skip(contents))]
fn create_files(
    num_files: usize,
    offset: usize,
    file: &mut FastPathBuf,
    contents: &mut impl FileContentsGenerator,
) -> CliResult<usize> {
    let mut bytes_written = 0;

    let mut start_file = 0;
    if num_files > 0 {
        with_file_name(offset, |s| file.push(s));

        match contents.create_file(file, 0, true) {
            Ok(bytes) => {
                bytes_written += bytes;
                start_file += 1;
                file.pop();
            }
            Err(e) => {
                if e.kind() == NotFound {
                    event!(Level::TRACE, file = ?file, "Parent directory not created in time");

                    file.pop();
                    create_dir_all(&file)
                        .with_context(|| format!("Failed to create directory {:?}", file))
                        .with_code(exitcode::IOERR)?;
                } else {
                    return Err(e)
                        .with_context(|| format!("Failed to create file {:?}", file))
                        .with_code(exitcode::IOERR);
                }
            }
        }
    }
    for i in start_file..num_files {
        with_file_name(i + offset, |s| file.push(s));

        bytes_written += contents
            .create_file(file, i, false)
            .with_context(|| format!("Failed to create file {:?}", file))
            .with_code(exitcode::IOERR)?;

        file.pop();
    }

    Ok(bytes_written)
}
