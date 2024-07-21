use std::{fs::create_dir_all, io, io::ErrorKind::NotFound};

use error_stack::{Report, Result, ResultExt};

use crate::{
    core::file_contents::FileContentsGenerator,
    utils::{with_dir_name, with_file_name, FastPathBuf},
};

pub struct GeneratorTaskParams<G: FileContentsGenerator> {
    pub target_dir: FastPathBuf,
    pub num_files: u64,
    pub num_dirs: usize,
    pub file_offset: u64,
    pub file_contents: G,
}

pub struct GeneratorTaskOutcome {
    pub files_generated: u64,
    pub dirs_generated: usize,
    pub bytes_generated: u64,

    pub pool_return_file: FastPathBuf,
    pub pool_return_byte_counts: Option<Vec<u64>>,
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "debug", skip(file_contents))
)]
pub fn create_files_and_dirs(
    GeneratorTaskParams {
        mut target_dir,
        num_files,
        num_dirs,
        file_offset,
        mut file_contents,
    }: GeneratorTaskParams<impl FileContentsGenerator>,
) -> Result<GeneratorTaskOutcome, io::Error> {
    create_dirs(num_dirs, &mut target_dir)?;
    create_files(num_files, file_offset, &mut target_dir, &mut file_contents).map(|bytes_written| {
        GeneratorTaskOutcome {
            files_generated: num_files,
            dirs_generated: num_dirs,
            bytes_generated: bytes_written,

            pool_return_file: target_dir,
            pool_return_byte_counts: file_contents.byte_counts_pool_return(),
        }
    })
}

#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
fn create_dirs(num_dirs: usize, dir: &mut FastPathBuf) -> Result<(), io::Error> {
    for i in 0..num_dirs {
        let dir = with_dir_name(i, |s| dir.push(s));

        create_dir_all(&dir)
            .attach_printable_lazy(|| format!("Failed to create directory {dir:?}"))?;

        dir.pop();
    }
    Ok(())
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(contents))
)]
fn create_files(
    num_files: u64,
    offset: u64,
    file: &mut FastPathBuf,
    contents: &mut impl FileContentsGenerator,
) -> Result<u64, io::Error> {
    let mut state = contents.initialize();
    let mut bytes_written = 0;

    let mut start_file = 0;
    if num_files > 0 {
        let mut guard = with_file_name(offset, |s| file.push(s));

        match contents.create_file(&mut guard, 0, true, &mut state) {
            Ok(bytes) => {
                bytes_written += bytes;
                start_file += 1;
                guard.pop();
            }
            Err(e) => {
                if e.kind() == NotFound {
                    #[cfg(feature = "tracing")]
                    tracing::event!(tracing::Level::TRACE, file = ?guard, "Parent directory not created in time");

                    guard.pop();
                    create_dir_all(&*file)
                        .attach_printable_lazy(|| format!("Failed to create directory {file:?}"))?;
                } else {
                    return Err(Report::new(e))
                        .attach_printable_lazy(|| format!("Failed to create file {file:?}"));
                }
            }
        }
    }
    for i in start_file..num_files {
        let mut file = with_file_name(i + offset, |s| file.push(s));

        bytes_written += contents
            .create_file(
                &mut file,
                i.try_into().unwrap_or(usize::MAX),
                false,
                &mut state,
            )
            .attach_printable_lazy(|| format!("Failed to create file {file:?}"))?;

        file.pop();
    }

    Ok(bytes_written)
}
