use std::{fs::create_dir_all, io, io::ErrorKind::NotFound};

use error_stack::{IntoReport, Report, Result, ResultExt};
use tracing::{event, instrument, Level};

use crate::{
    core::file_contents::FileContentsGenerator,
    utils::{with_dir_name, with_file_name, FastPathBuf},
};

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
) -> Result<GeneratorTaskOutcome, io::Error> {
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
fn create_dirs(num_dirs: usize, dir: &mut FastPathBuf) -> Result<(), io::Error> {
    for i in 0..num_dirs {
        with_dir_name(i, |s| dir.push(s));

        create_dir_all(&dir)
            .into_report()
            .attach_printable_lazy(|| format!("Failed to create directory {dir:?}"))?;

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
) -> Result<usize, io::Error> {
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
                        .into_report()
                        .attach_printable_lazy(|| format!("Failed to create directory {file:?}"))?;
                } else {
                    return Err(Report::new(e))
                        .attach_printable_lazy(|| format!("Failed to create file {file:?}"));
                }
            }
        }
    }
    for i in start_file..num_files {
        with_file_name(i + offset, |s| file.push(s));

        bytes_written += contents
            .create_file(file, i, false)
            .into_report()
            .attach_printable_lazy(|| format!("Failed to create file {file:?}"))?;

        file.pop();
    }

    Ok(bytes_written)
}
