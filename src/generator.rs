#![allow(
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]

use std::{
    cmp::max,
    fmt::Write,
    fs::create_dir_all,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    process::ExitCode,
    thread,
};

use error_stack::{IntoReport, Report, Result, ResultExt};
use rand::SeedableRng;
use rand_distr::Normal;
use rand_xoshiro::Xoshiro256PlusPlus;
use thiserror::Error;
use thousands::Separable;
use tracing::{event, Level};
use typed_builder::TypedBuilder;

use crate::core::{run, DynamicGenerator, GeneratorBytes, GeneratorStats, StaticGenerator};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to retrieve subtask results.")]
    TaskJoin,
    #[error("An IO error occurred in a subtask.")]
    Io,
    #[error("Failed to achieve valid generator environment.")]
    InvalidEnvironment,
    #[error("Failed to create the async runtime.")]
    RuntimeCreation,
}

#[derive(Debug)]
pub struct NumFilesWithRatio {
    num_files: NonZeroU64,
    file_to_dir_ratio: NonZeroU64,
}

#[derive(Error, Debug)]
pub enum NumFilesWithRatioError {
    #[error(
        "The file to dir ratio ({file_to_dir_ratio}) cannot be larger than the number of files to \
         generate ({num_files})."
    )]
    InvalidRatio {
        num_files: NonZeroU64,
        file_to_dir_ratio: NonZeroU64,
    },
}

impl NumFilesWithRatio {
    /// # Errors
    ///
    /// The file to directory ratio cannot be larger than the number of files to
    /// generate since it is impossible to satisfy that condition.
    pub fn new(
        num_files: NonZeroU64,
        file_to_dir_ratio: NonZeroU64,
    ) -> std::result::Result<Self, NumFilesWithRatioError> {
        if file_to_dir_ratio > num_files {
            return Err(NumFilesWithRatioError::InvalidRatio {
                num_files,
                file_to_dir_ratio,
            });
        }

        Ok(Self {
            num_files,
            file_to_dir_ratio,
        })
    }

    #[must_use]
    pub fn from_num_files(num_files: NonZeroU64) -> Self {
        Self {
            num_files,
            file_to_dir_ratio: NonZeroU64::new(max(num_files.get() / 1000, 1)).unwrap(),
        }
    }
}

#[derive(TypedBuilder, Debug)]
#[builder(doc)]
pub struct Generator {
    root_dir: PathBuf,
    num_files_with_ratio: NumFilesWithRatio,
    #[builder(default = false)]
    files_exact: bool,
    #[builder(default = 0)]
    num_bytes: u64,
    #[builder(default = None)]
    #[builder(setter(into))]
    fill_byte: Option<u8>,
    #[builder(default = false)]
    bytes_exact: bool,
    #[builder(default = 5)]
    max_depth: u32,
    #[builder(default = 0)]
    seed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_params_succeeds() {
        let g = Generator::builder()
            .root_dir(PathBuf::from("abc"))
            .num_files_with_ratio(NumFilesWithRatio::from_num_files(
                NonZeroU64::new(1).unwrap(),
            ))
            .build();

        assert_eq!(g.root_dir, PathBuf::from("abc"));
        assert_eq!(g.num_files_with_ratio.num_files.get(), 1);
        assert!(!g.files_exact);
        assert_eq!(g.num_bytes, 0);
        assert!(!g.bytes_exact);
        assert_eq!(g.max_depth, 5);
        assert_eq!(g.num_files_with_ratio.file_to_dir_ratio.get(), 1);
        assert_eq!(g.seed, 0);
    }

    #[test]
    fn ratio_greater_than_num_files_fails() {
        let r = NumFilesWithRatio::new(NonZeroU64::new(1).unwrap(), NonZeroU64::new(2).unwrap());

        r.unwrap_err();
    }
}

impl Generator {
    #[allow(clippy::missing_errors_doc)]
    pub fn generate(self, output: &mut impl Write) -> Result<(), Error> {
        let options = validated_options(self)?;
        print_configuration_info(&options, output)?;
        print_stats(run_generator(options)?, output);
        Ok(())
    }
}

#[derive(Debug)]
struct Configuration {
    root_dir: PathBuf,
    files: NonZeroU64,
    bytes: u64,
    files_exact: bool,
    bytes_exact: bool,
    fill_byte: Option<u8>,
    files_per_dir: f64,
    dirs_per_dir: f64,
    bytes_per_file: f64,
    max_depth: u32,
    seed: u64,
    human_info: HumanInfo,
}

#[derive(Debug)]
struct HumanInfo {
    dirs_per_dir: usize,
    total_dirs: usize,
    bytes_per_files: usize,
}

fn validated_options(
    Generator {
        root_dir,
        num_files_with_ratio,
        files_exact,
        num_bytes,
        fill_byte,
        bytes_exact,
        max_depth,
        seed,
    }: Generator,
) -> Result<Configuration, Error> {
    create_dir_all(&root_dir)
        .into_report()
        .attach_printable_lazy(|| format!("Failed to create directory {root_dir:?}"))
        .change_context(Error::InvalidEnvironment)
        .attach(ExitCode::from(sysexits::ExitCode::IoErr))?;
    if root_dir
        .read_dir()
        .into_report()
        .attach_printable_lazy(|| format!("Failed to read directory {root_dir:?}"))
        .change_context(Error::InvalidEnvironment)
        .attach(ExitCode::from(sysexits::ExitCode::IoErr))?
        .count()
        != 0
    {
        return Err(Report::new(Error::InvalidEnvironment))
            .attach_printable(format!("The root directory {root_dir:?} must be empty."))
            .attach(ExitCode::from(sysexits::ExitCode::DataErr));
    }

    let num_files = num_files_with_ratio.num_files.get() as f64;
    let bytes_per_file = num_bytes as f64 / num_files;

    if max_depth == 0 {
        return Ok(Configuration {
            root_dir,
            files: num_files_with_ratio.num_files,
            bytes: num_bytes,
            files_exact,
            bytes_exact,
            fill_byte,
            files_per_dir: num_files,
            dirs_per_dir: 0.,
            bytes_per_file,
            max_depth: 0,
            seed,
            human_info: HumanInfo {
                dirs_per_dir: 0,
                total_dirs: 1,
                bytes_per_files: bytes_per_file.round() as usize,
            },
        });
    }

    let ratio = num_files_with_ratio.file_to_dir_ratio.get() as f64;
    let num_dirs = num_files / ratio;
    // This formula was derived from the following equation:
    // num_dirs = unknown_num_dirs_per_dir^max_depth
    let dirs_per_dir = num_dirs.powf(1f64 / f64::from(max_depth));

    Ok(Configuration {
        root_dir,
        files: num_files_with_ratio.num_files,
        bytes: num_bytes,
        files_exact,
        bytes_exact,
        fill_byte,
        files_per_dir: ratio,
        bytes_per_file,
        dirs_per_dir,
        max_depth,
        seed,
        human_info: HumanInfo {
            dirs_per_dir: dirs_per_dir.round() as usize,
            total_dirs: num_dirs.round() as usize,
            bytes_per_files: bytes_per_file.round() as usize,
        },
    })
}

fn print_configuration_info(
    Configuration {
        root_dir: _,
        files,
        bytes,
        files_exact,
        bytes_exact,
        fill_byte: _,
        files_per_dir: _,
        dirs_per_dir: _,
        bytes_per_file: _,
        max_depth,
        seed: _,
        human_info:
            HumanInfo {
                dirs_per_dir,
                total_dirs,
                bytes_per_files,
            },
    }: &Configuration,
    output: &mut impl Write,
) -> Result<(), Error> {
    writeln!(
        output,
        "{file_count_type} {} {files_maybe_plural} will be generated in approximately {} \
         {directories_maybe_plural} distributed across a tree of maximum depth {} where each \
         directory contains approximately {} other {dpd_directories_maybe_plural}.{bytes_info}",
        files.separate_with_commas(),
        total_dirs.separate_with_commas(),
        max_depth.separate_with_commas(),
        dirs_per_dir.separate_with_commas(),
        file_count_type = if *files_exact { "Exactly" } else { "About" },
        files_maybe_plural = if files.get() == 1 { "file" } else { "files" },
        directories_maybe_plural = if *total_dirs == 1 {
            "directory"
        } else {
            "directories"
        },
        dpd_directories_maybe_plural = if *dirs_per_dir == 1 {
            "directory"
        } else {
            "directories"
        },
        bytes_info = if *bytes > 0 {
            format!(
                " Each file will contain approximately {} {bytes_maybe_plural} of random \
                 data{exact_bytes_total}.",
                bytes_per_files.separate_with_commas(),
                bytes_maybe_plural = if *bytes_per_files == 1 {
                    "byte"
                } else {
                    "bytes"
                },
                exact_bytes_total = if *bytes_exact {
                    format!(
                        " totaling exactly {} {bytes_maybe_plural}",
                        bytes,
                        bytes_maybe_plural = if *bytes == 1 { "byte" } else { "bytes" }
                    )
                } else {
                    String::new()
                },
            )
        } else {
            String::new()
        },
    )
    .into_report()
    .attach_printable("Failed to write to output stream")
    .change_context(Error::Io)
    .attach(ExitCode::from(sysexits::ExitCode::IoErr))
}

fn print_stats(GeneratorStats { files, dirs, bytes }: GeneratorStats, output: &mut impl Write) {
    // Ignore I/O errors since it'd be dumb to fail if we actually succeeded in
    // creating all the files
    let _ = writeln!(
        output,
        "Created {} {files_maybe_plural}{bytes_info} across {} {directories_maybe_plural}.",
        files.separate_with_commas(),
        dirs.separate_with_commas(),
        files_maybe_plural = if files == 1 { "file" } else { "files" },
        directories_maybe_plural = if dirs == 1 {
            "directory"
        } else {
            "directories"
        },
        bytes_info = if bytes > 0 {
            event!(Level::INFO, bytes, "Exact bytes written");
            format!(" ({})", bytesize::to_string(bytes, false))
        } else {
            String::new()
        }
    );
}

fn run_generator(config: Configuration) -> Result<GeneratorStats, Error> {
    let parallelism =
        thread::available_parallelism().unwrap_or(const { NonZeroUsize::new(1).unwrap() });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(parallelism.get())
        .build()
        .into_report()
        .change_context(Error::RuntimeCreation)
        .attach(ExitCode::from(sysexits::ExitCode::OsErr))?;

    event!(Level::INFO, config = ?config, "Starting config");
    runtime.block_on(run_generator_async(config, parallelism))
}

async fn run_generator_async(
    Configuration {
        root_dir,
        files,
        bytes,
        files_exact,
        bytes_exact,
        fill_byte,
        files_per_dir,
        dirs_per_dir,
        bytes_per_file,
        max_depth,
        seed,
        human_info: _,
    }: Configuration,
    parallelism: NonZeroUsize,
) -> Result<GeneratorStats, Error> {
    let random = {
        let seed = ((files.get().wrapping_add(max_depth.into()) as f64
            * (files_per_dir + dirs_per_dir)) as u64)
            .wrapping_add(seed);
        event!(Level::DEBUG, seed = ?seed, "Starting seed");
        Xoshiro256PlusPlus::seed_from_u64(seed)
    };
    let num_files_distr = Normal::new(files_per_dir, files_per_dir * 0.2).unwrap();
    let num_dirs_distr = Normal::new(dirs_per_dir, dirs_per_dir * 0.2).unwrap();
    let num_bytes_distr = Normal::new(bytes_per_file, bytes_per_file * 0.2).unwrap();

    macro_rules! run {
        ($generator:expr) => {{
            run(
                root_dir,
                max_depth.try_into().unwrap_or(usize::MAX),
                parallelism,
                $generator,
            )
            .await
        }};
    }

    let dynamic = DynamicGenerator {
        num_files_distr,
        num_dirs_distr,
        random,

        bytes: (bytes > 0).then_some(GeneratorBytes {
            num_bytes_distr,
            fill_byte,
        }),
    };
    if files_exact || bytes_exact {
        run!(StaticGenerator::new(
            dynamic,
            files_exact.then_some(files),
            bytes_exact.then_some(bytes),
        ))
    } else {
        run!(dynamic)
    }
}
