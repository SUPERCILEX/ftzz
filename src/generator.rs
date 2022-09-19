#![allow(
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]

use std::{cmp::max, fs::create_dir_all, io::Write, num::NonZeroUsize, path::PathBuf, thread};

use anyhow::{anyhow, Context};
use cli_errors::{CliExitAnyhowWrapper, CliResult};
use num_format::{Locale, ToFormattedString};
use rand::SeedableRng;
use rand_distr::Normal;
use rand_xoshiro::Xoshiro256PlusPlus;
use tracing::{event, Level};
use typed_builder::TypedBuilder;

use crate::core::{
    run, FilesAndContentsGenerator, FilesNoContentsGenerator, GeneratorStats,
    OtherFilesAndContentsGenerator,
};

#[derive(Debug)]
pub struct NumFilesWithRatio {
    num_files: NonZeroUsize,
    file_to_dir_ratio: NonZeroUsize,
}

impl NumFilesWithRatio {
    /// # Errors
    ///
    /// The file to directory ratio cannot be larger than the number of files to
    /// generate since it is impossible to satisfy that condition.
    pub fn new(
        num_files: NonZeroUsize,
        file_to_dir_ratio: NonZeroUsize,
    ) -> Result<Self, anyhow::Error> {
        if file_to_dir_ratio > num_files {
            return Err(anyhow!(
                "The file to dir ratio ({file_to_dir_ratio}) cannot be larger \
                than the number of files to generate ({num_files}).",
            ));
        }

        Ok(Self {
            num_files,
            file_to_dir_ratio,
        })
    }

    #[must_use]
    pub fn from_num_files(num_files: NonZeroUsize) -> Self {
        Self {
            num_files,
            file_to_dir_ratio: {
                let r = max(num_files.get() / 1000, 1);
                unsafe { NonZeroUsize::new_unchecked(r) }
            },
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
    num_bytes: usize,
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
                NonZeroUsize::new(1).unwrap(),
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
        let r =
            NumFilesWithRatio::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(2).unwrap());

        r.unwrap_err();
    }
}

impl Generator {
    #[allow(clippy::missing_errors_doc)]
    pub fn generate(self, output: &mut impl Write) -> CliResult<()> {
        let options = validated_options(self)?;
        print_configuration_info(&options, output);
        print_stats(run_generator(options)?, output);
        Ok(())
    }
}

#[derive(Debug)]
struct Configuration {
    root_dir: PathBuf,
    files: usize,
    bytes: usize,
    files_exact: bool,
    bytes_exact: bool,
    files_per_dir: f64,
    dirs_per_dir: f64,
    bytes_per_file: f64,
    max_depth: u32,
    seed: u64,

    informational_dirs_per_dir: usize,
    informational_total_dirs: usize,
    informational_bytes_per_files: usize,
}

fn validated_options(generator: Generator) -> CliResult<Configuration> {
    create_dir_all(&generator.root_dir)
        .with_context(|| format!("Failed to create directory {:?}", generator.root_dir))
        .with_code(exitcode::IOERR)?;
    if generator
        .root_dir
        .read_dir()
        .with_context(|| format!("Failed to read directory {:?}", generator.root_dir))
        .with_code(exitcode::IOERR)?
        .count()
        != 0
    {
        return Err(anyhow!(
            "The root directory {:?} must be empty.",
            generator.root_dir,
        ))
        .with_code(exitcode::DATAERR);
    }

    let num_files = generator.num_files_with_ratio.num_files.get() as f64;
    let bytes_per_file = generator.num_bytes as f64 / num_files;

    if generator.max_depth == 0 {
        return Ok(Configuration {
            root_dir: generator.root_dir,
            files: generator.num_files_with_ratio.num_files.get(),
            bytes: generator.num_bytes,
            files_exact: generator.files_exact,
            bytes_exact: generator.bytes_exact,
            files_per_dir: num_files,
            dirs_per_dir: 0.,
            bytes_per_file,
            max_depth: 0,
            seed: generator.seed,

            informational_dirs_per_dir: 0,
            informational_total_dirs: 1,
            informational_bytes_per_files: bytes_per_file.round() as usize,
        });
    }

    let ratio = generator.num_files_with_ratio.file_to_dir_ratio.get() as f64;
    let num_dirs = num_files / ratio;
    // This formula was derived from the following equation:
    // num_dirs = unknown_num_dirs_per_dir^max_depth
    let dirs_per_dir = num_dirs.powf(1f64 / f64::from(generator.max_depth));

    Ok(Configuration {
        root_dir: generator.root_dir,
        files: generator.num_files_with_ratio.num_files.get(),
        bytes: generator.num_bytes,
        files_exact: generator.files_exact,
        bytes_exact: generator.bytes_exact,
        files_per_dir: ratio,
        bytes_per_file,
        dirs_per_dir,
        max_depth: generator.max_depth,
        seed: generator.seed,

        informational_dirs_per_dir: dirs_per_dir.round() as usize,
        informational_total_dirs: num_dirs.round() as usize,
        informational_bytes_per_files: bytes_per_file.round() as usize,
    })
}

fn print_configuration_info(config: &Configuration, output: &mut impl Write) {
    let locale = Locale::en;
    writeln!(
        output,
        "{file_count_type} {} {files_maybe_plural} will be generated in approximately \
        {} {directories_maybe_plural} distributed across a tree of maximum depth {} where each \
        directory contains approximately {} other {dpd_directories_maybe_plural}.\
        {bytes_info}",
        config.files.to_formatted_string(&locale),
        config.informational_total_dirs.to_formatted_string(&locale),
        config.max_depth.to_formatted_string(&locale),
        config
            .informational_dirs_per_dir
            .to_formatted_string(&locale),
        file_count_type = if config.files_exact {
            "Exactly"
        } else {
            "About"
        },
        files_maybe_plural = if config.files == 1 { "file" } else { "files" },
        directories_maybe_plural = if config.informational_total_dirs == 1 {
            "directory"
        } else {
            "directories"
        },
        dpd_directories_maybe_plural = if config.informational_dirs_per_dir == 1 {
            "directory"
        } else {
            "directories"
        },
        bytes_info = if config.bytes > 0 {
            format!(
                " Each file will contain {byte_count_type} {} {bytes_maybe_plural} of random data.",
                config
                    .informational_bytes_per_files
                    .to_formatted_string(&locale),
                byte_count_type = if config.bytes_exact {
                    "exactly"
                } else {
                    "approximately"
                },
                bytes_maybe_plural = if config.informational_bytes_per_files == 1 {
                    "byte"
                } else {
                    "bytes"
                },
            )
        } else {
            String::new()
        },
    )
    .unwrap();
}

fn print_stats(stats: GeneratorStats, output: &mut impl Write) {
    let locale = Locale::en;
    writeln!(
        output,
        "Created {} {files_maybe_plural}{bytes_info} across {} {directories_maybe_plural}.",
        stats.files.to_formatted_string(&locale),
        stats.dirs.to_formatted_string(&locale),
        files_maybe_plural = if stats.files == 1 { "file" } else { "files" },
        directories_maybe_plural = if stats.dirs == 1 {
            "directory"
        } else {
            "directories"
        },
        bytes_info = if stats.bytes > 0 {
            event!(Level::INFO, bytes = stats.bytes, "Exact bytes written");
            format!(" ({})", bytesize::to_string(stats.bytes as u64, false))
        } else {
            String::new()
        }
    )
    .unwrap();
}

fn run_generator(config: Configuration) -> CliResult<GeneratorStats> {
    let parallelism =
        thread::available_parallelism().unwrap_or(unsafe { NonZeroUsize::new_unchecked(1) });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(parallelism.get())
        .build()
        .context("Failed to create tokio runtime")
        .with_code(exitcode::OSERR)?;

    event!(Level::INFO, config = ?config, "Starting config");
    runtime.block_on(run_generator_async(config, parallelism))
}

async fn run_generator_async(
    config: Configuration,
    parallelism: NonZeroUsize,
) -> CliResult<GeneratorStats> {
    let max_depth = config.max_depth as usize;
    let random = {
        let seed = ((config.files.wrapping_add(max_depth) as f64
            * (config.files_per_dir + config.dirs_per_dir)) as u64)
            .wrapping_add(config.seed);
        event!(Level::DEBUG, seed = ?seed, "Starting seed");
        Xoshiro256PlusPlus::seed_from_u64(seed)
    };
    let num_files_distr = Normal::new(config.files_per_dir, config.files_per_dir * 0.2).unwrap();
    let num_dirs_distr = Normal::new(config.dirs_per_dir, config.dirs_per_dir * 0.2).unwrap();
    let num_bytes_distr = Normal::new(config.bytes_per_file, config.bytes_per_file * 0.2).unwrap();

    macro_rules! run {
        ($generator:expr) => {{
            run(config.root_dir, max_depth, parallelism, $generator).await
        }};
    }

    if config.files_exact || config.bytes_exact {
        run!(OtherFilesAndContentsGenerator::new(
            num_files_distr,
            num_dirs_distr,
            if config.bytes > 0 {
                Some(num_bytes_distr)
            } else {
                None
            },
            random,
            if config.files_exact {
                Some(unsafe { NonZeroUsize::new_unchecked(config.files) })
            } else {
                None
            },
            if config.bytes_exact {
                Some(config.bytes)
            } else {
                None
            },
        ))
    } else if config.bytes > 0 {
        run!(FilesAndContentsGenerator {
            num_files_distr,
            num_dirs_distr,
            num_bytes_distr,
            random,
        })
    } else {
        run!(FilesNoContentsGenerator {
            num_files_distr,
            num_dirs_distr,
            random,
        })
    }
}
