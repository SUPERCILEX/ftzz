#![feature(string_remove_matches)]
#![allow(clippy::multiple_crate_versions)]

use std::{
    io,
    io::{stdout, Write},
    num::NonZeroU64,
    path::PathBuf,
    process::{ExitCode, Termination},
};

use clap::{ArgAction, Args, Parser, Subcommand, ValueHint};
use clap_num::si_number;
use clap_verbosity_flag::Verbosity;
use error_stack::{IntoReport, ResultExt};
use paste::paste;

use ftzz::generator::{Generator, NumFilesWithRatio, NumFilesWithRatioError};

/// A random file and directory generator
#[derive(Parser, Debug)]
#[clap(version, author = "Alex Saveau (@SUPERCILEX)")]
#[clap(infer_subcommands = true, infer_long_args = true)]
#[clap(next_display_order = None)]
#[clap(max_term_width = 100)]
#[command(disable_help_flag = true)]
#[cfg_attr(test, clap(help_expected = true))]
struct Ftzz {
    #[clap(subcommand)]
    cmd: Cmd,
    #[clap(flatten)]
    verbose: Verbosity,
    #[arg(short, long, short_alias = '?', global = true)]
    #[arg(action = ArgAction::Help, help = "Print help information (use `--help` for more detail)")]
    #[arg(long_help = "Print help information (use `-h` for a summary)")]
    help: Option<bool>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Generate a random directory hierarchy with some number of files
    ///
    /// A pseudo-random directory hierarchy will be generated (seeded by this
    /// command's input parameters) containing approximately the target
    /// number of files. The exact configuration of files and directories in
    /// the hierarchy is probabilistically determined to mostly match the
    /// specified parameters.
    ///
    /// Generated files and directories are named using monotonically increasing
    /// numbers, where files are named `n` and directories are named `n.dir`
    /// for a given natural number `n`.
    ///
    /// By default, generated files are empty, but random data can be used as
    /// the file contents with the `total-bytes` option.
    Generate(Generate),
}

#[derive(Args, Debug)]
struct Generate {
    /// The directory in which to generate files
    ///
    /// The directory will be created if it does not exist.
    #[clap(value_hint = ValueHint::DirPath)]
    root_dir: PathBuf,

    /// The number of files to generate
    ///
    /// Note: this value is probabilistically respected, meaning any number of
    /// files may be generated so long as we attempt to get close to N.
    #[clap(short = 'n', long = "files", alias = "num-files")]
    #[clap(value_parser = num_files_parser)]
    num_files: NonZeroU64,

    /// Whether or not to generate exactly N files
    #[clap(long = "files-exact")]
    files_exact: bool,

    /// The total amount of random data to be distributed across the generated
    /// files
    ///
    /// Note: this value is probabilistically respected, meaning any amount of
    /// data may be generated so long as we attempt to get close to N.
    #[clap(short = 'b', long = "total-bytes", aliases = & ["num-bytes", "num-total-bytes"])]
    #[clap(value_parser = num_bytes_parser)]
    #[clap(default_value = "0")]
    num_bytes: u64,

    /// Whether or not to generate exactly N bytes
    #[clap(long = "bytes-exact")]
    bytes_exact: bool,

    /// Whether or not to generate exactly N files and bytes
    #[clap(short = 'e', long = "exact")]
    #[clap(conflicts_with_all = & ["files_exact", "bytes_exact"])]
    exact: bool,

    /// The maximum directory tree depth
    #[clap(short = 'd', long = "max-depth", alias = "depth")]
    #[clap(value_parser = max_depth_parser)]
    #[clap(default_value = "5")]
    max_depth: u32,

    /// The number of files to generate per directory (default: files / 1000)
    ///
    /// Note: this value is probabilistically respected, meaning not all
    /// directories will have N files).
    #[clap(short = 'r', long = "ftd-ratio")]
    #[clap(value_parser = file_to_dir_ratio_parser)]
    file_to_dir_ratio: Option<NonZeroU64>,

    /// Change the PRNG's starting seed
    ///
    /// For example, you can use bash's `$RANDOM` function.
    #[clap(long = "seed", alias = "entropy")]
    #[clap(default_value = "0")]
    seed: u64,
}

impl TryFrom<Generate> for Generator {
    type Error = NumFilesWithRatioError;

    fn try_from(options: Generate) -> Result<Self, Self::Error> {
        let builder = Self::builder()
            .root_dir(options.root_dir)
            .files_exact(options.files_exact || options.exact)
            .num_bytes(options.num_bytes)
            .bytes_exact(options.bytes_exact || options.exact)
            .max_depth(options.max_depth);
        let builder = if let Some(ratio) = options.file_to_dir_ratio {
            builder.num_files_with_ratio(NumFilesWithRatio::new(options.num_files, ratio)?)
        } else {
            builder.num_files_with_ratio(NumFilesWithRatio::from_num_files(options.num_files))
        };
        Ok(builder.seed(options.seed).build())
    }
}

#[cfg(test)]
mod generate_tests {
    use rstest::rstest;

    use super::*;

    #[test]
    fn params_are_mapped_correctly() {
        let options = Generate {
            root_dir: PathBuf::from("abc"),
            num_files: NonZeroU64::new(373).unwrap(),
            num_bytes: 637,
            max_depth: 43,
            file_to_dir_ratio: Some(NonZeroU64::new(37).unwrap()),
            seed: 775,
            files_exact: false,
            bytes_exact: false,
            exact: false,
        };

        let generator = Generator::try_from(options).unwrap();
        let hack = format!("{generator:?}");

        assert!(hack.contains("root_dir: \"abc\""));
        assert!(hack.contains("num_files: 373"));
        assert!(hack.contains("num_bytes: 637"));
        assert!(hack.contains("max_depth: 43"));
        assert!(hack.contains("file_to_dir_ratio: 37"));
        assert!(hack.contains("seed: 775"));
    }

    #[rstest]
    fn files_exact_is_mapped_correctly(
        #[values(false, true)] files_exact: bool,
        #[values(false, true)] global_exact: bool,
    ) {
        let options = Generate {
            files_exact,
            exact: global_exact,

            root_dir: PathBuf::new(),
            num_files: NonZeroU64::new(1).unwrap(),
            num_bytes: 0,
            max_depth: 0,
            file_to_dir_ratio: None,
            seed: 0,
            bytes_exact: false,
        };

        let generator = Generator::try_from(options).unwrap();
        let hack = format!("{generator:?}");

        assert!(hack.contains(&format!("files_exact: {}", files_exact || global_exact)));
    }

    #[rstest]
    fn bytes_exact_is_mapped_correctly(
        #[values(false, true)] bytes_exact: bool,
        #[values(false, true)] global_exact: bool,
    ) {
        let options = Generate {
            bytes_exact,
            exact: global_exact,

            root_dir: PathBuf::new(),
            num_files: NonZeroU64::new(1).unwrap(),
            num_bytes: 0,
            max_depth: 0,
            file_to_dir_ratio: None,
            seed: 0,
            files_exact: false,
        };

        let generator = Generator::try_from(options).unwrap();
        let hack = format!("{generator:?}");

        assert!(hack.contains(&format!("bytes_exact: {}", bytes_exact || global_exact)));
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error("File generator failed.")]
    Generator,
    #[error("An argument combination was invalid.")]
    InvalidArgs,
}

fn main() -> ExitCode {
    let args = Ftzz::parse();

    #[cfg(not(feature = "trace"))]
    simple_logger::init_with_level(args.verbose.log_level().unwrap()).unwrap();
    #[cfg(feature = "trace")]
    let _guard = {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

        let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .include_args(true)
            .build();
        tracing_subscriber::registry().with(chrome_layer).init();
        guard
    };

    match ftzz(args) {
        Ok(o) => o.report(),
        Err(err) => {
            drop(writeln!(io::stderr(), "Error: {err:?}"));
            err.report()
        }
    }
}

fn ftzz(ftzz: Ftzz) -> error_stack::Result<(), CliError> {
    match ftzz.cmd {
        Cmd::Generate(options) => Generator::try_from(options)
            .into_report()
            .change_context(CliError::InvalidArgs)?
            .generate(&mut stdout())
            .change_context(CliError::Generator),
    }
}

fn num_files_parser(s: &str) -> Result<NonZeroU64, String> {
    let files = lenient_si_number_u64(s)?;
    if files > 0 {
        Ok(unsafe { NonZeroU64::new_unchecked(files) })
    } else {
        Err(String::from("At least one file must be generated."))
    }
}

fn num_bytes_parser(s: &str) -> Result<u64, String> {
    lenient_si_number_u64(s)
}

fn max_depth_parser(s: &str) -> Result<u32, String> {
    lenient_si_number_u32(s)
}

fn file_to_dir_ratio_parser(s: &str) -> Result<NonZeroU64, String> {
    let ratio = lenient_si_number_u64(s)?;
    if ratio > 0 {
        Ok(unsafe { NonZeroU64::new_unchecked(ratio) })
    } else {
        Err(String::from("Cannot have no files per directory."))
    }
}

macro_rules! lenient_si_number {
    ($ty:ty) => {
        paste! {
            fn [<lenient_si_number_$ty>](s: &str) -> Result<$ty, String> {
                let mut s = s.replace('K', "k");
                s.remove_matches(",");
                s.remove_matches("_");
                si_number(&s)
            }
        }
    };
}

lenient_si_number!(u64);
lenient_si_number!(u32);

#[cfg(test)]
mod cli_tests {
    use std::io::Write;

    use clap::{Command, CommandFactory};
    use goldenfile::Mint;

    use super::*;

    #[test]
    fn verify_app() {
        Ftzz::command().debug_assert();
    }

    #[test]
    #[cfg_attr(miri, ignore)] // wrap_help breaks miri
    fn help_for_review() {
        let mut command = Ftzz::command();

        command.build();

        let mut mint = Mint::new(".");
        let mut long = mint.new_goldenfile("command-reference.golden").unwrap();
        let mut short = mint
            .new_goldenfile("command-reference-short.golden")
            .unwrap();

        write_help(&mut long, &mut command, LongOrShortHelp::Long);
        write_help(&mut short, &mut command, LongOrShortHelp::Short);
    }

    #[derive(Copy, Clone)]
    enum LongOrShortHelp {
        Long,
        Short,
    }

    fn write_help(buffer: &mut impl Write, cmd: &mut Command, long_or_short_help: LongOrShortHelp) {
        match long_or_short_help {
            LongOrShortHelp::Long => cmd.write_long_help(buffer).unwrap(),
            LongOrShortHelp::Short => cmd.write_help(buffer).unwrap(),
        }

        for sub in cmd.get_subcommands_mut() {
            writeln!(buffer).unwrap();
            writeln!(buffer, "---").unwrap();
            writeln!(buffer).unwrap();

            write_help(buffer, sub, long_or_short_help);
        }
    }
}
