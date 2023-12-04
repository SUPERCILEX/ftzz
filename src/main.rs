#![feature(string_remove_matches)]
#![allow(clippy::multiple_crate_versions)]

use std::{
    io,
    io::{stdout, Write},
    num::NonZeroU64,
    path::PathBuf,
    process::{ExitCode, Termination},
};

use clap::{builder::ArgPredicate, ArgAction, Args, Parser, Subcommand, ValueHint};
use clap_num::si_number;
use clap_verbosity_flag::Verbosity;
use error_stack::ResultExt;
use ftzz::generator::{Generator, NumFilesWithRatio, NumFilesWithRatioError};
use paste::paste;

/// A random file and directory generator
#[derive(Parser, Debug)]
#[command(version, author = "Alex Saveau (@SUPERCILEX)")]
#[command(infer_subcommands = true, infer_long_args = true)]
#[command(disable_help_flag = true)]
#[command(max_term_width = 100)]
#[cfg_attr(test, command(help_expected = true))]
struct Ftzz {
    #[command(subcommand)]
    cmd: Cmd,

    #[command(flatten)]
    #[command(next_display_order = None)]
    verbose: Verbosity,

    #[arg(short, long, short_alias = '?', global = true)]
    #[arg(action = ArgAction::Help, help = "Print help (use `--help` for more detail)")]
    #[arg(long_help = "Print help (use `-h` for a summary)")]
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
#[command(arg_required_else_help = true)]
struct Generate {
    /// The directory in which to generate files
    ///
    /// The directory will be created if it does not exist.
    #[arg(value_hint = ValueHint::DirPath)]
    root_dir: PathBuf,

    /// The number of files to generate
    ///
    /// Note: this value is probabilistically respected, meaning any number of
    /// files may be generated so long as we attempt to get close to N.
    #[arg(short = 'n', long = "files", alias = "num-files")]
    #[arg(value_parser = num_files_parser)]
    num_files: NonZeroU64,

    /// Whether or not to generate exactly N files
    #[arg(long = "files-exact")]
    #[arg(default_value_if("exact", ArgPredicate::IsPresent, "true"))]
    files_exact: bool,

    /// The total amount of random data to be distributed across the generated
    /// files
    ///
    /// Note: this value is probabilistically respected, meaning any amount of
    /// data may be generated so long as we attempt to get close to N.
    #[arg(short = 'b', long = "total-bytes", aliases = & ["num-bytes", "num-total-bytes"])]
    #[arg(group = "num-bytes")]
    #[arg(value_parser = num_bytes_parser)]
    #[arg(default_value = "0")]
    num_bytes: u64,

    /// Specify a specific fill byte to be used instead of deterministically
    /// random data
    ///
    /// This can be used to improve compression ratios of the generated files.
    #[arg(long = "fill-byte")]
    #[arg(requires = "num-bytes")]
    fill_byte: Option<u8>,

    /// Whether or not to generate exactly N bytes
    #[arg(long = "bytes-exact")]
    #[arg(default_value_if("exact", ArgPredicate::IsPresent, "true"))]
    #[arg(requires = "num-bytes")]
    bytes_exact: bool,

    /// Whether or not to generate exactly N files and bytes
    #[arg(short = 'e', long = "exact")]
    #[arg(conflicts_with_all = & ["files_exact", "bytes_exact"])]
    exact: bool,

    /// The maximum directory tree depth
    #[arg(short = 'd', long = "max-depth", alias = "depth")]
    #[arg(value_parser = max_depth_parser)]
    #[arg(default_value = "5")]
    max_depth: u32,

    /// The number of files to generate per directory (default: files / 1000)
    ///
    /// Note: this value is probabilistically respected, meaning not all
    /// directories will have N files).
    #[arg(short = 'r', long = "ftd-ratio")]
    #[arg(value_parser = file_to_dir_ratio_parser)]
    file_to_dir_ratio: Option<NonZeroU64>,

    /// Change the PRNG's starting seed
    ///
    /// For example, you can use bash's `$RANDOM` function.
    #[arg(long = "seed", alias = "entropy")]
    #[arg(default_value = "0")]
    seed: u64,
}

impl TryFrom<Generate> for Generator {
    type Error = NumFilesWithRatioError;

    fn try_from(
        Generate {
            root_dir,
            num_files,
            files_exact,
            num_bytes,
            fill_byte,
            bytes_exact,
            exact: _,
            max_depth,
            file_to_dir_ratio,
            seed,
        }: Generate,
    ) -> Result<Self, Self::Error> {
        let builder = Self::builder();
        let builder = builder.root_dir(root_dir);
        let builder = builder.files_exact(files_exact);
        let builder = builder.num_bytes(num_bytes);
        let builder = builder.bytes_exact(bytes_exact);
        let builder = builder.max_depth(max_depth);
        let builder = builder.seed(seed);
        let builder = builder.fill_byte(fill_byte);
        let builder = if let Some(ratio) = file_to_dir_ratio {
            builder.num_files_with_ratio(NumFilesWithRatio::new(num_files, ratio)?)
        } else {
            builder.num_files_with_ratio(NumFilesWithRatio::from_num_files(num_files))
        };
        Ok(builder.build())
    }
}

#[cfg(test)]
mod generate_tests {
    use super::*;

    #[test]
    fn params_are_mapped_correctly() {
        let options = Generate {
            root_dir: PathBuf::from("abc"),
            num_files: NonZeroU64::new(373).unwrap(),
            num_bytes: 637,
            fill_byte: None,
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
}

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error("File generator failed.")]
    Generator,
    #[error("An argument combination was invalid.")]
    InvalidArgs,
}

fn main() -> ExitCode {
    #[cfg(not(debug_assertions))]
    error_stack::Report::install_debug_hook::<std::panic::Location>(|_, _| {});
    error_stack::Report::install_debug_hook::<ExitCode>(|_, _| {});

    let args = Ftzz::parse();

    #[cfg(not(feature = "trace"))]
    match simple_logger::init_with_level(args.verbose.log_level().unwrap_or_else(log::Level::max)) {
        Ok(()) => {}
        Err(e) => {
            drop(writeln!(io::stderr(), "Failed to initialize logger: {e:?}"));
        }
    }
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

fn ftzz(
    Ftzz {
        cmd,
        verbose: _,
        help: _,
    }: Ftzz,
) -> error_stack::Result<(), CliError> {
    let mut stdout = stdout();
    match cmd {
        Cmd::Generate(options) => Generator::try_from(options)
            .change_context(CliError::InvalidArgs)?
            .generate(&mut fmt_adapter::FmtWriteAdapter::from(&mut stdout))
            .change_context(CliError::Generator),
    }
}

fn num_files_parser(s: &str) -> Result<NonZeroU64, String> {
    NonZeroU64::new(lenient_si_number_u64(s)?)
        .ok_or_else(|| String::from("At least one file must be generated."))
}

fn num_bytes_parser(s: &str) -> Result<u64, String> {
    lenient_si_number_u64(s)
}

fn max_depth_parser(s: &str) -> Result<u32, String> {
    lenient_si_number_u32(s)
}

fn file_to_dir_ratio_parser(s: &str) -> Result<NonZeroU64, String> {
    NonZeroU64::new(lenient_si_number_u64(s)?)
        .ok_or_else(|| String::from("Cannot have no files per directory."))
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

// TODO https://github.com/rust-lang/rust/pull/104389
mod fmt_adapter {
    use std::{
        fmt,
        fmt::Debug,
        io::{Error, Write},
    };

    /// Adapter that enables writing through a [`fmt::Write`] to an underlying
    /// [`io::Write`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// #![feature(impl_fmt_write_for_io_write)]
    /// # use std::{fmt, io};
    /// # use std::io::FmtWriteAdapter;
    ///
    /// let mut output1 = String::new();
    /// let mut output2 = io::stdout();
    /// let mut output2 = FmtWriteAdapter::from(&mut output2);
    ///
    /// my_common_writer(&mut output1).unwrap();
    /// my_common_writer(&mut output2).unwrap();
    ///
    /// fn my_common_writer(output: &mut impl fmt::Write) -> fmt::Result {
    ///     writeln!(output, "Hello World!")
    /// }
    /// ```
    pub struct FmtWriteAdapter<'a, W: Write + ?Sized> {
        inner: &'a mut W,
        error: Option<Error>,
    }

    impl<'a, W: Write + ?Sized> From<&'a mut W> for FmtWriteAdapter<'a, W> {
        fn from(inner: &'a mut W) -> Self {
            Self { inner, error: None }
        }
    }

    impl<W: Write + ?Sized> fmt::Write for FmtWriteAdapter<'_, W> {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            match self.inner.write_all(s.as_bytes()) {
                Ok(()) => {
                    self.error = None;
                    Ok(())
                }
                Err(e) => {
                    self.error = Some(e);
                    Err(fmt::Error)
                }
            }
        }
    }

    impl<W: Write + ?Sized> Debug for FmtWriteAdapter<'_, W> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let mut builder = f.debug_struct("FmtWriteAdapter");
            builder.field("error", &self.error);
            builder.finish()
        }
    }
}

#[cfg(test)]
mod cli_tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn verify_app() {
        Ftzz::command().debug_assert();
    }

    #[test]
    fn help_for_review() {
        supercilex_tests::help_for_review(Ftzz::command());
    }
}
