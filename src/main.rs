use std::{
    borrow::Cow,
    io,
    io::{Write, stdout},
    num::NonZeroU64,
    path::PathBuf,
    process::{ExitCode, Termination},
};

use clap::{ArgAction, Args, Parser, ValueHint, builder::ArgPredicate};
use clap_num::si_number;
use clap_verbosity_flag::Verbosity;
use error_stack::ResultExt;
use ftzz::{Generator, NumFilesWithRatio, NumFilesWithRatioError};
use io_adapters::WriteExtension;

#[cfg(not(feature = "trace"))]
type DefaultLevel = clap_verbosity_flag::WarnLevel;
#[cfg(feature = "trace")]
type DefaultLevel = clap_verbosity_flag::TraceLevel;

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
#[derive(Parser, Debug)]
#[command(version, author = "Alex Saveau (@SUPERCILEX)")]
#[command(infer_subcommands = true, infer_long_args = true)]
#[command(disable_help_flag = true)]
#[command(max_term_width = 100)]
#[cfg_attr(test, command(help_expected = true))]
struct Ftzz {
    #[command(flatten)]
    options: Generate,

    #[command(flatten)]
    #[command(next_display_order = None)]
    verbose: Verbosity<DefaultLevel>,

    #[arg(short, long, short_alias = '?', global = true)]
    #[arg(action = ArgAction::Help, help = "Print help (use `--help` for more detail)")]
    #[arg(long_help = "Print help (use `-h` for a summary)")]
    help: Option<bool>,
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
    #[arg(value_parser = si_number::<u64>)]
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
    #[arg(value_parser = si_number::<u32>)]
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
        let builder = builder.maybe_fill_byte(fill_byte);
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

#[cfg(feature = "trace")]
#[global_allocator]
static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
    tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

fn main() -> ExitCode {
    #[cfg(not(debug_assertions))]
    error_stack::Report::install_debug_hook::<std::panic::Location>(|_, _| {});
    error_stack::Report::install_debug_hook::<ExitCode>(|_, _| {});

    let args = Ftzz::parse();

    {
        let level = args.verbose.log_level().unwrap_or_else(log::Level::max);

        #[cfg(not(feature = "trace"))]
        env_logger::builder()
            .format_timestamp(None)
            .filter_level(level.to_level_filter())
            .init();
        #[cfg(feature = "trace")]
        {
            use tracing_log::AsTrace;
            use tracing_subscriber::{
                fmt::format::DefaultFields, layer::SubscriberExt, util::SubscriberInitExt,
            };

            #[derive(Default)]
            struct Config(DefaultFields);

            impl tracing_tracy::Config for Config {
                type Formatter = DefaultFields;

                fn formatter(&self) -> &Self::Formatter {
                    &self.0
                }

                fn stack_depth(&self, _: &tracing::Metadata<'_>) -> u16 {
                    32
                }

                fn format_fields_in_zone_name(&self) -> bool {
                    false
                }
            }

            tracing_subscriber::registry()
                .with(tracing_tracy::TracyLayer::new(Config::default()))
                .with(tracing::level_filters::LevelFilter::from(level.as_trace()))
                .init();
        };
    }

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
        options,
        verbose: _,
        help: _,
    }: Ftzz,
) -> error_stack::Result<(), CliError> {
    let stdout = stdout();
    Generator::try_from(options)
        .change_context(CliError::InvalidArgs)?
        .generate(&mut stdout.write_adapter())
        .change_context(CliError::Generator)
}

fn num_files_parser(s: &str) -> Result<NonZeroU64, Cow<'static, str>> {
    NonZeroU64::new(si_number(s)?).ok_or_else(|| "At least one file must be generated.".into())
}

fn file_to_dir_ratio_parser(s: &str) -> Result<NonZeroU64, Cow<'static, str>> {
    NonZeroU64::new(si_number(s)?).ok_or_else(|| "Cannot have no files per directory.".into())
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
