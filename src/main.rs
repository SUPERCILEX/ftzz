#![feature(string_remove_matches)]

use std::path::PathBuf;

use anyhow::Context;
use clap::{AppSettings, Args, Parser, Subcommand, ValueHint};
use clap_num::si_number;
use clap_verbosity_flag::Verbosity;
use cli_errors::{CliExitAnyhowWrapper, CliExitError, CliResult};

use ftzz::generator::{Generator, GeneratorBuilder};

/// A random file and directory generator
#[derive(Parser, Debug)]
#[clap(version, author = "Alex Saveau (@SUPERCILEX)")]
#[clap(global_setting(AppSettings::InferSubcommands))]
#[clap(global_setting(AppSettings::UseLongFormatForHelpSubcommand))]
#[cfg_attr(test, clap(global_setting(AppSettings::HelpExpected)))]
struct Ftzz {
    #[clap(flatten)]
    verbose: Verbosity,
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Generate a random directory hierarchy with some number of files
    ///
    /// A pseudo-random directory hierarchy will be generated (seeded by this command's input
    /// parameters) containing approximately the target number of files. The exact configuration of
    /// files and directories in the hierarchy is probabilistically determined to mostly match the
    /// specified parameters.
    ///
    /// Generated files and directories are named using monotonically increasing numbers, where
    /// files are named `n` and directories are named `n.dir` for a given natural number `n`.
    ///
    /// By default, generated files are empty, but random data can be used as the file contents with
    /// the `total-bytes` option.
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
    /// Note: this value is probabilistically respected, meaning any number of files may be
    /// generated so long as we attempt to get close to N.
    #[clap(short = 'n', long = "files", alias = "num-files")]
    #[clap(parse(try_from_str = num_files_parser))]
    num_files: usize,

    /// Whether or not to generate exactly N files
    #[clap(long = "files-exact")]
    files_exact: bool,

    /// The total amount of random data to be distributed across the generated files
    ///
    /// Note: this value is probabilistically respected, meaning any amount of data may be
    /// generated so long as we attempt to get close to N.
    #[clap(short = 'b', long = "total-bytes", aliases = & ["num-bytes", "num-total-bytes"])]
    #[clap(parse(try_from_str = num_bytes_parser))]
    #[clap(default_value = "0")]
    num_bytes: usize,

    /// Whether or not to generate exactly N bytes
    #[clap(long = "bytes-exact")]
    bytes_exact: bool,

    /// Whether or not to generate exactly N files and bytes
    #[clap(short = 'e', long = "exact")]
    #[clap(conflicts_with_all = & ["files-exact", "bytes-exact"])]
    exact: bool,

    /// The maximum directory tree depth
    #[clap(short = 'd', long = "max-depth", alias = "depth")]
    #[clap(default_value = "5")]
    max_depth: u32,

    /// The number of files to generate per directory (default: files / 1000)
    ///
    /// Note: this value is probabilistically respected, meaning not all directories will have N
    /// files).
    #[clap(short = 'r', long = "ftd-ratio")]
    #[clap(parse(try_from_str = file_to_dir_ratio_parser))]
    file_to_dir_ratio: Option<usize>,

    /// Change the PRNG's starting seed
    ///
    /// For example, you can use bash's `$RANDOM` function.
    #[clap(long = "seed", alias = "entropy")]
    #[clap(default_value = "0")]
    seed: u64,
}

impl TryFrom<Generate> for Generator {
    type Error = CliExitError;

    fn try_from(options: Generate) -> Result<Self, Self::Error> {
        let mut builder = GeneratorBuilder::default();
        builder
            .root_dir(options.root_dir)
            .num_files(options.num_files)
            .files_exact(options.files_exact || options.exact)
            .num_bytes(options.num_bytes)
            .bytes_exact(options.bytes_exact || options.exact)
            .max_depth(options.max_depth);
        if let Some(ratio) = options.file_to_dir_ratio {
            builder.file_to_dir_ratio(ratio);
        }
        builder
            .seed(options.seed)
            .build()
            .context("Input validation failed")
            .with_code(exitcode::DATAERR)
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
            num_files: 373,
            num_bytes: 637,
            max_depth: 43,
            file_to_dir_ratio: Some(37),
            seed: 775,
            files_exact: false,
            bytes_exact: false,
            exact: false,
        };

        let generator = Generator::try_from(options).unwrap();
        let hack = format!("{:?}", generator);

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
            num_files: 1,
            num_bytes: 0,
            max_depth: 0,
            file_to_dir_ratio: None,
            seed: 0,
            bytes_exact: false,
        };

        let generator = Generator::try_from(options).unwrap();
        let hack = format!("{:?}", generator);

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
            num_files: 1,
            num_bytes: 0,
            max_depth: 0,
            file_to_dir_ratio: None,
            seed: 0,
            files_exact: false,
        };

        let generator = Generator::try_from(options).unwrap();
        let hack = format!("{:?}", generator);

        assert!(hack.contains(&format!("bytes_exact: {}", bytes_exact || global_exact)));
    }
}

#[cli_errors::main]
fn main() -> CliResult<()> {
    let args = Ftzz::parse();
    simple_logger::init_with_level(args.verbose.log_level().unwrap()).unwrap();

    match args.cmd {
        Cmd::Generate(options) => Generator::try_from(options)?.generate(),
    }
}

fn num_files_parser(s: &str) -> Result<usize, String> {
    let files = lenient_si_number(s)?;
    if files > 0 {
        Ok(files)
    } else {
        Err(String::from("At least one file must be generated."))
    }
}

fn num_bytes_parser(s: &str) -> Result<usize, String> {
    lenient_si_number(s)
}

fn file_to_dir_ratio_parser(s: &str) -> Result<usize, String> {
    let ratio = lenient_si_number(s)?;
    if ratio > 0 {
        Ok(ratio)
    } else {
        Err(String::from("Cannot have no files per directory."))
    }
}

fn lenient_si_number(s: &str) -> Result<usize, String> {
    let mut s = s.replace('K', "k");
    s.remove_matches(",");
    s.remove_matches("_");
    si_number(&s)
}

#[cfg(test)]
mod cli_tests {
    use clap::IntoApp;

    use super::*;

    #[test]
    fn verify_app() {
        Ftzz::into_app().debug_assert();
    }
}

#[cfg(test)]
mod cli_generate_tests {
    use clap::{
        ErrorKind::{
            ArgumentConflict, DisplayHelpOnMissingArgumentOrSubcommand, MissingRequiredArgument,
            UnknownArgument,
        },
        FromArgMatches, IntoApp,
    };

    use super::*;

    macro_rules! expect_error {
        ($args:expr, $error:expr) => {
            let f = Ftzz::try_parse_from($args);

            assert!(f.is_err());
            assert_eq!(f.unwrap_err().kind, $error);
        };
    }

    macro_rules! expect_success {
        ($args:expr) => {{
            let m = Ftzz::into_app().get_matches_from($args);
            <Generate as FromArgMatches>::from_arg_matches(
                m.subcommand_matches("generate").unwrap(),
            )
            .unwrap()
        }};
    }

    #[test]
    fn empty_args_displays_help() {
        expect_error!(
            Vec::<String>::new(),
            DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn generate_empty_args_displays_error() {
        expect_error!(vec!["ftzz", "generate"], MissingRequiredArgument);
    }

    #[test]
    fn generate_minimal_use_case_uses_defaults() {
        let g = expect_success!(vec!["ftzz", "generate", "-n", "1", "dir"]);

        assert_eq!(g.root_dir, PathBuf::from("dir"));
        assert_eq!(g.num_files, 1);
        assert_eq!(g.max_depth, 5);
        assert_eq!(g.file_to_dir_ratio, None);
        assert_eq!(g.seed, 0);
        assert!(!g.files_exact);
        assert!(!g.bytes_exact);
        assert!(!g.exact);
        assert_eq!(g.num_bytes, 0);
    }

    #[test]
    fn generate_num_files_rejects_negatives() {
        expect_error!(vec!["ftzz", "generate", "-n", "-1", "dir"], UnknownArgument);
    }

    #[test]
    fn generate_num_files_accepts_plain_nums() {
        let g = expect_success!(vec!["ftzz", "generate", "--files", "1000", "dir"]);

        assert_eq!(g.num_files, 1000);
    }

    #[test]
    fn generate_short_num_files_accepts_plain_nums() {
        let g = expect_success!(vec!["ftzz", "generate", "-n", "1000", "dir"]);

        assert_eq!(g.num_files, 1000);
    }

    #[test]
    fn generate_num_files_accepts_si_numbers() {
        let g = expect_success!(vec!["ftzz", "generate", "--files", "1K", "dir"]);

        assert_eq!(g.num_files, 1000);
    }

    #[test]
    fn generate_num_files_accepts_commas() {
        let g = expect_success!(vec!["ftzz", "generate", "--files", "1,000", "dir"]);

        assert_eq!(g.num_files, 1000);
    }

    #[test]
    fn generate_num_files_accepts_underscores() {
        let g = expect_success!(vec!["ftzz", "generate", "--files", "1_000", "dir"]);

        assert_eq!(g.num_files, 1000);
    }

    #[test]
    fn generate_max_depth_rejects_negatives() {
        expect_error!(
            vec!["ftzz", "generate", "--depth", "-1", "-n", "1", "dir"],
            UnknownArgument
        );
    }

    #[test]
    fn generate_max_depth_accepts_plain_nums() {
        let g = expect_success!(vec!["ftzz", "generate", "--depth", "123", "-n", "1", "dir"]);

        assert_eq!(g.max_depth, 123);
    }

    #[test]
    fn generate_short_max_depth_accepts_plain_nums() {
        let g = expect_success!(vec!["ftzz", "generate", "-d", "123", "-n", "1", "dir"]);

        assert_eq!(g.max_depth, 123);
    }

    #[test]
    fn generate_ratio_rejects_negatives() {
        expect_error!(
            vec!["ftzz", "generate", "--ftd-ratio", "-1", "-n", "1", "dir",],
            UnknownArgument
        );
    }

    #[test]
    fn generate_ratio_accepts_plain_nums() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "--ftd-ratio",
            "1000",
            "-n",
            "1",
            "dir",
        ]);

        assert_eq!(g.file_to_dir_ratio, Some(1000));
    }

    #[test]
    fn generate_short_ratio_accepts_plain_nums() {
        let g = expect_success!(vec!["ftzz", "generate", "-r", "321", "-n", "1", "dir"]);

        assert_eq!(g.file_to_dir_ratio, Some(321));
    }

    #[test]
    fn generate_ratio_accepts_si_numbers() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "--ftd-ratio",
            "1K",
            "-n",
            "1",
            "dir",
        ]);

        assert_eq!(g.file_to_dir_ratio, Some(1000));
    }

    #[test]
    fn generate_ratio_accepts_commas() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "--ftd-ratio",
            "1,000",
            "-n",
            "1",
            "dir",
        ]);

        assert_eq!(g.file_to_dir_ratio, Some(1000));
    }

    #[test]
    fn generate_ratio_accepts_underscores() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "--ftd-ratio",
            "1_000",
            "-n",
            "1",
            "dir",
        ]);

        assert_eq!(g.file_to_dir_ratio, Some(1000));
    }

    #[test]
    fn generate_seed_rejects_negatives() {
        expect_error!(
            vec!["ftzz", "generate", "--seed", "-1", "-n", "1", "dir",],
            UnknownArgument
        );
    }

    #[test]
    fn generate_seed_accepts_plain_nums() {
        let g = expect_success!(vec!["ftzz", "generate", "--seed", "231", "-n", "1", "dir",]);

        assert_eq!(g.seed, 231);
    }

    #[test]
    fn generate_num_bytes_accepts_plain_nums() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "-n",
            "1",
            "dir",
            "--total-bytes",
            "1000",
        ]);

        assert_eq!(g.num_bytes, 1000);
    }

    #[test]
    fn generate_short_num_bytes_accepts_plain_nums() {
        let g = expect_success!(vec!["ftzz", "generate", "-n", "1", "dir", "-b", "1000"]);

        assert_eq!(g.num_bytes, 1000);
    }

    #[test]
    fn generate_num_bytes_accepts_si_numbers() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "-n",
            "1",
            "dir",
            "--total-bytes",
            "1K",
        ]);

        assert_eq!(g.num_bytes, 1000);
    }

    #[test]
    fn generate_num_bytes_accepts_commas() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "-n",
            "1",
            "dir",
            "--total-bytes",
            "1,000",
        ]);

        assert_eq!(g.num_bytes, 1000);
    }

    #[test]
    fn generate_num_bytes_accepts_underscores() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "-n",
            "1",
            "dir",
            "--total-bytes",
            "1_000",
        ]);

        assert_eq!(g.num_bytes, 1000);
    }

    #[test]
    fn generate_files_exact_and_exact_conflict() {
        expect_error!(
            vec![
                "ftzz",
                "generate",
                "-n",
                "1",
                "dir",
                "--files-exact",
                "--exact",
            ],
            ArgumentConflict
        );
    }

    #[test]
    fn generate_bytes_exact_and_exact_conflict() {
        expect_error!(
            vec![
                "ftzz",
                "generate",
                "-n",
                "1",
                "dir",
                "--bytes-exact",
                "--exact",
            ],
            ArgumentConflict
        );
    }

    #[test]
    fn generate_files_exact_and_bytes_exact_can_be_used() {
        let g = expect_success!(vec![
            "ftzz",
            "generate",
            "-n",
            "1",
            "dir",
            "--files-exact",
            "--bytes-exact",
        ]);

        assert!(g.files_exact);
        assert!(g.bytes_exact);
    }

    #[test]
    fn generate_exact_can_be_used() {
        let g = expect_success!(vec!["ftzz", "generate", "-n", "1", "dir", "--exact"]);

        assert!(g.exact);
    }

    #[test]
    fn generate_short_exact_can_be_used() {
        let g = expect_success!(vec!["ftzz", "generate", "-n", "1", "dir", "-e"]);

        assert!(g.exact);
    }
}
