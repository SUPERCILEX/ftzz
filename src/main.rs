#![feature(string_remove_matches)]

use std::{path::PathBuf, process::exit};

use clap::{AppSettings, Args, Parser, Subcommand, ValueHint};
use clap_num::si_number;

use ftzz::{errors::CliResult, generator::GeneratorBuilder};

/// A random file and directory generator
#[derive(Parser, Debug)]
#[clap(version, author = "Alex Saveau (@SUPERCILEX)")]
#[clap(global_setting(AppSettings::InferSubcommands))]
#[clap(global_setting(AppSettings::UseLongFormatForHelpSubcommand))]
#[cfg_attr(test, clap(global_setting(AppSettings::HelpExpected)))]
struct Ftzz {
    // #[clap(flatten)]
    // verbose: Verbosity,
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
    #[clap(short = 'n', long = "files", parse(try_from_str = num_files_parser))]
    num_files: usize,

    /// The maximum directory tree depth
    #[clap(short = 'd', long = "depth", default_value = "5")]
    max_depth: u32,

    /// The number of files to generate per directory (default: files / 1000)
    ///
    /// Note: this value is probabilistically respected, meaning not all directories will have N
    /// files).
    #[clap(short = 'r', long = "ftd-ratio", parse(try_from_str = file_to_dir_ratio_parser))]
    file_to_dir_ratio: Option<usize>,

    /// Add some additional entropy to the PRNG's starting seed
    ///
    /// For example, you can use bash's `$RANDOM` function.
    #[clap(long = "entropy", default_value = "0")]
    entropy: u64,
}

fn main() {
    if let Err(e) = wrapped_main() {
        if let Some(source) = e.source {
            eprintln!("{:?}", source);
        }
        exit(e.code);
    }
}

fn wrapped_main() -> CliResult<()> {
    let args = Ftzz::parse();
    // TODO waiting on https://github.com/rust-cli/clap-verbosity-flag/issues/29
    // SimpleLogger::new()
    //     .with_level(args.verbose.log_level().unwrap().to_level_filter())
    //     .init()
    //     .unwrap();

    match args.cmd {
        Cmd::Generate(options) => GeneratorBuilder::default()
            .root_dir(options.root_dir)
            .num_files(options.num_files)
            .max_depth(options.max_depth)
            .file_to_dir_ratio(options.file_to_dir_ratio)
            .entropy(options.entropy)
            .build()
            .unwrap()
            .generate(),
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

fn file_to_dir_ratio_parser(s: &str) -> Result<usize, String> {
    let ratio = lenient_si_number(s)?;
    if ratio > 0 {
        Ok(ratio)
    } else {
        Err(String::from("Cannot have no files per directory."))
    }
}

fn lenient_si_number(s: &str) -> Result<usize, String> {
    let mut s = s.replace("K", "k");
    s.remove_matches(",");
    si_number(&s)
}
