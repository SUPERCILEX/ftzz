#![feature(string_remove_matches)]

use std::{path::PathBuf, process::exit};

use clap_num::si_number;
use clap_verbosity_flag::Verbosity;
use simple_logger::SimpleLogger;
use structopt::{clap::AppSettings, StructOpt};

use crate::{errors::CliResult, generator::generate};

mod errors;
mod generator;

/// A random file and directory generator
#[derive(Debug, StructOpt)]
#[structopt(
author = "Alex Saveau (@SUPERCILEX)",
global_settings = & [AppSettings::InferSubcommands, AppSettings::ColoredHelp],
)]
struct Ftzz {
    #[structopt(flatten)]
    verbose: Verbosity,

    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, StructOpt)]
enum Cmd {
    /// Generate a random directory hierarchy with some number of files
    ///
    /// A pseudo-random directory hierarchy will be generated (seeded by this command's input
    /// parameters) containing approximately the target number of files. The exact configuration of
    /// files and directories in the hierarchy is probabilistically determined to mostly match the
    /// specified parameters.
    Generate(Generate),
}

#[derive(Debug, StructOpt)]
pub struct Generate {
    /// The directory in which to generate files (will be created if it does not exist)
    root_dir: PathBuf,

    /// The number of files to generate (this value is probabilistically respected, meaning any
    /// number of files may be generated so long as we attempt to get close to N)
    #[structopt(short = "n", long = "files", parse(try_from_str = num_files_parser))]
    num_files: usize,

    /// The maximum directory tree depth
    #[structopt(short = "d", long = "depth", default_value = "5")]
    max_depth: u32,

    /// The number of files to generate per directory (this value is probabilistically respected,
    /// meaning not all directories will have N files) (default: files / 1000)
    #[structopt(short = "r", long = "ftd_ratio", parse(try_from_str = file_to_dir_ratio_parser))]
    file_to_dir_ratio: Option<usize>,

    /// Add some additional entropy to the starting seed of our PRNG
    #[structopt(long = "entropy", default_value = "0")]
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
    let args = Ftzz::from_args();
    SimpleLogger::new()
        .with_level(args.verbose.log_level().unwrap().to_level_filter())
        .init()
        .unwrap();

    match args.cmd {
        Cmd::Generate(options) => generate(options),
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
