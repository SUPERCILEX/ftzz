use std::process::exit;

use clap_verbosity_flag::Verbosity;
use simple_logger::SimpleLogger;
use structopt::{clap::AppSettings, StructOpt};

use ftzz::{
    errors::CliResult,
    generator::{generate, Generate},
};

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
