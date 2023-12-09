#![feature(let_chains)]
#![feature(const_option)]
#![feature(inline_const)]
#![allow(clippy::multiple_crate_versions)]
#![allow(clippy::module_name_repetitions)]

pub use generator::*;

mod core;
mod generator;
mod utils;
