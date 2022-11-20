#[cfg(unix)]
mod fast_path;
#[cfg(not(unix))]
pub use std::path::PathBuf as FastPathBuf;

#[cfg(unix)]
pub use fast_path::FastPathBuf;

mod file_names;
pub use file_names::*;
