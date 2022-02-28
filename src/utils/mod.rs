#[cfg(unix)]
mod fast_path;
#[cfg(unix)]
pub use fast_path::FastPathBuf;
#[cfg(not(unix))]
pub use std::path::PathBuf as FastPathBuf;

mod name_cache;
pub use name_cache::FileNameCache;
