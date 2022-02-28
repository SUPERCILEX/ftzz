use std::mem::ManuallyDrop;

use tracing::{event, instrument, span, Level};

/// Specialized cache for file names that takes advantage of our monotonically increasing integer
/// naming scheme.
///
/// We intentionally use a thread-*un*safe raw fixed-size buffer to eliminate an Arc.
#[derive(Copy, Clone)]
pub struct FileNameCache {
    file_cache: (*mut String, usize),
    dir_cache: (*mut String, usize),
}

unsafe impl Send for FileNameCache {}

impl FileNameCache {
    #[instrument(level = "trace")]
    pub fn alloc(files_per_dir: f64, dirs_per_dir: f64) -> Self {
        let num_cache_entries = files_per_dir + dirs_per_dir;
        let files_percentage = files_per_dir / num_cache_entries;

        // Overestimate since the cache can't grow
        let num_cache_entries = 1.5 * num_cache_entries;
        // Max out the cache size at 1MiB
        let num_cache_entries = f64::min((1 << 20) as f64, num_cache_entries);

        let file_entries = files_percentage * num_cache_entries;
        let dir_entries = num_cache_entries - file_entries;

        let alloc_span = span!(Level::TRACE, "file_cache_alloc");
        let span_guard = alloc_span.enter();
        let mut file_cache =
            ManuallyDrop::new(Vec::<String>::with_capacity(file_entries.round() as usize));
        for (i, entry) in file_cache.spare_capacity_mut().iter_mut().enumerate() {
            entry.write(FileNameCache::file_name(i));
        }
        drop(span_guard);

        let alloc_span = span!(Level::TRACE, "dir_cache_alloc");
        let span_guard = alloc_span.enter();
        let mut dir_cache =
            ManuallyDrop::new(Vec::<String>::with_capacity(dir_entries.round() as usize));
        for (i, entry) in dir_cache.spare_capacity_mut().iter_mut().enumerate() {
            entry.write(FileNameCache::dir_name(i));
        }
        drop(span_guard);

        unsafe {
            let cap = file_cache.capacity();
            file_cache.set_len(cap);
            let cap = dir_cache.capacity();
            dir_cache.set_len(cap);
        }

        event!(
            Level::DEBUG,
            files = file_cache.len(),
            dirs = dir_cache.len(),
            "Name cache allocations"
        );

        Self {
            file_cache: (file_cache.as_mut_ptr(), file_cache.len()),
            dir_cache: (dir_cache.as_mut_ptr(), dir_cache.len()),
        }
    }

    pub fn free(self) {
        unsafe {
            Vec::from_raw_parts(self.file_cache.0, self.file_cache.1, self.file_cache.1);
            Vec::from_raw_parts(self.dir_cache.0, self.dir_cache.1, self.dir_cache.1);
        }
    }

    pub fn with_file_name<T>(self, i: usize, f: impl FnOnce(&str) -> T) -> T {
        if i >= self.file_cache.1 {
            return f(&FileNameCache::file_name(i));
        }

        f(unsafe { self.file_cache.0.add(i).as_ref().unwrap_unchecked() })
    }

    pub fn with_dir_name<T>(self, i: usize, f: impl FnOnce(&str) -> T) -> T {
        if i >= self.dir_cache.1 {
            return f(&FileNameCache::dir_name(i));
        }

        f(unsafe { self.dir_cache.0.add(i).as_ref().unwrap_unchecked() })
    }

    #[inline]
    fn file_name(i: usize) -> String {
        i.to_string()
    }

    #[inline]
    fn dir_name(i: usize) -> String {
        format!("{}.dir", i)
    }
}
