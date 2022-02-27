use std::{
    cmp::{max, min},
    collections::VecDeque,
    ffi::{CStr, OsStr},
    fs::{create_dir_all, File},
    io,
    io::{ErrorKind::NotFound, Write},
    mem::{ManuallyDrop, MaybeUninit},
    ops::Deref,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf, MAIN_SEPARATOR},
};

use anyhow::{anyhow, Context};
use cli_errors::{CliExitAnyhowWrapper, CliResult};
use derive_builder::Builder;
use num_format::{Locale, ToFormattedString};
use rand::{distributions::Distribution, RngCore, SeedableRng};
use rand_distr::{LogNormal, Normal};
use rand_xoshiro::Xoshiro256PlusPlus;
use tokio::task;
use tracing::{event, instrument, span, Level};

#[derive(Builder, Debug)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct Generator {
    root_dir: PathBuf,
    num_files: usize,
    #[builder(default = "false")]
    files_exact: bool,
    #[builder(default = "0")]
    num_bytes: usize,
    #[builder(default = "false")]
    bytes_exact: bool,
    #[builder(default = "5")]
    max_depth: u32,
    #[builder(default = "self.default_ftd_ratio()")]
    file_to_dir_ratio: usize,
    #[builder(default = "0")]
    seed: u64,
}

impl GeneratorBuilder {
    fn validate(&self) -> Result<(), String> {
        // TODO use if let chains once that feature is stabilized
        if let Some(n) = self.num_files {
            if n < 1 {
                return Err("num_files must be strictly positive".to_string());
            }
        }
        if let Some(ratio) = self.file_to_dir_ratio {
            if ratio < 1 {
                return Err("file_to_dir_ratio must be strictly positive".to_string());
            }

            if let Some(num_files) = self.num_files {
                if ratio > num_files {
                    return Err(format!(
                        "The file to dir ratio ({}) cannot be larger than the number of files to generate ({}).",
                        ratio,
                        num_files,
                    ));
                }
            }
        }

        Ok(())
    }

    fn default_ftd_ratio(&self) -> usize {
        max(self.num_files.unwrap() / 1000, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_params_succeeds() {
        let g = GeneratorBuilder::default()
            .root_dir(PathBuf::from("abc"))
            .num_files(1)
            .build()
            .unwrap();

        assert_eq!(g.root_dir, PathBuf::from("abc"));
        assert_eq!(g.num_files, 1);
        assert!(!g.files_exact);
        assert_eq!(g.num_bytes, 0);
        assert!(!g.bytes_exact);
        assert_eq!(g.max_depth, 5);
        assert_eq!(g.file_to_dir_ratio, 1);
        assert_eq!(g.seed, 0);
    }

    #[test]
    fn zero_files_fails() {
        let g = GeneratorBuilder::default()
            .root_dir(PathBuf::from("abc"))
            .num_files(0)
            .build();

        assert!(g.is_err());
    }

    #[test]
    fn zero_ratio_fails() {
        let g = GeneratorBuilder::default()
            .root_dir(PathBuf::from("abc"))
            .num_files(1)
            .file_to_dir_ratio(0)
            .build();

        assert!(g.is_err());
    }

    #[test]
    fn ratio_greater_than_num_files_fails() {
        let g = GeneratorBuilder::default()
            .root_dir(PathBuf::from("abc"))
            .num_files(1)
            .file_to_dir_ratio(2)
            .build();

        assert!(g.is_err());
    }
}

impl Generator {
    pub fn generate(self) -> CliResult<()> {
        let options = validated_options(self)?;
        print_configuration_info(&options);
        print_stats(run_generator(options)?);
        Ok(())
    }
}

#[derive(Debug)]
struct Configuration {
    root_dir: PathBuf,
    files: usize,
    bytes: usize,
    files_exact: bool,
    bytes_exact: bool,
    files_per_dir: f64,
    dirs_per_dir: f64,
    bytes_per_file: f64,
    max_depth: u32,
    seed: u64,

    informational_dirs_per_dir: usize,
    informational_total_dirs: usize,
    informational_bytes_per_files: usize,
}

struct GeneratorStats {
    files: usize,
    dirs: usize,
    bytes: usize,
}

fn validated_options(generator: Generator) -> CliResult<Configuration> {
    create_dir_all(&generator.root_dir)
        .with_context(|| format!("Failed to create directory {:?}", generator.root_dir))
        .with_code(exitcode::IOERR)?;
    if generator
        .root_dir
        .read_dir()
        .with_context(|| format!("Failed to read directory {:?}", generator.root_dir))
        .with_code(exitcode::IOERR)?
        .count()
        != 0
    {
        return Err(anyhow!(format!(
            "The root directory {:?} must be empty.",
            generator.root_dir,
        )))
        .with_code(exitcode::DATAERR);
    }

    let num_files = generator.num_files as f64;
    let bytes_per_file = generator.num_bytes as f64 / num_files;

    if generator.max_depth == 0 {
        return Ok(Configuration {
            root_dir: generator.root_dir,
            files: generator.num_files,
            bytes: generator.num_bytes,
            files_exact: generator.files_exact,
            bytes_exact: generator.bytes_exact,
            files_per_dir: num_files,
            dirs_per_dir: 0.,
            bytes_per_file,
            max_depth: 0,
            seed: generator.seed,

            informational_dirs_per_dir: 0,
            informational_total_dirs: 1,
            informational_bytes_per_files: bytes_per_file.round() as usize,
        });
    }

    let ratio = generator.file_to_dir_ratio as f64;
    let num_dirs = num_files / ratio;
    // This formula was derived from the following equation:
    // num_dirs = unknown_num_dirs_per_dir^max_depth
    let dirs_per_dir = num_dirs.powf(1f64 / generator.max_depth as f64);

    Ok(Configuration {
        root_dir: generator.root_dir,
        files: generator.num_files,
        bytes: generator.num_bytes,
        files_exact: generator.files_exact,
        bytes_exact: generator.bytes_exact,
        files_per_dir: ratio,
        bytes_per_file,
        dirs_per_dir,
        max_depth: generator.max_depth,
        seed: generator.seed,

        informational_dirs_per_dir: dirs_per_dir.round() as usize,
        informational_total_dirs: num_dirs.round() as usize,
        informational_bytes_per_files: bytes_per_file.round() as usize,
    })
}

fn print_configuration_info(config: &Configuration) {
    let locale = Locale::en;
    println!(
        "{file_count_type} {} {files_maybe_plural} will be generated in approximately \
        {} {directories_maybe_plural} distributed across a tree of maximum depth {} where each \
        directory contains approximately {} other {dpd_directories_maybe_plural}.\
        {bytes_info}",
        config.files.to_formatted_string(&locale),
        config.informational_total_dirs.to_formatted_string(&locale),
        config.max_depth.to_formatted_string(&locale),
        config
            .informational_dirs_per_dir
            .to_formatted_string(&locale),
        file_count_type = if config.files_exact {
            "Exactly"
        } else {
            "About"
        },
        files_maybe_plural = if config.files == 1 { "file" } else { "files" },
        directories_maybe_plural = if config.informational_total_dirs == 1 {
            "directory"
        } else {
            "directories"
        },
        dpd_directories_maybe_plural = if config.informational_dirs_per_dir == 1 {
            "directory"
        } else {
            "directories"
        },
        bytes_info = if config.bytes > 0 {
            format!(
                " Each file will contain approximately {} {bytes_maybe_plural} of random data.",
                config
                    .informational_bytes_per_files
                    .to_formatted_string(&locale),
                bytes_maybe_plural = if config.informational_bytes_per_files == 1 {
                    "byte"
                } else {
                    "bytes"
                },
            )
        } else {
            "".to_string()
        },
    );
}

fn print_stats(stats: GeneratorStats) {
    let locale = Locale::en;
    println!(
        "Created {} {files_maybe_plural}{bytes_info} across {} {directories_maybe_plural}.",
        stats.files.to_formatted_string(&locale),
        stats.dirs.to_formatted_string(&locale),
        files_maybe_plural = if stats.files == 1 { "file" } else { "files" },
        directories_maybe_plural = if stats.dirs == 1 {
            "directory"
        } else {
            "directories"
        },
        bytes_info = if stats.bytes > 0 {
            event!(Level::INFO, bytes = stats.bytes, "Exact bytes written");
            format!(" ({})", bytesize::to_string(stats.bytes as u64, false))
        } else {
            "".to_string()
        }
    );
}

/// Specialized cache for file names that takes advantage of our monotonically increasing integer
/// naming scheme.
///
/// We intentionally use a thread-*un*safe raw fixed-size buffer to eliminate an Arc.
#[derive(Copy, Clone)]
struct FileNameCache {
    file_cache: (*mut String, usize),
    dir_cache: (*mut String, usize),
}

unsafe impl Send for FileNameCache {}

impl FileNameCache {
    #[instrument(level = "trace")]
    fn alloc(files_per_dir: f64, dirs_per_dir: f64) -> Self {
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

    fn free(self) {
        unsafe {
            Vec::from_raw_parts(self.file_cache.0, self.file_cache.1, self.file_cache.1);
            Vec::from_raw_parts(self.dir_cache.0, self.dir_cache.1, self.dir_cache.1);
        }
    }

    fn with_file_name<T>(self, i: usize, f: impl FnOnce(&str) -> T) -> T {
        if i >= self.file_cache.1 {
            return f(&FileNameCache::file_name(i));
        }

        f(unsafe { self.file_cache.0.add(i).as_ref().unwrap_unchecked() })
    }

    fn with_dir_name<T>(self, i: usize, f: impl FnOnce(&str) -> T) -> T {
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

/// A specialized PathBuf implementation that takes advantage of a few assumptions. Specifically,
/// it *only* supports adding single-level directories (e.g. "foo", "foo/bar" is not allowed) and
/// updating the current file name. Any other usage is UB.
struct FastPathBuf {
    inner: Vec<u8>,
    last_len: usize,
}

impl FastPathBuf {
    fn push(&mut self, name: &str) {
        self.last_len = self.inner.len();

        self.inner.reserve(name.len() + 1);
        self.inner.push(MAIN_SEPARATOR as u8);
        self.inner.extend_from_slice(name.as_bytes());
    }

    fn pop(&mut self) {
        if self.inner.len() > self.last_len {
            self.inner.truncate(self.last_len);
        } else {
            self.inner.truncate(
                unsafe { self.parent().unwrap_unchecked() }
                    .as_os_str()
                    .len(),
            );
        }
    }

    fn set_file_name(&mut self, name: &str) {
        self.pop();
        self.push(name);
    }

    fn push_cloned(&self, name: &str) -> FastPathBuf {
        let mut buf = FastPathBuf {
            inner: Vec::with_capacity(self.inner.len() + 1 + name.len()),
            last_len: 0,
        };

        buf.inner.clone_from(&self.inner);
        buf.push(name);
        buf.last_len = self.last_len;

        buf
    }

    fn to_cstr_mut(&mut self) -> CStrFastPathBufGuard {
        CStrFastPathBufGuard::new(self)
    }
}

impl From<PathBuf> for FastPathBuf {
    fn from(p: PathBuf) -> Self {
        let inner = p.into_os_string().into_vec();
        Self {
            last_len: inner.len(),
            inner,
        }
    }
}

impl Deref for FastPathBuf {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        OsStr::from_bytes(&self.inner).as_ref()
    }
}

impl AsRef<Path> for FastPathBuf {
    fn as_ref(&self) -> &Path {
        self
    }
}

impl Clone for FastPathBuf {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            last_len: self.last_len,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.inner.clone_from(&source.inner);
        self.last_len = source.last_len;
    }
}

struct CStrFastPathBufGuard<'a> {
    buf: &'a mut FastPathBuf,
}

impl<'a> CStrFastPathBufGuard<'a> {
    fn new(buf: &mut FastPathBuf) -> CStrFastPathBufGuard {
        buf.inner.push(0); // NUL terminator
        CStrFastPathBufGuard { buf }
    }
}

impl<'a> Deref for CStrFastPathBufGuard<'a> {
    type Target = CStr;

    fn deref(&self) -> &Self::Target {
        if cfg!(debug_assertions) {
            CStr::from_bytes_with_nul(&self.buf.inner).unwrap()
        } else {
            unsafe { CStr::from_bytes_with_nul_unchecked(&self.buf.inner) }
        }
    }
}

impl<'a> AsRef<CStr> for CStrFastPathBufGuard<'a> {
    fn as_ref(&self) -> &CStr {
        self
    }
}

impl<'a> Drop for CStrFastPathBufGuard<'a> {
    fn drop(&mut self) {
        self.buf.inner.pop();
    }
}

struct GeneratorTaskParams {
    target_dir: FastPathBuf,
    num_files: usize,
    num_dirs: usize,
    file_offset: usize,
    file_contents: GeneratedFileContents,
}

enum GeneratedFileContents {
    None,
    OnTheFly {
        bytes_per_file: f64,
        random: Xoshiro256PlusPlus,
    },
    PreDefined {
        byte_counts: Vec<usize>,
        random: Xoshiro256PlusPlus,
    },
}

fn run_generator(config: Configuration) -> CliResult<GeneratorStats> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(num_cpus::get())
        .build()
        .context("Failed to create tokio runtime")
        .with_code(exitcode::OSERR)?;

    event!(Level::INFO, config = ?config, "Starting config");
    runtime.block_on(run_generator_async(config))
}

async fn run_generator_async(config: Configuration) -> CliResult<GeneratorStats> {
    let cache_task = task::spawn_blocking(move || {
        // spawn_blocking b/c we're using a single-threaded runtime
        FileNameCache::alloc(config.files_per_dir, config.dirs_per_dir)
    });
    let max_depth = config.max_depth as usize;
    let cache: FileNameCache;
    let mut random = {
        let seed = ((config.files.wrapping_add(config.max_depth as usize) as f64
            * (config.files_per_dir + config.dirs_per_dir)) as u64)
            .wrapping_add(config.seed);
        event!(Level::DEBUG, seed = ?seed, "Starting seed");
        Xoshiro256PlusPlus::seed_from_u64(seed)
    };
    let mut stack = Vec::with_capacity(max_depth);
    // Minus 1 because VecDeque adds 1 and then rounds to a power of 2
    let mut tasks = VecDeque::with_capacity(num_cpus::get().pow(2) - 1);
    let mut target_dir = FastPathBuf::from(config.root_dir);
    let mut stats = GeneratorStats {
        files: 0,
        dirs: 0,
        bytes: 0,
    };

    let mut vec_pool = Vec::with_capacity(max_depth);
    let mut path_pool = Vec::with_capacity(tasks.capacity() / 2);
    let mut byte_counts_pool = Vec::with_capacity(if config.bytes_exact && config.bytes > 0 {
        path_pool.capacity()
    } else {
        0
    });

    event!(
        Level::DEBUG,
        task_queue = tasks.capacity(),
        object_pool.dirs = vec_pool.capacity(),
        object_pool.paths = path_pool.capacity(),
        object_pool.file_sizes = byte_counts_pool.capacity(),
        "Entry allocations"
    );

    macro_rules! flush_tasks {
        () => {
            event!(Level::TRACE, "Flushing pending task queue");
            for task in tasks.drain(..tasks.len() / 2) {
                #[cfg(not(dry_run))]
                {
                    let (bytes_written, buf, byte_counts) = task
                        .await
                        .context("Failed to retrieve task result")
                        .with_code(exitcode::SOFTWARE)??;

                    stats.bytes += bytes_written;
                    path_pool.push(buf);
                    if let Some(mut vec) = byte_counts {
                        vec.clear();
                        byte_counts_pool.push(vec);
                    }
                }
                #[cfg(dry_run)]
                path_pool.push(task);
            }
        };
    }

    // WARNING: HAS SIDE EFFECTS! Must be consumed or stats.bytes must be corrected.
    macro_rules! gen_file_contents_params {
        ($num_files:expr, $is_last_files: expr) => {{
            let num_files = $num_files;
            if config.bytes > 0 && num_files > 0 {
                if config.bytes_exact {
                    if stats.bytes < config.bytes {
                        let mut byte_counts: Vec<usize> =
                            byte_counts_pool.pop().unwrap_or_default();
                        debug_assert!(byte_counts.is_empty());
                        byte_counts.reserve(num_files);
                        let raw_byte_counts = byte_counts.spare_capacity_mut();

                        for i in 0..num_files {
                            let mut num_bytes = config.bytes_per_file.num_to_generate(&mut random);
                            if stats.bytes + num_bytes > config.bytes {
                                num_bytes = config.bytes - stats.bytes;
                            }
                            stats.bytes += num_bytes;

                            raw_byte_counts[i].write(num_bytes);
                        }
                        unsafe {
                            byte_counts.set_len(num_files);
                        }
                        if $is_last_files {
                            let mut leftover_bytes = config.bytes - stats.bytes;
                            let mut i = 0;
                            while leftover_bytes > 0 {
                                byte_counts[i % num_files] += 1;

                                leftover_bytes -= 1;
                                i += 1;
                            }
                            stats.bytes = config.bytes;
                        }

                        GeneratedFileContents::PreDefined {
                            byte_counts,
                            random: random.clone(),
                        }
                    } else {
                        GeneratedFileContents::None
                    }
                } else {
                    GeneratedFileContents::OnTheFly {
                        bytes_per_file: config.bytes_per_file,
                        random: random.clone(),
                    }
                }
            } else {
                GeneratedFileContents::None
            }
        }};
    }

    macro_rules! gen_params {
        ($target_dir:expr, $should_gen_dirs:expr) => {{
            let num_files = config.files_per_dir.num_to_generate(&mut random);
            GeneratorTaskParams {
                target_dir: $target_dir,
                num_files,
                num_dirs: if $should_gen_dirs {
                    config.dirs_per_dir.num_to_generate(&mut random)
                } else {
                    0
                },
                file_offset: 0,
                file_contents: gen_file_contents_params!(num_files, false),
            }
        }};
    }

    macro_rules! queue_gen {
        ($params:expr) => {
            let params = $params;
            stats.files += params.num_files;
            stats.dirs += params.num_dirs;

            debug_assert!(
                matches!(params.file_contents, GeneratedFileContents::None) || params.num_files > 0,
                "Some strictly positive number of files must be generated for bytes to be written"
            );
            if params.num_files > 0 || params.num_dirs > 0 {
                #[cfg(not(dry_run))]
                tasks.push_back(task::spawn_blocking(move || {
                    create_files_and_dirs(params, cache)
                }));
                #[cfg(dry_run)]
                tasks.push_back(params.target_dir);
            }
        };
    }

    let root_num_files;
    {
        let mut params = gen_params!(target_dir.clone(), max_depth > 0);
        root_num_files = params.num_files;

        if config.files_exact && root_num_files >= config.files {
            stats.bytes = 0; // Reset after gen_params modification
            params = GeneratorTaskParams {
                target_dir: params.target_dir,
                num_files: config.files,
                num_dirs: 0,
                file_offset: 0,
                file_contents: gen_file_contents_params!(config.files, true),
            };
        }
        if params.num_dirs > 0 {
            stack.push((1, vec![params.num_dirs]));
        }

        cache = cache_task
            .await
            .context("Failed to create name cache")
            .with_code(exitcode::SOFTWARE)?;
        queue_gen!(params);
    }

    let gen_span = span!(Level::TRACE, "dir_gen");
    'outer: while let Some((tot_dirs, dirs_left)) = stack.last_mut() {
        let num_dirs_to_generate = dirs_left.pop();

        if num_dirs_to_generate == None {
            vec_pool.push(unsafe { stack.pop().unwrap_unchecked().1 });

            if let Some((tot_dirs, dirs_left)) = stack.last() {
                target_dir.pop();

                if !dirs_left.is_empty() {
                    cache.with_dir_name(*tot_dirs - dirs_left.len(), |s| {
                        target_dir.set_file_name(s);
                    });
                }
            }

            continue;
        }

        let num_dirs_to_generate = unsafe { num_dirs_to_generate.unwrap_unchecked() };
        let next_stack_dir = *tot_dirs - dirs_left.len();
        let is_completing = dirs_left.is_empty();
        let gen_next_dirs = stack.len() < max_depth;

        if tasks.len() + num_dirs_to_generate >= tasks.capacity() {
            flush_tasks!();
        }

        let mut next_dirs = vec_pool.pop().unwrap_or_default();
        debug_assert!(next_dirs.is_empty());
        next_dirs.reserve(if gen_next_dirs {
            // TODO figure out if we can bound this memory usage
            num_dirs_to_generate
        } else {
            0
        });
        // Allocate a queue without VecDeque since we know the queue length will only shrink.
        // We want a queue so that the first task that is scheduled is the directory we investigate
        // first such that it will hopefully have finished creating its directories (and thus
        // minimize lock contention).
        let raw_next_dirs = next_dirs.spare_capacity_mut();

        let span_guard = gen_span.enter();
        for i in 0..num_dirs_to_generate {
            let prev_stats_bytes = stats.bytes;
            let params = gen_params!(
                cache.with_dir_name(i, |s| {
                    if let Some(mut buf) = path_pool.pop() {
                        buf.clone_from(&target_dir);
                        buf.push(s);
                        buf
                    } else {
                        target_dir.push_cloned(s)
                    }
                }),
                gen_next_dirs
            );

            if config.files_exact && stats.files + params.num_files >= config.files {
                stats.bytes = prev_stats_bytes; // Reset after gen_params modification
                let num_files = config.files - stats.files;
                queue_gen!(GeneratorTaskParams {
                    num_files,
                    num_dirs: 0,
                    file_contents: gen_file_contents_params!(num_files, true),
                    ..params
                });
                break 'outer;
            }
            if gen_next_dirs {
                raw_next_dirs[num_dirs_to_generate - i - 1].write(params.num_dirs);
            }
            queue_gen!(params);
        }
        drop(span_guard);

        if gen_next_dirs {
            unsafe {
                next_dirs.set_len(num_dirs_to_generate);
            }
            stack.push((num_dirs_to_generate, next_dirs));

            cache.with_dir_name(0, |s| target_dir.push(s));
        } else {
            if !is_completing {
                cache.with_dir_name(next_stack_dir, |s| target_dir.set_file_name(s));
            }
            vec_pool.push(next_dirs);
        }
    }

    // TODO Dumping all the remaining files or bytes in the root directory is very dumb and wrong
    //  1. If there are a lot of files, we're missing out on performance gains from generating
    //     the files in separate directories
    //  2. The distribution will be totally wrong
    //  Ideally we would continue the while loop above until enough files have been generated,
    //  but I haven't had time to think about how to do so properly.
    if config.files_exact && stats.files < config.files {
        let num_files = config.files - stats.files;
        queue_gen!(GeneratorTaskParams {
            target_dir,
            num_files,
            num_dirs: 0,
            file_offset: root_num_files,
            file_contents: gen_file_contents_params!(num_files, true),
        });
    } else if config.bytes_exact && stats.bytes < config.bytes {
        queue_gen!(GeneratorTaskParams {
            target_dir,
            num_files: 1,
            num_dirs: 0,
            file_offset: root_num_files,
            file_contents: gen_file_contents_params!(1, true),
        });
    }

    #[cfg(not(dry_run))]
    for task in tasks {
        stats.bytes += task
            .await
            .context("Failed to retrieve task result")
            .with_code(exitcode::SOFTWARE)??
            .0;
    }

    cache.free();
    Ok(stats)
}

#[instrument(level = "trace", skip(params, cache))]
fn create_files_and_dirs(
    params: GeneratorTaskParams,
    cache: FileNameCache,
) -> CliResult<(usize, FastPathBuf, Option<Vec<usize>>)> {
    event!(
        Level::TRACE,
        files = params.num_files,
        dirs = params.num_dirs,
        target = ?&*params.target_dir,
        "Generating files and dirs"
    );
    let dir_span = span!(Level::TRACE, "directory_creation");
    let file_span = span!(Level::TRACE, "file_creation");

    let mut file = params.target_dir;

    let span_guard = dir_span.enter();
    for i in 0..params.num_dirs {
        cache.with_dir_name(i, |s| file.push(s));

        create_dir_all(&file)
            .with_context(|| format!("Failed to create directory {:?}", &*file))
            .with_code(exitcode::IOERR)?;

        file.pop();
    }
    drop(span_guard);

    let mut file_contents = params.file_contents;
    let mut bytes_written = 0;

    macro_rules! create_file {
        ($file:expr, $file_num:expr, $first_time:expr) => {{
            let num_bytes = match file_contents {
                GeneratedFileContents::None => 0,
                GeneratedFileContents::OnTheFly {
                    bytes_per_file,
                    ref mut random,
                } => bytes_per_file.num_to_generate(random),
                GeneratedFileContents::PreDefined {
                    ref byte_counts, ..
                } => byte_counts[$file_num],
            };

            let needs_slow_path_for_determinism =
                $first_time && matches!(file_contents, GeneratedFileContents::OnTheFly { .. });
            if num_bytes > 0 || needs_slow_path_for_determinism {
                match file_contents {
                    GeneratedFileContents::None => {
                        #[cfg(debug_assertions)]
                        unreachable!("num_bytes should be 0");
                        #[cfg(not(debug_assertions))]
                        unsafe {
                            std::hint::unreachable_unchecked();
                        }
                    }
                    GeneratedFileContents::OnTheFly {
                        bytes_per_file,
                        ref mut random,
                    } => File::create($file).and_then(|f| {
                        // To stay deterministic, we need to ensure `random` is mutated in exactly
                        // the same way regardless of whether or not creating the file fails and
                        // needs to be retried. To do this, we always run num_to_generate() twice
                        // for the initial file creation attempt. Thus, the branching looks like
                        // this:
                        //
                        // FAILURE
                        // 1. Call num_to_generate() in initial retry-aware if check
                        // 2. Perform retry by moving to for loop
                        // 3. Call write_random_bytes(num_to_generate())
                        //
                        // SUCCESS
                        // 1. Call num_to_generate() in initial retry-aware if check
                        //    - This value is ignored.
                        // 2. Call write_random_bytes(num_to_generate()) below
                        //    - Notice that num_to_generate can be 0 which is a bummer b/c we can't
                        //      use mknod even though we'd like to.
                        let num_bytes = if $first_time {
                            bytes_per_file.num_to_generate(random)
                        } else {
                            num_bytes
                        };

                        if !$first_time || num_bytes > 0 {
                            bytes_written += num_bytes;
                            write_random_bytes(f, num_bytes, random)
                        } else {
                            Ok(())
                        }
                    }),
                    GeneratedFileContents::PreDefined { ref mut random, .. } => {
                        File::create($file).and_then(|f| write_random_bytes(f, num_bytes, random))
                        // Don't update bytes_written b/c it's already known and GeneratorStats will
                        // already have been updated.
                    }
                }
            } else {
                #[cfg(target_os = "linux")]
                {
                    use nix::sys::stat::{mknod, Mode, SFlag};
                    let cstr = $file.to_cstr_mut();
                    mknod(
                        &*cstr,
                        SFlag::S_IFREG,
                        Mode::S_IRUSR
                            | Mode::S_IWUSR
                            | Mode::S_IRGRP
                            | Mode::S_IWGRP
                            | Mode::S_IROTH,
                        0,
                    )
                    .map_err(|e| io::Error::from(e))
                }
                #[cfg(not(target_os = "linux"))]
                File::create($file).map(|_| ())
            }
        }};
    }

    let span_guard = file_span.enter();
    let mut start_file = 0;
    if params.num_files > 0 {
        cache.with_file_name(params.file_offset, |s| file.push(s));

        if let Err(e) = create_file!(&mut file, 0, true) {
            if e.kind() == NotFound {
                event!(Level::TRACE, file = ?&*file, "Parent directory not created in time");

                file.pop();
                create_dir_all(&file)
                    .with_context(|| format!("Failed to create directory {:?}", &*file))
                    .with_code(exitcode::IOERR)?;
            } else {
                return Err(e)
                    .with_context(|| format!("Failed to create file {:?}", &*file))
                    .with_code(exitcode::IOERR);
            }
        } else {
            start_file += 1;
            file.pop();
        }
    }
    for i in start_file..params.num_files {
        cache.with_file_name(i + params.file_offset, |s| file.push(s));

        create_file!(&mut file, i, false)
            .with_context(|| format!("Failed to create file {:?}", &*file))
            .with_code(exitcode::IOERR)?;

        file.pop();
    }
    drop(span_guard);

    if let GeneratedFileContents::PreDefined { byte_counts, .. } = file_contents {
        Ok((bytes_written, file, Some(byte_counts)))
    } else {
        Ok((bytes_written, file, None))
    }
}

#[instrument(level = "trace", skip(file, random))]
#[inline(never)] // Don't muck the stack for the GeneratedFileContents::None case
fn write_random_bytes(mut file: File, mut num: usize, random: &mut impl RngCore) -> io::Result<()> {
    #[allow(clippy::uninit_assumed_init)] // u8s do nothing when dropped
    let mut buf: [u8; 4096] = unsafe { MaybeUninit::uninit().assume_init() };
    while num > 0 {
        let used = min(num, buf.len());
        random.fill_bytes(&mut buf[0..used]);
        file.write_all(&buf[0..used])?;

        num -= used;
    }
    Ok(())
}

trait GeneratorUtils {
    fn num_to_generate(self, random: &mut impl RngCore) -> usize;
}

impl GeneratorUtils for f64 {
    fn num_to_generate(self, random: &mut impl RngCore) -> usize {
        let sample = if self > 10_000. {
            LogNormal::from_mean_cv(self, 2.).unwrap().sample(random)
        } else {
            // LogNormal doesn't perform well with values under 10K for our purposes,
            // so default to normal
            Normal::new(self, self * 0.2).unwrap().sample(random)
        };

        sample.round() as usize
    }
}
