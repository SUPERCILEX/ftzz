use std::{
    cmp::{max, min},
    fs::{create_dir_all, File},
    io,
    io::{ErrorKind::NotFound, Write},
    mem::{ManuallyDrop, MaybeUninit},
    path::PathBuf,
};

use anyhow::{anyhow, Context};
use cli_errors::{CliExitAnyhowWrapper, CliResult};
use derive_builder::Builder;
use log::{debug, info};
use num_format::{SystemLocale, ToFormattedString};
use rand::{distributions::Distribution, RngCore, SeedableRng};
use rand_distr::{LogNormal, Normal};
use rand_xorshift::XorShiftRng;
use tokio::task;

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
    entropy: u64,
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
        assert_eq!(g.entropy, 0);
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
    entropy: u64,

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
            entropy: generator.entropy,

            informational_dirs_per_dir: 0,
            informational_total_dirs: 1,
            informational_bytes_per_files: bytes_per_file.round() as usize,
        });
    }

    let ratio = generator.file_to_dir_ratio as f64;
    let num_dirs = num_files / ratio;
    // This formula was derived from the following equation:
    // num_dirs = unknown_num_dirs_per_dir^max_depth
    let dirs_per_dir = 2f64.powf(num_dirs.log2() / generator.max_depth as f64);

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
        entropy: generator.entropy,

        informational_dirs_per_dir: dirs_per_dir.round() as usize,
        informational_total_dirs: num_dirs.round() as usize,
        informational_bytes_per_files: bytes_per_file.round() as usize,
    })
}

fn print_configuration_info(config: &Configuration) {
    let locale = SystemLocale::new().unwrap();
    println!(
        "About {} {files_maybe_plural} will be generated in approximately \
        {} {directories_maybe_plural} distributed across a tree of maximum depth {} where each \
        directory contains approximately {} other {dpd_directories_maybe_plural}.\
        {bytes_info}",
        config.files.to_formatted_string(&locale),
        config.informational_total_dirs.to_formatted_string(&locale),
        config.max_depth.to_formatted_string(&locale),
        config
            .informational_dirs_per_dir
            .to_formatted_string(&locale),
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
    let locale = SystemLocale::new().unwrap();
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
            info!("Wrote exactly {} bytes.", stats.bytes);
            format!(" ({})", bytesize::to_string(stats.bytes as u64, true))
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
    fn alloc(files_per_dir: f64, dirs_per_dir: f64) -> Self {
        let num_cache_entries = files_per_dir + dirs_per_dir;
        let files_percentage = files_per_dir / num_cache_entries;

        // Overestimate since the cache can't grow
        let num_cache_entries = 1.5 * num_cache_entries;
        // Max out the cache size at 1MiB
        let num_cache_entries = f64::min((1 << 20) as f64, num_cache_entries);

        let file_entries = files_percentage * num_cache_entries;
        let dir_entries = num_cache_entries - file_entries;

        let mut file_cache =
            ManuallyDrop::new(Vec::<String>::with_capacity(file_entries.round() as usize));
        let mut dir_cache =
            ManuallyDrop::new(Vec::<String>::with_capacity(dir_entries.round() as usize));

        for (i, entry) in file_cache.spare_capacity_mut().iter_mut().enumerate() {
            entry.write(FileNameCache::file_name(i));
        }
        for (i, entry) in dir_cache.spare_capacity_mut().iter_mut().enumerate() {
            entry.write(FileNameCache::dir_name(i));
        }

        unsafe {
            let cap = file_cache.capacity();
            file_cache.set_len(cap);
            let cap = dir_cache.capacity();
            dir_cache.set_len(cap);
        }

        debug!("Allocated {} file cache entries.", file_cache.len());
        debug!("Allocated {} directory cache entries.", dir_cache.len());

        FileNameCache {
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

    fn push_file_name(self, i: usize, path: &mut PathBuf) {
        if i >= self.file_cache.1 {
            path.push(FileNameCache::file_name(i));
            return;
        }

        let file_cache = ManuallyDrop::new(unsafe {
            Vec::from_raw_parts(self.file_cache.0, self.file_cache.1, self.file_cache.1)
        });
        path.push(&file_cache[i]);
    }

    fn push_dir_name(self, i: usize, path: &mut PathBuf) {
        if i >= self.dir_cache.1 {
            path.push(FileNameCache::dir_name(i));
            return;
        }

        let dir_cache = ManuallyDrop::new(unsafe {
            Vec::from_raw_parts(self.dir_cache.0, self.dir_cache.1, self.dir_cache.1)
        });
        path.push(&dir_cache[i]);
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

struct GeneratorTaskParams {
    target_dir: PathBuf,
    num_files: usize,
    num_dirs: usize,
    file_offset: usize,
    file_contents: GeneratedFileContents,
}

enum GeneratedFileContents {
    None,
    OnTheFly {
        bytes_per_file: f64,
        random: XorShiftRng,
    },
    PreDefined {
        byte_counts: Vec<usize>,
        random: XorShiftRng,
    },
}

fn run_generator(config: Configuration) -> CliResult<GeneratorStats> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(num_cpus::get())
        .build()
        .context("Failed to create tokio runtime")
        .with_code(exitcode::OSERR)?;

    info!("Starting config: {:?}", config);
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
            .wrapping_add(config.entropy);
        debug!("Starting seed: {}", seed);
        XorShiftRng::seed_from_u64(seed)
    };
    let mut stack = Vec::with_capacity(max_depth);
    let mut tasks = Vec::with_capacity(num_cpus::get() * 100);
    let mut target_dir = config.root_dir;
    let mut stats = GeneratorStats {
        files: 0,
        dirs: 0,
        bytes: 0,
    };

    let mut vec_pool = Vec::with_capacity(max_depth);
    let mut path_pool = Vec::with_capacity(tasks.capacity() - (tasks.capacity() >> 4));
    let mut byte_counts_pool = Vec::with_capacity(if config.bytes > 0 {
        path_pool.capacity()
    } else {
        0
    });

    debug!("Allocated {} task entries.", tasks.capacity());

    macro_rules! flush_tasks {
        () => {
            debug!("Flushing pending task queue.");
            for task in tasks.drain(..tasks.len() - (tasks.len() >> 4)) {
                #[cfg(not(dry_run))]
                {
                    let (bytes_written, buf, byte_counts) = task
                        .await
                        .context("Failed to retrieve task result")
                        .with_code(exitcode::SOFTWARE)??;

                    stats.bytes += bytes_written;
                    path_pool.push(buf);
                    if let Some(vec) = byte_counts {
                        byte_counts_pool.push(vec);
                    }
                }
                #[cfg(dry_run)]
                path_pool.push(task);
            }
        };
    }

    macro_rules! gen_params {
        ($target_dir:expr, $should_gen_dirs:expr) => {{
            GeneratorTaskParams {
                target_dir: $target_dir,
                num_files: config.files_per_dir.num_to_generate(&mut random),
                num_dirs: if $should_gen_dirs {
                    config.dirs_per_dir.num_to_generate(&mut random)
                } else {
                    0
                },
                file_offset: 0,
                file_contents: GeneratedFileContents::None,
            }
        }};
    }

    macro_rules! queue_gen {
        ($params:expr) => {
            let params = $params;
            stats.files += params.num_files;
            stats.dirs += params.num_dirs;

            #[cfg(not(dry_run))]
            tasks.push(task::spawn_blocking(move || {
                create_files_and_dirs(params, cache)
            }));
            #[cfg(dry_run)]
            tasks.push(params.target_dir)
        };
    }

    let root_num_files;
    {
        let mut params = gen_params!(target_dir.clone(), max_depth > 0);
        root_num_files = params.num_files;

        if config.files_exact && root_num_files >= config.files {
            params = GeneratorTaskParams {
                target_dir: params.target_dir,
                num_files: config.files,
                num_dirs: 0,
                file_offset: 0,
                file_contents: GeneratedFileContents::None,
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

    'outer: while let Some((tot_dirs, dirs_left)) = stack.last_mut() {
        if dirs_left.is_empty() {
            vec_pool.push(stack.pop().unwrap().1);
            if !stack.is_empty() {
                target_dir.pop();
            }

            if let Some((tot_dirs, dirs_left)) = stack.last() {
                if !dirs_left.is_empty() {
                    target_dir.pop();
                    cache.push_dir_name(*tot_dirs - dirs_left.len(), &mut target_dir);
                }
            }

            continue;
        }

        let num_dirs_to_generate = dirs_left.pop().unwrap();
        let next_stack_dir = *tot_dirs - dirs_left.len();
        let is_completing = dirs_left.is_empty();
        let gen_next_dirs = stack.len() < max_depth;

        if tasks.len() + num_dirs_to_generate >= tasks.capacity() {
            flush_tasks!();
        }

        let mut next_dirs = vec_pool.pop().unwrap_or_else(Vec::new);
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

        for i in 0..num_dirs_to_generate {
            cache.push_dir_name(i, &mut target_dir);
            let params = gen_params!(
                if let Some(mut buf) = path_pool.pop() {
                    buf.clone_from(&target_dir);
                    buf
                } else {
                    target_dir.clone()
                },
                gen_next_dirs
            );
            target_dir.pop();

            if config.files_exact && stats.files + params.num_files >= config.files {
                queue_gen!(GeneratorTaskParams {
                    num_files: config.files - stats.files,
                    num_dirs: 0,
                    ..params
                });
                break 'outer;
            }
            if gen_next_dirs {
                raw_next_dirs[num_dirs_to_generate - i - 1].write(params.num_dirs);
            }
            queue_gen!(params);
        }

        if gen_next_dirs {
            unsafe {
                next_dirs.set_len(num_dirs_to_generate);
            }
            stack.push((num_dirs_to_generate, next_dirs));

            cache.push_dir_name(0, &mut target_dir);
        } else {
            if !is_completing {
                target_dir.pop();
                cache.push_dir_name(next_stack_dir, &mut target_dir);
            }
            vec_pool.push(next_dirs);
        }
    }

    if config.files_exact && stats.files < config.files {
        // TODO Dumping all the remaining files in the root directory is very dumb and wrong
        //  1. If there are a lot of files, we're missing out on performance gains from generating
        //     the files in separate directories
        //  2. The distribution will be totally wrong
        //  Ideally we would continue the while loop above until enough files have been generated,
        //  but I haven't had time to think about how to do so properly.

        let params = GeneratorTaskParams {
            target_dir,
            num_files: config.files - stats.files,
            num_dirs: 0,
            file_offset: root_num_files,
            file_contents: GeneratedFileContents::None,
        };
        queue_gen!(params);
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

fn create_files_and_dirs(
    params: GeneratorTaskParams,
    cache: FileNameCache,
) -> CliResult<(usize, PathBuf, Option<Vec<usize>>)> {
    debug!(
        "Creating {} files and {} directories in {:?}",
        params.num_files, params.num_dirs, params.target_dir,
    );

    let mut file = params.target_dir;

    for i in 0..params.num_dirs {
        cache.push_dir_name(i, &mut file);

        create_dir_all(&file)
            .with_context(|| format!("Failed to create directory {:?}", file))
            .with_code(exitcode::IOERR)?;

        file.pop();
    }

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
                    GeneratedFileContents::None => panic!("Impossible condition"),
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
                    mknod(
                        $file,
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

    let mut start_file = 0;
    if params.num_files > 0 {
        cache.push_file_name(params.file_offset, &mut file);

        if let Err(e) = create_file!(&file, 0, true) {
            if e.kind() == NotFound {
                file.pop();
                create_dir_all(&file)
                    .with_context(|| format!("Failed to create directory {:?}", file))
                    .with_code(exitcode::IOERR)?;
            } else {
                return Err(e)
                    .with_context(|| format!("Failed to create file {:?}", file))
                    .with_code(exitcode::IOERR);
            }
        } else {
            start_file += 1;
            file.pop();
        }
    }
    for i in start_file..params.num_files {
        cache.push_file_name(i + params.file_offset, &mut file);

        create_file!(&file, i, false)
            .with_context(|| format!("Failed to create file {:?}", file))
            .with_code(exitcode::IOERR)?;

        file.pop();
    }

    if let GeneratedFileContents::PreDefined { byte_counts, .. } = file_contents {
        Ok((bytes_written, file, Some(byte_counts)))
    } else {
        Ok((bytes_written, file, None))
    }
}

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
