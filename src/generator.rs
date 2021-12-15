use std::{
    cmp::max,
    fs::{create_dir, create_dir_all},
    mem::ManuallyDrop,
    ops::AddAssign,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context};
use derive_builder::Builder;
use futures::{stream::FuturesUnordered, StreamExt};
use log::{debug, info};
use num_format::{SystemLocale, ToFormattedString};
use rand::{distributions::Distribution, RngCore, SeedableRng};
use rand_distr::{LogNormal, Normal};
use rand_xorshift::XorShiftRng;
use tokio::{task, task::JoinHandle};

use crate::errors::{CliExitAnyhowWrapper, CliResult};

#[derive(Builder, Debug)]
pub struct Generator {
    root_dir: PathBuf,
    num_files: usize,
    max_depth: u32,
    file_to_dir_ratio: Option<usize>,
    entropy: u64,
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
    files_per_dir: f64,
    dirs_per_dir: f64,
    max_depth: u32,
    entropy: u64,

    informational_dirs_per_dir: usize,
    informational_total_dirs: usize,
}

#[derive(Debug)]
struct GeneratorStats {
    files: usize,
    dirs: usize,
}

impl AddAssign for GeneratorStats {
    fn add_assign(&mut self, rhs: Self) {
        self.files += rhs.files;
        self.dirs += rhs.dirs;
    }
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

    if generator.max_depth == 0 {
        return Ok(Configuration {
            root_dir: generator.root_dir,
            files: generator.num_files,
            files_per_dir: generator.num_files as f64,
            dirs_per_dir: 0.,
            max_depth: 0,
            entropy: generator.entropy,

            informational_dirs_per_dir: 0,
            informational_total_dirs: 1,
        });
    }

    let ratio = generator
        .file_to_dir_ratio
        .unwrap_or_else(|| max(generator.num_files / 1000, 1));
    if ratio > generator.num_files {
        return Err(anyhow!(format!(
            "The file to dir ratio ({}) cannot be larger than the number of files to generate ({}).",
            ratio,
            generator.num_files,
        ))).with_code(exitcode::DATAERR);
    }

    let num_dirs = generator.num_files as f64 / ratio as f64;
    // This formula was derived from the following equation:
    // num_dirs = unknown_num_dirs_per_dir^max_depth
    let dirs_per_dir = 2f64.powf(num_dirs.log2() / generator.max_depth as f64);

    Ok(Configuration {
        root_dir: generator.root_dir,
        files: generator.num_files,
        files_per_dir: ratio as f64,
        dirs_per_dir,
        max_depth: generator.max_depth,
        entropy: generator.entropy,

        informational_dirs_per_dir: dirs_per_dir.round() as usize,
        informational_total_dirs: num_dirs.round() as usize,
    })
}

fn print_configuration_info(config: &Configuration) {
    let locale = SystemLocale::new().unwrap();
    println!(
        "About {} {files_maybe_plural} will be generated in approximately \
        {} {directories_maybe_plural} distributed across a tree of maximum depth {} where each \
        directory contains approximately {} other {dpd_directories_maybe_plural}.",
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
    );
}

fn print_stats(stats: GeneratorStats) {
    let locale = SystemLocale::new().unwrap();
    println!(
        "Created {} {files_maybe_plural} across {} {directories_maybe_plural}.",
        stats.files.to_formatted_string(&locale),
        stats.dirs.to_formatted_string(&locale),
        files_maybe_plural = if stats.files == 1 { "file" } else { "files" },
        directories_maybe_plural = if stats.dirs == 1 {
            "directory"
        } else {
            "directories"
        },
    );
}

#[derive(Debug)]
struct GeneratorState {
    files_per_dir: f64,
    dirs_per_dir: f64,
    max_depth: u32,

    root_dir: PathBuf,
    seed: <XorShiftRng as SeedableRng>::Seed,
    cache: FileNameCache,
}

impl GeneratorState {
    fn next(&self, root_dir: PathBuf, random: &mut XorShiftRng) -> GeneratorState {
        GeneratorState {
            root_dir,
            seed: random.next_seed(),
            max_depth: self.max_depth - 1,
            ..*self
        }
    }
}

impl From<Configuration> for GeneratorState {
    fn from(config: Configuration) -> Self {
        GeneratorState {
            files_per_dir: config.files_per_dir,
            dirs_per_dir: config.dirs_per_dir,
            max_depth: config.max_depth,

            cache: FileNameCache::alloc(&config),
            root_dir: config.root_dir,
            seed: XorShiftRng::seed_from_u64(
                ((config.files.wrapping_add(config.max_depth as usize) as f64
                    * (config.files_per_dir + config.dirs_per_dir)) as u64)
                    .wrapping_add(config.entropy),
            )
            .next_seed(),
        }
    }
}

/// Specialized cache for file names that takes advantage of our monotonically increasing integer
/// naming scheme.
///
/// We intentionally use a thread-*un*safe raw fixed-size buffer to eliminate an Arc.
#[derive(Debug, Copy, Clone)]
struct FileNameCache {
    file_cache: (*mut String, usize),
    dir_cache: (*mut String, usize),
}

unsafe impl Send for FileNameCache {}

impl FileNameCache {
    fn alloc(config: &Configuration) -> Self {
        let num_cache_entries = config.files_per_dir + config.dirs_per_dir;
        let files_percentage = config.files_per_dir / num_cache_entries;

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
        unsafe {
            let cap = file_cache.capacity();
            file_cache.set_len(cap);
            let cap = dir_cache.capacity();
            dir_cache.set_len(cap);

            for i in 0..file_cache.len() {
                file_cache
                    .as_mut_ptr()
                    .add(i)
                    .write(FileNameCache::file_name(i));
            }
            for i in 0..dir_cache.len() {
                dir_cache
                    .as_mut_ptr()
                    .add(i)
                    .write(FileNameCache::dir_name(i));
            }
        }

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

    fn join_dir_name(self, i: usize, path: &Path) -> PathBuf {
        if i >= self.dir_cache.1 {
            return path.join(FileNameCache::dir_name(i));
        }

        let dir_cache = ManuallyDrop::new(unsafe {
            Vec::from_raw_parts(self.dir_cache.0, self.dir_cache.1, self.dir_cache.1)
        });
        path.join(&dir_cache[i])
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

fn run_generator(config: Configuration) -> CliResult<GeneratorStats> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(num_cpus::get())
        .build()
        .with_context(|| "Failed to create tokio runtime")
        .with_code(exitcode::OSERR)?;

    let state: GeneratorState = config.into();
    info!("Starting state: {:?}", state);

    let cache = state.cache;
    let results = runtime.block_on(run_generator_async(state));
    cache.free();

    results
}

async fn run_generator_async(state: GeneratorState) -> CliResult<GeneratorStats> {
    let mut random = XorShiftRng::from_seed(state.seed);
    let num_files_to_generate = state.files_per_dir.num_to_generate(&mut random);
    let num_dirs_to_generate = if state.max_depth == 0 {
        0
    } else {
        state.dirs_per_dir.num_to_generate(&mut random)
    };

    debug!(
        "Creating {} files and {} directories in {:?}",
        num_files_to_generate, num_dirs_to_generate, state.root_dir
    );

    let tasks = task::spawn_blocking(move || -> CliResult<_> {
        let mut dir_tasks = Vec::with_capacity(num_dirs_to_generate);

        for i in 0..num_dirs_to_generate {
            let dir = state.cache.join_dir_name(i, &state.root_dir);

            create_dir(&dir)
                .with_context(|| format!("Failed to create directory {:?}", dir))
                .with_code(exitcode::IOERR)?;
            dir_tasks.push(spawn_run_generator_async(state.next(dir, &mut random)))
        }

        let mut file = state.root_dir;
        for i in 0..num_files_to_generate {
            state.cache.push_file_name(i, &mut file);

            #[cfg(target_os = "linux")]
            {
                use nix::sys::stat::{mknod, Mode, SFlag};
                mknod(
                    &file,
                    SFlag::S_IFREG,
                    Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH,
                    0,
                )
                .with_context(|| format!("Failed to create file {:?}", file))
                .with_code(exitcode::IOERR)?;
            }
            #[cfg(not(target_os = "linux"))]
            {
                use std::fs::File;
                File::create(&file)
                    .with_context(|| format!("Failed to create file {:?}", file))
                    .with_code(exitcode::IOERR)?;
            }

            file.pop();
        }

        Ok(dir_tasks)
    })
    .await
    .with_context(|| "Failed to retrieve task result")
    .with_code(exitcode::SOFTWARE)??;

    let mut stats = GeneratorStats {
        files: num_files_to_generate,
        dirs: num_dirs_to_generate,
    };

    // We want to poll every future continuously instead of going one-by-one because each future
    // recursively spawns more I/O bound children that wouldn't otherwise get a head start.
    let mut tasks = tasks.into_iter().collect::<FuturesUnordered<_>>();
    while let Some(task) = tasks.next().await {
        stats += task
            .with_context(|| "Failed to retrieve task result")
            .with_code(exitcode::SOFTWARE)??;
    }

    Ok(stats)
}

fn spawn_run_generator_async(state: GeneratorState) -> JoinHandle<CliResult<GeneratorStats>> {
    task::spawn(run_generator_async(state))
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

trait RandomUtils {
    type Seed;

    fn next_seed(&mut self) -> Self::Seed;
}

impl RandomUtils for XorShiftRng {
    type Seed = <XorShiftRng as SeedableRng>::Seed;

    fn next_seed(&mut self) -> Self::Seed {
        let seed = [
            self.next_u32(),
            self.next_u32(),
            self.next_u32(),
            self.next_u32(),
        ]
        .as_ptr() as *const [u8; 16];
        unsafe { *seed }
    }
}
