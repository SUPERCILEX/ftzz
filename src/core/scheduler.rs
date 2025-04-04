use std::{
    cmp::max,
    io,
    num::NonZeroU64,
    ops::AddAssign,
    path::PathBuf,
    process::ExitCode,
    result,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
    },
};

use error_stack::{Report, Result, ResultExt};
use lockness_executor::{
    LocknessExecutorBuilder, Spawner,
    config::{Config, False, Fifo},
};
use rand_distr::Normal;
use rustix::thread::{UnshareFlags, unshare};

use crate::{
    core::{
        tasks::{QueueErrors, QueueOutcome, TaskGenerator},
        truncatable_normal,
    },
    generator::Error,
    utils::{FastPathBuf, with_dir_name, with_file_name},
};

#[derive(Debug, Copy, Clone, Default)]
pub struct GeneratorStats {
    pub files: u64,
    pub dirs: usize,
    pub bytes: u64,
}

impl AddAssign<Self> for GeneratorStats {
    fn add_assign(&mut self, rhs: Self) {
        let Self { files, dirs, bytes } = self;
        let Self {
            files: files_,
            dirs: dirs_,
            bytes: bytes_,
        } = rhs;
        *files += files_;
        *dirs += dirs_;
        *bytes += bytes_;
    }
}

impl From<AtomicGeneratorStats> for GeneratorStats {
    fn from(AtomicGeneratorStats { files, dirs, bytes }: AtomicGeneratorStats) -> Self {
        Self {
            files: files.load(Relaxed),
            dirs: dirs.load(Relaxed),
            bytes: bytes.load(Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct AtomicGeneratorStats {
    files: AtomicU64,
    dirs: AtomicUsize,
    bytes: AtomicU64,
}

pub struct ThreadState {
    buffered: GeneratorStats,
    totals: Arc<AtomicGeneratorStats>,
}

impl ThreadState {
    pub fn add(&mut self, stats: GeneratorStats) {
        self.buffered += stats;
    }
}

impl Drop for ThreadState {
    fn drop(&mut self) {
        let Self {
            buffered:
                GeneratorStats {
                    files: files_,
                    dirs: dirs_,
                    bytes: bytes_,
                },
            ref totals,
        } = *self;
        let AtomicGeneratorStats { files, dirs, bytes } = &**totals;
        files.fetch_add(files_, Relaxed);
        dirs.fetch_add(dirs_, Relaxed);
        bytes.fetch_add(bytes_, Relaxed);
    }
}

#[derive(Clone)]
pub struct Params {
    totals: Arc<AtomicGeneratorStats>,
}

impl Config for Params {
    const NUM_TASK_TYPES: usize = 1;
    type AllowTasksToSpawnMoreTasks = False;
    type DequeBias = Fifo;

    type Error = Report<io::Error>;
    type ThreadLocalState = ThreadState;

    fn thread_initializer(self) -> result::Result<Self::ThreadLocalState, Self::Error> {
        let Self { totals } = self;
        #[cfg(all(not(miri), target_os = "linux"))]
        unshare(UnshareFlags::FILES)
            .map_err(io::Error::from)
            .attach_printable("Failed to unshare I/O")?;
        Ok(ThreadState {
            buffered: GeneratorStats::default(),
            totals,
        })
    }
}

struct Scheduler {
    spawner: Spawner<Params>,
    stack: Vec<Directory>,
    target_dir: FastPathBuf,

    cache: ObjectPool,
}

/// Given a directory
/// root/
/// ├── a/
/// ├── b/
/// └── c/
/// [`total_dirs`] is 3 and [`child_dir_counts`] contains 3 entries, each of
/// which specifies the number of directories to generate within `a`, `b`, and
/// `c` respectively.
struct Directory {
    total_dirs: usize,
    child_dir_counts: Vec<DirChild>,
}

struct DirChild {
    files: u64,
    dirs: usize,
}

struct ObjectPool {
    directories: Vec<Vec<DirChild>>,
    paths: Vec<FastPathBuf>,
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(generator))
)]
pub fn run(
    root_dir: PathBuf,
    target_file_count: NonZeroU64,
    dirs_per_dir: f64,
    max_depth: usize,
    mut generator: impl TaskGenerator + Send,
) -> Result<GeneratorStats, Error> {
    let stats = Arc::new(AtomicGeneratorStats::default());
    let executor = LocknessExecutorBuilder::new().build(Params {
        totals: stats.clone(),
    });

    let mut scheduler = Scheduler {
        spawner: executor.spawner(),
        stack: Vec::with_capacity(max_depth),
        target_dir: FastPathBuf::from(root_dir),

        cache: ObjectPool {
            directories: Vec::with_capacity(max_depth),
            paths: Vec::new(),
        },
    };

    #[cfg(feature = "tracing")]
    tracing::event!(
        tracing::Level::DEBUG,
        object_pool.dirs = scheduler.cache.directories.capacity(),
        object_pool.paths = scheduler.cache.paths.capacity(),
        "Entry allocations"
    );

    schedule_root_dir(
        &mut generator,
        target_file_count,
        dirs_per_dir,
        max_depth,
        &mut scheduler,
    );

    #[cfg(feature = "tracing")]
    let gen_span = tracing::span!(tracing::Level::TRACE, "dir_gen");
    while let Some(&mut Directory {
        total_dirs,
        ref mut child_dir_counts,
    }) = scheduler.stack.last_mut()
    {
        let Some(DirChild {
            files: target_file_count,
            dirs: num_dirs_to_generate,
        }) = child_dir_counts.pop()
        else {
            handle_directory_completion(&mut scheduler);
            continue;
        };

        let next_stack_dir = total_dirs - child_dir_counts.len();
        let is_completing = child_dir_counts.is_empty();

        scheduler.spawner.drain();
        let Ok(directory) = schedule_dir(
            target_file_count,
            num_dirs_to_generate,
            dirs_per_dir,
            max_depth,
            &mut generator,
            &mut scheduler,
            #[cfg(feature = "tracing")]
            &gen_span,
        ) else {
            break;
        };

        if let Some(directory) = directory {
            scheduler.stack.push(directory);
            with_dir_name(0, |s| scheduler.target_dir.push(s));
        } else if !is_completing {
            with_dir_name(next_stack_dir, |s| unsafe {
                scheduler.target_dir.set_file_name(s);
            });
        }
    }
    #[cfg(feature = "tracing")]
    drop(gen_span);

    schedule_last_dir(generator, scheduler);

    if let Some(e) = executor.finisher().next() {
        return match e {
            lockness_executor::Error::Panic(p) => Err(p)
                .change_context(Error::TaskJoin)
                .attach(ExitCode::from(sysexits::ExitCode::Software)),
            lockness_executor::Error::Error(e) => Err(e)
                .change_context(Error::Io)
                .attach(ExitCode::from(sysexits::ExitCode::IoErr)),
        };
    }

    Ok(Arc::into_inner(stats).unwrap().into())
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        level = "trace",
        skip(generator, spawner, stack, path_pool, byte_counts_pool)
    )
)]
fn schedule_root_dir(
    generator: &mut impl TaskGenerator,
    target_file_count: NonZeroU64,
    dirs_per_dir: f64,
    max_depth: usize,
    &mut Scheduler {
        ref spawner,
        ref mut stack,
        ref target_dir,
        cache:
            ObjectPool {
                directories: _,
                paths: ref mut path_pool,
            },
    }: &mut Scheduler,
) {
    match generator.queue_gen(
        &num_files_distr(target_file_count.get(), dirs_per_dir, max_depth),
        target_dir.clone(),
        max_depth > 0,
        spawner,
    ) {
        Ok(QueueOutcome {
            num_files,
            num_dirs,
            done: _,
        }) => {
            if num_dirs > 0 {
                stack.push(Directory {
                    total_dirs: 1,
                    child_dir_counts: vec![DirChild {
                        files: next_target_file_count(target_file_count.get(), num_dirs, num_files),
                        dirs: num_dirs,
                    }],
                });
            }
        }
        Err(QueueErrors::NothingToDo(path)) => path_pool.push(path),
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        level = "trace",
        skip(generator, spawner, stack, dir_pool, byte_counts_pool)
    )
)]
fn schedule_dir(
    target_file_count: u64,
    num_dirs_to_generate: usize,
    dirs_per_dir: f64,
    max_depth: usize,
    generator: &mut impl TaskGenerator,
    &mut Scheduler {
        ref spawner,
        ref stack,
        ref target_dir,
        cache:
            ObjectPool {
                directories: ref mut dir_pool,
                paths: ref mut path_pool,
            },
    }: &mut Scheduler,
    #[cfg(feature = "tracing")] gen_span: &tracing::Span,
) -> result::Result<Option<Directory>, ()> {
    let depth = stack.len();
    let gen_next_dirs = depth < max_depth;

    let mut next_dirs = dir_pool.pop().unwrap_or_default();
    debug_assert!(next_dirs.is_empty());
    if gen_next_dirs {
        // TODO figure out if we can bound this memory usage
        next_dirs.reserve(num_dirs_to_generate);
    }
    // Allocate a queue without VecDeque since we know the queue length will only
    // shrink. We want a queue so that the first task that is scheduled
    // is the directory we investigate first such that it will hopefully
    // have finished creating its directories (and thus minimize lock
    // contention).
    let raw_next_dirs = next_dirs.spare_capacity_mut();

    let num_files_distr = num_files_distr(target_file_count, dirs_per_dir, max_depth - depth);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let expected_file_name_length = max(
        with_dir_name(dirs_per_dir.round() as usize, str::len),
        with_file_name(num_files_distr.mean().round() as u64, str::len),
    );

    #[cfg(feature = "tracing")]
    let span_guard = gen_span.enter();
    for i in 0..num_dirs_to_generate {
        let path = with_dir_name(i, |s| {
            let mut buf = path_pool.pop().unwrap_or_else(FastPathBuf::new);

            // Space for the parent dir, the path separator, the target dir, child separator
            // and name, and a NUL terminator
            buf.reserve(
                (target_dir.capacity() + 1 + s.len() + 1 + expected_file_name_length + 1)
                    .saturating_sub(buf.capacity()),
            );

            buf.clone_from(target_dir);
            buf.push(s);

            buf
        });

        let child = match generator.queue_gen(&num_files_distr, path, gen_next_dirs, spawner) {
            Ok(QueueOutcome {
                num_files,
                num_dirs,
                done,
            }) => {
                if done {
                    return Err(());
                }
                DirChild {
                    files: next_target_file_count(target_file_count, num_dirs, num_files),
                    dirs: num_dirs,
                }
            }
            Err(QueueErrors::NothingToDo(path)) => {
                path_pool.push(path);
                DirChild { files: 0, dirs: 0 }
            }
        };

        if gen_next_dirs {
            raw_next_dirs[num_dirs_to_generate - i - 1].write(child);
        }
    }
    #[cfg(feature = "tracing")]
    drop(span_guard);

    if gen_next_dirs {
        unsafe {
            next_dirs.set_len(num_dirs_to_generate);
        }
        Ok(Some(Directory {
            total_dirs: num_dirs_to_generate,
            child_dir_counts: next_dirs,
        }))
    } else {
        dir_pool.push(next_dirs);
        Ok(None)
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
fn schedule_last_dir(mut generator: impl TaskGenerator, scheduler: Scheduler) {
    let Scheduler {
        ref spawner,
        stack: _,
        target_dir,
        cache: _,
    } = scheduler;

    let _ = generator.maybe_queue_final_gen(target_dir, spawner);
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(stack, directory_pool))
)]
fn handle_directory_completion(
    &mut Scheduler {
        spawner: _,
        ref mut stack,
        ref mut target_dir,
        cache:
            ObjectPool {
                directories: ref mut directory_pool,
                paths: _,
            },
    }: &mut Scheduler,
) {
    if let Some(Directory {
        total_dirs: _,
        child_dir_counts,
    }) = stack.pop()
    {
        directory_pool.push(child_dir_counts);
    }

    if let Some(&Directory {
        total_dirs,
        ref child_dir_counts,
    }) = stack.last()
    {
        unsafe {
            target_dir.pop();
        }

        if !child_dir_counts.is_empty() {
            with_dir_name(total_dirs - child_dir_counts.len(), |s| unsafe {
                target_dir.set_file_name(s);
            });
        }
    }
}

fn next_target_file_count(target_file_count: u64, dirs_created: usize, files_created: u64) -> u64 {
    let files = target_file_count.saturating_sub(files_created);
    files
        .checked_div(u64::try_from(dirs_created).unwrap_or(u64::MAX))
        .unwrap_or(files_created)
}

#[allow(clippy::cast_precision_loss)]
#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
fn num_files_distr(
    target_file_count: u64,
    dirs_per_dir: f64,
    remaining_depth: usize,
) -> Normal<f64> {
    fn files_per_dir(total_files: u64, dirs_per_dir: f64, remaining_depth: usize) -> f64 {
        (total_files as f64) * dirs_per_dir.powf(-(remaining_depth as f64))
    }

    truncatable_normal(files_per_dir(
        target_file_count,
        dirs_per_dir,
        remaining_depth,
    ))
}
