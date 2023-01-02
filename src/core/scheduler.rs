use std::{
    collections::VecDeque, num::NonZeroUsize, ops::AddAssign, path::PathBuf, process::ExitCode,
};

use error_stack::{IntoReport, Result, ResultExt};
use tracing::{event, span, Level};

use crate::{
    core::{
        files::GeneratorTaskOutcome,
        tasks::{QueueErrors, QueueOutcome, TaskGenerator},
    },
    generator::Error,
    utils::{with_dir_name, FastPathBuf},
};

#[derive(Debug, Copy, Clone)]
pub struct GeneratorStats {
    pub files: u64,
    pub dirs: usize,
    pub bytes: u64,
}

impl AddAssign<&GeneratorTaskOutcome> for GeneratorStats {
    fn add_assign(
        &mut self,
        GeneratorTaskOutcome {
            files_generated,
            dirs_generated,
            bytes_generated,
            ..
        }: &GeneratorTaskOutcome,
    ) {
        self.files += files_generated;
        self.dirs += dirs_generated;
        self.bytes += bytes_generated;
    }
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    root_dir: PathBuf,
    max_depth: usize,
    parallelism: NonZeroUsize,
    mut generator: impl TaskGenerator + Send,
) -> Result<GeneratorStats, Error> {
    // Minus 1 because VecDeque adds 1 and then rounds to a power of 2
    let mut tasks = VecDeque::with_capacity(parallelism.get().pow(2) - 1);
    let mut stats = GeneratorStats {
        files: 0,
        dirs: 0,
        bytes: 0,
    };

    macro_rules! handle_task_result {
        ($task:expr) => {{
            #[cfg(not(dry_run))]
            let outcome = $task
                .await
                .into_report()
                .change_context(Error::TaskJoin)
                .attach(ExitCode::from(sysexits::ExitCode::Software))?
                .change_context(Error::Io)
                .attach(ExitCode::from(sysexits::ExitCode::IoErr))?;
            #[cfg(dry_run)]
            let outcome = task;

            stats += &outcome;

            outcome
        }};
    }

    {
        let mut stack = Vec::with_capacity(max_depth);
        #[cfg(unix)]
        let mut target_dir = FastPathBuf::from(root_dir);
        #[cfg(not(unix))]
        let mut target_dir = root_dir;

        let mut vec_pool = Vec::with_capacity(max_depth);
        let mut path_pool = Vec::with_capacity(tasks.capacity() / 2);
        let mut byte_counts_pool = Vec::with_capacity(if generator.uses_byte_counts_pool() {
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
                    let outcome = handle_task_result!(task);
                    path_pool.push(outcome.pool_return_file);
                    if let Some(mut vec) = outcome.pool_return_byte_counts {
                        vec.clear();
                        byte_counts_pool.push(vec);
                    }
                }
            };
        }

        match generator.queue_gen(target_dir.clone(), max_depth > 0, &mut byte_counts_pool) {
            Ok(QueueOutcome {
                task,
                num_dirs,
                done: _,
            }) => {
                tasks.push_back(task);
                if num_dirs > 0 {
                    stack.push((1, vec![num_dirs]));
                }
            }
            Err(QueueErrors::NothingToDo(path)) => path_pool.push(path),
        }

        let gen_span = span!(Level::TRACE, "dir_gen");
        'outer: while let Some((tot_dirs, dirs_left)) = stack.last_mut() {
            let Some(num_dirs_to_generate) = dirs_left.pop() else {
                vec_pool.push(unsafe { stack.pop().unwrap_unchecked().1 });

                if let Some((tot_dirs, dirs_left)) = stack.last() {
                    target_dir.pop();

                    if !dirs_left.is_empty() {
                        with_dir_name(*tot_dirs - dirs_left.len(), |s| {
                            target_dir.set_file_name(s);
                        });
                    }
                }

                continue;
            };

            if tasks.len() + num_dirs_to_generate >= tasks.capacity() {
                flush_tasks!();
            }

            let next_stack_dir = *tot_dirs - dirs_left.len();
            let is_completing = dirs_left.is_empty();
            let gen_next_dirs = stack.len() < max_depth;

            let mut next_dirs = vec_pool.pop().unwrap_or_default();
            debug_assert!(next_dirs.is_empty());
            next_dirs.reserve(if gen_next_dirs {
                // TODO figure out if we can bound this memory usage
                num_dirs_to_generate
            } else {
                0
            });
            // Allocate a queue without VecDeque since we know the queue length will only
            // shrink. We want a queue so that the first task that is scheduled
            // is the directory we investigate first such that it will hopefully
            // have finished creating its directories (and thus minimize lock
            // contention).
            let raw_next_dirs = next_dirs.spare_capacity_mut();

            let span_guard = gen_span.enter();
            for i in 0..num_dirs_to_generate {
                let path = with_dir_name(i, |s| {
                    let mut buf = path_pool.pop().unwrap_or_else(|| {
                        // Space for inner, the path separator, name, and a NUL terminator
                        FastPathBuf::with_capacity(target_dir.capacity() + 1 + s.len() + 1)
                    });

                    buf.clone_from(&target_dir);
                    buf.push(s);

                    buf
                });

                let num_dirs = match generator.queue_gen(path, gen_next_dirs, &mut byte_counts_pool)
                {
                    Ok(QueueOutcome {
                        task,
                        num_dirs,
                        done,
                    }) => {
                        tasks.push_back(task);
                        if done {
                            break 'outer;
                        }
                        num_dirs
                    }
                    Err(QueueErrors::NothingToDo(path)) => {
                        path_pool.push(path);
                        0
                    }
                };

                if gen_next_dirs {
                    raw_next_dirs[num_dirs_to_generate - i - 1].write(num_dirs);
                }
            }
            drop(span_guard);

            if gen_next_dirs {
                unsafe {
                    next_dirs.set_len(num_dirs_to_generate);
                }
                stack.push((num_dirs_to_generate, next_dirs));

                with_dir_name(0, |s| target_dir.push(s));
            } else {
                if !is_completing {
                    with_dir_name(next_stack_dir, |s| target_dir.set_file_name(s));
                }
                vec_pool.push(next_dirs);
            }
        }

        if let Ok(QueueOutcome {
            task,
            num_dirs: _,
            done: _,
        }) = generator.maybe_queue_final_gen(target_dir, &mut byte_counts_pool)
        {
            tasks.push_back(task);
        }
    }

    for task in tasks {
        handle_task_result!(task);
    }

    Ok(stats)
}
