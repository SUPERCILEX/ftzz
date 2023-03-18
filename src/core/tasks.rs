#![allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]

use std::{cmp::min, io, num::NonZeroU64};

use rand::{distributions::Distribution, RngCore};
use tokio::{task, task::JoinHandle};

use crate::{
    core::{
        file_contents::{
            FileContentsGenerator, NoGeneratedFileContents, OnTheFlyGeneratedFileContents,
            PreDefinedGeneratedFileContents,
        },
        files::{create_files_and_dirs, GeneratorTaskOutcome, GeneratorTaskParams},
    },
    utils::FastPathBuf,
};

pub type QueueResult = Result<QueueOutcome, QueueErrors>;

pub struct QueueOutcome {
    #[cfg(not(feature = "dry_run"))]
    pub task: JoinHandle<error_stack::Result<GeneratorTaskOutcome, io::Error>>,
    #[cfg(feature = "dry_run")]
    pub task: GeneratorTaskOutcome,

    pub num_dirs: usize,
    pub done: bool,
}

#[derive(Debug)]
pub enum QueueErrors {
    NothingToDo(FastPathBuf),
}

pub trait TaskGenerator {
    fn queue_gen(
        &mut self,
        file: FastPathBuf,
        gen_dirs: bool,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult;

    fn maybe_queue_final_gen(&mut self, file: FastPathBuf, _: &mut Vec<Vec<u64>>) -> QueueResult {
        Err(QueueErrors::NothingToDo(file))
    }

    fn uses_byte_counts_pool(&self) -> bool {
        false
    }
}

fn queue(
    params @ GeneratorTaskParams {
        num_files,
        num_dirs,
        ..
    }: GeneratorTaskParams<impl FileContentsGenerator + Send + 'static>,
    done: bool,
) -> QueueResult {
    if num_files > 0 || num_dirs > 0 {
        Ok(QueueOutcome {
            num_dirs,
            done,

            #[cfg(not(feature = "dry_run"))]
            task: task::spawn_blocking(move || create_files_and_dirs(params)),
            #[cfg(feature = "dry_run")]
            task: {
                std::hint::black_box(&params);
                GeneratorTaskOutcome {
                    files_generated: num_files,
                    dirs_generated: num_dirs,
                    bytes_generated: 0,

                    pool_return_file: params.target_dir,
                    pool_return_byte_counts: None,
                }
            },
        })
    } else {
        Err(QueueErrors::NothingToDo(params.target_dir))
    }
}

pub struct GeneratorBytes<DB> {
    pub num_bytes_distr: DB,
    pub fill_byte: Option<u8>,
}

pub struct DynamicGenerator<DF, DD, DB, R> {
    pub num_files_distr: DF,
    pub num_dirs_distr: DD,
    pub random: R,

    pub bytes: Option<GeneratorBytes<DB>>,
}

impl<
    DF: Distribution<f64>,
    DD: Distribution<f64>,
    DB: Distribution<f64> + Clone + Send + 'static,
    R: RngCore + Clone + Send + 'static,
> TaskGenerator for DynamicGenerator<DF, DD, DB, R>
{
    fn queue_gen(
        &mut self,
        file: FastPathBuf,
        gen_dirs: bool,
        _: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let Self {
            ref num_files_distr,
            ref num_dirs_distr,
            ref mut random,
            ref bytes,
        } = *self;

        let num_files = num_files_distr.sample(random).round() as u64;
        let num_dirs = if gen_dirs {
            num_dirs_distr.sample(random).round() as usize
        } else {
            0
        };

        macro_rules! build_params {
            ($file_contents:expr) => {{
                GeneratorTaskParams {
                    target_dir: file,
                    num_files,
                    num_dirs,
                    file_offset: 0,
                    file_contents: $file_contents,
                }
            }};
        }

        if let Some(GeneratorBytes {
            ref num_bytes_distr,
            fill_byte,
        }) = *bytes
        {
            queue(
                build_params!(OnTheFlyGeneratedFileContents {
                    num_bytes_distr: num_bytes_distr.clone(),
                    random: random.clone(),
                    fill_byte,
                }),
                false,
            )
        } else {
            queue(build_params!(NoGeneratedFileContents), false)
        }
    }
}

pub struct StaticGenerator<DF, DD, DB, R> {
    dynamic: DynamicGenerator<DF, DD, DB, R>,

    files_exact: Option<u64>,
    bytes_exact: Option<u64>,

    done: bool,
    root_num_files_hack: Option<u64>,
}

impl<
    DF: Distribution<f64>,
    DD: Distribution<f64>,
    DB: Distribution<f64> + Clone + Send + 'static,
    R: RngCore + Clone + Send + 'static,
> TaskGenerator for StaticGenerator<DF, DD, DB, R>
{
    fn queue_gen(
        &mut self,
        file: FastPathBuf,
        gen_dirs: bool,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let Self {
            dynamic:
                DynamicGenerator {
                    ref num_files_distr,
                    ref num_dirs_distr,
                    ref mut random,
                    bytes: _,
                },
            ref mut files_exact,
            bytes_exact: _,
            ref mut done,
            ref mut root_num_files_hack,
        } = *self;

        debug_assert!(!*done);

        let mut num_files = num_files_distr.sample(random).round() as u64;
        if let Some(files) = files_exact {
            if num_files >= *files {
                *done = true;
                num_files = *files;
            } else {
                *files -= num_files;
            }
        }

        if root_num_files_hack.is_none() {
            *root_num_files_hack = Some(num_files);
        }

        let num_dirs = if gen_dirs && !*done {
            num_dirs_distr.sample(random).round() as usize
        } else {
            0
        };

        self.queue_gen_internal(file, num_files, num_dirs, 0, byte_counts_pool)
    }

    fn maybe_queue_final_gen(
        &mut self,
        file: FastPathBuf,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let Self {
            dynamic: _,
            files_exact,
            bytes_exact,
            ref mut done,
            root_num_files_hack,
        } = *self;

        if *done {
            return Err(QueueErrors::NothingToDo(file));
        }
        *done = true;

        // TODO Dumping all the remaining files or bytes in the root directory is very
        // dumb and wrong  1. If there are a lot of files, we're missing out on
        // performance gains from generating     the files in separate
        // directories  2. The distribution will be totally wrong
        //  Ideally we would continue the while loop above until enough files have been
        // generated,  but I haven't had time to think about how to do so
        // properly.
        if let Some(files) = files_exact {
            self.queue_gen_internal(
                file,
                files,
                0,
                root_num_files_hack.unwrap_or(0),
                byte_counts_pool,
            )
        } else if matches!(bytes_exact, Some(b) if b > 0) {
            self.queue_gen_internal(
                file,
                1,
                0,
                root_num_files_hack.unwrap_or(0),
                byte_counts_pool,
            )
        } else {
            Err(QueueErrors::NothingToDo(file))
        }
    }

    fn uses_byte_counts_pool(&self) -> bool {
        let Self {
            dynamic: DynamicGenerator { ref bytes, .. },
            bytes_exact,
            ..
        } = *self;

        bytes.is_some() && matches!(bytes_exact, Some(b) if b > 0)
    }
}

impl<
    DF: Distribution<f64>,
    DD: Distribution<f64>,
    DB: Distribution<f64> + Clone + Send + 'static,
    R: RngCore + Clone + Send + 'static,
> StaticGenerator<DF, DD, DB, R>
{
    pub fn new(
        dynamic: DynamicGenerator<DF, DD, DB, R>,
        files_exact: Option<NonZeroU64>,
        bytes_exact: Option<NonZeroU64>,
    ) -> Self {
        debug_assert!(files_exact.is_some() || bytes_exact.is_some());
        Self {
            dynamic,
            files_exact: files_exact.map(NonZeroU64::get),
            bytes_exact: bytes_exact.map(NonZeroU64::get),
            done: false,
            root_num_files_hack: None,
        }
    }

    fn queue_gen_internal(
        &mut self,
        file: FastPathBuf,
        num_files: u64,
        num_dirs: usize,
        offset: u64,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        macro_rules! build_params {
            ($file_contents:expr) => {{
                GeneratorTaskParams {
                    target_dir: file,
                    num_files,
                    num_dirs,
                    file_offset: offset,
                    file_contents: $file_contents,
                }
            }};
        }

        let Self {
            dynamic:
                DynamicGenerator {
                    num_files_distr: _,
                    num_dirs_distr: _,
                    ref mut random,
                    ref bytes,
                },
            files_exact: _,
            ref mut bytes_exact,
            done,
            root_num_files_hack: _,
        } = *self;

        if num_files > 0 && let Some(GeneratorBytes { ref num_bytes_distr, fill_byte }) = *bytes {
            if let Some(bytes) = bytes_exact {
                if *bytes > 0 {
                    let mut byte_counts: Vec<u64> = byte_counts_pool.pop().unwrap_or_default();
                    debug_assert!(byte_counts.is_empty());
                    let num_files_usize = num_files.try_into().unwrap_or(usize::MAX);
                    byte_counts.reserve(num_files_usize);
                    let raw_byte_counts = byte_counts
                        .spare_capacity_mut()
                        .split_at_mut(num_files_usize)
                        .0;

                    for count in raw_byte_counts {
                        let num_bytes = min(*bytes, num_bytes_distr.sample(random).round() as u64);
                        *bytes -= num_bytes;

                        count.write(num_bytes);
                    }

                    unsafe {
                        byte_counts.set_len(num_files_usize);
                    }

                    if done {
                        let base = *bytes / num_files;
                        let mut leftovers = *bytes % num_files;
                        for count in &mut byte_counts {
                            if leftovers > 0 {
                                *count += base + 1;
                                leftovers -= 1;
                            } else {
                                *count += base;
                            }
                        }
                    }

                    queue(
                        build_params!(PreDefinedGeneratedFileContents {
                            byte_counts,
                            random: random.clone(),
                            fill_byte,
                        }),
                        done,
                    )
                } else {
                    queue(build_params!(NoGeneratedFileContents), done)
                }
            } else {
                queue(
                    build_params!(OnTheFlyGeneratedFileContents {
                        num_bytes_distr: num_bytes_distr.clone(),
                        random: random.clone(),
                        fill_byte,
                    }),
                    done,
                )
            }
        } else {
            queue(build_params!(NoGeneratedFileContents), done)
        }
    }
}
