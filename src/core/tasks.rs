#![allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]

use std::{cmp::min, io, num::NonZeroU64};

use rand::RngCore;
use rand_distr::Normal;
use tokio::{task, task::JoinHandle};

use crate::{
    core::{
        file_contents::{
            FileContentsGenerator, NoGeneratedFileContents, OnTheFlyGeneratedFileContents,
            PreDefinedGeneratedFileContents,
        },
        files::{GeneratorTaskOutcome, GeneratorTaskParams, create_files_and_dirs},
        sample_truncated,
    },
    utils::FastPathBuf,
};

pub type QueueResult = Result<QueueOutcome, QueueErrors>;

pub struct QueueOutcome {
    #[cfg(not(feature = "dry_run"))]
    pub task: JoinHandle<error_stack::Result<GeneratorTaskOutcome, io::Error>>,
    #[cfg(feature = "dry_run")]
    pub task: GeneratorTaskOutcome,

    pub num_files: u64,
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
        num_files_distr: &Normal<f64>,
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

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(params))
)]
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
            num_files,
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

fn dirs_to_gen<R: RngCore + ?Sized>(
    files_created: u64,
    gen_dirs: bool,
    num_dirs_distr: &Normal<f64>,
    random: &mut R,
) -> usize {
    if gen_dirs {
        let dirs = usize::try_from(sample_truncated(num_dirs_distr, random)).unwrap_or(usize::MAX);
        if files_created > 0 && dirs == 0 {
            1
        } else {
            dirs
        }
    } else {
        0
    }
}

pub struct GeneratorBytes {
    pub num_bytes_distr: Normal<f64>,
    pub fill_byte: Option<u8>,
}

pub struct DynamicGenerator<R> {
    pub num_dirs_distr: Normal<f64>,
    pub random: R,

    pub bytes: Option<GeneratorBytes>,
}

impl<R: RngCore + Clone + Send + 'static> TaskGenerator for DynamicGenerator<R> {
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip(self)))]
    fn queue_gen(
        &mut self,
        num_files_distr: &Normal<f64>,
        file: FastPathBuf,
        gen_dirs: bool,
        _: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let Self {
            ref num_dirs_distr,
            ref mut random,
            ref bytes,
        } = *self;

        let num_files = sample_truncated(num_files_distr, random);
        let num_dirs = dirs_to_gen(num_files, gen_dirs, num_dirs_distr, random);

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
            num_bytes_distr,
            fill_byte,
        }) = *bytes
        {
            queue(
                build_params!(OnTheFlyGeneratedFileContents {
                    num_bytes_distr,
                    seed: random.next_u64(),
                    fill_byte,
                }),
                false,
            )
        } else {
            queue(build_params!(NoGeneratedFileContents), false)
        }
    }
}

pub struct StaticGenerator<R> {
    dynamic: DynamicGenerator<R>,

    files_exact: Option<u64>,
    bytes_exact: Option<u64>,

    done: bool,
    root_num_files_hack: Option<u64>,
}

impl<R: RngCore + Clone + Send + 'static> TaskGenerator for StaticGenerator<R> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "trace", skip(self, byte_counts_pool))
    )]
    fn queue_gen(
        &mut self,
        num_files_distr: &Normal<f64>,
        file: FastPathBuf,
        gen_dirs: bool,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let Self {
            dynamic:
                DynamicGenerator {
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

        let mut num_files = sample_truncated(num_files_distr, random);
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

        let num_dirs = if *done {
            0
        } else {
            dirs_to_gen(num_files, gen_dirs, num_dirs_distr, random)
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

impl<R: RngCore + Clone + Send + 'static> StaticGenerator<R> {
    pub fn new(
        dynamic: DynamicGenerator<R>,
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

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "trace", skip(self, byte_counts_pool))
    )]
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
                    num_dirs_distr: _,
                    ref mut random,
                    ref bytes,
                },
            files_exact: _,
            ref mut bytes_exact,
            done,
            root_num_files_hack: _,
        } = *self;

        if num_files > 0
            && let Some(GeneratorBytes {
                num_bytes_distr,
                fill_byte,
            }) = *bytes
        {
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
                        let num_bytes = min(*bytes, sample_truncated(&num_bytes_distr, random));
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
                            seed: random.next_u64(),
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
                        num_bytes_distr,
                        seed: random.next_u64(),
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
