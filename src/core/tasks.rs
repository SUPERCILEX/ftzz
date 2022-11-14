#![allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]

use std::{cmp::min, io, num::NonZeroU64, os::unix::io::OwnedFd};

use rand::{distributions::Distribution, RngCore};
use tokio::{task, task::JoinHandle};

use crate::core::{
    file_contents::{
        NoGeneratedFileContents, OnTheFlyGeneratedFileContents, PreDefinedGeneratedFileContents,
    },
    files::{create_files_and_dirs, GeneratorTaskOutcome, GeneratorTaskParams},
};

pub type QueueResult = Result<QueueOutcome, QueueErrors>;

pub struct QueueOutcome {
    #[cfg(not(dry_run))]
    pub task: JoinHandle<error_stack::Result<GeneratorTaskOutcome, io::Error>>,
    #[cfg(dry_run)]
    pub task: GeneratorTaskOutcome,

    pub num_dirs: usize,
    pub done: bool,
}

#[derive(Debug)]
pub enum QueueErrors {
    NothingToDo(OwnedFd),
}

pub trait TaskGenerator {
    fn queue_gen(
        &mut self,
        dir: OwnedFd,
        gen_dirs: bool,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult;

    fn maybe_queue_final_gen(&mut self, dir: OwnedFd, _: &mut Vec<Vec<u64>>) -> QueueResult {
        Err(QueueErrors::NothingToDo(dir))
    }

    fn uses_byte_counts_pool(&self) -> bool {
        false
    }
}

macro_rules! queue {
    ($params:expr, $done:expr) => {{
        let params = $params;
        if params.num_files > 0 || params.num_dirs > 0 {
            Ok(QueueOutcome {
                num_dirs: params.num_dirs,
                done: $done,

                #[cfg(not(dry_run))]
                task: task::spawn_blocking(move || create_files_and_dirs(params)),
                #[cfg(dry_run)]
                task: GeneratorTaskOutcome {
                    files_generated: params.num_files,
                    dirs_generated: params.num_dirs,
                    bytes_generated: 0,

                    pool_return_file: params.target_dir,
                    pool_return_byte_counts: None,
                },
            })
        } else {
            Err(QueueErrors::NothingToDo(params.target_dir))
        }
    }};
}

pub struct FilesNoContentsGenerator<DF, DD, R> {
    pub num_files_distr: DF,
    pub num_dirs_distr: DD,
    pub random: R,
}

impl<DF: Distribution<f64>, DD: Distribution<f64>, R: RngCore> TaskGenerator
    for FilesNoContentsGenerator<DF, DD, R>
{
    fn queue_gen(
        &mut self,
        target_dir: OwnedFd,
        gen_dirs: bool,
        _: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let num_files = self.num_files_distr.sample(&mut self.random).round() as u64;
        let params = GeneratorTaskParams {
            target_dir,
            num_files,
            num_dirs: if gen_dirs {
                self.num_dirs_distr.sample(&mut self.random).round() as usize
            } else {
                0
            },
            file_offset: 0,
            file_contents: NoGeneratedFileContents,
        };

        queue!(params, false)
    }
}

pub struct FilesAndContentsGenerator<DF, DD, DB, R> {
    pub num_files_distr: DF,
    pub num_dirs_distr: DD,
    pub num_bytes_distr: DB,
    pub random: R,
}

impl<
        DF: Distribution<f64>,
        DD: Distribution<f64>,
        DB: Distribution<f64> + Clone + Send + 'static,
        R: RngCore + Clone + Send + 'static,
    > TaskGenerator for FilesAndContentsGenerator<DF, DD, DB, R>
{
    fn queue_gen(
        &mut self,
        target_dir: OwnedFd,
        gen_dirs: bool,
        _: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let num_files = self.num_files_distr.sample(&mut self.random).round() as u64;
        let params = GeneratorTaskParams {
            target_dir,
            num_files,
            num_dirs: if gen_dirs {
                self.num_dirs_distr.sample(&mut self.random).round() as usize
            } else {
                0
            },
            file_offset: 0,
            file_contents: OnTheFlyGeneratedFileContents {
                num_bytes_distr: self.num_bytes_distr.clone(),
                random: self.random.clone(),
            },
        };

        queue!(params, false)
    }
}

pub struct OtherFilesAndContentsGenerator<DF, DD, DB, R> {
    num_files_distr: DF,
    num_dirs_distr: DD,
    num_bytes_distr: Option<DB>,
    random: R,

    files_exact: Option<NonZeroU64>,
    bytes_exact: Option<u64>,

    done: bool,
    root_num_files_hack: Option<u64>,
}

impl<
        DF: Distribution<f64>,
        DD: Distribution<f64>,
        DB: Distribution<f64> + Clone + Send + 'static,
        R: RngCore + Clone + Send + 'static,
    > TaskGenerator for OtherFilesAndContentsGenerator<DF, DD, DB, R>
{
    fn queue_gen(
        &mut self,
        dir: OwnedFd,
        gen_dirs: bool,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        debug_assert!(!self.done);

        let mut num_files = self.num_files_distr.sample(&mut self.random).round() as u64;
        if let Some(ref mut files) = self.files_exact {
            if num_files >= files.get() {
                self.done = true;
                num_files = files.get();
            } else {
                *files = unsafe { NonZeroU64::new_unchecked(files.get() - num_files) };
            }
        }

        if self.root_num_files_hack.is_none() {
            self.root_num_files_hack = Some(num_files);
        }

        let num_dirs = if gen_dirs && !self.done {
            self.num_dirs_distr.sample(&mut self.random).round() as usize
        } else {
            0
        };

        self.queue_gen_internal(dir, num_files, num_dirs, 0, byte_counts_pool)
    }

    fn maybe_queue_final_gen(
        &mut self,
        dir: OwnedFd,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        if self.done {
            return Err(QueueErrors::NothingToDo(dir));
        }
        self.done = true;

        // TODO Dumping all the remaining files or bytes in the root directory is very
        // dumb and wrong  1. If there are a lot of files, we're missing out on
        // performance gains from generating     the files in separate
        // directories  2. The distribution will be totally wrong
        //  Ideally we would continue the while loop above until enough files have been
        // generated,  but I haven't had time to think about how to do so
        // properly.
        if let Some(files) = self.files_exact {
            self.queue_gen_internal(
                dir,
                files.get(),
                0,
                self.root_num_files_hack.unwrap_or(0),
                byte_counts_pool,
            )
        } else if matches!(self.bytes_exact, Some(b) if b > 0) {
            self.queue_gen_internal(
                dir,
                1,
                0,
                self.root_num_files_hack.unwrap_or(0),
                byte_counts_pool,
            )
        } else {
            Err(QueueErrors::NothingToDo(dir))
        }
    }

    fn uses_byte_counts_pool(&self) -> bool {
        self.num_bytes_distr.is_some() && matches!(self.bytes_exact, Some(b) if b > 0)
    }
}

impl<
        DF: Distribution<f64>,
        DD: Distribution<f64>,
        DB: Distribution<f64> + Clone + Send + 'static,
        R: RngCore + Clone + Send + 'static,
    > OtherFilesAndContentsGenerator<DF, DD, DB, R>
{
    pub const fn new(
        num_files_distr: DF,
        num_dirs_distr: DD,
        num_bytes_distr: Option<DB>,
        random: R,
        files_exact: Option<NonZeroU64>,
        bytes_exact: Option<u64>,
    ) -> Self {
        Self {
            num_files_distr,
            num_dirs_distr,
            num_bytes_distr,
            random,
            files_exact,
            bytes_exact,
            done: false,
            root_num_files_hack: None,
        }
    }

    fn queue_gen_internal(
        &mut self,
        target_dir: OwnedFd,
        num_files: u64,
        num_dirs: usize,
        offset: u64,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        macro_rules! build_params {
            ($file_contents:expr) => {{
                GeneratorTaskParams {
                    target_dir,
                    num_files,
                    num_dirs,
                    file_offset: offset,
                    file_contents: $file_contents,
                }
            }};
        }

        if num_files > 0 && let Some(bytes_distr) = &self.num_bytes_distr {
            if let Some(ref mut bytes) = self.bytes_exact {
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
                        let num_bytes =
                            min(*bytes, bytes_distr.sample(&mut self.random).round() as u64);
                        *bytes -= num_bytes;

                        count.write(num_bytes);
                    }

                    unsafe {
                        byte_counts.set_len(num_files_usize);
                    }

                    if self.done {
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

                    queue!(
                        build_params!(PreDefinedGeneratedFileContents {
                            byte_counts,
                            random: self.random.clone(),
                        }),
                        self.done
                    )
                } else {
                    queue!(build_params!(NoGeneratedFileContents), self.done)
                }
            } else {
                queue!(
                    build_params!(OnTheFlyGeneratedFileContents {
                        num_bytes_distr: bytes_distr.clone(),
                        random: self.random.clone(),
                    }),
                    self.done
                )
            }
        } else {
            queue!(build_params!(NoGeneratedFileContents), self.done)
        }
    }
}
