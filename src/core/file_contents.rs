use cfg_if::cfg_if;
use std::{fs::File, io, io::Read};

use rand::{distributions::Distribution, RngCore};
use tracing::instrument;

use crate::utils::FastPathBuf;

pub trait FileContentsGenerator {
    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        file_num: usize,
        retryable: bool,
    ) -> io::Result<u64>;

    fn byte_counts_pool_return(self) -> Option<Vec<u64>>;
}

pub struct NoGeneratedFileContents;

impl FileContentsGenerator for NoGeneratedFileContents {
    #[inline]
    fn create_file(&mut self, file: &mut FastPathBuf, _: usize, _: bool) -> io::Result<u64> {
        cfg_if! {
            if #[cfg(any(not(unix), miri))] {
                File::create(file).map(|_| 0)
            } else if #[cfg(target_os = "linux")] {
                use nix::sys::stat::{mknod, Mode, SFlag};

                let cstr = file.to_cstr_mut();
                mknod(
                    &*cstr,
                    SFlag::S_IFREG,
                    Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH,
                    0,
                )
                .map_err(io::Error::from)
                .map(|_| 0)
            } else {
                use nix::{
                    fcntl::{open, OFlag},
                    sys::stat::Mode,
                };
                use std::os::fd::{FromRawFd, OwnedFd};

                let cstr = file.to_cstr_mut();
                open(
                    &*cstr,
                    OFlag::O_CREAT,
                    Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH,
                )
                .map_err(io::Error::from)
                .map(|fd| {
                    unsafe {
                        OwnedFd::from_raw_fd(fd);
                    }
                    0
                })
            }
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        None
    }
}

pub struct OnTheFlyGeneratedFileContents<D: Distribution<f64>, R: RngCore> {
    pub num_bytes_distr: D,
    pub random: R,
}

impl<D: Distribution<f64>, R: RngCore + 'static> FileContentsGenerator
    for OnTheFlyGeneratedFileContents<D, R>
{
    #[inline]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        file_num: usize,
        retryable: bool,
    ) -> io::Result<u64> {
        let num_bytes = self.num_bytes_distr.sample(&mut self.random).round() as u64;
        if num_bytes > 0 || retryable {
            File::create(file).and_then(|f| {
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
                //    - Notice that num_to_generate can be 0 which is a bummer b/c we can't use
                //      mknod even though we'd like to.
                let num_bytes = if retryable {
                    self.num_bytes_distr.sample(&mut self.random).round() as u64
                } else {
                    num_bytes
                };
                write_random_bytes(f, num_bytes, &mut self.random)?;
                Ok(num_bytes)
            })
        } else {
            NoGeneratedFileContents.create_file(file, file_num, retryable)
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        None
    }
}

pub struct PreDefinedGeneratedFileContents<R: RngCore> {
    pub byte_counts: Vec<u64>,
    pub random: R,
}

impl<R: RngCore + 'static> FileContentsGenerator for PreDefinedGeneratedFileContents<R> {
    #[inline]
    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        file_num: usize,
        retryable: bool,
    ) -> io::Result<u64> {
        let num_bytes = self.byte_counts[file_num];
        if num_bytes > 0 {
            File::create(file)
                .and_then(|f| write_random_bytes(f, num_bytes, &mut self.random))
                .map(|_| num_bytes)
        } else {
            NoGeneratedFileContents.create_file(file, file_num, retryable)
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        Some(self.byte_counts)
    }
}

#[instrument(level = "trace", skip(file, random))]
fn write_random_bytes(
    mut file: File,
    num: u64,
    random: &mut (impl RngCore + 'static),
) -> io::Result<()> {
    let copied = io::copy(&mut (random as &mut dyn RngCore).take(num), &mut file)?;
    debug_assert_eq!(num, copied);
    Ok(())
}
