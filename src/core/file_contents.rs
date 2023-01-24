use std::{fs::File, io, io::Read};

use cfg_if::cfg_if;
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
                use rustix::fs::{mknodat, FileType, Mode};

                let cstr = file.to_cstr_mut();
                mknodat(
                    rustix::fs::cwd(),
                    &*cstr,
                    FileType::RegularFile,
                    Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP | Mode::ROTH,
                    0,
                )
                .map_err(io::Error::from)
                .map(|_| 0)
            } else {
                use rustix::fs::{openat, OFlags, Mode};

                let cstr = file.to_cstr_mut();
                openat(
                    rustix::fs::cwd(),
                    &*cstr,
                    OFlags::CREATE,
                    Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP | Mode::ROTH,
                )
                .map_err(io::Error::from)
                .map(|_| 0)
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
    pub fill_byte: Option<u8>,
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
        let Self {
            ref num_bytes_distr,
            ref mut random,
            fill_byte,
        } = *self;

        let num_bytes = num_bytes_distr.sample(random).round() as u64;
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
                    num_bytes_distr.sample(random).round() as u64
                } else {
                    num_bytes
                };
                write_bytes(f, num_bytes, (fill_byte, random))?;
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
    pub fill_byte: Option<u8>,
}

impl<R: RngCore + 'static> FileContentsGenerator for PreDefinedGeneratedFileContents<R> {
    #[inline]
    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        file_num: usize,
        retryable: bool,
    ) -> io::Result<u64> {
        let Self {
            ref byte_counts,
            ref mut random,
            fill_byte,
        } = *self;

        let num_bytes = byte_counts[file_num];
        if num_bytes > 0 {
            File::create(file)
                .and_then(|f| write_bytes(f, num_bytes, (fill_byte, random)))
                .map(|_| num_bytes)
        } else {
            NoGeneratedFileContents.create_file(file, file_num, retryable)
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        Some(self.byte_counts)
    }
}

enum BytesKind<'a, R> {
    Random(&'a mut R),
    Fixed(u8),
}

impl<'a, R> From<(Option<u8>, &'a mut R)> for BytesKind<'a, R> {
    fn from((fill_byte, random): (Option<u8>, &'a mut R)) -> Self {
        fill_byte.map_or(BytesKind::Random(random), |byte| BytesKind::Fixed(byte))
    }
}

#[instrument(level = "trace", skip(file, kind))]
fn write_bytes<'a, R: RngCore + 'static>(
    mut file: File,
    num: u64,
    kind: impl Into<BytesKind<'a, R>>,
) -> io::Result<()> {
    let copied = match kind.into() {
        BytesKind::Random(random) => {
            io::copy(&mut (random as &mut dyn RngCore).take(num), &mut file)
        }
        BytesKind::Fixed(byte) => io::copy(&mut io::repeat(byte).take(num), &mut file),
    }?;
    debug_assert_eq!(num, copied);
    Ok(())
}
