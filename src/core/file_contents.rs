use cfg_if::cfg_if;
use std::{ffi::CStr, fs::File, io, io::Read, os::unix::io::AsFd};

use rand::{distributions::Distribution, RngCore};
use tracing::instrument;

pub trait FileContentsGenerator {
    fn create_file(&mut self, dir: &impl AsFd, file: &str, file_num: usize) -> io::Result<u64>;

    fn byte_counts_pool_return(self) -> Option<Vec<u64>>;
}

pub struct NoGeneratedFileContents;

impl FileContentsGenerator for NoGeneratedFileContents {
    #[inline]
    fn create_file(&mut self, dir: &impl AsFd, file: &str, _: usize) -> io::Result<u64> {
        cfg_if! {
            if #[cfg(any(not(unix), miri))] {
                File::create(file).map(|_| 0)
            } else if #[cfg(target_os = "linux")] {
                use nix::sys::stat::{mknodat, Mode, SFlag};

                mknodat(
                    dir,
                    file,
                    SFlag::S_IFREG,
                    Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH,
                    0,
                )
                .map_err(io::Error::from)
                .map(|_| 0)
            } else {
                use nix::{
                    fcntl::{openat, OFlag},
                    sys::stat::Mode,
                };
                use std::os::fd::{FromRawFd, OwnedFd};

                let cstr = file.to_cstr_mut();
                openat(
                    dir,
                    file,
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
    fn create_file(&mut self, dir: &impl AsFd, file: &str, file_num: usize) -> io::Result<u64> {
        let num_bytes = self.num_bytes_distr.sample(&mut self.random).round() as u64;
        if num_bytes > 0 {
            open_file(dir, file).and_then(|f| write_random_bytes(f, num_bytes, &mut self.random))
        } else {
            NoGeneratedFileContents.create_file(dir, file, file_num)
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
    fn create_file(&mut self, dir: &impl AsFd, file: &str, file_num: usize) -> io::Result<u64> {
        let num_bytes = self.byte_counts[file_num];
        if num_bytes > 0 {
            open_file(dir, file).and_then(|f| write_random_bytes(f, num_bytes, &mut self.random))
        } else {
            NoGeneratedFileContents.create_file(dir, file, file_num)
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        Some(self.byte_counts)
    }
}

fn open_file(dir: &impl AsFd, file: &str) -> io::Result<File> {
    use nix::{
        fcntl::{openat, OFlag},
        sys::stat::Mode,
    };

    openat(
        dir,
        file,
        OFlag::O_CREAT,
        Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH,
    )
    .map(|f| File::from(f))
    .map_err(io::Error::from)
}

#[instrument(level = "trace", skip(file, random))]
fn write_random_bytes(
    mut file: File,
    num: u64,
    random: &mut (impl RngCore + 'static),
) -> io::Result<u64> {
    let copied = io::copy(&mut (random as &mut dyn RngCore).take(num), &mut file)?;
    debug_assert_eq!(num, copied);
    Ok(copied)
}
