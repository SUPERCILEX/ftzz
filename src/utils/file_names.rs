use std::{mem::MaybeUninit, ptr, slice};

use itoa_const as itoa;

struct FileNameCache {
    cache: [MaybeUninit<u8>; 3000],
}

/// Specialized cache for file names that takes advantage of our monotonically
/// increasing integer naming scheme.
///
/// The cache can be thought of as an arena, meaning all strings are written
/// into a single buffer that only needs to be allocated once. The memory layout
/// sizes every item equally such that the minimal number of instructions can be
/// used to retrieve items. To strike a balance between compute and memory
/// usage, the numbers 0-999 are cached leading to 3 * 1000 = 3000 bytes
/// being allocated (thus likely residing in a 32 KiB L1 cache). Furthermore,
/// since this cache is so small, we construct it at compile time and ship it
/// with the binary.
impl FileNameCache {
    const fn new() -> Self {
        let mut buf = [MaybeUninit::<u8>::uninit(); 3000];
        let mut num_buf = itoa::Buffer::new();

        let buf_ptr = buf.as_mut_ptr().cast::<u8>();
        // TODO use for loop once possible
        let mut i = 0;
        while i < Self::max_cache_size() {
            let bytes = num_buf.format(i).as_bytes();
            unsafe {
                ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    buf_ptr.add((i * 3) as usize),
                    bytes.len(),
                );
            }
            i += 1;
        }

        Self { cache: buf }
    }

    const fn with_file_name<T, F: ~const FnOnce(&str) -> T>(&self, i: u16, f: F) -> T {
        debug_assert!(i < Self::max_cache_size());
        f(unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(
                self.cache.as_ptr().add((i * 3) as usize).cast::<u8>(),
                Self::str_bytes_used(i),
            ))
        })
    }

    const fn max_cache_size() -> u16 {
        1000
    }

    /// Inspired by
    /// <https://github.com/rust-lang/rust/blob/7b0bf9efc939341b48c6e9a335dee8a280085100/library/core/src/num/int_log10.rs>
    const fn str_bytes_used(val: u16) -> usize {
        const C1: u16 = 0b100_0000_0000 - 10;
        const C2: u16 = 0b010_0000_0000 - 100;

        (((val + C1) | (val + C2)) >> 9) as usize
    }
}

static CACHE: FileNameCache = FileNameCache::new();

pub fn with_file_name<T>(i: u64, f: impl FnOnce(&str) -> T) -> T {
    if i < FileNameCache::max_cache_size().into() {
        CACHE.with_file_name(i.try_into().unwrap(), f)
    } else {
        f(itoa::Buffer::new().format(i))
    }
}

pub fn with_dir_name<T>(i: usize, f: impl FnOnce(&str) -> T) -> T {
    const SUFFIX: &str = ".dir";
    with_file_name(i.try_into().unwrap(), |s| {
        let mut buf = [MaybeUninit::<u8>::uninit(); 39 + SUFFIX.len()]; // 39 to support u128
        unsafe {
            let buf_ptr = buf.as_mut_ptr().cast::<u8>();
            ptr::copy_nonoverlapping(s.as_ptr(), buf_ptr, s.len());
            ptr::copy_nonoverlapping(SUFFIX.as_ptr(), buf_ptr.add(s.len()), SUFFIX.len());

            f(std::str::from_utf8_unchecked(slice::from_raw_parts(
                buf.as_ptr().cast::<u8>(),
                s.len() + SUFFIX.len(),
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_bytes_used_correctness() {
        for i in 0..FileNameCache::max_cache_size() {
            let used = FileNameCache::str_bytes_used(i);
            if i >= 100 {
                assert_eq!(used, 3);
            } else if i >= 10 {
                assert_eq!(used, 2);
            } else {
                assert_eq!(used, 1);
            }
        }
    }

    #[test]
    fn names_are_returned() {
        for i in 0..FileNameCache::max_cache_size() * 2 {
            with_file_name(i.into(), |s| {
                assert_eq!(s, i.to_string());
            });
            with_dir_name(i.try_into().unwrap(), |s| {
                assert_eq!(s, format!("{i}.dir"));
            });
        }
    }
}
