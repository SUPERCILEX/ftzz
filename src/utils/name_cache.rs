use std::{
    cmp::min,
    mem::{ManuallyDrop, MaybeUninit},
    ptr, slice,
};

use tracing::{event, instrument, span, Level};

/// Specialized cache for file names that takes advantage of our monotonically increasing integer
/// naming scheme.
///
/// A raw fixed-size buffer is intentionally used to eliminate an Arc.
#[derive(Copy, Clone)]
pub struct FileNameCache {
    /// The cache can be thought of as an arena, meaning all strings are written into a single
    /// buffer that only needs to be allocated once. The following memory layout is used:
    ///
    /// ```not_rust
    /// [                                    len                                    ]
    /// [                 Indices and lengths               |        Strings        ]
    /// [ [     4-value packed representation      ], ... ] | [ Packed string bytes ]
    /// [ [ index, index,    length  ,    length   ], ... ] | [ Packed string bytes ]
    /// [ [  u16 ,  u16 , [ u4 | u4 ], [ u4 | u4 ] ], ... ] | [ Packed string bytes ]
    /// ```
    ///
    /// The goal of this representation is to minimize wasted space storing the string's location
    /// and size. Thus, a u16 is used to index into the text bytes followed by two u4s, each of
    /// which represent the length of a string. We can use u4s because the maximum text size will be
    /// 5 characters long (see below). Using a u16 allows us to store a maximum of 65536 text bytes.
    /// Consequentially, the maximum number of entries that can be stored is found by this equation:
    /// `1 + sum_1^5{n * (min(x , 10^n) - 10^(n - 1))} = 2^16`. Solving for `x` yields 15329
    /// entries. To keep the array 2-byte aligned, entries must come in pairs of four since a single
    /// entry only uses 1.5 bytes, making the true maximum 15328 entries. Finally, the maximum
    /// possible overall allocation is `15328 * 1.5 + 65530 = 88522` bytes which seems reasonable as
    /// far as cache sizes go.
    buffer: *const u8,
    len: u32,
    num_entries: u16,
    data_start: u16,
}

unsafe impl Send for FileNameCache {}

impl FileNameCache {
    #[instrument(level = "trace")]
    pub fn alloc(files_per_dir: f64, dirs_per_dir: f64) -> Self {
        // Overestimate since the cache can't grow
        let num_cache_entries = 1.5 * f64::max(files_per_dir, dirs_per_dir);
        // Round up to the nearst mod 4 to guarantee 2-byte alignment
        let num_cache_entries = (num_cache_entries.round() as usize + 3) & !3;
        // Max out the cache size at 15328
        let num_cache_entries = min(15328, num_cache_entries);

        let text_bytes_start = num_cache_entries + (num_cache_entries >> 1); // Multiply by 1.5
        let mut text_bytes = 0;
        {
            let mut num_cache_entries = num_cache_entries as u32;
            for n in (1..=5).rev() {
                // `& !1` to account for x^0 being 1 instead of 0
                let count = num_cache_entries - min(num_cache_entries, 10u32.pow(n - 1) & !1);
                text_bytes += n * count;
                num_cache_entries -= count;
            }
        }
        let mut buf = ManuallyDrop::new(Vec::with_capacity(text_bytes_start + text_bytes as usize));

        let alloc_span = span!(Level::TRACE, "name_gen");
        let span_guard = alloc_span.enter();

        let raw_buf: *mut u8 = buf.as_mut_ptr();
        let mut text_index = 0;
        for i in (0..num_cache_entries).step_by(2) {
            let (index_index, length_index) = Self::index_to_metadata(i);
            let mut s1 = itoa::Buffer::new();
            let s1 = s1.format(i);
            let mut s2 = itoa::Buffer::new();
            let s2 = s2.format(i + 1);

            unsafe {
                (raw_buf.add(index_index) as *mut u16).write(text_index as u16);
                raw_buf
                    .add(length_index)
                    .write(((s1.len() << 4) | s2.len()) as u8);

                ptr::copy_nonoverlapping(
                    s1.as_ptr(),
                    raw_buf.add(text_bytes_start + text_index) as *mut u8,
                    s1.len(),
                );
                ptr::copy_nonoverlapping(
                    s2.as_ptr(),
                    raw_buf.add(text_bytes_start + text_index + s1.len()) as *mut u8,
                    s2.len(),
                );
            }

            text_index += s1.len() + s2.len();
        }

        unsafe {
            let cap = buf.capacity();
            buf.set_len(cap);
        }

        drop(span_guard);
        event!(Level::DEBUG, size = buf.len(), "Name cache allocated");

        Self {
            buffer: buf.as_ptr(),
            len: buf.len() as u32,
            num_entries: num_cache_entries as u16,
            data_start: text_bytes_start as u16,
        }
    }

    pub fn free(self) {
        unsafe {
            Vec::from_raw_parts(self.buffer as *mut u8, self.len as usize, self.len as usize);
        }
    }

    pub fn with_file_name<T>(self, i: usize, f: impl FnOnce(&str) -> T) -> T {
        if i >= self.num_entries as usize {
            return Self::raw_with_file_name(i, f);
        }

        let (index_index, length_index) = Self::index_to_metadata(i);
        unsafe {
            let index = (self.buffer.add(index_index) as *const u16).read();
            let start = self.data_start as usize + index as usize;
            let len = self.buffer.add(length_index).read() as usize;

            let s1_len = len >> 4;
            let s2_len = len & 0xF;
            let is_half_mask = ((i & 1) ^ 1).wrapping_sub(1);
            let start = start + (s1_len & is_half_mask);
            let len = (s1_len & !is_half_mask) | (s2_len & is_half_mask);

            f(std::str::from_utf8_unchecked(slice::from_raw_parts(
                self.buffer.add(start),
                len,
            )))
        }
    }

    pub fn with_dir_name<T>(self, i: usize, f: impl FnOnce(&str) -> T) -> T {
        self.with_file_name(i, |s| {
            let mut buf = [MaybeUninit::<u8>::uninit(); 9];
            unsafe {
                let buf_ptr = buf.as_mut_ptr() as *mut u8;
                ptr::copy_nonoverlapping(s.as_ptr(), buf_ptr, s.len());
                ptr::copy_nonoverlapping(".dir".as_ptr(), buf_ptr.add(s.len()), 4);

                f(std::str::from_utf8_unchecked(slice::from_raw_parts(
                    buf.as_ptr() as *const u8,
                    s.len() + 4,
                )))
            }
        })
    }

    #[inline]
    fn index_to_metadata(i: usize) -> (usize, usize) {
        let metadata_group = (i & !1) + ((i >> 1) & !1);
        (metadata_group, metadata_group + 4 - ((i >> 1) & 1))
    }

    #[cold]
    fn raw_with_file_name<T>(i: usize, f: impl FnOnce(&str) -> T) -> T {
        f(itoa::Buffer::new().format(i))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;

    struct SafeCache(FileNameCache);

    impl Deref for SafeCache {
        type Target = FileNameCache;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Drop for SafeCache {
        fn drop(&mut self) {
            self.0.free();
        }
    }

    #[test]
    fn zero_files_allocates_nothing() {
        let cache = SafeCache(FileNameCache::alloc(0., 0.));

        assert_eq!(cache.len, 0);
        assert_eq!(cache.num_entries, 0);
    }

    #[test]
    fn one_file_allocates_minimal_bytes() {
        let cache = SafeCache(FileNameCache::alloc(0.9, 0.));

        assert_eq!(cache.len, 10);
        assert_eq!(cache.num_entries, 4);
    }

    #[test]
    fn fifty_files_allocates_minimal_bytes() {
        let cache = SafeCache(FileNameCache::alloc(50., 0.));

        assert_eq!(cache.len, 256);
        assert_eq!(cache.num_entries, 76);
    }

    #[test]
    fn max_files_allocates_max_bytes() {
        let cache = SafeCache(FileNameCache::alloc(1e10, 0.));

        assert_eq!(cache.len, 88522);
        assert_eq!(cache.num_entries, 15328);
    }

    #[test]
    fn cached_names_are_returned() {
        let cache = SafeCache(FileNameCache::alloc(1e10, 0.));

        for i in 0..cache.num_entries as usize * 2 {
            cache.with_file_name(i, |s| {
                assert_eq!(s, i.to_string());
            });
            cache.with_dir_name(i, |s| {
                assert_eq!(s, format!("{}.dir", i));
            });
        }
    }
}
