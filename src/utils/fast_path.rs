use std::{
    ffi::OsStr,
    fmt,
    ops::{Deref, DerefMut},
    path::{MAIN_SEPARATOR, Path, PathBuf},
};

/// A specialized [`PathBuf`][std::path::PathBuf] implementation that takes
/// advantage of a few assumptions. Specifically, it *only* supports adding
/// single-level directories (e.g. "foo", "foo/bar" is not allowed) and updating
/// the current file name.
pub struct FastPathBuf {
    inner: Vec<u8>,
    last_len: usize,
}

impl FastPathBuf {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
            last_len: 0,
        }
    }

    pub const fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional);
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
    pub fn push(&mut self, name: &str) -> PopGuard<'_> {
        PopGuard::push(self, name)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
    pub unsafe fn pop(&mut self) {
        let Self {
            ref mut inner,
            last_len,
        } = *self;

        if inner.len() > last_len {
            inner.truncate(last_len);
        } else {
            self.inner.truncate({
                let parent = self.parent();
                let parent = unsafe { parent.unwrap_unchecked() };
                parent.as_os_str().len()
            });
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
    pub unsafe fn set_file_name(&mut self, name: &str) {
        unsafe {
            self.pop();
        }
        self.push(name);
    }

    #[cfg(all(unix, not(miri)))]
    pub fn to_cstr_mut(&mut self) -> unix::CStrFastPathBufGuard<'_> {
        unix::CStrFastPathBufGuard::new(self)
    }
}

impl From<PathBuf> for FastPathBuf {
    fn from(p: PathBuf) -> Self {
        let inner = p.into_os_string().into_encoded_bytes();
        Self {
            last_len: inner.len(),
            inner,
        }
    }
}

impl Default for FastPathBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for FastPathBuf {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        let Self {
            ref inner,
            last_len: _,
        } = *self;

        unsafe { OsStr::from_encoded_bytes_unchecked(inner) }.as_ref()
    }
}

impl AsRef<Path> for FastPathBuf {
    fn as_ref(&self) -> &Path {
        self
    }
}

impl fmt::Debug for FastPathBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl Clone for FastPathBuf {
    fn clone(&self) -> Self {
        let Self {
            ref inner,
            last_len,
        } = *self;

        Self {
            inner: inner.clone(),
            last_len,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        let Self {
            ref mut inner,
            ref mut last_len,
        } = *self;

        inner.clone_from(&source.inner);
        *last_len = source.last_len;
    }
}

pub struct PopGuard<'a>(&'a mut FastPathBuf);

impl<'a> PopGuard<'a> {
    fn push(path: &'a mut FastPathBuf, name: &str) -> Self {
        let FastPathBuf {
            ref mut inner,
            ref mut last_len,
        } = *path;

        *last_len = inner.len();

        // Reserve an extra byte for the eventually appended NUL terminator
        inner.reserve(1 + name.len() + 1);
        inner.push(MAIN_SEPARATOR as u8);
        inner.extend_from_slice(name.as_bytes());

        Self(path)
    }

    pub fn pop(self) {
        unsafe { self.0.pop() }
    }
}

impl Deref for PopGuard<'_> {
    type Target = FastPathBuf;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl DerefMut for PopGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl AsRef<Path> for PopGuard<'_> {
    fn as_ref(&self) -> &Path {
        self.0
    }
}

impl fmt::Debug for PopGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(all(unix, not(miri)))]
mod unix {
    use std::{ffi::CStr, ops::Deref};

    use super::FastPathBuf;

    #[must_use]
    pub struct CStrFastPathBufGuard<'a> {
        buf: &'a mut FastPathBuf,
    }

    impl CStrFastPathBufGuard<'_> {
        pub fn new(buf: &mut FastPathBuf) -> CStrFastPathBufGuard<'_> {
            let FastPathBuf {
                ref mut inner,
                last_len: _,
            } = *buf;

            inner.push(0); // NUL terminator
            CStrFastPathBufGuard { buf }
        }
    }

    impl Deref for CStrFastPathBufGuard<'_> {
        type Target = CStr;

        fn deref(&self) -> &Self::Target {
            let Self {
                buf:
                    &mut FastPathBuf {
                        ref inner,
                        last_len: _,
                    },
            } = *self;

            if cfg!(debug_assertions) {
                CStr::from_bytes_with_nul(inner).unwrap()
            } else {
                unsafe { CStr::from_bytes_with_nul_unchecked(inner) }
            }
        }
    }

    impl AsRef<CStr> for CStrFastPathBufGuard<'_> {
        fn as_ref(&self) -> &CStr {
            self
        }
    }

    impl Drop for CStrFastPathBufGuard<'_> {
        fn drop(&mut self) {
            let Self {
                buf:
                    &mut FastPathBuf {
                        ref mut inner,
                        last_len: _,
                    },
            } = *self;

            inner.pop();
        }
    }
}
