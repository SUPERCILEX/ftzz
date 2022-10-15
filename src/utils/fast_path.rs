use std::{
    ffi::OsStr,
    fmt,
    ops::Deref,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf, MAIN_SEPARATOR},
};

/// A specialized [`PathBuf`][std::path::PathBuf] implementation that takes
/// advantage of a few assumptions. Specifically, it *only* supports adding
/// single-level directories (e.g. "foo", "foo/bar" is not allowed) and updating
/// the current file name. Any other usage is UB.
pub struct FastPathBuf {
    inner: Vec<u8>,
    last_len: usize,
}

impl FastPathBuf {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
            last_len: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn push(&mut self, name: &str) {
        self.last_len = self.inner.len();

        self.inner.reserve(name.len() + 1);
        self.inner.push(MAIN_SEPARATOR as u8);
        self.inner.extend_from_slice(name.as_bytes());
    }

    pub fn pop(&mut self) {
        if self.inner.len() > self.last_len {
            self.inner.truncate(self.last_len);
        } else {
            self.inner.truncate(
                unsafe { self.parent().unwrap_unchecked() }
                    .as_os_str()
                    .len(),
            );
        }
    }

    pub fn set_file_name(&mut self, name: &str) {
        self.pop();
        self.push(name);
    }

    #[cfg(all(unix, not(miri)))]
    pub fn to_cstr_mut(&mut self) -> unix::CStrFastPathBufGuard {
        unix::CStrFastPathBufGuard::new(self)
    }
}

impl From<PathBuf> for FastPathBuf {
    fn from(p: PathBuf) -> Self {
        let inner = p.into_os_string().into_vec();
        Self {
            last_len: inner.len(),
            inner,
        }
    }
}

impl Deref for FastPathBuf {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        OsStr::from_bytes(&self.inner).as_ref()
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
        Self {
            inner: self.inner.clone(),
            last_len: self.last_len,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.inner.clone_from(&source.inner);
        self.last_len = source.last_len;
    }
}

#[cfg(all(unix, not(miri)))]
mod unix {
    use std::{ffi::CStr, ops::Deref};

    use super::FastPathBuf;

    pub struct CStrFastPathBufGuard<'a> {
        buf: &'a mut FastPathBuf,
    }

    impl<'a> CStrFastPathBufGuard<'a> {
        pub fn new(buf: &mut FastPathBuf) -> CStrFastPathBufGuard {
            buf.inner.push(0); // NUL terminator
            CStrFastPathBufGuard { buf }
        }
    }

    impl<'a> Deref for CStrFastPathBufGuard<'a> {
        type Target = CStr;

        fn deref(&self) -> &Self::Target {
            if cfg!(debug_assertions) {
                CStr::from_bytes_with_nul(&self.buf.inner).unwrap()
            } else {
                unsafe { CStr::from_bytes_with_nul_unchecked(&self.buf.inner) }
            }
        }
    }

    impl<'a> AsRef<CStr> for CStrFastPathBufGuard<'a> {
        fn as_ref(&self) -> &CStr {
            self
        }
    }

    impl<'a> Drop for CStrFastPathBufGuard<'a> {
        fn drop(&mut self) {
            self.buf.inner.pop();
        }
    }
}
