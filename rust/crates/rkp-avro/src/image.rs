//! The bytes a container reads from, and where they live.
//!
//! A container opened from a path maps the file instead of reading it, so
//! residency is the pages actually touched rather than the whole file: an
//! append reaches the header and nothing else, and an indexed read reaches the
//! block framing plus the one block it decodes.
//!
//! Blocks framed after the map accumulate in a separate tail, so appending to
//! a file never copies the file being appended to.  Nothing in the format
//! spans that seam — a block is written whole — so every read a container
//! makes lands inside one region or the other.

use std::fs::File;
use std::io;
use std::path::Path;

use memmap2::Mmap;

/// One container image: an owned buffer, or a mapped file plus its tail.
#[derive(Debug)]
pub enum Image {
    /// Bytes this container owns outright.
    Owned(Vec<u8>),
    /// A mapped file, plus whatever has been framed since it was mapped.
    Mapped {
        /// The file as mapped when the container was opened.
        map: Mmap,
        /// Blocks framed after the map, in image order.
        tail: Vec<u8>,
    },
}

impl Image {
    /// Map a file read-only.
    ///
    /// # Safety of mapping
    ///
    /// A mapped file that another process truncates or rewrites under us can
    /// fault on access.  The same is true of every mmap-based reader; a
    /// container is a single-writer format, and callers that cannot guarantee
    /// that should read the bytes and use [`Image::Owned`].
    pub fn map(path: &Path) -> io::Result<Image> {
        let file = File::open(path)?;
        if file.metadata()?.is_dir() {
            // Opening a directory succeeds on Unix and only mapping it fails,
            // with an errno that says nothing useful; hosts expect the same
            // "is a directory" they got when this read the file instead.
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("{} is a directory", path.display()),
            ));
        }
        // SAFETY: as documented above, the caller owns the guarantee that the
        // file is not concurrently truncated.
        let map = unsafe { Mmap::map(&file)? };
        Ok(Image::Mapped {
            map,
            tail: Vec::new(),
        })
    }

    /// Return the total number of bytes, tail included.
    pub fn len(&self) -> usize {
        match self {
            Image::Owned(data) => data.len(),
            Image::Mapped { map, tail } => map.len() + tail.len(),
        }
    }

    /// Return whether the image holds no bytes at all.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the offset at which the tail begins.
    ///
    /// For an owned image this is its length: there is no seam.
    pub fn seam(&self) -> usize {
        match self {
            Image::Owned(data) => data.len(),
            Image::Mapped { map, .. } => map.len(),
        }
    }

    /// Return whether the whole image is addressable as one slice.
    pub fn is_contiguous(&self) -> bool {
        match self {
            Image::Owned(_) => true,
            Image::Mapped { tail, .. } => tail.is_empty(),
        }
    }

    /// Return one range of bytes.
    ///
    /// The range must lie wholly within the mapped region or wholly within the
    /// tail.  Every read a container makes satisfies that, because a block is
    /// framed as a unit and never straddles the seam.
    pub fn slice(&self, start: usize, end: usize) -> &[u8] {
        match self {
            Image::Owned(data) => &data[start..end],
            Image::Mapped { map, tail } => {
                let seam = map.len();
                if end <= seam {
                    &map[start..end]
                } else {
                    debug_assert!(
                        start >= seam,
                        "container read spans the mapped/tail seam: {start}..{end}"
                    );
                    &tail[start - seam..end - seam]
                }
            }
        }
    }

    /// Copy one range onto the end of `out`, crossing the seam if it must.
    pub fn copy_range(&self, start: usize, end: usize, out: &mut Vec<u8>) {
        match self {
            Image::Owned(data) => out.extend_from_slice(&data[start..end]),
            Image::Mapped { map, tail } => {
                let seam = map.len();
                if end <= seam {
                    out.extend_from_slice(&map[start..end]);
                } else if start >= seam {
                    out.extend_from_slice(&tail[start - seam..end - seam]);
                } else {
                    out.extend_from_slice(&map[start..seam]);
                    out.extend_from_slice(&tail[..end - seam]);
                }
            }
        }
    }

    /// Append bytes to the image.
    pub fn extend(&mut self, bytes: &[u8]) {
        match self {
            Image::Owned(data) => data.extend_from_slice(bytes),
            Image::Mapped { tail, .. } => tail.extend_from_slice(bytes),
        }
    }

    /// Return the bytes framed after the given durable length.
    ///
    /// This is what lets a host append to a file without ever materializing
    /// the file it is appending to.
    pub fn tail_from(&self, persisted: usize) -> Option<&[u8]> {
        match self {
            Image::Owned(data) => data.get(persisted..),
            Image::Mapped { map, tail } => {
                let seam = map.len();
                if persisted >= seam {
                    tail.get(persisted - seam..)
                } else if tail.is_empty() {
                    map.get(persisted..)
                } else {
                    // The requested range straddles the seam, so no single
                    // slice describes it; the caller falls back to `bytes()`.
                    None
                }
            }
        }
    }

    /// Return the whole image as one slice, collapsing the tail if needed.
    pub fn bytes(&mut self) -> &[u8] {
        if !self.is_contiguous() {
            let mut owned = Vec::with_capacity(self.len());
            let len = self.len();
            self.copy_range(0, len, &mut owned);
            *self = Image::Owned(owned);
        }
        match self {
            Image::Owned(data) => data,
            Image::Mapped { map, .. } => map,
        }
    }

    /// Return the whole image as one slice when it already is one.
    pub fn contiguous(&self) -> Option<&[u8]> {
        match self {
            Image::Owned(data) => Some(data),
            Image::Mapped { map, tail } if tail.is_empty() => Some(map),
            Image::Mapped { .. } => None,
        }
    }

    /// Copy a mapped image into owned memory, releasing the file.
    pub fn detach(&mut self) {
        if let Image::Mapped { .. } = self {
            let len = self.len();
            let mut owned = Vec::with_capacity(len);
            self.copy_range(0, len, &mut owned);
            *self = Image::Owned(owned);
        }
    }

    /// Return how many bytes are resident rather than mapped.
    ///
    /// A mapped region costs address space, not memory, so a container over a
    /// mapped file reports only what it actually allocated.
    pub fn resident(&self) -> usize {
        match self {
            Image::Owned(data) => data.len(),
            Image::Mapped { tail, .. } => tail.len(),
        }
    }
}

impl From<Vec<u8>> for Image {
    fn from(data: Vec<u8>) -> Image {
        Image::Owned(data)
    }
}
