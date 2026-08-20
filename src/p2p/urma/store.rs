//! Registered-memory arenas backing published artifacts and read bounce
//! buffers.
//!
//! Both sides of a one-sided read must live inside registered regions, so the
//! transport keeps two anonymous, page-aligned mmaps registered once at
//! startup:
//!
//! - [`UbArena`] is a bump arena holding published artifacts that arrived as
//!   bytes (or path artifacts published with `P2pPublishMode::Copy`). Bump
//!   allocation never reclaims space; capacity exhaustion fails the publish,
//!   and the arena is expected to be reset by process restarts. This keeps
//!   the first iteration simple at the cost of arena pressure over a very
//!   long-lived node.
//! - [`BouncePool`] is a fixed-size block pool of short-lived destination
//!   buffers for remote reads. Blocks are exactly one `slice_size`, so free
//!   and reuse are trivial.
//!
//! Path artifacts published with `P2pPublishMode::Reference` skip the arena
//! entirely: the file is mapped read-only and registered individually.

use std::collections::HashMap;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Mutex;

/// Page-alignment applied to arena allocations so every published region
/// starts on a page boundary inside the registered arena.
const ALLOC_ALIGN: u64 = 4096;

/// An owned `mmap` mapping.
#[derive(Debug)]
pub(crate) struct Mmap {
    ptr: *mut u8,
    len: usize,
}

// Mappings are plain memory; concurrent reads/writes are coordinated by the
// transport's read lifecycle (a bounce block is only reused after its read
// completed or its completion was reaped).
unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl Mmap {
    pub(crate) fn anonymous(len: usize) -> io::Result<Self> {
        // SAFETY: mmap of anonymous private memory with a non-zero length.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: ptr as *mut u8,
            len,
        })
    }

    /// Map a file read-only and shared, returning the mapping and its length.
    pub(crate) fn file_read_only(path: &Path) -> io::Result<(Self, u64)> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cannot map empty file {}", path.display()),
            ));
        }
        // SAFETY: mmap of a valid fd with a non-zero length; the mapping is
        // read-only so no writes can fault on a full filesystem.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len as usize,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok((
            Self {
                ptr: ptr as *mut u8,
                len: len as usize,
            },
            len,
        ))
    }

    pub(crate) fn base_addr(&self) -> u64 {
        self.ptr as u64
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Copy `src.len()` bytes out of the mapping starting at `offset`.
    pub(crate) fn read_at(&self, offset: u64, len: usize) -> Vec<u8> {
        let start = offset as usize;
        assert!(
            start + len <= self.len,
            "mmap read [{start}, {}) out of bounds (len {})",
            start + len,
            self.len,
        );
        // SAFETY: range validated above; the mapping outlives the call.
        unsafe { std::slice::from_raw_parts(self.ptr.add(start), len).to_vec() }
    }

    /// Copy `data` into the mapping starting at `offset`.
    pub(crate) fn write_at(&self, offset: u64, data: &[u8]) {
        let start = offset as usize;
        assert!(
            start + data.len() <= self.len,
            "mmap write [{start}, {}) out of bounds (len {})",
            start + data.len(),
            self.len,
        );
        // SAFETY: range validated above; the mapping is writable (anonymous
        // mappings are created with PROT_WRITE).
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(start), data.len());
        }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: the mapping was created by us and is dropped exactly once.
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

fn align_up(value: u64, align: u64) -> u64 {
    value.div_ceil(align) * align
}

/// Bump allocator over the registered publish arena.
#[derive(Debug)]
pub(crate) struct UbArena {
    map: Mmap,
    cursor: Mutex<u64>,
    /// Live allocations (offset → size), retained for unpublish bookkeeping.
    live: Mutex<HashMap<u64, u64>>,
}

impl UbArena {
    pub(crate) fn new(size: u64) -> io::Result<Self> {
        let map = Mmap::anonymous(size as usize)?;
        Ok(Self {
            map,
            cursor: Mutex::new(0),
            live: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn map(&self) -> &Mmap {
        &self.map
    }

    /// Allocate `len` bytes, returning the arena offset (page-aligned).
    pub(crate) fn alloc(&self, len: u64) -> Option<u64> {
        let aligned = align_up(len, ALLOC_ALIGN);
        let mut cursor = self.cursor.lock().expect("ub arena cursor lock");
        let offset = *cursor;
        let end = offset.checked_add(aligned)?;
        if end > self.map.len() as u64 {
            return None;
        }
        *cursor = end;
        self.live
            .lock()
            .expect("ub arena live lock")
            .insert(offset, aligned);
        Some(offset)
    }

    /// Release a previous allocation. The bump arena does not reclaim the
    /// space; this only updates bookkeeping.
    pub(crate) fn free(&self, offset: u64) {
        self.live
            .lock()
            .expect("ub arena live lock")
            .remove(&offset);
    }

    pub(crate) fn write(&self, offset: u64, data: &[u8]) {
        self.map.write_at(offset, data);
    }
}

/// Fixed-size block pool of registered read-destination buffers.
#[derive(Debug)]
pub(crate) struct BouncePool {
    map: Mmap,
    block_size: u64,
    free_blocks: Mutex<Vec<u64>>,
}

impl BouncePool {
    pub(crate) fn new(block_size: u64, blocks: u32) -> io::Result<Self> {
        let total = block_size
            .checked_mul(blocks as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bounce pool overflow"))?;
        let map = Mmap::anonymous(total as usize)?;
        let free_blocks = (0..blocks as u64).map(|i| i * block_size).rev().collect();
        Ok(Self {
            map,
            block_size,
            free_blocks: Mutex::new(free_blocks),
        })
    }

    pub(crate) fn map(&self) -> &Mmap {
        &self.map
    }

    /// Take one block, returning its pool offset.
    pub(crate) fn alloc(&self) -> Option<u64> {
        self.free_blocks.lock().expect("ub bounce pool lock").pop()
    }

    pub(crate) fn free(&self, offset: u64) {
        debug_assert_eq!(offset % self.block_size, 0);
        self.free_blocks
            .lock()
            .expect("ub bounce pool lock")
            .push(offset);
    }

    pub(crate) fn block_addr(&self, offset: u64) -> u64 {
        self.map.base_addr() + offset
    }

    /// Copy `len` bytes out of a block into a fresh Vec.
    pub(crate) fn read_block(&self, offset: u64, len: usize) -> Vec<u8> {
        self.map.read_at(offset, len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_bump_allocates_aligned_and_fails_when_full() {
        let arena = UbArena::new(16 * 1024).expect("arena");
        let first = arena.alloc(100).expect("first alloc");
        assert_eq!(first % ALLOC_ALIGN, 0);
        let second = arena.alloc(4096).expect("second alloc");
        assert_eq!(second, 4096);
        // 16 KiB arena: 4 KiB + 4 KiB used, another 16 KiB must fail.
        assert!(arena.alloc(16 * 1024).is_none());
    }

    #[test]
    fn arena_write_then_read_back() {
        let arena = UbArena::new(64 * 1024).expect("arena");
        let offset = arena.alloc(10).expect("alloc");
        arena.write(offset, b"hello-ub!!");
        assert_eq!(arena.map().read_at(offset, 10), b"hello-ub!!");
        arena.free(offset);
    }

    #[test]
    fn bounce_pool_recycles_blocks() {
        let pool = BouncePool::new(4096, 2).expect("pool");
        let a = pool.alloc().expect("block a");
        let b = pool.alloc().expect("block b");
        assert!(pool.alloc().is_none(), "pool of 2 must be exhausted");
        pool.free(a);
        let c = pool.alloc().expect("block c after free");
        assert_eq!(c, a);
        pool.free(b);
        pool.free(c);
    }

    #[test]
    fn file_mapping_reads_file_contents() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("blob.bin");
        std::fs::write(&path, b"artifact-bytes").expect("write");
        let (map, len) = Mmap::file_read_only(&path).expect("map");
        assert_eq!(len, 14);
        assert_eq!(map.read_at(0, 14), b"artifact-bytes");
    }
}
