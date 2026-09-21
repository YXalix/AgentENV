//! Process-global, size-classed byte buffer pool.
//!
//! The server allocator is jemalloc with `dirty_decay_ms:1000` and a
//! background purge thread (`src/bin/server.rs`): freed buffers are purged
//! (`MADV_DONTNEED`) about a second later, and calloc serving those extents
//! skips the memset, so a fresh `vec![0u8; n]` can arrive as lazily-zeroed
//! pages that fault on first touch. When the first touch is the kernel
//! filling the buffer inside `process_vm_readv`, the per-page fault cost is
//! charged to that syscall. Holding buffers in this pool keeps their pages
//! permanently mapped: steady-state reuse has zero page faults and no
//! repeated zeroing.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

/// Retention limit per size class. 64 covers the largest in-flight usage
/// (the uffd-core snapshot path compacts with concurrency 64).
const MAX_RETAINED_BUFFERS_PER_CLASS: usize = 64;
/// Secondary retention cap so large size classes cannot linger unboundedly
/// (e.g. an 8 MiB class retains at most 8 buffers).
const MAX_RETAINED_BYTES_PER_CLASS: usize = 64 * 1024 * 1024;

type Pool = HashMap<usize, Vec<Vec<u8>>>;

fn pool() -> &'static Mutex<Pool> {
    static POOL: OnceLock<Mutex<Pool>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_pool() -> MutexGuard<'static, Pool> {
    pool().lock().unwrap_or_else(|err| err.into_inner())
}

/// A byte buffer leased from the global pool, returned to it on drop.
///
/// **Contents on checkout are unspecified**: reused buffers contain stale
/// bytes from their previous user and are not re-zeroed. Callers must fill
/// (or otherwise bound reads to) exactly the region they later consume. All
/// current consumers do this: `compact_to` fills `[0, total_len)` before
/// writing `total_len` bytes, `CompactWriter::write_all_at` copies then
/// writes the same number of bytes, and the zfile merged-batch pread
/// `read_exact`s the whole `[0, batch_len)` span before slicing it.
///
/// The length is fixed at construction; only slice access is exposed.
pub struct PooledBuffer {
    inner: Option<Vec<u8>>,
}

impl PooledBuffer {
    /// Lease a buffer of exactly `size` bytes, reusing a pooled buffer of
    /// the same size class when available.
    pub fn new(size: usize) -> Self {
        let inner = lock_pool()
            .get_mut(&size)
            .and_then(Vec::pop)
            .unwrap_or_else(|| vec![0u8; size]);
        Self { inner: Some(inner) }
    }
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        self.inner
            .as_deref()
            .expect("PooledBuffer inner is always Some before drop")
    }
}

impl AsMut<[u8]> for PooledBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.inner
            .as_deref_mut()
            .expect("PooledBuffer inner is always Some before drop")
    }
}

impl std::fmt::Debug for PooledBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledBuffer")
            .field("len", &self.as_ref().len())
            .finish()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        let Some(buf) = self.inner.take() else {
            return;
        };
        let size = buf.len();
        let mut pool = lock_pool();
        let class = pool.entry(size).or_default();
        if class.len() < MAX_RETAINED_BUFFERS_PER_CLASS
            && class.len() * size < MAX_RETAINED_BYTES_PER_CLASS
        {
            class.push(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each test uses its own size class: the pool is process-global and tests
    // run in parallel threads, so sharing a class would race.

    #[test]
    fn fresh_allocation_is_zeroed() {
        let buf = PooledBuffer::new(100_003);
        assert_eq!(buf.as_ref().len(), 100_003);
        assert!(buf.as_ref().iter().all(|&b| b == 0));
    }

    #[test]
    fn checkout_reuses_returned_buffer() {
        let size = 200_005;
        let ptr = PooledBuffer::new(size).as_ref().as_ptr();
        // Drop of the temporary above returns it to the pool; the next lease
        // of the same class must reuse that allocation.
        let buf = PooledBuffer::new(size);
        assert_eq!(buf.as_ref().as_ptr(), ptr);
    }

    #[test]
    fn reused_buffers_are_not_rezeroed() {
        let size = 300_007;
        {
            let mut buf = PooledBuffer::new(size);
            buf.as_mut().fill(0xAB);
        }
        let buf = PooledBuffer::new(size);
        assert!(buf.as_ref().iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn size_classes_do_not_mix() {
        let small = PooledBuffer::new(400_011);
        let large = PooledBuffer::new(500_013);
        assert_eq!(small.as_ref().len(), 400_011);
        assert_eq!(large.as_ref().len(), 500_013);
    }

    #[test]
    fn retention_is_capped_by_count() {
        let size = 600_017;
        let buffers: Vec<PooledBuffer> = (0..MAX_RETAINED_BUFFERS_PER_CLASS * 2)
            .map(|_| PooledBuffer::new(size))
            .collect();
        drop(buffers);
        let retained = lock_pool().get(&size).map(Vec::len).unwrap_or(0);
        assert_eq!(retained, MAX_RETAINED_BUFFERS_PER_CLASS);
    }

    #[test]
    fn retention_is_capped_by_bytes() {
        let size = 8 * 1024 * 1024;
        let buffers: Vec<PooledBuffer> = (0..16).map(|_| PooledBuffer::new(size)).collect();
        drop(buffers);
        let retained = lock_pool().get(&size).map(Vec::len).unwrap_or(0);
        assert_eq!(
            retained,
            MAX_RETAINED_BYTES_PER_CLASS / size,
            "byte cap must bound the 8 MiB class"
        );
    }
}
