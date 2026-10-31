//! On-disk format for `mem.upper`, the private per-sandbox variable-length
//! packed memory layer.
//!
//! Layout (design §4.1):
//!
//! ```text
//! [file header 4K]  magic / version / C / mem_spec / epoch / flags (+crc32c)
//! [metadata area]   8B per 16KiB block, page-granular, count = mem_spec / C
//! [data area]       512B-granular variable-length payloads, sparse
//! ```
//!
//! The 8B entry is the authoritative incremental index: `len == 0` means the
//! block is absent and the lower stack (template/legacy layers) is the
//! authority for its content.

use anyhow::{ensure, Context, Result};

/// File magic: `"MUP1"` little-endian.
pub const MEM_UPPER_MAGIC: u32 = u32::from_le_bytes(*b"MUP1");
pub const MEM_UPPER_VERSION: u32 = 1;

/// Guest RAM block size `C` (bytes). Fixed for the format in this version;
/// recorded in the header so future reseals can vary it.
pub const BLOCK_SIZE: u32 = 16 * 1024;
/// Sectors (512B) covered by one block.
pub const BLOCK_SECTORS: u32 = BLOCK_SIZE / 512;
/// Data-area allocation granularity.
pub const SECTOR_SIZE: u64 = 512;

/// Payload encodings stored in the 2-bit `enc` field.
pub const ENC_LZ4: u8 = 0;
pub const ENC_RAW: u8 = 1;

const HEADER_PAYLOAD_LEN: usize = 64;
const HEADER_CRC_OFFSET: usize = 40;
const HEADER_RESERVED_OFFSET: usize = 44;

/// File header, serialized into the first 4K of the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemUpperHeader {
    pub magic: u32,
    pub version: u32,
    /// Block size `C` in bytes.
    pub block_size: u32,
    pub flags: u32,
    /// Guest RAM size in bytes (`mem_spec`).
    pub mem_spec: u64,
    /// Monotonic harvest generation; the paused-record epoch must match.
    pub epoch: u64,
    /// Byte offset where the data area starts.
    pub data_start: u64,
}

impl MemUpperHeader {
    pub fn new(mem_spec: u64, epoch: u64) -> Self {
        let metadata_pages = metadata_pages_for(mem_spec);
        Self {
            magic: MEM_UPPER_MAGIC,
            version: MEM_UPPER_VERSION,
            block_size: BLOCK_SIZE,
            flags: 0,
            mem_spec,
            epoch,
            data_start: 4096 + metadata_pages * 4096,
        }
    }

    /// Number of metadata entries (one per block).
    pub fn block_count(&self) -> u64 {
        block_count_for(self.mem_spec)
    }

    pub fn encode_into(&self, buf: &mut [u8]) {
        buf[..HEADER_PAYLOAD_LEN].fill(0);
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.block_size.to_le_bytes());
        buf[12..16].copy_from_slice(&self.flags.to_le_bytes());
        buf[16..24].copy_from_slice(&self.mem_spec.to_le_bytes());
        buf[24..32].copy_from_slice(&self.epoch.to_le_bytes());
        buf[32..40].copy_from_slice(&self.data_start.to_le_bytes());
        let crc = crc32c::crc32c(&buf[..HEADER_CRC_OFFSET]);
        buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        buf[HEADER_RESERVED_OFFSET..HEADER_PAYLOAD_LEN].fill(0);
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        ensure!(
            buf.len() >= HEADER_PAYLOAD_LEN,
            "mem.upper header too short"
        );
        let header = Self {
            magic: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            version: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            block_size: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            flags: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            mem_spec: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            epoch: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            data_start: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
        };
        let crc = u32::from_le_bytes(
            buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        ensure!(
            crc == crc32c::crc32c(&buf[..HEADER_CRC_OFFSET]),
            "mem.upper header crc mismatch"
        );
        ensure!(
            header.magic == MEM_UPPER_MAGIC,
            "not a mem.upper file (magic {:#x})",
            header.magic
        );
        ensure!(
            header.version == MEM_UPPER_VERSION,
            "unsupported mem.upper version {}",
            header.version
        );
        ensure!(
            header.block_size == BLOCK_SIZE,
            "unsupported mem.upper block size {}",
            header.block_size
        );
        ensure!(
            header.mem_spec.is_multiple_of(u64::from(BLOCK_SIZE)),
            "mem_spec {} not block aligned",
            header.mem_spec
        );
        let expected_data_start = 4096 + metadata_pages_for(header.mem_spec) * 4096;
        ensure!(
            header.data_start == expected_data_start,
            "mem.upper data_start {} mismatch (expected {expected_data_start})",
            header.data_start
        );
        Ok(header)
    }
}

pub fn block_count_for(mem_spec: u64) -> u64 {
    mem_spec / u64::from(BLOCK_SIZE)
}

pub fn metadata_pages_for(mem_spec: u64) -> u64 {
    (block_count_for(mem_spec) * 8).div_ceil(4096)
}

/// One metadata entry (8B, little-endian packed bitfield):
///
/// ```text
/// bits 63..62  enc  (2 bits, ENC_*)
/// bits 61..47  len  (15 bits, payload bytes; 0 = block absent)
/// bits 46..16  off  (31 bits, data-area offset in 512B sectors)
/// bits 15..0   crc16 (over the first `len` payload bytes)
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockEntry {
    pub enc: u8,
    pub len: u16,
    pub off: u32,
    pub crc: u16,
}

impl BlockEntry {
    pub const ABSENT: BlockEntry = BlockEntry {
        enc: ENC_LZ4,
        len: 0,
        off: 0,
        crc: 0,
    };

    pub fn present(&self) -> bool {
        self.len != 0
    }

    /// Extent size in 512B sectors occupied by this payload.
    pub fn extent_sectors(&self) -> u32 {
        u32::from(self.len).div_ceil(512)
    }

    pub fn encode(&self) -> u64 {
        let enc = u64::from(self.enc & 0x3);
        let len = u64::from(self.len & 0x7FFF);
        let off = u64::from(self.off & 0x7FFF_FFFF);
        let crc = u64::from(self.crc);
        (enc << 62) | (len << 47) | (off << 16) | crc
    }

    pub fn decode(v: u64) -> Self {
        Self {
            enc: ((v >> 62) & 0x3) as u8,
            len: ((v >> 47) & 0x7FFF) as u16,
            off: ((v >> 16) & 0x7FFF_FFFF) as u32,
            crc: (v & 0xFFFF) as u16,
        }
    }
}

/// CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF) over the payload bytes.
/// Cross-checked against `len`/`enc` to fail loud on torn metadata.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for &byte in data {
        crc = (crc << 8) ^ CRC16_TABLE[usize::from((crc >> 8) ^ u16::from(byte))];
    }
    crc
}

const CRC16_TABLE: [u16; 256] = build_crc16_table();

const fn build_crc16_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Reads exactly `buf.len()` bytes at `offset` from a positional reader.
pub(crate) fn pread_exact(
    file: &std::fs::File,
    offset: u64,
    buf: &mut [u8],
    what: &str,
) -> Result<()> {
    use std::os::unix::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file
            .read_at(&mut buf[done..], offset + done as u64)
            .with_context(|| format!("read mem.upper {what} at {offset}"))?;
        ensure!(n > 0, "short read of mem.upper {what} at {offset}");
        done += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_bitfield_roundtrip() {
        let entry = BlockEntry {
            enc: ENC_RAW,
            len: 16384,
            off: 0x7FFF_FFFF,
            crc: 0xABCD,
        };
        assert_eq!(BlockEntry::decode(entry.encode()), entry);
        let entry = BlockEntry {
            enc: ENC_LZ4,
            len: 1,
            off: 1,
            crc: 1,
        };
        assert_eq!(BlockEntry::decode(entry.encode()), entry);
    }

    #[test]
    fn absent_entry_is_all_zero_word() {
        assert_eq!(BlockEntry::ABSENT.encode(), 0);
        assert!(!BlockEntry::decode(0).present());
        assert!(BlockEntry::decode(u64::MAX).present());
    }

    #[test]
    fn max_off_reaches_1tb_data_area() {
        let entry = BlockEntry {
            enc: ENC_RAW,
            len: 16384,
            off: 0x7FFF_FFFF,
            crc: 0,
        };
        assert_eq!(u64::from(entry.off) * SECTOR_SIZE, (1u64 << 40) - 512);
        assert_eq!(entry.extent_sectors(), 32);
        // A raw block is exactly one full block.
        assert_eq!(16384usize, BLOCK_SIZE as usize);
    }

    #[test]
    fn header_roundtrip_and_rejections() {
        let mut buf = [0u8; 4096];
        let header = MemUpperHeader::new(2 * 1024 * 1024 * 1024, 7);
        // 2GiB / 16KiB = 131072 blocks * 8B = 1MiB metadata = 256 pages.
        assert_eq!(metadata_pages_for(2 * 1024 * 1024 * 1024), 256);
        header.encode_into(&mut buf);
        assert_eq!(MemUpperHeader::decode(&buf).unwrap(), header);

        let mut corrupted = buf;
        corrupted[24] ^= 0xFF; // tamper epoch -> crc mismatch
        assert!(MemUpperHeader::decode(&corrupted).is_err());

        let mut bad_magic = buf;
        bad_magic[0] ^= 0xFF;
        // Re-stamp the crc so only the magic check fires.
        let crc = crc32c::crc32c(&bad_magic[..HEADER_CRC_OFFSET]);
        bad_magic[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        assert!(MemUpperHeader::decode(&bad_magic).is_err());
    }

    #[test]
    fn crc16_matches_known_vectors() {
        // CRC-16/CCITT-FALSE("123456789") = 0x29B1.
        assert_eq!(crc16(b"123456789"), 0x29B1);
        assert_eq!(crc16(b""), 0xFFFF);
    }
}
