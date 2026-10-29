//! Identifier that changes on every host boot, for detecting loss of the
//! page cache.

use super::SysResult;

/// Return a stable identifier for the current boot of the host: Linux
/// `/proc/sys/kernel/random/boot_id`, macOS `kern.boottime`.
///
/// It is constant within one boot and changes on every boot, which is exactly
/// the signal needed to decide whether in-memory kernel state — the page
/// cache — could have survived since some persisted data was written: a
/// different id than the one recorded earlier proves the host rebooted and
/// the page cache died with it. Callers treat failure (unsupported platform,
/// unreadable source) as "rebooted", so every failure direction degrades to
/// discarding the persisted state rather than trusting it.
pub fn boot_id() -> SysResult<String> {
    imp::boot_id()
}

#[cfg(target_os = "linux")]
mod imp {
    use std::fs;

    use nix::errno::Errno;

    use super::super::{SysError, SysResult};

    pub(super) fn boot_id() -> SysResult<String> {
        let raw = fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(|err| {
            SysError::Errno(Errno::from_raw(err.raw_os_error().unwrap_or(libc::EIO)))
        })?;
        Ok(raw.trim().to_string())
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::ffi::CString;

    use super::super::{SysError, SysResult};

    pub(super) fn boot_id() -> SysResult<String> {
        let name = CString::new("kern.boottime").expect("sysctl name has no NUL");
        let mut tv: libc::timeval = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::timeval>();
        // SAFETY: `name` is NUL-terminated and the output buffer is sized for
        // exactly one `timeval`.
        if unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                std::ptr::addr_of_mut!(tv).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        } != 0
        {
            return Err(SysError::last());
        }
        Ok(format!("{}.{}", tv.tv_sec, tv.tv_usec))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use super::super::{SysError, SysResult};

    pub(super) fn boot_id() -> SysResult<String> {
        Err(SysError::Unsupported("boot_id"))
    }
}
