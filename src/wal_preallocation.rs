//! Best-effort physical allocation without extending a WAL's logical length.
use std::fs::File;

// Bound unused reserved space per WAL; allocate only when a write needs it.
#[cfg(target_os = "linux")]
const CHUNK: u64 = 256 * 1024;

pub(crate) struct Preallocation {
    #[cfg(target_os = "linux")]
    end: u64,
    #[cfg(target_os = "linux")]
    disabled: bool,
}

impl Preallocation {
    pub fn new(_existing_len: u64) -> Self {
        Self {
            #[cfg(target_os = "linux")]
            end: _existing_len,
            #[cfg(target_os = "linux")]
            disabled: false,
        }
    }

    pub fn reserve(&mut self, _file: &File, _needed: u64) {
        #[cfg(target_os = "linux")]
        self.reserve_with(_needed, |offset, len| {
            rustix::fs::fallocate(_file, rustix::fs::FallocateFlags::KEEP_SIZE, offset, len)
                .map_err(std::io::Error::from)
        });
    }

    #[cfg(target_os = "linux")]
    fn reserve_with(
        &mut self,
        needed: u64,
        allocate: impl FnOnce(u64, u64) -> std::io::Result<()>,
    ) {
        if self.disabled || needed <= self.end {
            return;
        }
        let Some(end) = needed
            .checked_add(CHUNK - 1)
            .map(|n| n / CHUNK * CHUNK)
            .filter(|&n| n <= i64::MAX as u64)
        else {
            return;
        };
        if allocate(self.end, end - self.end).is_ok() {
            self.end = end;
        } else {
            // Unsupported filesystems and scarce free space must still permit
            // ordinary writes. Let write_all/sync_all report actual write errors;
            // do not repeat a failed optional reservation on every commit.
            self.disabled = true;
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn allocation_is_bounded_and_only_crossings_call_the_os() {
        let mut allocation = Preallocation::new(8);
        allocation.reserve_with(9, |offset, len| {
            assert_eq!((offset, len), (8, CHUNK - 8));
            Ok(())
        });
        allocation.reserve_with(CHUNK, |_, _| panic!("already reserved"));
        allocation.reserve_with(CHUNK + 1, |offset, len| {
            assert_eq!((offset, len), (CHUNK, CHUNK));
            Ok(())
        });
        allocation.reserve_with(10 * CHUNK + 1, |offset, len| {
            assert_eq!((offset, len), (2 * CHUNK, 9 * CHUNK));
            Ok(())
        });
        allocation.reserve_with(u64::MAX, |_, _| panic!("overflow"));
        assert_eq!(allocation.end, 11 * CHUNK);
    }

    #[test]
    fn failed_optional_reservation_is_not_retried() {
        for errno in [28, 95, 5] {
            // ENOSPC, EOPNOTSUPP, EIO on Linux
            let mut allocation = Preallocation::new(8);
            allocation.reserve_with(9, |_, _| Err(io::Error::from_raw_os_error(errno)));
            assert_eq!(allocation.end, 8);
            allocation.reserve_with(CHUNK + 1, |_, _| panic!("retry"));
        }
    }

    #[test]
    fn physical_reservation_preserves_contents_length_and_cursor() {
        use std::io::{Read, Seek, Write};
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"RKVWAL01").unwrap();
        // Exercise the real syscall. Only unsupported filesystems may skip it;
        // permission, I/O, and space errors are test failures here.
        match rustix::fs::fallocate(&file, rustix::fs::FallocateFlags::KEEP_SIZE, 8, CHUNK - 8) {
            Ok(()) => {}
            Err(rustix::io::Errno::OPNOTSUPP | rustix::io::Errno::NOSYS) => return,
            Err(error) => panic!("fallocate: {error}"),
        }
        assert_eq!(file.metadata().unwrap().len(), 8);
        assert_eq!(file.stream_position().unwrap(), 8);
        file.write_all(b"tail").unwrap();
        file.sync_all().unwrap();
        file.rewind().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"RKVWAL01tail");
    }
}
