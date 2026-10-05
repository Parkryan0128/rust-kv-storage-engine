use crate::{error::corrupt, memtable::Record, Result};
use bytes::Bytes;
use std::io::{Read, Write};

pub(crate) const MAX_FRAME: usize = 64 * 1024 * 1024;
pub(crate) const MAX_RECORD: usize = 32 * 1024 * 1024;
pub(crate) const HEADER: usize = 12;
pub(crate) fn put_u32(b: &mut Vec<u8>, n: u32) {
    b.extend(n.to_le_bytes());
}
pub(crate) fn put_u64(b: &mut Vec<u8>, n: u64) {
    b.extend(n.to_le_bytes());
}
pub(crate) struct Cursor<'a> {
    pub b: &'a [u8],
}
impl<'a> Cursor<'a> {
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.b.len() {
            return Err(corrupt("truncated payload"));
        }
        let (a, b) = self.b.split_at(n);
        self.b = b;
        Ok(a)
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn done(&self) -> Result<()> {
        if self.b.is_empty() {
            Ok(())
        } else {
            Err(corrupt("trailing payload bytes"))
        }
    }
}
pub(crate) fn encode_record(key: &[u8], r: &Record, out: &mut Vec<u8>) {
    put_u64(out, r.seq);
    out.push(u8::from(r.value.is_some()));
    put_u32(out, key.len() as u32);
    put_u32(out, r.value.as_ref().map_or(0, Bytes::len) as u32);
    out.extend(key);
    if let Some(v) = &r.value {
        out.extend(v);
    }
}
pub(crate) fn decode_record(c: &mut Cursor<'_>) -> Result<(Vec<u8>, Record)> {
    let r = decode_record_ref(c)?;
    Ok((r.key.to_vec(), r.to_owned()))
}
pub(crate) struct RecordRef<'a> {
    pub key: &'a [u8],
    pub seq: u64,
    pub value: Option<&'a [u8]>,
}
impl RecordRef<'_> {
    pub fn to_owned(&self) -> Record {
        Record {
            seq: self.seq,
            value: self.value.map(Bytes::copy_from_slice),
        }
    }
}
#[inline]
pub(crate) fn decode_record_ref<'a>(c: &mut Cursor<'a>) -> Result<RecordRef<'a>> {
    // Validate each contiguous region once, retaining all header/body checks.
    let header = c.take(17)?;
    let seq = u64::from_le_bytes(header[..8].try_into().unwrap());
    let tag = header[8];
    let kl = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
    let vl = u32::from_le_bytes(header[13..17].try_into().unwrap()) as usize;
    if seq == 0
        || tag > 1
        || (tag == 0 && vl != 0)
        || kl.saturating_add(vl).saturating_add(17) > MAX_RECORD
    {
        return Err(corrupt("invalid record header"));
    }
    let (key, v) = c.take(kl + vl)?.split_at(kl);
    Ok(RecordRef {
        key,
        seq,
        value: (tag == 1).then_some(v),
    })
}
// Validate an entire in-memory frame without allocating a second payload buffer.
pub(crate) fn frame_payload(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < HEADER {
        return Err(corrupt("truncated frame header"));
    }
    if crc32fast::hash(&bytes[..4]) != u32::from_le_bytes(bytes[4..8].try_into().unwrap()) {
        return Err(corrupt("frame header checksum"));
    }
    let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    if len > MAX_FRAME || bytes.len() - HEADER != len {
        return Err(corrupt("data frame length"));
    }
    let payload = &bytes[HEADER..];
    if crc32fast::hash(payload) != u32::from_le_bytes(bytes[8..12].try_into().unwrap()) {
        return Err(corrupt("frame payload checksum"));
    }
    Ok(payload)
}
pub(crate) fn write_frame(w: &mut impl Write, payload: &[u8]) -> Result<u64> {
    if payload.len() > MAX_FRAME {
        return Err(corrupt("frame exceeds format limit"));
    }
    let len = (payload.len() as u32).to_le_bytes();
    w.write_all(&len)?;
    w.write_all(&crc32fast::hash(&len).to_le_bytes())?;
    w.write_all(&crc32fast::hash(payload).to_le_bytes())?;
    w.write_all(payload)?;
    Ok((HEADER + payload.len()) as u64)
}
// A torn tail is recoverable; a complete frame with a bad CRC is not.
pub(crate) fn read_frame(r: &mut impl Read, allow_tail: bool) -> Result<Option<Vec<u8>>> {
    let mut head = [0u8; HEADER];
    let mut n = 0;
    while n < head.len() {
        match r.read(&mut head[n..]) {
            Ok(0) => {
                return if n == 0 || allow_tail {
                    Ok(None)
                } else {
                    Err(corrupt("truncated frame header"))
                }
            }
            Ok(k) => n += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    if crc32fast::hash(&head[..4]) != u32::from_le_bytes(head[4..8].try_into().unwrap()) {
        return Err(corrupt("frame header checksum"));
    }
    let len = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
    if len > MAX_FRAME {
        return Err(corrupt("frame length exceeds limit"));
    }
    let mut b = vec![0; len];
    if let Err(e) = r.read_exact(&mut b) {
        if allow_tail && e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(corrupt("truncated frame payload"));
        }
        return Err(e.into());
    }
    if crc32fast::hash(&b) != u32::from_le_bytes(head[8..12].try_into().unwrap()) {
        return Err(corrupt("frame payload checksum"));
    }
    Ok(Some(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineError;
    use std::io::{self, ErrorKind};

    // Alternate interruptions and short transfers, including within the payload.
    struct Fragmented<T> {
        inner: T,
        interrupt: bool,
    }
    impl<T: Read> Read for Fragmented<T> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.interrupt = !self.interrupt;
            if self.interrupt {
                return Err(ErrorKind::Interrupted.into());
            }
            let n = buf.len().min(3);
            self.inner.read(&mut buf[..n])
        }
    }
    impl<T: Write> Write for Fragmented<T> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.interrupt = !self.interrupt;
            if self.interrupt {
                return Err(ErrorKind::Interrupted.into());
            }
            self.inner.write(&buf[..buf.len().min(3)])
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(ErrorKind::PermissionDenied.into())
        }
    }

    #[test]
    fn frames_survive_short_and_interrupted_io() {
        let payload = b"a payload longer than one transfer";
        let mut writer = Fragmented {
            inner: Vec::new(),
            interrupt: false,
        };
        assert_eq!(
            write_frame(&mut writer, payload).unwrap(),
            (HEADER + payload.len()) as u64
        );
        write_frame(&mut writer, b"").unwrap();
        let mut reader = Fragmented {
            inner: writer.inner.as_slice(),
            interrupt: false,
        };
        assert_eq!(read_frame(&mut reader, false).unwrap().unwrap(), payload);
        assert_eq!(read_frame(&mut reader, false).unwrap(), Some(vec![]));
        assert_eq!(read_frame(&mut reader, false).unwrap(), None);
    }

    #[test]
    fn io_errors_are_not_mistaken_for_recoverable_tails() {
        let mut frame = Vec::new();
        write_frame(&mut frame, b"payload").unwrap();
        for cut in 0..frame.len() {
            for allow_tail in [false, true] {
                let mut reader = (&frame[..cut]).chain(FailedRead);
                assert!(matches!(
                    read_frame(&mut reader, allow_tail),
                    Err(EngineError::Io(e)) if e.kind() == ErrorKind::PermissionDenied
                ));
            }
        }
    }

    #[test]
    fn incomplete_writes_are_reported_as_errors() {
        let payload = b"payload";
        for capacity in 0..HEADER + payload.len() {
            let mut storage = vec![0; capacity];
            assert!(matches!(
                write_frame(&mut storage.as_mut_slice(), payload),
                Err(EngineError::Io(e)) if e.kind() == ErrorKind::WriteZero
            ));
        }
    }
}
