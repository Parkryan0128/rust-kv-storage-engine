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
    let seq = c.u64()?;
    let tag = c.take(1)?[0];
    let kl = c.u32()? as usize;
    let vl = c.u32()? as usize;
    if seq == 0
        || tag > 1
        || (tag == 0 && vl != 0)
        || kl.saturating_add(vl).saturating_add(17) > MAX_RECORD
    {
        return Err(corrupt("invalid record header"));
    }
    let key = c.take(kl)?.to_vec();
    let v = c.take(vl)?;
    Ok((
        key,
        Record {
            seq,
            value: if tag == 1 {
                Some(Bytes::copy_from_slice(v))
            } else {
                None
            },
        },
    ))
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
// Only a physically incomplete final frame is recoverable. Intact but invalid CRCs are errors.
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
