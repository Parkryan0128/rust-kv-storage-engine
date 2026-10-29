use crate::{codec::MAX_FRAME, EngineError, Result};
use bytes::Bytes;

pub(crate) const BATCH_HEADER: usize = 16;

pub(crate) struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Bytes>,
}

/// An ordered set of puts and deletes, committed by [`crate::Engine::write_batch`].
///
/// Operations copy their inputs. Repeated keys are applied in insertion order,
/// so the last operation on a key wins. A batch is not a read/write transaction.
/// Size limits are checked when submitting the batch to an engine.
#[derive(Default)]
pub struct WriteBatch {
    pub(crate) entries: Vec<Entry>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a put, including an empty key or value.
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.entries.push(Entry {
            key: key.as_ref().to_vec(),
            value: Some(Bytes::copy_from_slice(value.as_ref())),
        });
        self
    }

    /// Add a deletion. Deleting a missing key is allowed.
    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> &mut Self {
        self.entries.push(Entry {
            key: key.as_ref().to_vec(),
            value: None,
        });
        self
    }

    /// Number of operations, including repeated keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove all operations while retaining the operation-vector capacity.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    #[cfg(test)]
    pub(crate) fn encoded_len(&self) -> Result<usize> {
        self.validated_encoded_len(|_| Ok(()))
    }

    pub(crate) fn validated_encoded_len(
        &self,
        mut validate: impl FnMut(&Entry) -> Result<()>,
    ) -> Result<usize> {
        self.entries.iter().try_fold(BATCH_HEADER, |size, entry| {
            validate(entry)?;
            size.checked_add(17)
                .and_then(|n| n.checked_add(entry.key.len()))
                .and_then(|n| n.checked_add(entry.value.as_ref().map_or(0, Bytes::len)))
                .filter(|&n| n <= MAX_FRAME)
                .ok_or_else(|| {
                    EngineError::InvalidConfig("batch exceeds 64 MiB WAL payload limit".into())
                })
        })
    }
}
