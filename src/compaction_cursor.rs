use super::*;

// One validated encoded block per input. Values remain borrowed until the
// merge has selected the winning version; old versions need no owned copies.
pub(crate) struct TableCursor {
    table: Arc<Table>,
    next_block: usize,
    block: Option<ReadBlock>,
    record: usize,
}

impl TableCursor {
    pub fn new(table: Arc<Table>) -> Result<Self> {
        let mut cursor = Self {
            table,
            next_block: 0,
            block: None,
            record: 0,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    pub fn current(&self) -> Result<Option<RecordRef<'_>>> {
        self.block
            .as_ref()
            .map(|block| block.record_at(self.record))
            .transpose()
    }

    pub fn advance(&mut self) -> Result<()> {
        if let Some(block) = &self.block {
            self.record += 1;
            if self.record < block.record_count() {
                return Ok(());
            }
        }
        let Some(entry) = self.table.index.get(self.next_block) else {
            self.block = None;
            return Ok(());
        };
        let (mut bytes, offsets) = self
            .block
            .take()
            .map(ReadBlock::into_buffers)
            .unwrap_or_default();
        let len = entry.len as usize;
        // Release a rare large-record allocation before reading small blocks.
        if bytes.capacity() > len.saturating_mul(2) {
            bytes = Vec::new();
        }
        resize_frame(&mut bytes, len);
        self.table.file.read_exact_at(&mut bytes, entry.offset)?;
        self.block = Some(self.table.decode(self.next_block, bytes, offsets)?);
        self.next_block += 1;
        self.record = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn cursor_reuses_blocks_and_releases_large_buffers_and_eof() {
        let dir = tempfile::tempdir().unwrap();
        let sizes = [128, 128, 32 * 1024, 128];
        let table = Arc::new(
            Table::write(
                &dir.path().join("1.sst"),
                1,
                sizes.into_iter().enumerate().map(|(i, len)| {
                    Ok((
                        vec![i as u8],
                        Record {
                            seq: i as u64 + 1,
                            value: Some(Bytes::from(vec![i as u8; len])),
                        },
                    ))
                }),
                4,
                64,
                10,
            )
            .unwrap(),
        );
        let mut cursor = TableCursor::new(table).unwrap();
        let first = cursor.block.as_ref().unwrap().buffer_addresses().0;
        assert_eq!(cursor.current().unwrap().unwrap().key, [0]);
        cursor.advance().unwrap();
        assert_eq!(cursor.block.as_ref().unwrap().buffer_addresses().0, first);
        cursor.advance().unwrap();
        assert!(cursor.block.as_ref().unwrap().frame_capacity() > 32 * 1024);
        cursor.advance().unwrap();
        assert!(cursor.block.as_ref().unwrap().frame_capacity() < 1024);
        assert_eq!(cursor.current().unwrap().unwrap().value.unwrap(), &[3; 128]);
        cursor.advance().unwrap();
        assert!(cursor.block.is_none());
        assert!(cursor.current().unwrap().is_none());
        cursor.advance().unwrap();
        assert!(cursor.current().unwrap().is_none());
    }
}
