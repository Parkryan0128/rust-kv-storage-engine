use super::*;

// One validated encoded block per input. Compaction can select a winning
// version and inspection can truncate a preview before copying any value.
pub(crate) struct TableCursor {
    table: Arc<Table>,
    next_block: usize,
    block: Option<BlockCursor>,
}

impl TableCursor {
    pub fn new(table: Arc<Table>) -> Result<Self> {
        let mut cursor = Self {
            table,
            next_block: 0,
            block: None,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    pub fn current(&self) -> Option<RecordRef<'_>> {
        self.block.as_ref().and_then(BlockCursor::current)
    }

    pub(crate) fn has_prefix_blocks(&self) -> bool {
        self.table.expanded_frame_limit.is_some()
    }

    pub(crate) fn current_for<const PREFIX: bool>(&self) -> Option<RecordRef<'_>> {
        self.block
            .as_ref()
            .and_then(BlockCursor::current_for::<PREFIX>)
    }

    pub fn advance(&mut self) -> Result<()> {
        self.advance_for::<true>()
    }

    pub(crate) fn advance_for<const PREFIX: bool>(&mut self) -> Result<()> {
        debug_assert!(PREFIX || !self.has_prefix_blocks());
        if let Some(block) = &mut self.block {
            block.advance_for::<PREFIX>()?;
            if block.current_for::<PREFIX>().is_some() {
                return Ok(());
            }
        }
        if self.next_block == self.table.index.len() {
            self.block = None;
            return Ok(());
        }
        let (reusable, prefix_key) = BlockCursor::recycle(self.block.take());
        let block = self.table.read_stream_block(self.next_block, reusable)?;
        self.block = Some(BlockCursor::new_for::<PREFIX>(block, 0, prefix_key)?);
        self.next_block += 1;
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
        assert_eq!(cursor.current().unwrap().key, [0]);
        cursor.advance().unwrap();
        assert_eq!(cursor.block.as_ref().unwrap().buffer_addresses().0, first);
        cursor.advance().unwrap();
        assert!(cursor.block.as_ref().unwrap().frame_capacity() > 32 * 1024);
        cursor.advance().unwrap();
        assert!(cursor.block.as_ref().unwrap().frame_capacity() < 1024);
        assert_eq!(cursor.current().unwrap().value.unwrap(), &[3; 128]);
        cursor.advance().unwrap();
        assert!(cursor.block.is_none());
        assert!(cursor.current().is_none());
        cursor.advance().unwrap();
        assert!(cursor.current().is_none());
    }
}
