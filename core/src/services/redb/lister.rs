use crate::{EntryMode, Metadata, Result, raw::oio};

use super::error::parse_storage_error;

pub struct RedbLister {
    range: redb::Range<'static, &'static str, &'static [u8]>,
    pattern: String,
}

impl RedbLister {
    pub fn new(range: redb::Range<'static, &'static str, &'static [u8]>, pattern: String) -> Self {
        Self { range, pattern }
    }
}

impl oio::BlockingList for RedbLister {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let Some(result) = self.range.next() else {
                return Ok(None);
            };

            let (key, value) = result.map_err(parse_storage_error)?;
            let key = key.value();
            if key.starts_with(&self.pattern) {
                let mode = if key.ends_with('/') {
                    EntryMode::DIR
                } else {
                    EntryMode::FILE
                };
                return Ok(Some(oio::Entry::new(key, Metadata::new(mode))));
            }
        }
    }
}
