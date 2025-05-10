use tokio::task;

use crate::raw::oio;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

use super::error::parse_storage_error;

#[derive(Debug)]
pub struct RedbLister {
    receiver: flume::Receiver<Result<oio::Entry>>,
}

impl RedbLister {
    pub fn new(mut filter: RedbFilter) -> Self {
        let (tx, rx) = flume::bounded(1);

        task::spawn_blocking(move || loop {
            let Some(result) = filter.range.next() else {
                break;
            };

            let (key, value) = match result {
                Ok(pair) => pair,
                Err(e) => {
                    let e = parse_storage_error(e);
                    if tx.send(Err(e)).is_err() {
                        break;
                    }
                    continue;
                }
            };

            let key = key.value();
            let size = value.value().len() as u64;
            if key.starts_with(&filter.pattern) {
                let mode = EntryMode::from_path(key);
                let entry = oio::Entry::new(key, Metadata::new(mode).with_content_length(size));
                if tx.send(Ok(entry)).is_err() {
                    break;
                }
            }
        });

        Self { receiver: rx }
    }
}

impl oio::List for RedbLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self.receiver.recv_async().await {
            Ok(entry) => entry.map(Some),
            Err(_) => Ok(None),
        }
    }
}

pub struct RedbFilter {
    range: redb::Range<'static, &'static str, &'static [u8]>,
    pattern: String,
}

impl RedbFilter {
    pub fn new(range: redb::Range<'static, &'static str, &'static [u8]>, pattern: String) -> Self {
        Self { range, pattern }
    }
}

impl oio::BlockingList for RedbFilter {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let Some(result) = self.range.next() else {
                return Ok(None);
            };

            let (key, value) = result.map_err(parse_storage_error)?;
            let key = key.value();
            let size = value.value().len() as u64;
            if key.starts_with(&self.pattern) {
                let mode = EntryMode::from_path(key);
                return Ok(Some(oio::Entry::new(
                    key,
                    Metadata::new(mode).with_content_length(size),
                )));
            }
        }
    }
}
