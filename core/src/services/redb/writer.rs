use std::sync::Arc;

use tokio::task;

use crate::raw::new_task_join_error;
use crate::raw::oio::{self, QueueBuf};
use crate::Buffer;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

use super::core::RedbCore;

pub struct RedbWriter {
    core: Arc<RedbCore>,
    path: String,
    buffer: QueueBuf,
}

impl RedbWriter {
    pub fn new(core: Arc<RedbCore>, path: String) -> Self {
        RedbWriter {
            core,
            path,
            buffer: QueueBuf::new(),
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for KvWriter.
unsafe impl Sync for RedbWriter {}

impl oio::Write for RedbWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.buffer.push(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let buf = self.buffer.clone().collect();

        let core = self.core.clone();
        let cloned_path = self.path.clone();

        task::spawn_blocking(move || {
            let value = buf.to_vec();
            core.set(&cloned_path, &value)?;

            let meta = Metadata::new(EntryMode::from_path(&cloned_path))
                .with_content_length(value.len() as _);
            Ok(meta)
        })
        .await
        .map_err(new_task_join_error)
        .and_then(|inner_result| inner_result)
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer.clear();
        Ok(())
    }
}

impl oio::BlockingWrite for RedbWriter {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        self.buffer.push(bs);
        Ok(())
    }

    fn close(&mut self) -> Result<Metadata> {
        let buf = self.buffer.clone().collect();
        let value = buf.to_vec();
        self.core.set(&self.path, &value)?;

        let meta =
            Metadata::new(EntryMode::from_path(&self.path)).with_content_length(value.len() as _);
        Ok(meta)
    }
}
