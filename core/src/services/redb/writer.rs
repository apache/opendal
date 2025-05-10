use std::sync::Arc;

use tokio::task;

use crate::Buffer;
use crate::Result;
use crate::raw::new_task_join_error;
use crate::raw::oio::{self, QueueBuf};

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

    async fn close(&mut self) -> Result<()> {
        let buf = self.buffer.clone().collect();

        let core = self.core.clone();
        let cloned_path = self.path.clone();

        task::spawn_blocking(move || core.set(&cloned_path, &buf.to_vec()))
            .await
            .map_err(new_task_join_error)
            .and_then(|inner_result| inner_result)?;

        Ok(())
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

    fn close(&mut self) -> Result<()> {
        let buf = self.buffer.clone().collect();
        self.core.set(&self.path, &buf.to_vec())?;
        Ok(())
    }
}
