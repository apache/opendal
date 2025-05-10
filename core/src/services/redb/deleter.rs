use std::sync::Arc;

use tokio::task;

use crate::raw::build_abs_path;
use crate::raw::new_task_join_error;
use crate::raw::oio;
use crate::raw::OpDelete;
use crate::Result;

use super::core::RedbCore;

pub struct RedbDeleter {
    core: Arc<RedbCore>,
}

impl RedbDeleter {
    pub fn new(core: Arc<RedbCore>) -> Self {
        RedbDeleter { core }
    }
}

impl oio::OneShotDelete for RedbDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let p = build_abs_path(&self.core.root, &path);
        let core = self.core.clone();

        task::spawn_blocking(move || core.delete(&p))
            .await
            .map_err(new_task_join_error)
            .and_then(|inner_result| inner_result)
    }
}

impl oio::BlockingOneShotDelete for RedbDeleter {
    fn blocking_delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let p = build_abs_path(&self.core.root, &path);

        self.core.delete(&p)?;
        Ok(())
    }
}
