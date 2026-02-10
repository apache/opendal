// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use super::core::{DeletedFile, HfCore};
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

pub struct HfDeleter {
    core: Arc<HfCore>,
}

impl HfDeleter {
    pub fn new(core: Arc<HfCore>) -> Self {
        Self { core }
    }

    async fn commit_delete(&self, deleted_files: Vec<DeletedFile>) -> Result<()> {
        match self.core.commit_files(vec![], vec![], deleted_files).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}

impl oio::BatchDelete for HfDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        self.commit_delete(vec![DeletedFile { path }]).await
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let deleted_files: Vec<DeletedFile> = batch
            .iter()
            .map(|(path, _)| DeletedFile { path: path.clone() })
            .collect();

        self.commit_delete(deleted_files).await?;
        Ok(BatchDeleteResult {
            succeeded: batch,
            failed: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::core::HfCore;
    use super::super::uri::{HfRepo, RepoType};
    use super::super::writer::HfWriter;
    use super::*;
    use oio::BatchDelete;
    use oio::OneShotWrite;

    fn testing_core() -> HfCore {
        let repo_id = std::env::var("HF_OPENDAL_DATASET").expect("HF_OPENDAL_DATASET must be set");

        let info = AccessorInfo::default();
        info.set_scheme("huggingface")
            .set_native_capability(Capability {
                write: true,
                delete: true,
                ..Default::default()
            });

        HfCore {
            info: info.into(),
            repo: HfRepo::new(RepoType::Dataset, repo_id, Some("main".to_string())),
            root: "/".to_string(),
            token: std::env::var("HF_OPENDAL_TOKEN").ok(),
            endpoint: "https://huggingface.co".to_string(),
            #[cfg(feature = "xet")]
            xet_enabled: false,
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_once() {
        let core = Arc::new(testing_core());

        // First write a file so we have something to delete
        let writer = HfWriter::new(&core, "delete-test.txt", OpWrite::default());
        writer
            .write_once(Buffer::from("temporary content"))
            .await
            .expect("write should succeed");

        // Now delete it
        let deleter = HfDeleter::new(core);
        deleter
            .delete_once("delete-test.txt".to_string(), OpDelete::default())
            .await
            .expect("delete should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_nonexistent() {
        let core = Arc::new(testing_core());

        let deleter = HfDeleter::new(core);
        deleter
            .delete_once("nonexistent-file.txt".to_string(), OpDelete::default())
            .await
            .expect("deleting nonexistent file should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_batch() {
        let core = Arc::new(testing_core());

        // Write two files
        for name in &["batch-del-1.txt", "batch-del-2.txt"] {
            let writer = HfWriter::new(&core, name, OpWrite::default());
            writer
                .write_once(Buffer::from("temp"))
                .await
                .expect("write should succeed");
        }

        // Batch delete them in a single commit
        let deleter = HfDeleter::new(core);
        let batch = vec![
            ("batch-del-1.txt".to_string(), OpDelete::default()),
            ("batch-del-2.txt".to_string(), OpDelete::default()),
        ];
        let result = deleter
            .delete_batch(batch)
            .await
            .expect("batch delete should succeed");

        assert_eq!(result.succeeded.len(), 2);
        assert!(result.failed.is_empty());
    }
}
