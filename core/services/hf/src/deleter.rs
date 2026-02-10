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
            Ok(_) => Ok(()),
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
    use super::super::backend::test_utils::testing_operator;
    use opendal_core::*;

    #[tokio::test]
    #[ignore]
    async fn test_delete_once() {
        let op = testing_operator();
        let path = "tests/delete-test.txt";

        op.write(path, b"temporary content".as_slice())
            .await
            .expect("write should succeed");

        op.delete(path).await.expect("delete should succeed");

        let err = op
            .stat(path)
            .await
            .expect_err("stat should fail after delete");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_nonexistent() {
        let op = testing_operator();

        op.delete("nonexistent-file.txt")
            .await
            .expect("deleting nonexistent file should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_batch() {
        let op = testing_operator();
        let paths = ["tests/batch-del-1.txt", "tests/batch-del-2.txt"];

        for path in &paths {
            op.write(path, b"temp".as_slice())
                .await
                .expect("write should succeed");
        }

        for path in &paths {
            op.delete(path).await.expect("delete should succeed");
        }

        for path in &paths {
            let err = op
                .stat(path)
                .await
                .expect_err("stat should fail after delete");
            assert_eq!(err.kind(), ErrorKind::NotFound);
        }
    }
}
