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

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use backon::Retryable;



use super::core::*;

use super::reader::DropboxReader;
use super::writer::DropboxWriter;
use crate::raw::*;
use crate::*;

#[derive(Clone, Debug)]
pub struct DropboxBackend {
    pub core: Arc<DropboxCore>,
}

#[async_trait]
impl Accessor for DropboxBackend {
    type Reader = DropboxReader;
    type Writer = oio::OneShotWriter<DropboxWriter>;
    type Lister = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Dropbox)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,

                create_dir: true,

                delete: true,

                batch: true,
                batch_delete: true,

                ..Default::default()
            });
        ma
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        // Check if the folder already exists.
        let resp = self.core.dropbox_get_metadata(path).await;
        match resp {
            Ok(meta) if meta.is_dir() => return Ok(RpCreateDir::default()),
            Ok(meta) if meta.is_file() => {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    &format!("it's not a directory {}", path),
                ));
            }
            _ => ()
        }

        // Dropbox has very, very, very strong limitation on the create_folder requests.
        //
        // Let's try our best to make sure it won't failed for rate limited issues.
        { || self.core.dropbox_create_folder(path) }
            .retry(&*BACKOFF)
            .when(|e| e.is_temporary())
            .await
            // Set this error to permanent to avoid retrying.
            .map_err(|e| e.set_permanent())?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        self.core.dropbox_get_metadata(path).await.map(RpStat::new)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            DropboxReader::new(self.core.clone(), path, args),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(DropboxWriter::new(
                self.core.clone(),
                args,
                String::from(path),
            )),
        ))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        self.core
            .dropbox_delete(path)
            .await
            .map(|_| RpDelete::default())
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let ops = args.into_operation();
        if ops.len() > 1000 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "dropbox services only allow delete up to 1000 keys at once",
            )
            .with_context("length", ops.len().to_string()));
        }

        let paths = ops.into_iter().map(|(p, _)| p).collect::<Vec<_>>();

        let decoded_response = self.core.dropbox_delete_batch(paths).await?;
        match decoded_response.tag.as_str() {
            "complete" => {
                let entries = decoded_response.entries.unwrap_or_default();
                let results = self.core.handle_batch_delete_complete_result(entries);
                Ok(RpBatch::new(results))
            }
            "async_job_id" => {
                let job_id = decoded_response
                    .async_job_id
                    .expect("async_job_id should be present");
                let res = { || self.core.dropbox_delete_batch_check(job_id.clone()) }
                    .retry(&*BACKOFF)
                    .when(|e| e.is_temporary())
                    .await?;

                Ok(res)
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                &format!(
                    "delete batch failed with unexpected tag {}",
                    decoded_response.tag
                ),
            )),
        }
    }
}
