// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::ready;
use futures::Stream;
use redis::aio::Connection;
use redis::AsyncCommands;
use redis::AsyncIter;

use crate::error::other;
use crate::error::ObjectError;
use crate::ops::Operation;
use crate::services::redis::backend::Backend;
use crate::services::redis::error::new_deserialize_metadata_error;
use crate::services::redis::error::new_exec_async_cmd_error;
use crate::services::redis::REDIS_API_VERSION;
use crate::DirEntry;
use crate::ObjectMetadata;

pub struct DirStream<'a> {
    backend: Arc<Backend>,
    connection: Arc<Connection>,
    iter: AsyncIter<'a, String>,
    path: String,
}

impl<'a> DirStream<'a> {
    pub fn new(
        backend: Arc<Backend>,
        connection: Arc<Connection>,
        path: &str,
        iter: AsyncIter<'a, String>,
    ) -> Self {
        Self {
            backend,
            connection,
            path: path.to_string(),
            iter,
        }
    }
}

impl<'a> Stream for DirStream<'a> {
    type Item = Result<DirEntry>;

    fn poll_next(self: &mut Pin<&'a mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut con = self.connection.clone();
        let path = self.path.clone();
        let backend = self.backend.clone();

        match self.iter.next_item() {
            Poll::Ready(Some(entry)) => {
                // get metadata of current entry
                let m_path = format!("v{}:m:{}", REDIS_API_VERSION, entry);
                let get_bin = con.get(m_path);
                let bin = ready!(Pin::new(get_bin).poll(cx))
                    .map_err(|err| new_exec_async_cmd_error(err, Operation::List, path.as_str()))?;
                let metadata: ObjectMetadata = bincode::deserialize(bin).map_err(|err| {
                    new_deserialize_metadata_error(err, Operation::List, path.as_str())
                })?;

                // record metadata to DirEntry
                let mut entry = DirEntry::new(backend, metadata.mode(), entry.as_str());
                let content_length = metadata.content_length().ok_or_else(|err| {
                    other(ObjectError::new(Operation::List, path.as_str(), err))
                })?;

                // folders have no last_modified field
                if let Some(last_modified) = metadata.last_modified() {
                    entry.set_last_modified(last_modified);
                }
                entry.set_last_modified(last_modified);
                entry.set_content_length(content_length);
                Poll::Ready(Some(Ok(entry)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
