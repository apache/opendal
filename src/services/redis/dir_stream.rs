use crate::error::{other, ObjectError};
use crate::ops::Operation;
use crate::path::build_rel_path;
use crate::services::redis::backend::Backend;
use crate::services::redis::error::{new_deserialize_metadata_error, new_exec_async_cmd_error};
use crate::services::redis::REDIS_API_VERSION;
use crate::{DirEntry, ObjectMetadata};
use futures::Stream;
use redis::aio::Connection;
use redis::{AsyncCommands, AsyncIter};
use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct DirStream<'a> {
    backend: Arc<Backend>,
    connection: Arc<Connection>,
    iter: AsyncIter<'a, String>,
    path: String,
}

impl DirStream {
    pub fn new(
        backend: Arc<Backend>,
        connection: Arc<Connection>,
        path: &str,
        iter: AsyncIter<String>,
    ) -> Self {
        Self {
            backend,
            connection,
            path: path.to_string(),
            iter,
        }
    }
}

impl Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(self: &mut Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut con = self.connection.clone();
        let path = self.path.clone();
        let backend = self.backend.clone();

        match self.iter.next_item() {
            Poll::Ready(Some(entry)) => {
                let m_path = format!("v{}:m:{}", REDIS_API_VERSION, entry);
                let bin = con
                    .get(m_path)
                    .await
                    .map_err(|err| new_exec_async_cmd_error(err, Operation::List, path.as_str()))?;
                let metadata: ObjectMetadata = bincode::deserialize(bin).map_err(|err| {
                    new_deserialize_metadata_error(err, Operation::List, path.as_str())
                })?;
                let mut entry = DirEntry::new(backend, metadata.mode(), entry.as_str());
                let last_modified = metadata.last_modified().ok_or_else(|err| {
                    other(ObjectError::new(Operation::List, path.as_str(), err))
                })?;
                let content_length = metadata.content_length().ok_or_else(|err| {
                    other(ObjectError::new(Operation::List, path.as_str(), err))
                })?;
                entry.set_last_modified(last_modified);
                entry.set_content_length(content_length);
                Poll::Ready(Some(Ok(entry)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
