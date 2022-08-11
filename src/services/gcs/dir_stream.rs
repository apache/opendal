use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::task::Spawn;
use futures::{ready, Stream};
use isahc::AsyncReadResponseExt;
use serde_json::de;

use crate::error::{other, ObjectError};
use crate::http_util::{parse_error_response, parse_error_status_code};
use crate::services::gcs::backend::Backend;
use crate::{DirEntry, ObjectMode};

enum State {
    Standby,
    Pending(BoxFuture<'static, Result<Vec<u8>>>),
    Walking((Output, usize, usize)),
}

/// DirStream takes over task of listing objects and
/// helps walking directory
pub struct DirStream {
    backend: Arc<Backend>,
    path: String,
    page_token: String,

    done: bool,
    state: State,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, path: &str) -> Self {
        Self {
            backend,
            path: path.to_string(),
            page_token: "".to_string(), // don't know if it works?

            done: false,
            state: State::Standby,
        }
    }
}

impl Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();

        match &mut self.state {
            State::Standby => {
                let path = self.path.clone();
                let token = self.page_token.clone();

                let fut = async move {
                    let mut resp = backend.list_objects(&path, token.as_str()).await?;

                    let status = resp.status();
                    if status > 300 {
                        let e = parse_error_response("list", &path, parse_error_status_code, resp)
                            .await;
                        return Err(e);
                    }
                    let bytes = resp.bytes().await.map_err(|e| {
                        other(ObjectError::new(
                            "list",
                            &path,
                            anyhow!("read body: {:?}", e),
                        ))
                    })?;

                    Ok(bytes)
                };

                self.state = State::Pending(Box::pin(fut));
                self.pool_next(cx)
            }
            State::Pending(fut) => {
                let bytes = ready!(Pin::new(fut).pool(cx))?;
                let output: Output = serde_json::de::from_reader(bytes.reader()).map_err(|e| {
                    other(ObjectError::new(
                        "list",
                        &self.path,
                        anyhow!("deserialize list_bucket output: {:?}", e),
                    ))
                })?;

                if let Some(token) = &output.next_page_token {
                    self.page_token = token.clone();
                } else {
                    self.done = true;
                }
                self.state = State::Walking((output, 0, 0));
                self.poll_next(cx)
            }
            State::Walking((output, common_prefixes_idx, objects_idx)) => {
                let prefixes = &output.prefixes;
                if *common_prefixes_idx < prefixes.len() {
                    let prefix = &prefixes[*common_prefixes_idx];
                    *common_prefixes_idx += 1;

                    let de = DirEntry::new(
                        backend.clone(),
                        ObjectMode::DIR,
                        &backend.get_rel_path(prefix),
                    );

                    log::debug!(
                        "object {} got entry, mode: {}, path: {}",
                        &self.path,
                        de.path(),
                        de.mode()
                    );
                    return Poll::Ready(Some(Ok(de)));
                }
                let objects = &output.items;
                while *objects_idx < objects.len() {
                    let object = &objects[*objects_idx];
                    *objects_idx += 1;

                    // TODO: don't really know if GCS also return the dir itself in contents
                    // just in case
                    if object.key.ends_with('/') {
                        continue;
                    }

                    let de = DirEntry::new(
                        backend.clone(),
                        ObjectMode::File,
                        &backend.get_rel_path(&object.key),
                    );

                    log::debug!(
                        "dir object {} got entry, mode: {}, path: {}",
                        &self.path,
                        de.mode(),
                        de.path()
                    );
                    return Poll::Ready(Some(Ok(de)));
                }

                // end of asynchronous iteration
                if self.done {
                    log::debug!("object {} list done", &self.path);
                    return Poll::Ready(None);
                }

                self.state = State::Standby;
                self.pool_next(cx)
            }
        }
    }
}

/// Response JSON from GCS list objects API.
///
/// refer to https://cloud.google.com/storage/docs/json_api/v1/objects/list for details
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Output {
    /// The kind of item this is.
    /// For lists of objects, this is always "storage#objects"
    kind: String,
    /// The continuation token.
    ///
    /// If this is the last page of results, then no continuation token is returned.
    next_page_token: Option<String>,
    /// Object name prefixes for objects that matched the listing request
    /// but were excluded from [items] because of a delimiter.
    prefixes: Vec<String>,
    /// The list of objects, ordered lexicographically by name.
    items: Vec<OutputContent>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutputContent {
    key: String,
    size: u64,
}

// TODO: Add tests for GCS DirStream
