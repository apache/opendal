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

use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};
use js_sys::{Array, AsyncIterator, IteratorNext, JsString};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemHandle, FileSystemHandleKind};

use crate::raw::{normalize_path, oio};
use crate::services::opfs::error::parse_js_error;
use crate::{EntryMode, Error, ErrorKind, Metadata, Result};

use super::core::OpfsCore;

enum ListRequest {
    Next {
        tx: oneshot::Sender<Result<Option<oio::Entry>>>,
    },
}

pub struct OpfsLister {
    done: bool,
    current_path: Option<String>,
    tx: mpsc::UnboundedSender<ListRequest>,
}

impl OpfsLister {
    pub(crate) async fn new(core: Arc<OpfsCore>, path: &str) -> Result<Self> {
        let mut current_path = path.to_string();
        if !current_path.ends_with('/') {
            current_path.push('/');
        }

        let (tx, rx) = mpsc::unbounded();

        match core.opfs_list(path).await {
            Ok(iter) => {
                let prefix = current_path.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    Self::run(iter, prefix, rx).await;
                });
                Ok(Self {
                    current_path: Some(current_path),
                    done: false,
                    tx,
                })
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(Self {
                tx,
                done: true,
                current_path: Some(current_path),
            }),
            Err(err) => Err(err),
        }
    }

    async fn run(
        iter: AsyncIterator,
        prefix: String,
        mut rx: mpsc::UnboundedReceiver<ListRequest>,
    ) {
        loop {
            let Some(req) = rx.next().await else {
                // OpfsLister is dropped, exit worker task
                break;
            };

            match req {
                ListRequest::Next { tx } => {
                    let fut = match iter.next().map(JsFuture::from) {
                        Ok(fut) => fut,
                        Err(err) => {
                            let _ = tx.send(Err(parse_js_error(err)));
                            return;
                        }
                    };

                    let entry_result = match fut.await {
                        Ok(entry) => {
                            let next = entry.unchecked_into::<IteratorNext>();
                            let entry = if next.done() {
                                let _ = tx.send(Ok(None));
                                return;
                            } else {
                                next.value()
                            };

                            let array = entry.dyn_into::<Array>().unwrap().to_vec();
                            debug_assert_eq!(array.len(), 2, "expected 2 elements in array");

                            let Ok(mut path) =
                                array[0].clone().dyn_into::<JsString>().map(String::from)
                            else {
                                let _ = tx.send(Err(Error::new(
                                    ErrorKind::Unexpected,
                                    format!("cast {:?} to JsString failed", array[0]),
                                )));
                                break;
                            };

                            let Ok(handle) = array[1].clone().dyn_into::<FileSystemHandle>() else {
                                let _ = tx.send(Err(Error::new(
                                    ErrorKind::Unexpected,
                                    format!("cast {:?} to FileSystemHandle failed", array[1]),
                                )));
                                break;
                            };

                            let mode = match handle.kind() {
                                FileSystemHandleKind::File => EntryMode::FILE,
                                FileSystemHandleKind::Directory => {
                                    if !path.ends_with('/') {
                                        path.push('/');
                                    }
                                    EntryMode::DIR
                                }
                                _ => unreachable!(),
                            };
                            let meta = Metadata::new(mode);

                            Ok(Some(oio::Entry::new(
                                &normalize_path(&format!("{prefix}{path}")),
                                meta,
                            )))
                        }
                        Err(err) => Err(parse_js_error(err)),
                    };

                    let _ = tx.send(entry_result);
                }
            }
        }
    }
}

impl oio::List for OpfsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.done {
            return Ok(None);
        }

        // since list should return path itself, we return it first
        if let Some(path) = self.current_path.take() {
            let e = oio::Entry::new(path.as_str(), Metadata::new(EntryMode::DIR));
            return Ok(Some(e));
        }

        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ListRequest::Next { tx }).await;

        match rx.await.unwrap() {
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Ok(e) => Ok(e),
            Err(err) => Err(err),
        }
    }
}
