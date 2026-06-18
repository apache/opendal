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

use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use monoio::fs::OpenOptions;
use opendal_core::raw::*;
use opendal_core::*;

use super::core::MonoiofsCore;

enum ReaderRequest {
    Read {
        offset: u64,
        buf: Box<[u8]>,
        tx: oneshot::Sender<Result<(usize, Box<[u8]>)>>,
    },
}

/// Reader returned by this backend.
pub struct MonoiofsReader {
    core: Arc<MonoiofsCore>,
    tx: mpsc::UnboundedSender<ReaderRequest>,
}

impl MonoiofsReader {
    pub async fn new(core: Arc<MonoiofsCore>, path: PathBuf) -> Result<Self> {
        let (open_result_tx, open_result_rx) = oneshot::channel();
        let (tx, rx) = mpsc::unbounded();
        core.spawn(move || Self::worker_entrypoint(path, rx, open_result_tx))
            .await;
        core.unwrap(open_result_rx.await)?;
        Ok(Self { core, tx })
    }

    /// entrypoint of worker task that runs in context of monoio
    async fn worker_entrypoint(
        path: PathBuf,
        mut rx: mpsc::UnboundedReceiver<ReaderRequest>,
        open_result_tx: oneshot::Sender<Result<()>>,
    ) {
        let result = OpenOptions::new().read(true).open(path).await;
        // [`monoio::fs::File`] is non-Send, hence it is kept within
        // worker thread
        let file = match result {
            Ok(file) => {
                let Ok(()) = open_result_tx.send(Ok(())) else {
                    // MonoiofsReader::new is cancelled, exit worker task
                    return;
                };
                file
            }
            Err(e) => {
                // discard the result if send failed due to MonoiofsReader::new
                // cancelled since we are going to exit anyway
                let _ = open_result_tx.send(Err(new_std_io_error(e)));
                return;
            }
        };
        // wait for read request and send back result to main thread
        loop {
            let Some(req) = rx.next().await else {
                // MonoiofsReader is dropped, exit worker task
                break;
            };
            match req {
                ReaderRequest::Read { offset, buf, tx } => {
                    let (result, buf) = file.read_at(buf, offset).await;
                    let result = result.map(move |n| (n, buf)).map_err(new_std_io_error);
                    // discard the result if send failed due to
                    // MonoiofsReader::read_at cancelled
                    let _ = tx.send(result);
                }
            }
        }
    }
}

impl MonoiofsReader {
    /// Send read request to worker thread and wait for result. Actual
    /// read happens in [`MonoiofsReader::worker_entrypoint`] running
    /// on worker thread.
    pub async fn read_at(&self, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }

        // send read request to worker thread and wait for result
        let (tx, rx) = oneshot::channel();
        self.core
            .unwrap(self.tx.unbounded_send(ReaderRequest::Read {
                offset,
                buf: vec![0; size].into_boxed_slice(),
                tx,
            }));
        let (n, buf) = self.core.unwrap(rx.await)?;

        let mut buf = Vec::from(buf);
        buf.truncate(n);
        Ok(Buffer::from(buf))
    }
}
