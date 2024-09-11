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

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::SinkExt;
use futures::StreamExt;
use monoio::fs::OpenOptions;

use super::core::MonoiofsCore;
use super::core::BUFFER_SIZE;
use crate::raw::*;
use crate::*;

enum ReaderRequest {
    Read {
        pos: u64,
        buf: BytesMut,
        tx: oneshot::Sender<Result<BytesMut>>,
    },
}

pub struct MonoiofsReader {
    core: Arc<MonoiofsCore>,
    tx: mpsc::UnboundedSender<ReaderRequest>,
    pos: u64,
    end_pos: Option<u64>,
}

impl MonoiofsReader {
    pub async fn new(core: Arc<MonoiofsCore>, path: PathBuf, range: BytesRange) -> Result<Self> {
        let (open_result_tx, open_result_rx) = oneshot::channel();
        let (tx, rx) = mpsc::unbounded();
        core.spawn(move || Self::worker_entrypoint(path, rx, open_result_tx))
            .await;
        core.unwrap(open_result_rx.await)?;
        Ok(Self {
            core,
            tx,
            pos: range.offset(),
            end_pos: range.size().map(|size| range.offset() + size),
        })
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
                ReaderRequest::Read { pos, buf, tx } => {
                    let (result, buf) = file.read_at(buf, pos).await;
                    // buf.len() will be set to n by monoio if read
                    // successfully, so n is dropped
                    let result = result.map(move |_| buf).map_err(new_std_io_error);
                    // discard the result if send failed due to
                    // MonoiofsReader::read cancelled
                    let _ = tx.send(result);
                }
            }
        }
    }
}

impl oio::Read for MonoiofsReader {
    /// Send read request to worker thread and wait for result. Actual
    /// read happens in [`MonoiofsReader::worker_entrypoint`] running
    /// on worker thread.
    async fn read(&mut self) -> Result<Buffer> {
        if let Some(end_pos) = self.end_pos {
            if self.pos >= end_pos {
                return Ok(Buffer::new());
            }
        }

        // allocate and resize buffer
        let mut buf = self.core.buf_pool.get();
        let size = self
            .end_pos
            .map_or(BUFFER_SIZE, |end_pos| (end_pos - self.pos) as usize);
        // set capacity of buf to exact size to avoid excessive read
        buf.reserve(size);
        let _ = buf.split_off(size);

        // send read request to worker thread and wait for result
        let (tx, rx) = oneshot::channel();
        self.core.unwrap(
            self.tx
                .send(ReaderRequest::Read {
                    pos: self.pos,
                    buf,
                    tx,
                })
                .await,
        );
        let mut buf = self.core.unwrap(rx.await)?;

        // advance cursor if read successfully
        self.pos += buf.len() as u64;
        let buffer = Buffer::from(buf.split().freeze());
        self.core.buf_pool.put(buf);
        Ok(buffer)
    }
}
