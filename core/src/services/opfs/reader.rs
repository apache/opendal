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
use std::u64;

use bytes::{Bytes, BytesMut};
use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};
use js_sys::Uint8Array;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{File, ReadableStreamDefaultReader, ReadableStreamReadResult};

use crate::raw::{oio, OpRead};
use crate::*;

use super::core::OpfsCore;
use super::error::parse_js_error;

enum ReadRequest {
    Read { tx: oneshot::Sender<Result<Bytes>> },
}

pub struct OpfsReader {
    pos: usize,
    end_pos: usize,
    tx: mpsc::UnboundedSender<ReadRequest>,
}

impl OpfsReader {
    pub async fn new(core: Arc<OpfsCore>, path: &str, op: &OpRead) -> Result<Self> {
        let pos = op.range().offset();
        let end_pos = op
            .range()
            .size()
            .map(|sz| pos + sz)
            .unwrap_or(i32::MAX as u64);

        let file_handle = core.file_handle(path).await?;
        let file: File = JsFuture::from(file_handle.get_file())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;

        let read_stream = file
            .slice_with_i32_and_i32(pos as i32, end_pos as i32)
            .map_err(parse_js_error)?
            .stream()
            .get_reader()
            .dyn_into::<ReadableStreamDefaultReader>()
            .map_err(|obj| parse_js_error(obj.into()))?;

        let (tx, rx) = mpsc::unbounded();

        // everything in wasm-bindgen is non-send, so we need to spawn a new task
        wasm_bindgen_futures::spawn_local(async move {
            Self::run(read_stream, rx).await;
        });

        Ok(Self {
            pos: pos as usize,
            end_pos: end_pos as usize,
            tx,
        })
    }

    async fn run(
        reader: ReadableStreamDefaultReader,
        mut rx: mpsc::UnboundedReceiver<ReadRequest>,
    ) {
        loop {
            let Some(req) = rx.next().await else {
                // OpfsReader is dropped, exit worker task
                break;
            };
            match req {
                ReadRequest::Read { tx } => {
                    let read_res = match JsFuture::from(reader.read()).await {
                        Ok(js_value) => ReadableStreamReadResult::from(js_value),
                        Err(err) => {
                            let _ = tx.send(Err(parse_js_error(err)));
                            break;
                        }
                    };

                    if read_res.get_done().unwrap_or(false) {
                        let _ = tx.send(Ok(Bytes::new()));
                        break;
                    }

                    let chunk = match read_res.get_value().dyn_into::<Uint8Array>() {
                        Ok(chunk) => chunk,
                        Err(err) => {
                            let _ = tx.send(Err(parse_js_error(err)));
                            break;
                        }
                    };

                    let mut buf = BytesMut::with_capacity(chunk.byte_length() as usize);
                    buf.resize(chunk.byte_length() as usize, 0);
                    chunk.copy_to(buf.as_mut());

                    let _ = tx.send(Ok(buf.freeze()));
                }
            }
        }
    }
}

impl oio::Read for OpfsReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.pos > self.end_pos {
            return Ok(Buffer::new());
        }

        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ReadRequest::Read { tx }).await;

        let bytes = rx.await.unwrap()?;
        self.pos += bytes.len();

        Ok(Buffer::from(bytes))
    }
}
