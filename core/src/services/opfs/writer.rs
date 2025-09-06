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

use bytes::{Buf, Bytes};
use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    File, FileSystemCreateWritableOptions, FileSystemGetFileOptions, FileSystemWritableFileStream,
};

use crate::raw::{oio, OpWrite};
use crate::*;

use super::core::OpfsCore;
use super::error::parse_js_error;

enum WriterRequest {
    Write {
        buf: Bytes,
        tx: oneshot::Sender<Result<()>>,
    },
    Close {
        tx: oneshot::Sender<Result<()>>,
    },
}

pub struct OpfsWriter {
    tx: mpsc::UnboundedSender<WriterRequest>,
    pos: u64,
}

impl OpfsWriter {
    pub async fn new(core: Arc<OpfsCore>, path: &str, op: OpWrite) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded();
        let mut pos = 0;

        core.ensure_write_path(path).await?;

        let opt = FileSystemGetFileOptions::new();
        opt.set_create(true);

        let file_handle = core.file_handle_with_option(path, &opt).await?;

        let opt = FileSystemCreateWritableOptions::new();
        opt.set_keep_existing_data(op.append());

        let write_stream: FileSystemWritableFileStream =
            JsFuture::from(file_handle.create_writable_with_options(&opt))
                .await
                .and_then(JsCast::dyn_into)
                .map_err(parse_js_error)?;

        if op.append() {
            let file: File = JsFuture::from(file_handle.get_file())
                .await
                .and_then(JsCast::dyn_into)
                .map_err(parse_js_error)?;

            JsFuture::from(
                write_stream
                    .seek_with_f64(file.size())
                    .map_err(parse_js_error)?,
            )
            .await
            .map_err(parse_js_error)?;

            pos += file.size() as u64;
        }

        wasm_bindgen_futures::spawn_local(async move {
            Self::run(write_stream, rx).await;
        });

        Ok(Self { tx, pos })
    }

    async fn run(
        write_stream: FileSystemWritableFileStream,
        mut rx: mpsc::UnboundedReceiver<WriterRequest>,
    ) {
        loop {
            let Some(req) = rx.next().await else {
                // OpfsWriter is dropped, exit worker task
                break;
            };

            match req {
                WriterRequest::Write { buf, tx } => {
                    let result = match write_stream.write_with_u8_array(&buf).map(JsFuture::from) {
                        Ok(fut) => match fut.await {
                            Ok(_) => Ok(()),
                            Err(err) => Err(parse_js_error(err)),
                        },
                        Err(err) => Err(parse_js_error(err)),
                    };

                    let _ = tx.send(result);
                }
                WriterRequest::Close { tx } => {
                    let _ = match JsFuture::from(write_stream.close()).await {
                        Ok(_) => tx.send(Ok(())),
                        Err(err) => tx.send(Err(parse_js_error(err))),
                    };
                    return;
                }
            };
        }
    }
}

impl oio::Write for OpfsWriter {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        while bs.has_remaining() {
            let buf = bs.current();
            let n = buf.len();

            let (tx, rx) = oneshot::channel();
            let _ = self.tx.send(WriterRequest::Write { buf, tx }).await;

            rx.await.unwrap()?;
            self.pos += n as u64;
            bs.advance(n);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(WriterRequest::Close { tx }).await;

        rx.await.unwrap()?;

        Ok(Metadata::default().with_content_length(self.pos))
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "OPFS doesn't support abort",
        ))
    }
}
