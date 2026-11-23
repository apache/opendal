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

use asyncband::mpsc;
use asyncband::oneshot;
use bytes::Buf;
use bytes::Bytes;
use monoio::fs::OpenOptions;
use opendal_core::raw::*;
use opendal_core::*;

use super::core::MonoiofsCore;

enum WriterRequest {
    Write {
        pos: u64,
        buf: Bytes,
        tx: oneshot::Sender<Result<()>>,
    },
    Close {
        tx: oneshot::Sender<Result<monoio::fs::Metadata>>,
    },
}

pub struct MonoiofsWriter {
    core: Arc<MonoiofsCore>,
    tx: Option<mpsc::UnboundedSender<WriterRequest>>,
    pos: u64,
}

impl MonoiofsWriter {
    pub async fn new(core: Arc<MonoiofsCore>, path: PathBuf, append: bool) -> Result<Self> {
        let (open_result_tx, open_result_rx) = oneshot::channel();
        let (tx, rx) = mpsc::unbounded();
        core.spawn(move || Self::worker_entrypoint(path, append, rx, open_result_tx))
            .await;
        let pos = core.unwrap(open_result_rx.await)?;
        Ok(Self {
            core,
            tx: Some(tx),
            pos,
        })
    }

    /// entrypoint of worker task that runs in context of monoio
    async fn worker_entrypoint(
        path: PathBuf,
        append: bool,
        mut rx: mpsc::UnboundedReceiver<WriterRequest>,
        open_result_tx: oneshot::Sender<Result<u64>>,
    ) {
        let result = OpenOptions::new()
            .write(true)
            .create(true)
            .append(append)
            .truncate(!append)
            .open(path)
            .await;
        // [`monoio::fs::File`] is non-Send, hence it is kept within
        // worker thread
        let file = match result {
            Ok(file) => file,
            Err(e) => {
                // discard the result if send failed due to MonoiofsWriter::new
                // cancelled since we are going to exit anyway
                let _ = open_result_tx.send(Err(new_std_io_error(e)));
                return;
            }
        };
        let initial_pos = if append {
            match file.metadata().await {
                Ok(meta) => meta.len(),
                Err(e) => {
                    let _ = file.close().await;
                    let _ = open_result_tx.send(Err(new_std_io_error(e)));
                    return;
                }
            }
        } else {
            0
        };
        if open_result_tx.send(Ok(initial_pos)).is_err() {
            // MonoiofsWriter::new is cancelled, exit worker task
            let _ = file.close().await;
            return;
        }

        let mut file = Some(file);
        // wait for write or close request and send back result to main thread
        loop {
            let Ok(req) = rx.recv().await else {
                // MonoiofsWriter is closed or dropped, exit worker task
                break;
            };
            match req {
                WriterRequest::Write { pos, buf, tx } => {
                    let result = match &file {
                        Some(file) => {
                            let (result, _) = file.write_all_at(buf, pos).await;
                            result.map_err(new_std_io_error)
                        }
                        None => Err(Error::new(ErrorKind::Unexpected, "writer has been closed")),
                    };
                    // discard the result if send failed due to
                    // MonoiofsWriter::write cancelled
                    let _ = tx.send(result);
                }
                WriterRequest::Close { tx } => {
                    let result = match &file {
                        Some(f) => match f.sync_all().await {
                            Ok(()) => match f.metadata().await {
                                Ok(meta) => {
                                    let file = file.take().expect("file must be open");
                                    // file is closed in background and result is useless
                                    let _ = file.close().await;
                                    Ok(meta)
                                }
                                Err(e) => Err(new_std_io_error(e)),
                            },
                            Err(e) => Err(new_std_io_error(e)),
                        },
                        None => Err(Error::new(ErrorKind::Unexpected, "writer has been closed")),
                    };
                    // discard the result if send failed due to
                    // MonoiofsWriter::close cancelled
                    let _ = tx.send(result);
                }
            }
        }
    }
}

impl oio::Write for MonoiofsWriter {
    /// Send write request to worker thread and wait for result. Actual
    /// write happens in [`MonoiofsWriter::worker_entrypoint`] running
    /// on worker thread.
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been closed"));
        };
        while bs.has_remaining() {
            let buf = bs.current();
            let n = buf.len();
            let (resp_tx, resp_rx) = oneshot::channel();
            self.core.unwrap(tx.send(WriterRequest::Write {
                pos: self.pos,
                buf,
                tx: resp_tx,
            }));
            self.core.unwrap(resp_rx.await)?;
            self.pos += n as u64;
            bs.advance(n);
        }
        Ok(())
    }

    /// Send close request to worker thread and wait for result. Actual
    /// close happens in [`MonoiofsWriter::worker_entrypoint`] running
    /// on worker thread.
    async fn close(&mut self) -> Result<Metadata> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been closed"));
        };
        let (resp_tx, resp_rx) = oneshot::channel();
        self.core
            .unwrap(tx.send(WriterRequest::Close { tx: resp_tx }));
        let file_meta = self.core.unwrap(resp_rx.await)?;
        self.tx = None;

        let mode = if file_meta.is_dir() {
            EntryMode::DIR
        } else if file_meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let mut meta = match mode {
            EntryMode::FILE => MetadataBuilder::file(file_meta.len()),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };
        meta.last_modified(Timestamp::try_from(
            file_meta.modified().map_err(new_std_io_error)?,
        )?);
        Ok(meta.build())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "Monoiofs doesn't support abort",
        ))
    }
}

#[cfg(test)]
mod tests {
    use oio::Write;

    use super::*;
    use crate::Monoiofs;

    #[tokio::test]
    async fn test_writer_use_after_close_returns_unexpected_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = Monoiofs::default()
            .root(temp_dir.path().to_str().unwrap())
            .build()
            .unwrap();
        let ctx = OperationContext::default();

        // write + close + close must return Unexpected instead of panicking.
        let mut writer = backend
            .write(&ctx, "close_twice.txt", OpWrite::default())
            .unwrap();
        writer.write(Buffer::from("hello")).await.unwrap();
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 5);

        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        // close + write must return Unexpected instead of panicking.
        let mut writer = backend
            .write(&ctx, "write_after_close.txt", OpWrite::default())
            .unwrap();
        writer.close().await.unwrap();

        let err = writer.write(Buffer::from("late")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        // Worker thread must still be alive and serve subsequent operations.
        let mut writer = backend
            .write(&ctx, "after_errors.txt", OpWrite::default())
            .unwrap();
        writer.write(Buffer::from("ok")).await.unwrap();
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 2);
    }

    #[tokio::test]
    async fn test_writer_append_initializes_and_advances_pos() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root = temp_dir.path();
        let op = Operator::new(Monoiofs::default().root(root.to_str().unwrap())).unwrap();

        let core = Arc::new(MonoiofsCore::new(root.to_path_buf(), 1, 1024));

        // Appending to a new file starts at pos 0.
        let mut new_writer = MonoiofsWriter::new(core.clone(), root.join("new"), true)
            .await
            .unwrap();
        assert_eq!(new_writer.pos, 0);
        new_writer.write(Buffer::from("init")).await.unwrap();
        assert_eq!(new_writer.pos, 4);
        new_writer.close().await.unwrap();
        assert_eq!(op.read("new").await.unwrap().to_bytes(), b"init".as_slice());

        // Appending to an existing file initializes pos to its current length.
        op.write("f", "hello").await.unwrap();

        let mut writer = MonoiofsWriter::new(core, root.join("f"), true)
            .await
            .unwrap();
        assert_eq!(writer.pos, 5);

        writer
            .write(Buffer::from(vec![
                Bytes::from_static(b"wo"),
                Bytes::from_static(b"rld"),
            ]))
            .await
            .unwrap();
        assert_eq!(writer.pos, 10);
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 10);

        assert_eq!(
            op.read("f").await.unwrap().to_bytes(),
            b"helloworld".as_slice()
        );

        op.write_with("f", "!").append(true).await.unwrap();
        assert_eq!(
            op.read("f").await.unwrap().to_bytes(),
            b"helloworld!".as_slice()
        );
    }
}
