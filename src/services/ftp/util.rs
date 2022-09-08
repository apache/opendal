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

use crate::BytesReader;
use futures::AsyncRead;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use suppaftp::FtpStream;
use suppaftp::Status;
use tokio::sync::Mutex;
use tokio::task;

/// Wrapper for ftp data stream and command stream.
pub struct FtpReader {
    reader: BytesReader,
    client: Arc<Mutex<FtpStream>>,
}

impl FtpReader {
    /// Create an instance of FtpReader.
    pub fn new(r: BytesReader, c: FtpStream) -> Self {
        Self {
            reader: r,
            client: Arc::new(Mutex::new(c)),
        }
    }
}

impl AsyncRead for FtpReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let result = Pin::new(&mut self.reader).poll_read(cx, buf);

        // Drop data stream and close command stream when hit Error or EOF.
        match result {
            Poll::Ready(Err(_)) | Poll::Ready(Ok(0)) => {
                let c = self.client.clone();

                drop(self.reader.as_mut());

                task::spawn(async move {
                    let mut guard = c.lock().await;

                    guard
                        .read_response_in(&[
                            Status::ClosingDataConnection,
                            Status::RequestedFileActionOk,
                        ])
                        .await
                        .unwrap();

                    guard.quit().await.unwrap();
                    drop(guard);
                });
            }
            _ => (),
        };

        result
    }
}
